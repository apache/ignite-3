/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.TopologyService;

/**
 * Raft Group Events listener that registers Logical Topology listeners for removing learners that have left the topology.
 */
public class MetaStorageRaftGroupEventsListener implements RaftGroupEventsListener {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageManagerImpl.class);

    private final IgniteSpinBusyLock busyLock;

    private final TopologyService physicalTopologyService;

    private final LogicalTopologyService logicalTopologyService;

    private final CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut;

    /**
     * Future required to enable serialized processing of topology events.
     *
     * <p>While Logical Topology events are linearized, we also concurrently react to Physical Topology changes and/or update Raft
     * configuration after a new leader has been elected.
     *
     * <p>Multi-threaded access is guarded by {@code serializationFutureMux}.
     */
    private CompletableFuture<Void> serializationFuture = null;

    private final Object serializationFutureMux = new Object();

    MetaStorageRaftGroupEventsListener(
            IgniteSpinBusyLock busyLock,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut
    ) {
        this.busyLock = busyLock;
        this.physicalTopologyService = clusterService.topologyService();
        this.logicalTopologyService = logicalTopologyService;
        this.metaStorageSvcFut = metaStorageSvcFut;
    }

    @Override
    public void onLeaderElected(long term) {
        synchronized (serializationFutureMux) {
            registerTopologyEventListeners();

            // Update learner configuration in case we missed some topology updates and initialize the serialization future.
            serializationFuture = executeIfLeaderImpl(service -> resetLearners(service.raftGroupService()));
        }
    }

    private void registerTopologyEventListeners() {
        logicalTopologyService.addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onNodeValidated(ClusterNode validatedNode) {
                executeIfLeader(service -> addLearner(service.raftGroupService(), validatedNode));
            }

            @Override
            public void onNodeInvalidated(ClusterNode invalidatedNode) {
                executeIfLeader(service -> {
                    CompletableFuture<Void> closeCursorsFuture = service.closeCursors(invalidatedNode.id())
                            .whenComplete((v, e) -> {
                                if (e != null) {
                                    LOG.error("Unable to close cursor for " + invalidatedNode, e);
                                }
                            });

                    CompletableFuture<Void> removeLearnersFuture = removeLearner(service.raftGroupService(), invalidatedNode);

                    return allOf(closeCursorsFuture, removeLearnersFuture);
                });
            }

            @Override
            public void onNodeLeft(ClusterNode leftNode, LogicalTopologySnapshot newTopology) {
                onNodeInvalidated(leftNode);
            }

            @Override
            public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
                executeIfLeader(service -> resetLearners(service.raftGroupService()));
            }
        });
    }

    /**
     * Executes the given action if the current node is the Meta Storage leader.
     */
    private void executeIfLeader(Function<MetaStorageServiceImpl, CompletableFuture<Void>> action) {
        if (!busyLock.enterBusy()) {
            LOG.info("Skipping Meta Storage configuration update because the node is stopping");

            return;
        }

        try {
            synchronized (serializationFutureMux) {
                // We are definitely not a leader if the serialization future has not been initialized.
                if (serializationFuture == null) {
                    return;
                }

                serializationFuture = serializationFuture
                        // we don't care about exceptions here, they should be logged independently
                        .handle((v, e) -> executeIfLeaderImpl(action))
                        .thenCompose(Function.identity());
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> executeIfLeaderImpl(Function<MetaStorageServiceImpl, CompletableFuture<Void>> action) {
        return metaStorageSvcFut.thenCompose(service -> isCurrentNodeLeader(service.raftGroupService())
                .thenCompose(isLeader -> isLeader ? action.apply(service) : completedFuture(null)));
    }

    private CompletableFuture<Void> addLearner(RaftGroupService raftService, ClusterNode learner) {
        return updateLearner(raftService, learner, raftService::addLearners);
    }

    private CompletableFuture<Void> removeLearner(RaftGroupService raftService, ClusterNode learner) {
        return updateLearner(raftService, learner, raftService::removeLearners);
    }

    private CompletableFuture<Void> updateLearner(
            RaftGroupService raftService,
            ClusterNode learner,
            Function<List<Peer>, CompletableFuture<Void>> updateAction
    ) {
        return updateConfigUnderLock(() -> {
            boolean isPeer = raftService.peers().stream().anyMatch(peer -> peer.consistentId().equals(learner.name()));

            if (isPeer) {
                return completedFuture(null);
            }

            List<Peer> diffPeers = List.of(new Peer(learner.name()));

            return updateAction.apply(diffPeers)
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOG.error("Unable to change peers on topology update", e);
                        }
                    });
        });
    }

    private CompletableFuture<Void> resetLearners(RaftGroupService raftService) {
        return updateConfigUnderLock(() -> getLearners(raftService)
                .thenCombine(logicalTopologyService.validatedNodesOnLeader(), (learners, validatedNodes) -> {
                    Set<String> aliveNodeNames = validatedNodes.stream().map(ClusterNode::name).collect(toSet());

                    return learners.stream()
                            .filter(learner -> !aliveNodeNames.contains(learner.consistentId()))
                            .collect(toList());
                })
                .thenCompose(learnersToRemove -> learnersToRemove.isEmpty()
                        ? completedFuture(null)
                        : raftService.resetLearners(learnersToRemove)
                )
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Unable to change peers on topology update", e);
                    }
                })
        );
    }

    private static CompletableFuture<List<Peer>> getLearners(RaftGroupService raftService) {
        return raftService.refreshMembers(false).thenApply(v -> raftService.learners());
    }

    private CompletableFuture<Boolean> isCurrentNodeLeader(RaftGroupService raftService) {
        String name = physicalTopologyService.localMember().name();

        return raftService.refreshLeader()
                .thenApply(v -> raftService.leader().consistentId().equals(name));
    }

    private CompletableFuture<Void> updateConfigUnderLock(Supplier<CompletableFuture<Void>> action) {
        if (!busyLock.enterBusy()) {
            LOG.info("Skipping Meta Storage configuration update because the node is stopping");

            return completedFuture(null);
        }

        try {
            return action.get();
        } finally {
            busyLock.leaveBusy();
        }
    }
}
