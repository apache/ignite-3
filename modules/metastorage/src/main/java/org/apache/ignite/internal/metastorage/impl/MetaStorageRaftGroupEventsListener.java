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
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/**
 * Raft Group Events listener that registers Logical Topology listener for updating the list of Meta Storage Raft group listeners.
 */
public class MetaStorageRaftGroupEventsListener implements RaftGroupEventsListener {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageManagerImpl.class);

    private final IgniteSpinBusyLock busyLock;

    private final String nodeName;

    private final LogicalTopologyService logicalTopologyService;

    private final CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut;

    /**
     * Future required to enable serialized processing of topology events.
     *
     * <p>While Logical Topology events are linearized, we usually start a bunch of async operations inside the event listener and need
     * them to finish in the same order.
     *
     * <p>Multi-threaded access is guarded by {@code serializationFutureMux}.
     */
    private CompletableFuture<Void> serializationFuture = null;

    private final Object serializationFutureMux = new Object();

    private final ClusterTimeImpl clusterTime;

    MetaStorageRaftGroupEventsListener(
            IgniteSpinBusyLock busyLock,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut,
            ClusterTimeImpl clusterTime
    ) {
        this.busyLock = busyLock;
        this.nodeName = clusterService.nodeName();
        this.logicalTopologyService = logicalTopologyService;
        this.metaStorageSvcFut = metaStorageSvcFut;
        this.clusterTime = clusterTime;
    }

    @Override
    public void onLeaderElected(long term) {
        synchronized (serializationFutureMux) {
            registerTopologyEventListeners();

            // Update learner configuration (in case we missed some topology updates) and initialize the serialization future.
            serializationFuture = executeIfLeaderImpl(this::resetLearners);

            serializationFuture = executeWithStatus((service, term1, isLeader) -> {
                if (isLeader) {
                    this.resetLearners(service, term1);

                    clusterTime.startLeaderTimer(service);
                } else {
                    clusterTime.stopLeaderTimer();
                }

                return null;
            });
        }
    }

    private void registerTopologyEventListeners() {
        logicalTopologyService.addEventListener(new LogicalTopologyEventListener() {
            @Override
            public void onNodeValidated(LogicalNode validatedNode) {
                executeIfLeader((service, term) -> addLearner(service.raftGroupService(), validatedNode));
            }

            @Override
            public void onNodeInvalidated(LogicalNode invalidatedNode) {
                executeIfLeader((service, term) -> {
                    CompletableFuture<Void> closeCursorsFuture = service.closeCursors(invalidatedNode.id())
                            .exceptionally(e -> {
                                LOG.error("Unable to close cursor for " + invalidatedNode, e);

                                return null;
                            });

                    CompletableFuture<Void> removeLearnersFuture = removeLearner(service.raftGroupService(), invalidatedNode);

                    return allOf(closeCursorsFuture, removeLearnersFuture);
                });
            }

            @Override
            public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
                onNodeInvalidated(leftNode);
            }

            @Override
            public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
                executeIfLeader(MetaStorageRaftGroupEventsListener.this::resetLearners);
            }
        });
    }

    @FunctionalInterface
    private interface OnLeaderAction {
        CompletableFuture<Void> apply(MetaStorageServiceImpl service, long term);
    }

    private interface OnStatusAction {
        CompletableFuture<Void> apply(MetaStorageServiceImpl service, long term, boolean isLeader);
    }

    /**
     * Executes the given action if the current node is the Meta Storage leader.
     */
    private void executeIfLeader(OnLeaderAction action) {
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

    private CompletableFuture<Void> executeIfLeaderImpl(OnLeaderAction action) {
        return executeWithStatus((service, term, isLeader) -> action.apply(service, term));
    }

    private CompletableFuture<Void> executeWithStatus(OnStatusAction action) {
        return metaStorageSvcFut.thenCompose(service -> service.raftGroupService().refreshAndGetLeaderWithTerm()
                .thenCompose(leaderWithTerm -> {
                    String leaderName = leaderWithTerm.leader().consistentId();

                    boolean isLeader = leaderName.equals(nodeName);
                    return action.apply(service, leaderWithTerm.term(), isLeader);
                }));
    }

    private CompletableFuture<Void> addLearner(RaftGroupService raftService, ClusterNode learner) {
        return updateConfigUnderLock(() -> isPeer(raftService, learner)
                ? completedFuture(null)
                : raftService.addLearners(List.of(new Peer(learner.name()))));
    }

    private static boolean isPeer(RaftGroupService raftService, ClusterNode node) {
        return raftService.peers().stream().anyMatch(peer -> peer.consistentId().equals(node.name()));
    }

    private CompletableFuture<Void> removeLearner(RaftGroupService raftService, ClusterNode learner) {
        return updateConfigUnderLock(() -> logicalTopologyService.validatedNodesOnLeader()
                .thenCompose(validatedNodes -> updateConfigUnderLock(() -> {
                    if (isPeer(raftService, learner)) {
                        return completedFuture(null);
                    }

                    // Due to possible races, we can have multiple versions of the same node in the validated set. We only remove
                    // a learner if there are no such versions left.
                    if (validatedNodes.stream().anyMatch(n -> n.name().equals(learner.name()))) {
                        return completedFuture(null);
                    }

                    return raftService.removeLearners(List.of(new Peer(learner.name())));
                })));
    }

    private CompletableFuture<Void> resetLearners(MetaStorageServiceImpl service, long term) {
        return updateConfigUnderLock(() -> logicalTopologyService.validatedNodesOnLeader()
                .thenCompose(validatedNodes -> updateConfigUnderLock(() -> {
                    RaftGroupService raftService = service.raftGroupService();

                    Set<String> peers = raftService.peers().stream().map(Peer::consistentId).collect(toSet());

                    Set<String> learners = validatedNodes.stream()
                            .map(ClusterNode::name)
                            .filter(name -> !peers.contains(name))
                            .collect(toSet());

                    PeersAndLearners newPeerConfiguration = PeersAndLearners.fromConsistentIds(peers, learners);

                    // We can't use 'resetLearners' call here because it does not support empty lists of learners.
                    return raftService.changePeersAsync(newPeerConfiguration, term);
                })));
    }

    private CompletableFuture<Void> updateConfigUnderLock(Supplier<CompletableFuture<Void>> action) {
        if (!busyLock.enterBusy()) {
            LOG.info("Skipping Meta Storage configuration update because the node is stopping");

            return completedFuture(null);
        }

        try {
            return action.get()
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOG.error("Unable to change peers on topology update", e);
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }
}
