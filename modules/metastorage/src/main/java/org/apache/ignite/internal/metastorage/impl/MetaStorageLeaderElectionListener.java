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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.configuration.MetaStorageConfiguration;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Meta Storage leader election listener.
 */
public class MetaStorageLeaderElectionListener implements LeaderElectionListener {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageLeaderElectionListener.class);

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
    @Nullable
    private CompletableFuture<Void> serializationFuture = null;

    private final Object serializationFutureMux = new Object();

    private final ClusterTimeImpl clusterTime;

    private final LogicalTopologyEventListener logicalTopologyEventListener = new MetaStorageLogicalTopologyEventListener();

    private final CompletableFuture<MetaStorageConfiguration> metaStorageConfigurationFuture;

    private final List<ElectionListener> electionListeners;

    /**
     * Leader term if this node is a leader, {@code null} otherwise.
     *
     * <p>Multi-threaded access is guarded by {@code serializationFutureMux}.
     */
    @Nullable
    private Long thisNodeTerm = null;

    MetaStorageLeaderElectionListener(
            IgniteSpinBusyLock busyLock,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut,
            ClusterTimeImpl clusterTime,
            CompletableFuture<MetaStorageConfiguration> metaStorageConfigurationFuture,
            List<ElectionListener> electionListeners
    ) {
        this.busyLock = busyLock;
        this.nodeName = clusterService.nodeName();
        this.logicalTopologyService = logicalTopologyService;
        this.metaStorageSvcFut = metaStorageSvcFut;
        this.clusterTime = clusterTime;
        this.metaStorageConfigurationFuture = metaStorageConfigurationFuture;
        this.electionListeners = electionListeners;
    }

    @Override
    public void onLeaderElected(ClusterNode node, long term) {
        electionListeners.forEach(listener -> listener.onLeaderElected(node));

        synchronized (serializationFutureMux) {
            if (node.name().equals(nodeName) && serializationFuture == null) {
                LOG.info("Node has been elected as the leader, starting Idle Safe Time scheduler");

                thisNodeTerm = term;

                logicalTopologyService.addEventListener(logicalTopologyEventListener);

                metaStorageSvcFut
                        .thenAcceptBoth(metaStorageConfigurationFuture, (service, metaStorageConfiguration) -> {
                            clusterTime.startSafeTimeScheduler(
                                    safeTime -> service.syncTime(safeTime, term),
                                    metaStorageConfiguration
                            );
                        })
                        .whenComplete((v, e) -> {
                            if (e != null) {
                                LOG.error("Unable to start Idle Safe Time scheduler", e);
                            }
                        });

                // Update learner configuration (in case we missed some topology updates between elections).
                serializationFuture = metaStorageSvcFut.thenCompose(service -> resetLearners(service.raftGroupService(), term));
            } else if (serializationFuture != null) {
                LOG.info("Node has lost the leadership, stopping Idle Safe Time scheduler");

                thisNodeTerm = null;

                logicalTopologyService.removeEventListener(logicalTopologyEventListener);

                clusterTime.stopSafeTimeScheduler();

                serializationFuture.cancel(false);

                serializationFuture = null;
            }
        }
    }

    private class MetaStorageLogicalTopologyEventListener implements LogicalTopologyEventListener {
        @Override
        public void onNodeValidated(LogicalNode validatedNode) {
            execute((raftService, term) -> addLearner(raftService, validatedNode));
        }

        @Override
        public void onNodeInvalidated(LogicalNode invalidatedNode) {
            execute((raftService, term) -> removeLearner(raftService, invalidatedNode));
        }

        @Override
        public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
            onNodeInvalidated(leftNode);
        }

        @Override
        public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
            execute(MetaStorageLeaderElectionListener.this::resetLearners);
        }
    }

    @FunctionalInterface
    private interface Action {
        CompletableFuture<Void> apply(RaftGroupService raftService, long term);
    }

    /**
     * Executes the given action if the current node is the Meta Storage leader.
     */
    private void execute(Action action) {
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

                // Term and serialization future are initialized together.
                assert thisNodeTerm != null;

                long term = thisNodeTerm;

                serializationFuture = serializationFuture
                        // we don't care about exceptions here, they should be logged independently
                        .handle((v, e) -> metaStorageSvcFut.thenCompose(service -> action.apply(service.raftGroupService(), term)))
                        .thenCompose(Function.identity());
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> addLearner(RaftGroupService raftService, ClusterNode learner) {
        return updateConfigUnderLock(() -> isPeer(raftService, learner)
                ? nullCompletedFuture()
                : raftService.addLearners(List.of(new Peer(learner.name()))));
    }

    private static boolean isPeer(RaftGroupService raftService, ClusterNode node) {
        return raftService.peers().stream().anyMatch(peer -> peer.consistentId().equals(node.name()));
    }

    private CompletableFuture<Void> removeLearner(RaftGroupService raftService, ClusterNode learner) {
        return updateConfigUnderLock(() -> logicalTopologyService.validatedNodesOnLeader()
                .thenCompose(validatedNodes -> updateConfigUnderLock(() -> {
                    if (isPeer(raftService, learner)) {
                        return nullCompletedFuture();
                    }

                    // Due to possible races, we can have multiple versions of the same node in the validated set. We only remove
                    // a learner if there are no such versions left.
                    if (validatedNodes.stream().anyMatch(n -> n.name().equals(learner.name()))) {
                        return nullCompletedFuture();
                    }

                    return raftService.removeLearners(List.of(new Peer(learner.name())));
                })));
    }

    private CompletableFuture<Void> resetLearners(RaftGroupService raftService, long term) {
        return updateConfigUnderLock(() -> logicalTopologyService.validatedNodesOnLeader()
                .thenCompose(validatedNodes -> updateConfigUnderLock(() -> {
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

            return nullCompletedFuture();
        }

        try {
            return action.get()
                    .whenComplete((v, e) -> {
                        if (e != null && !(unwrapCause(e) instanceof CancellationException)) {
                            LOG.error("Unable to change peers on topology update", e);
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }
}
