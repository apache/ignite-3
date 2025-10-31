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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Meta Storage leader election listener.
 */
public class MetaStorageLeaderElectionListener implements LeaderElectionListener {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageLeaderElectionListener.class);

    private final IgniteSpinBusyLock busyLock;

    private final String nodeName;

    private final LogicalTopologyService logicalTopologyService;

    private final FailureProcessor failureProcessor;

    private final CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut;

    private final MetaStorageLearnerManager learnerManager;

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

    private final CompletableFuture<SystemDistributedConfiguration> systemConfigurationFuture;

    private final List<ElectionListener> electionListeners;

    /**
     * Becomes {@code true} when the node, even being formally a leader, should not perform secondary leader duties (these are managing
     * learners and propagating idle safe time through Metastorage).
     *
     * <p>The flag is raised when we appoint a node as a leader forcefully via resetPeers(). The flag gets cleared when first non-forced
     * configuration update comes after forcing leadership.
     */
    private final BooleanSupplier leaderSecondaryDutiesPaused;

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
            FailureProcessor failureProcessor,
            CompletableFuture<MetaStorageServiceImpl> metaStorageSvcFut,
            MetaStorageLearnerManager learnerManager,
            ClusterTimeImpl clusterTime,
            CompletableFuture<SystemDistributedConfiguration> systemConfigurationFuture,
            List<ElectionListener> electionListeners,
            BooleanSupplier leaderSecondaryDutiesPaused
    ) {
        this.busyLock = busyLock;
        this.nodeName = clusterService.nodeName();
        this.logicalTopologyService = logicalTopologyService;
        this.failureProcessor = failureProcessor;
        this.metaStorageSvcFut = metaStorageSvcFut;
        this.learnerManager = learnerManager;
        this.clusterTime = clusterTime;
        this.systemConfigurationFuture = systemConfigurationFuture;
        this.electionListeners = electionListeners;
        this.leaderSecondaryDutiesPaused = leaderSecondaryDutiesPaused;
    }

    @Override
    public void onLeaderElected(InternalClusterNode node, long term) {
        LOG.info("New leader is elected for Metastorage, leader {}, term {}", node, term);

        electionListeners.forEach(listener -> listener.onLeaderElected(node));

        boolean weAreNewLeader = node.name().equals(nodeName);

        synchronized (serializationFutureMux) {
            boolean weWerePreviousLeader = serializationFuture != null;

            if (weWerePreviousLeader && !weAreNewLeader) {
                LOG.info("Node has lost the leadership, stopping doing secondary duties");

                thisNodeTerm = null;

                clusterTime.stopSafeTimeScheduler(term);

                logicalTopologyService.removeEventListener(logicalTopologyEventListener);

                serializationFuture.cancel(false);

                serializationFuture = null;
            }

            if (weAreNewLeader) {
                thisNodeTerm = term;

                if (!weWerePreviousLeader) {
                    LOG.info("Node has been elected as the leader (and it wasn't previous leader), so starting doing secondary duties");

                    startSafeTimeScheduler(term);

                    // The node was not previous leader, and it becomes a leader.
                    logicalTopologyService.addEventListener(logicalTopologyEventListener);

                    // Update learner configuration (in case we missed some topology updates between elections).
                    serializationFuture = (serializationFuture == null ? nullCompletedFuture() : serializationFuture)
                            .thenCompose(unused -> updateLearnersIfSecondaryDutiesAreNotPaused(term));
                } else {
                    LOG.info("Node has been reelected as the leader");
                }
            }
        }
    }

    private void startSafeTimeScheduler(long term) {
        metaStorageSvcFut
                .thenAcceptBoth(systemConfigurationFuture, (service, metaStorageConfiguration) -> {
                    clusterTime.startSafeTimeScheduler(
                            safeTime -> syncTimeIfSecondaryDutiesAreNotPaused(safeTime, service),
                            metaStorageConfiguration,
                            term
                    );
                })
                .whenComplete((v, e) -> {
                    if (e != null) {
                        if (!hasCause(e, NodeStoppingException.class)) {
                            failureProcessor.process(new FailureContext(e, "Unable to start Idle Safe Time scheduler"));
                        }
                    }
                });
    }

    private CompletableFuture<Void> updateLearnersIfSecondaryDutiesAreNotPaused(long term) {
        if (leaderSecondaryDutiesPaused.getAsBoolean()) {
            LOG.info("Skipping learners update as secondary duties are still paused");

            return nullCompletedFuture();
        }

        LOG.info("Actually updating learners with term {}", term);

        return learnerManager.updateLearners(term);
    }

    private CompletableFuture<Void> syncTimeIfSecondaryDutiesAreNotPaused(HybridTimestamp safeTime, MetaStorageServiceImpl service) {
        if (leaderSecondaryDutiesPaused.getAsBoolean()) {
            return nullCompletedFuture();
        }

        Long term = thisNodeTerm;
        if (term == null) {
            // We seized to be a leader, do nothing.
            return nullCompletedFuture();
        }

        return service.syncTime(safeTime, term);
    }

    private class MetaStorageLogicalTopologyEventListener implements LogicalTopologyEventListener {
        @Override
        public void onNodeValidated(LogicalNode validatedNode) {
            execute((raftService, term) -> learnerManager.addLearner(raftService, validatedNode));
        }

        @Override
        public void onNodeInvalidated(LogicalNode invalidatedNode) {
            execute((raftService, term) -> learnerManager.removeLearner(raftService, invalidatedNode));
        }

        @Override
        public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
            onNodeInvalidated(leftNode);
        }

        @Override
        public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
            // TODO: https://issues.apache.org/jira/browse/IGNITE-26854
            execute((raftService, term) -> learnerManager.resetLearners(raftService, term, 0));
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
            if (leaderSecondaryDutiesPaused.getAsBoolean()) {
                LOG.info("Skipping Meta Storage configuration update because the leader's secondary duties are paused");

                return;
            }

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
}
