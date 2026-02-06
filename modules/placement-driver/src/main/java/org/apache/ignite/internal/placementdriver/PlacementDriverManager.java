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

package org.apache.ignite.internal.placementdriver;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.placementdriver.metrics.PlacementDriverMetricSource;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.StoppingExceptionFactories;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.TestOnly;

/**
 * Placement driver manager.
 * The manager is a leaseholder tracker: response of renewal of the lease and discover of appear/disappear of a replication group member.
 * The another role of the manager is providing a node, which is leaseholder at the moment, for a particular replication group.
 */
public class PlacementDriverManager implements IgniteComponent {
    private static final String PLACEMENTDRIVER_LEASES_KEY_STRING = "placementdriver.leases";

    public static final ByteArray PLACEMENTDRIVER_LEASES_KEY = ByteArray.fromString(PLACEMENTDRIVER_LEASES_KEY_STRING);

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Cluster service. */
    private final ClusterService clusterService;

    /** Replication group id to store placement driver data. */
    private final ReplicationGroupId replicationGroupId;

    /** The closure determines nodes where are participants of placement driver. */
    private final Supplier<CompletableFuture<Set<String>>> placementDriverNodesNamesProvider;

    private final RaftManager raftManager;

    private final TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory;

    /** Raft client future. Can contain null, if this node is not in placement driver group. */
    private final CompletableFuture<TopologyAwareRaftGroupService> raftClientFuture;

    /** Lease tracker. */
    private final LeaseTracker leaseTracker;

    /** Lease updater. */
    private final LeaseUpdater leaseUpdater;

    /** Meta Storage manager. */
    private final MetaStorageManager metastore;

    private final FailureProcessor failureProcessor;

    private final AssignmentsTracker assignmentsTracker;

    private final PlacementDriver placementDriver;

    private final MetricManager metricManager;

    private final PlacementDriverMetricSource metricSource;

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param metastore Meta Storage manager.
     * @param replicationGroupId Id of placement driver group.
     * @param clusterService Cluster service.
     * @param placementDriverNodesNamesProvider Provider of the set of placement driver nodes' names.
     * @param logicalTopologyService Logical topology service.
     * @param raftManager Raft manager.
     * @param topologyAwareRaftGroupServiceFactory Raft client factory.
     * @param clockService Clock service.
     * @param failureProcessor Failure processor.
     * @param throttledLogExecutor Executor to clean up the throttled logger cache.
     * @param metricManager Metric manager.
     * @param currentDataNodesProvider Provider of the current data nodes in the cluster.
     */
    public PlacementDriverManager(
            String nodeName,
            MetaStorageManager metastore,
            ReplicationGroupId replicationGroupId,
            ClusterService clusterService,
            Supplier<CompletableFuture<Set<String>>> placementDriverNodesNamesProvider,
            LogicalTopologyService logicalTopologyService,
            RaftManager raftManager,
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            ClockService clockService,
            FailureProcessor failureProcessor,
            ReplicationConfiguration replicationConfiguration,
            Executor throttledLogExecutor,
            MetricManager metricManager,
            Function<Integer, CompletableFuture<Set<String>>> currentDataNodesProvider
    ) {
        this.replicationGroupId = replicationGroupId;
        this.clusterService = clusterService;
        this.placementDriverNodesNamesProvider = placementDriverNodesNamesProvider;
        this.raftManager = raftManager;
        this.topologyAwareRaftGroupServiceFactory = topologyAwareRaftGroupServiceFactory;
        this.metastore = metastore;
        this.failureProcessor = failureProcessor;

        this.raftClientFuture = new CompletableFuture<>();

        this.leaseTracker = new LeaseTracker(
                metastore,
                clusterService.topologyService(),
                clockService,
                currentDataNodesProvider
        );

        this.assignmentsTracker = new AssignmentsTracker(
                metastore,
                failureProcessor,
                currentDataNodesProvider
        );

        this.leaseUpdater = new LeaseUpdater(
                nodeName,
                clusterService,
                metastore,
                failureProcessor,
                logicalTopologyService,
                leaseTracker,
                clockService,
                assignmentsTracker,
                replicationConfiguration,
                throttledLogExecutor
        );

        this.placementDriver = createPlacementDriver();
        this.metricManager = metricManager;
        this.metricSource = new PlacementDriverMetricSource(leaseTracker, assignmentsTracker);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        inBusyLock(busyLock, () -> {
            placementDriverNodesNamesProvider.get()
                    .thenCompose(placementDriverNodes -> {
                        String thisNodeName = clusterService.topologyService().localMember().name();

                        if (!placementDriverNodes.contains(thisNodeName)) {
                            return nullCompletedFuture();
                        }

                        try {
                            leaseUpdater.init();

                            TopologyAwareRaftGroupService raftClient = raftManager.startRaftGroupService(
                                    replicationGroupId,
                                    PeersAndLearners.fromConsistentIds(placementDriverNodes),
                                    topologyAwareRaftGroupServiceFactory,
                                    null, // Use default commands marshaller.
                                    StoppingExceptionFactories.indicateNodeStop(),
                                    true
                            );

                            return raftClient.subscribeLeader(this::onLeaderChange).thenApply(v -> raftClient);
                        } catch (NodeStoppingException e) {
                            return failedFuture(e);
                        }
                    })
                    .whenComplete((client, ex) -> {
                        if (ex == null) {
                            raftClientFuture.complete(client);
                        } else {
                            if (!hasCause(ex, NodeStoppingException.class)) {
                                failureProcessor.process(new FailureContext(ex, "Placement driver initialization exception"));
                            }

                            raftClientFuture.completeExceptionally(ex);
                        }
                    });

            recoverInternalComponentsBusy();

            metricManager.registerSource(metricSource);
            metricManager.enable(PlacementDriverMetricSource.SOURCE_NAME);
        });

        return nullCompletedFuture();
    }

    @Override
    public void beforeNodeStop() {
        inBusyLock(busyLock, () -> {
            withRaftClientIfPresent(c -> {
                c.unsubscribeLeader().join();

                leaseUpdater.deInit();
            });

            assignmentsTracker.stopTrack();

            leaseTracker.stopTrack();
        });
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        withRaftClientIfPresent(TopologyAwareRaftGroupService::shutdown);

        leaseUpdater.deactivate();

        metricManager.disable(PlacementDriverMetricSource.SOURCE_NAME);

        return nullCompletedFuture();
    }

    private void withRaftClientIfPresent(Consumer<TopologyAwareRaftGroupService> closure) {
        raftClientFuture.thenAccept(client -> {
            if (client != null) {
                closure.accept(client);
            }
        });
    }

    private void onLeaderChange(InternalClusterNode leader, long term) {
        inBusyLock(busyLock, () -> {
            if (leader.equals(clusterService.topologyService().localMember())) {
                takeOverActiveActorBusy();
            } else {
                stepDownActiveActorBusy();
            }
        });
    }

    /** Takes over active actor of placement driver group. */
    private void takeOverActiveActorBusy() {
        leaseUpdater.activate();
    }

    /** Steps down as active actor. */
    private void stepDownActiveActorBusy() {
        leaseUpdater.deactivate();
    }

    @TestOnly
    boolean isActiveActor() {
        return leaseUpdater.active();
    }

    /** Returns placement driver service. */
    public PlacementDriver placementDriver() {
        return placementDriver;
    }

    private void recoverInternalComponentsBusy() {
        CompletableFuture<Revisions> recoveryFinishedFuture = metastore.recoveryFinishedFuture();

        assert recoveryFinishedFuture.isDone();

        long recoveryRevision = recoveryFinishedFuture.join().revision();

        assignmentsTracker.startTrack();

        leaseTracker.startTrack(recoveryRevision);
    }

    private PlacementDriver createPlacementDriver() {
        return new PlacementDriver() {
            @Override
            public boolean isActualAt(HybridTimestamp timestamp) {
                return metastore.clusterTime().currentSafeTime().compareTo(timestamp) >= 0;
            }

            @Override
            public CompletableFuture<List<TokenizedAssignments>> getAssignments(
                    List<? extends ReplicationGroupId> replicationGroupIds,
                    HybridTimestamp timestamp
            ) {
                return assignmentsTracker.getAssignments(replicationGroupIds, timestamp);
            }

            @Override
            public CompletableFuture<List<TokenizedAssignments>> awaitNonEmptyAssignments(
                    List<? extends ReplicationGroupId> replicationGroupIds, long timeoutMillis) {
                return assignmentsTracker.awaitNonEmptyAssignments(replicationGroupIds, timeoutMillis);
            }

            @Override
            public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(
                    ReplicationGroupId groupId,
                    HybridTimestamp timestamp,
                    long timeout,
                    TimeUnit unit) {
                return leaseTracker.awaitPrimaryReplica(
                        groupId,
                        timestamp,
                        timeout,
                        unit
                );
            }

            @Override
            public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
                return leaseTracker.getPrimaryReplica(replicationGroupId, timestamp);
            }

            @Override
            public ReplicaMeta getCurrentPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
                return leaseTracker.getCurrentPrimaryReplica(replicationGroupId, timestamp);
            }

            @Override
            public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId replicationGroupId) {
                return leaseTracker.previousPrimaryExpired(replicationGroupId);
            }

            @Override
            public void listen(PrimaryReplicaEvent evt, EventListener<? extends PrimaryReplicaEventParameters> listener) {
                leaseTracker.listen(evt, listener);
            }

            @Override
            public void removeListener(PrimaryReplicaEvent evt, EventListener<? extends PrimaryReplicaEventParameters> listener) {
                leaseTracker.removeListener(evt, listener);
            }
        };
    }
}
