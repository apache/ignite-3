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

package org.apache.ignite.internal.partition.replicator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_CREATE;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceRaftGroupEventsListener.handleReduceChanged;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.ASSIGNMENTS_SWITCH_REDUCE_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.PENDING_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.extractPartitionNumber;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zoneAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zonePartitionAssignmentsGetLocally;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.rebalance.PartitionMover;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceRaftGroupEventsListener;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.snapshot.FailFastSnapshotStorageFactory;
import org.apache.ignite.internal.raft.ExecutorInclinedRaftCommandRunner;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * The main responsibilities of this class:
 * - Start the appropriate replication nodes on the zone creation.
 * - Stop the same nodes on the zone removing.
 * - Support the rebalance mechanism and start the new replication nodes when the rebalance triggers occurred.
 */
public class PartitionReplicaLifecycleManager  extends
        AbstractEventProducer<PartitionReplicaLifecycleEvent, PartitionReplicaLifecycleEventParameters> implements IgniteComponent {
    public static final String FEATURE_FLAG_NAME = "IGNITE_ZONE_BASED_REPLICATION";
    /* Feature flag for zone based collocation track */
    // TODO IGNITE-22115 remove it
    public static final boolean ENABLED = getBoolean(FEATURE_FLAG_NAME, false);

    private final CatalogManager catalogMgr;

    private final ReplicaManager replicaMgr;

    private final DistributionZoneManager distributionZoneMgr;

    private final MetaStorageManager metaStorageMgr;

    private final TopologyService topologyService;

    private final LowWatermark lowWatermark;

    /** Meta storage listener for pending assignments. */
    private final WatchListener pendingAssignmentsRebalanceListener;

    /** Meta storage listener for stable assignments. */
    private final WatchListener stableAssignmentsRebalanceListener;

    /** Meta storage listener for switch reduce assignments. */
    private final WatchListener assignmentsSwitchRebalanceListener;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PartitionReplicaLifecycleManager.class);

    private final Set<ReplicationGroupId> replicationGroupIds = ConcurrentHashMap.newKeySet();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * Separate executor for IO operations like partition storage initialization or partition raft group meta data persisting.
     */
    private final ExecutorService ioExecutor;

    /** Executor for scheduling rebalance routine. */
    private final ScheduledExecutorService rebalanceScheduler;

    /**
     * Executes partition operations (that might cause I/O and/or be blocked on locks).
     */
    private final Executor partitionOperationsExecutor;

    /**
     * The constructor.
     *
     * @param catalogMgr Catalog manager.
     * @param replicaMgr Replica manager.
     * @param distributionZoneMgr Distribution zone manager.
     * @param metaStorageMgr Metastorage manager.
     * @param topologyService Topology service.
     * @param rebalanceScheduler Executor for scheduling rebalance routine.
     * @param partitionOperationsExecutor Striped executor on which partition operations (potentially requiring I/O with storages)
     *     will be executed.
     */
    public PartitionReplicaLifecycleManager(
            CatalogManager catalogMgr,
            ReplicaManager replicaMgr,
            DistributionZoneManager distributionZoneMgr,
            MetaStorageManager metaStorageMgr,
            TopologyService topologyService,
            LowWatermark lowWatermark,
            ExecutorService ioExecutor,
            ScheduledExecutorService rebalanceScheduler,
            Executor partitionOperationsExecutor
    ) {
        this.catalogMgr = catalogMgr;
        this.replicaMgr = replicaMgr;
        this.distributionZoneMgr = distributionZoneMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.topologyService = topologyService;
        this.lowWatermark = lowWatermark;
        this.ioExecutor = ioExecutor;
        this.rebalanceScheduler = rebalanceScheduler;
        this.partitionOperationsExecutor = partitionOperationsExecutor;

        pendingAssignmentsRebalanceListener = createPendingAssignmentsRebalanceListener();
        stableAssignmentsRebalanceListener = createStableAssignmentsRebalanceListener();
        assignmentsSwitchRebalanceListener = createAssignmentsSwitchRebalanceListener();
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        if (!ENABLED) {
            return nullCompletedFuture();
        }

        CompletableFuture<Long> recoveryFinishFuture = metaStorageMgr.recoveryFinishedFuture();

        assert recoveryFinishFuture.isDone();

        long recoveryRevision = recoveryFinishFuture.join();

        cleanUpResourcesForDroppedZonesOnRecovery();

        CompletableFuture<Void> processZonesAndAssignmentsOnStart = processZonesOnStart(recoveryRevision, lowWatermark.getLowWatermark())
                .thenCompose(ignored -> processAssignmentsOnRecovery(recoveryRevision));

        metaStorageMgr.registerPrefixWatch(ByteArray.fromString(PENDING_ASSIGNMENTS_PREFIX), pendingAssignmentsRebalanceListener);

        metaStorageMgr.registerPrefixWatch(ByteArray.fromString(STABLE_ASSIGNMENTS_PREFIX), stableAssignmentsRebalanceListener);

        metaStorageMgr.registerPrefixWatch(ByteArray.fromString(ASSIGNMENTS_SWITCH_REDUCE_PREFIX), assignmentsSwitchRebalanceListener);

        catalogMgr.listen(ZONE_CREATE,
                (CreateZoneEventParameters parameters) ->
                        inBusyLock(busyLock, () -> onCreateZone(parameters).thenApply((ignored) -> false))
        );

        return processZonesAndAssignmentsOnStart;
    }

    private CompletableFuture<Void> processZonesOnStart(long recoveryRevision, @Nullable HybridTimestamp lwm) {
        int earliestCatalogVersion = catalogMgr.activeCatalogVersion(hybridTimestampToLong(lwm));
        // TODO https://issues.apache.org/jira/browse/IGNITE-22679
        int latestCatalogVersion = catalogMgr.latestCatalogVersion();

        var startedZones = new IntOpenHashSet();
        var startZoneFutures = new ArrayList<CompletableFuture<?>>();

        for (int ver = latestCatalogVersion; ver >= earliestCatalogVersion; ver--) {
            int ver0 = ver;
            catalogMgr.zones(ver).stream()
                    .filter(zone -> startedZones.add(zone.id()))
                    .forEach(zoneDescriptor -> startZoneFutures.add(
                            calculateZoneAssignmentsAndCreateReplicationNodes(recoveryRevision, ver0, zoneDescriptor)));
        }

        return allOf(startZoneFutures.toArray(CompletableFuture[]::new))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error starting zones", throwable);
                    } else {
                        LOG.debug(
                                "Zones started successfully [earliestCatalogVersion={}, latestCatalogVersion={}, startedZoneIds={}]",
                                earliestCatalogVersion,
                                latestCatalogVersion,
                                startedZones
                        );
                    }
                });
    }

    private CompletableFuture<Void> processAssignmentsOnRecovery(long recoveryRevision) {
        var stableAssignmentsPrefix = new ByteArray(STABLE_ASSIGNMENTS_PREFIX);
        var pendingAssignmentsPrefix = new ByteArray(PENDING_ASSIGNMENTS_PREFIX);

        // It's required to handle stable assignments changes on recovery in order to cleanup obsolete resources.
        CompletableFuture<Void> stableFuture = handleAssignmentsOnRecovery(
                stableAssignmentsPrefix,
                recoveryRevision,
                (entry, rev) -> handleChangeStableAssignmentEvent(entry, rev, true),
                "stable"
        );

        CompletableFuture<Void> pendingFuture = handleAssignmentsOnRecovery(
                pendingAssignmentsPrefix,
                recoveryRevision,
                (entry, rev) -> handleChangePendingAssignmentEvent(entry, rev, true),
                "pending"
        );

        return allOf(stableFuture, pendingFuture);
    }

    private CompletableFuture<Void> handleAssignmentsOnRecovery(
            ByteArray prefix,
            long revision,
            BiFunction<Entry, Long, CompletableFuture<Void>> assignmentsEventHandler,
            String assignmentsType
    ) {
        try (Cursor<Entry> cursor = metaStorageMgr.prefixLocally(prefix, revision)) {
            CompletableFuture<?>[] futures = cursor.stream()
                    .map(entry -> {
                        if (LOG.isInfoEnabled()) {
                            LOG.info(
                                    "Non handled {} assignments for key '{}' discovered, performing recovery",
                                    assignmentsType,
                                    new String(entry.key(), UTF_8)
                            );
                        }

                        return assignmentsEventHandler.apply(entry, revision);
                    })
                    .toArray(CompletableFuture[]::new);

            return allOf(futures)
                    .whenComplete((res, e) -> {
                        if (e != null) {
                            LOG.error("Error when performing assignments recovery", e);
                        }
                    });
        }
    }

    private void cleanUpResourcesForDroppedZonesOnRecovery() {
        // TODO: IGNITE-20384 Clean up abandoned resources for dropped zones from vault and metastore
    }

    private CompletableFuture<Void> onCreateZone(CreateZoneEventParameters createZoneEventParameters) {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-22535 start replica must be moved from metastore thread
        return calculateZoneAssignmentsAndCreateReplicationNodes(
                createZoneEventParameters.causalityToken(),
                createZoneEventParameters.catalogVersion(),
                createZoneEventParameters.zoneDescriptor()
        );
    }

    private CompletableFuture<Void> calculateZoneAssignmentsAndCreateReplicationNodes(
            long causalityToken,
            int catalogVersion,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        return inBusyLockAsync(busyLock, () -> {
            CompletableFuture<List<Assignments>> assignmentsFuture = getOrCreateAssignments(
                    zoneDescriptor,
                    causalityToken,
                    catalogVersion
            );

            CompletableFuture<List<Assignments>> assignmentsFutureAfterInvoke =
                    writeZoneAssignmentsToMetastore(zoneDescriptor.id(), assignmentsFuture);

            return createZoneReplicationNodes(assignmentsFutureAfterInvoke, zoneDescriptor.id(), causalityToken);
        });
    }

    private CompletableFuture<Void> createZoneReplicationNodes(CompletableFuture<List<Assignments>> assignmentsFuture, int zoneId, long revision) {
        return inBusyLockAsync(busyLock, () -> assignmentsFuture.thenCompose(assignments -> {
            assert assignments != null : IgniteStringFormatter.format("Zone has empty assignments [id={}].", zoneId);

            List<CompletableFuture<?>> partitionsStartFutures = new ArrayList<>();

            for (int partId = 0; partId < assignments.size(); partId++) {
                Assignments zoneAssignment = assignments.get(partId);

                Assignment localMemberAssignment = localMemberAssignment(zoneAssignment);

                partitionsStartFutures.add(createZonePartitionReplicationNode(zoneId, partId, localMemberAssignment, zoneAssignment, revision));
            }

            return allOf(partitionsStartFutures.toArray(new CompletableFuture<?>[0]));
        }));
    }

    /**
     * Start a replica for the corresponding {@code zoneId} and {@code partId} on the local node if {@code localMemberAssignment} is not
     * null, meaning that the local node is part of the assignment.
     *
     * @param zoneId Zone id.
     * @param partId Partition id.
     * @param localMemberAssignment Assignment of the local member, or null if local member is not part of the assignment.
     * @param stableAssignments Stable assignments.
     * @return Future that completes when a replica is started.
     */
    private CompletableFuture<Void> createZonePartitionReplicationNode(
            int zoneId,
            int partId,
            @Nullable Assignment localMemberAssignment,
            Assignments stableAssignments,
            long revision
    ) {
        if (localMemberAssignment == null) {
            return nullCompletedFuture();
        }

        // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 We need to integrate PartitionReplicatorNodeRecovery logic here when
        //  we in the recovery phase.

        PeersAndLearners stablePeersAndLearners = fromAssignments(stableAssignments.nodes());

        ZonePartitionId replicaGrpId = new ZonePartitionId(zoneId, partId);

        RaftGroupListener raftGroupListener = new ZonePartitionRaftListener();

        ZoneRebalanceRaftGroupEventsListener raftGroupEventsListener = new ZoneRebalanceRaftGroupEventsListener(
                metaStorageMgr,
                replicaGrpId,
                busyLock,
                createPartitionMover(replicaGrpId),
                rebalanceScheduler,
                catalogMgr,
                distributionZoneMgr
        );

        replicationGroupIds.add(replicaGrpId);
        CatalogZoneDescriptor zoneDescriptor = catalogMgr.zone(replicaGrpId.zoneId(), catalogMgr.latestCatalogVersion());

        try {
            return replicaMgr.startReplica(
                    replicaGrpId,
                    (raftClient) -> new ZonePartitionReplicaListener(
                            new ExecutorInclinedRaftCommandRunner(raftClient, partitionOperationsExecutor)),
                    new FailFastSnapshotStorageFactory(),
                    stablePeersAndLearners,
                    raftGroupListener,
                    raftGroupEventsListener,
                    busyLock
                    ).thenCompose(unused ->
                            fireEvent(
                                    PartitionReplicaLifecycleEvent.REPLICA_STARTED,
                                    new PartitionReplicaLifecycleEventParameters(revision, zoneDescriptor)
                            ))
                    .thenApply(ignored -> null);
        } catch (NodeStoppingException e) {
            return failedFuture(e);
        }
    }

    private PartitionMover createPartitionMover(ZonePartitionId replicaGrpId) {
        return new PartitionMover(busyLock, () -> {
            CompletableFuture<Replica> replicaFut = replicaMgr.replica(replicaGrpId);
            if (replicaFut == null) {
                return failedFuture(new IgniteInternalException("No such replica for partition " + replicaGrpId.partitionId()
                        + " in zone " + replicaGrpId.zoneId()));
            }
            return replicaFut.thenApply(Replica::raftClient);
        });
    }

    private ClusterNode localNode() {
        return topologyService.localMember();
    }

    @Override
    public void beforeNodeStop() {
        busyLock.block();

        metaStorageMgr.unregisterWatch(pendingAssignmentsRebalanceListener);
        metaStorageMgr.unregisterWatch(stableAssignmentsRebalanceListener);
        metaStorageMgr.unregisterWatch(assignmentsSwitchRebalanceListener);

        cleanUpPartitionsResources(replicationGroupIds);
    }

    /**
     * Writes the set of assignments to meta storage. If there are some assignments already, gets them from meta storage. Returns
     * the list of assignments that really are in meta storage.
     *
     * @param zoneId  Zone id.
     * @param assignmentsFuture Assignments future, to get the assignments that should be written.
     * @return Real list of assignments.
     */
    private CompletableFuture<List<Assignments>> writeZoneAssignmentsToMetastore(
            int zoneId,
            CompletableFuture<List<Assignments>> assignmentsFuture
    ) {
        return assignmentsFuture.thenCompose(newAssignments -> {
            assert !newAssignments.isEmpty();

            List<Operation> partitionAssignments = new ArrayList<>(newAssignments.size());

            for (int i = 0; i < newAssignments.size(); i++) {
                ByteArray stableAssignmentsKey = stablePartAssignmentsKey(new ZonePartitionId(zoneId, i));
                byte[] anAssignment = newAssignments.get(i).toBytes();
                Operation op = put(stableAssignmentsKey, anAssignment);
                partitionAssignments.add(op);
            }

            Condition condition = notExists(new ByteArray(toByteArray(partitionAssignments.get(0).key())));

            return metaStorageMgr
                    .invoke(condition, partitionAssignments, Collections.emptyList())
                    .handle((invokeResult, e) -> {
                        if (e != null) {
                            LOG.error(
                                    "Couldn't write assignments [assignmentsList={}] to metastore during invoke.",
                                    e,
                                    Assignments.assignmentListToString(newAssignments)
                            );

                            throw ExceptionUtils.sneakyThrow(e);
                        }

                        return invokeResult;
                    })
                    .thenCompose(invokeResult -> {
                        if (invokeResult) {
                            LOG.info(
                                    "Assignments calculated from data nodes are successfully written to meta storage"
                                            + " [zoneId={}, assignments={}].",
                                    zoneId,
                                    Assignments.assignmentListToString(newAssignments)
                            );

                            return completedFuture(newAssignments);
                        } else {
                            Set<ByteArray> partKeys = IntStream.range(0, newAssignments.size())
                                    .mapToObj(p -> stablePartAssignmentsKey(new ZonePartitionId(zoneId, p)))
                                    .collect(toSet());

                            CompletableFuture<Map<ByteArray, Entry>> resFuture = metaStorageMgr.getAll(partKeys);

                            return resFuture.thenApply(metaStorageAssignments -> {
                                List<Assignments> realAssignments = new ArrayList<>();

                                for (int p = 0; p < newAssignments.size(); p++) {
                                    var partId = new ZonePartitionId(zoneId, p);
                                    Entry assignmentsEntry = metaStorageAssignments.get(stablePartAssignmentsKey(partId));

                                    assert assignmentsEntry != null && !assignmentsEntry.empty() && !assignmentsEntry.tombstone()
                                            : "Unexpected assignments for partition [" + partId + ", entry=" + assignmentsEntry + "].";

                                    Assignments real = Assignments.fromBytes(assignmentsEntry.value());

                                    realAssignments.add(real);
                                }

                                LOG.info(
                                        "Assignments picked up from meta storage [zoneId={}, assignments={}].",
                                        zoneId,
                                        Assignments.assignmentListToString(realAssignments)
                                );

                                return realAssignments;
                            });
                        }
                    })
                    .handle((realAssignments, e) -> {
                        if (e != null) {
                            LOG.error("Couldn't get assignments from metastore for zone [zoneId={}].", e, zoneId);

                            throw ExceptionUtils.sneakyThrow(e);
                        }

                        return realAssignments;
                    });
        });
    }

    /**
     * Check if the zone already has assignments in the meta storage locally.
     * So, it means, that it is a recovery process and we should use the meta storage local assignments instead of calculation
     * of the new ones.
     */
    private CompletableFuture<List<Assignments>> getOrCreateAssignments(
            CatalogZoneDescriptor zoneDescriptor,
            long causalityToken,
            int catalogVersion
    ) {
        CompletableFuture<List<Assignments>> assignmentsFuture;

        if (zonePartitionAssignmentsGetLocally(metaStorageMgr, zoneDescriptor.id(), 0, causalityToken) != null) {
            assignmentsFuture = completedFuture(
                    zoneAssignmentsGetLocally(metaStorageMgr, zoneDescriptor.id(), zoneDescriptor.partitions(), causalityToken));
        } else {
            assignmentsFuture = distributionZoneMgr.dataNodes(causalityToken, catalogVersion, zoneDescriptor.id())
                    .thenApply(dataNodes -> AffinityUtils.calculateAssignments(
                            dataNodes,
                            zoneDescriptor.partitions(),
                            zoneDescriptor.replicas()
                    ).stream().map(Assignments::of).collect(toList()));

            assignmentsFuture.thenAccept(assignmentsList -> LOG.info(
                    "Assignments calculated from data nodes [zone={}, zoneId={}, assignments={}, revision={}]",
                    zoneDescriptor.name(),
                    zoneDescriptor.id(),
                    Assignments.assignmentListToString(assignmentsList),
                    causalityToken
            ));
        }

        return assignmentsFuture;
    }

    /**
     * Check if the current node has local replica for this {@link ZonePartitionId}.
     *
     * @param zonePartitionId Zone partition id.
     * @return true if local replica exists, false otherwise.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22624 replace this method by the replicas await process.
    public boolean hasLocalPartition(ZonePartitionId zonePartitionId) {
        return replicationGroupIds.contains(zonePartitionId);
    }


    /**
     * Creates meta storage listener for pending assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createPendingAssignmentsRebalanceListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    return failedFuture(new NodeStoppingException());
                }

                try {
                    Entry newEntry = evt.entryEvent().newEntry();

                    return handleChangePendingAssignmentEvent(newEntry, evt.revision(), false);
                } finally {
                    busyLock.leaveBusy();
                }
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process pending assignments event", e);
            }
        };
    }

    /**
     * Creates Meta storage listener for stable assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createStableAssignmentsRebalanceListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    return failedFuture(new NodeStoppingException());
                }

                try {
                    return handleChangeStableAssignmentEvent(evt);
                } finally {
                    busyLock.leaveBusy();
                }
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process stable assignments event", e);
            }
        };
    }

    /** Creates Meta storage listener for switch reduce assignments updates. */
    private WatchListener createAssignmentsSwitchRebalanceListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                return inBusyLockAsync(busyLock, () -> {
                    byte[] key = evt.entryEvent().newEntry().key();

                    int partitionId = extractPartitionNumber(key);
                    int zoneId = extractZoneId(key, ASSIGNMENTS_SWITCH_REDUCE_PREFIX);

                    ZonePartitionId replicaGrpId = new ZonePartitionId(zoneId, partitionId);

                    // It is safe to get the latest version of the catalog as we are in the metastore thread.
                    // TODO: IGNITE-22661 Potentially unsafe to use the latest catalog version. Better to take the version from Assignments.
                    int catalogVersion = catalogMgr.latestCatalogVersion();

                    CatalogZoneDescriptor zoneDescriptor = catalogMgr.zone(zoneId, catalogVersion);

                    long causalityToken = zoneDescriptor.updateToken();

                    return distributionZoneMgr.dataNodes(causalityToken, catalogVersion, zoneId)
                            .thenCompose(dataNodes -> handleReduceChanged(
                                    metaStorageMgr,
                                    dataNodes,
                                    zoneDescriptor.replicas(),
                                    replicaGrpId,
                                    evt
                            ));

                });
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process switch reduce event", e);
            }
        };
    }

    /**
     * Handles the {@link org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil#STABLE_ASSIGNMENTS_PREFIX} update event.
     *
     * @param evt Event.
     */
    protected CompletableFuture<Void> handleChangeStableAssignmentEvent(WatchEvent evt) {
        if (evt.entryEvents().stream().allMatch(e -> e.oldEntry().value() == null)) {
            // It's the initial write to zone stable assignments on zone create event.
            return nullCompletedFuture();
        }

        if (!evt.single()) {
            // If there is not a single entry, then all entries must be tombstones (this happens after zone drop).
            assert evt.entryEvents().stream().allMatch(entryEvent -> entryEvent.newEntry().tombstone()) : evt;

            return nullCompletedFuture();
        }

        // here we can receive only update from the rebalance logic
        // these updates always processing only 1 partition, so, only 1 stable partition key.
        assert evt.single() : evt;

        if (evt.entryEvent().oldEntry() == null) {
            // This means it's an event on zone creation.
            return nullCompletedFuture();
        }

        Entry stableAssignmentsWatchEvent = evt.entryEvent().newEntry();

        long revision = evt.revision();

        assert stableAssignmentsWatchEvent.revision() == revision : stableAssignmentsWatchEvent;

        if (stableAssignmentsWatchEvent.value() == null) {
            return nullCompletedFuture();
        }

        return handleChangeStableAssignmentEvent(stableAssignmentsWatchEvent, evt.revision(), false);
    }

    protected CompletableFuture<Void> handleChangeStableAssignmentEvent(
            Entry stableAssignmentsWatchEvent,
            long revision,
            boolean isRecovery
    ) {
        int partitionId = extractPartitionNumber(stableAssignmentsWatchEvent.key());
        int zoneId = extractZoneId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX);

        ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, partitionId);

        Set<Assignment> stableAssignments = stableAssignmentsWatchEvent.value() == null
                ? emptySet()
                : Assignments.fromBytes(stableAssignmentsWatchEvent.value()).nodes();


        return supplyAsync(() -> {
            Entry pendingAssignmentsEntry = metaStorageMgr.getLocally(pendingPartAssignmentsKey(zonePartitionId), revision);

            byte[] pendingAssignmentsFromMetaStorage = pendingAssignmentsEntry.value();

            Assignments pendingAssignments = pendingAssignmentsFromMetaStorage == null
                    ? Assignments.EMPTY
                    : Assignments.fromBytes(pendingAssignmentsFromMetaStorage);

            return stopAndDestroyPartitionAndUpdateClients(
                    zonePartitionId,
                    stableAssignments,
                    pendingAssignments,
                    isRecovery
            );
        }, ioExecutor).thenCompose(identity());
    }

    private CompletableFuture<Void> updatePartitionClients(
            ZonePartitionId zonePartitionId,
            Set<Assignment> stableAssignments
    ) {
        // Update raft client peers and learners according to the actual assignments.
        if (replicaMgr.isReplicaStarted(zonePartitionId)) {
            replicaMgr.replica(zonePartitionId).join()
                    .raftClient().updateConfiguration(fromAssignments(stableAssignments));
        }

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> stopAndDestroyPartitionAndUpdateClients(
            ZonePartitionId zonePartitionId,
            Set<Assignment> stableAssignments,
            Assignments pendingAssignments,
            boolean isRecovery
    ) {
        CompletableFuture<Void> clientUpdateFuture = isRecovery
                // Updating clients is not needed on recovery.
                ? nullCompletedFuture()
                : updatePartitionClients(zonePartitionId, stableAssignments);

        boolean shouldStopLocalServices = (pendingAssignments.force()
                ? pendingAssignments.nodes().stream()
                : Stream.concat(stableAssignments.stream(), pendingAssignments.nodes().stream())
        )
                .noneMatch(assignment -> assignment.consistentId().equals(localNode().name()));

        if (shouldStopLocalServices) {
            return allOf(
                    clientUpdateFuture,
                    stopAndDestroyPartition(zonePartitionId)
            );
        } else {
            return clientUpdateFuture;
        }
    }

    private CompletableFuture<?> stopAndDestroyPartition(ReplicationGroupId zonePartitionId) {
        return stopPartition(zonePartitionId);
    }

    private CompletableFuture<Void> handleChangePendingAssignmentEvent(
            Entry pendingAssignmentsEntry,
            long revision,
            boolean isRecovery
    ) {
        if (pendingAssignmentsEntry.value() == null || pendingAssignmentsEntry.empty()) {
            return nullCompletedFuture();
        }

        int partId = extractPartitionNumber(pendingAssignmentsEntry.key());
        int zoneId = extractZoneId(pendingAssignmentsEntry.key(), PENDING_ASSIGNMENTS_PREFIX);

        var zonePartitionId = new ZonePartitionId(zoneId, partId);

        // Stable assignments from the meta store, which revision is bounded by the current pending event.
        Assignments stableAssignments = stableAssignments(zonePartitionId, revision);

        Assignments pendingAssignments = Assignments.fromBytes(pendingAssignmentsEntry.value());

        if (!busyLock.enterBusy()) {
            return CompletableFuture.<Void>failedFuture(new NodeStoppingException());
        }

        try {
            if (LOG.isInfoEnabled()) {
                var stringKey = new String(pendingAssignmentsEntry.key(), UTF_8);

                LOG.info("Received update on pending assignments. Check if new replication node should be started [key={}, "
                                + "partition={}, zoneId={}, localMemberAddress={}, pendingAssignments={}, revision={}]",
                        stringKey, partId, zoneId, localNode().address(), pendingAssignments, revision);
            }

            return handleChangePendingAssignmentEvent(
                    zonePartitionId,
                    stableAssignments,
                    pendingAssignments,
                    revision
            ).thenCompose(v -> changePeersOnRebalance(
                    replicaMgr,
                    zonePartitionId,
                    pendingAssignments.nodes(),
                    stableAssignments == null ? emptySet() : stableAssignments.nodes(),
                    revision)
            );
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> handleChangePendingAssignmentEvent(
            ZonePartitionId replicaGrpId,
            @Nullable Assignments stableAssignments,
            Assignments pendingAssignments,
            long revision
    ) {
        boolean pendingAssignmentsAreForced = pendingAssignments.force();
        Set<Assignment> pendingAssignmentsNodes = pendingAssignments.nodes();

        // Start a new Raft node and Replica if this node has appeared in the new assignments.
        Assignment localMemberAssignment = localMemberAssignment(pendingAssignments);

        boolean shouldStartLocalGroupNode = localMemberAssignment != null
                && (stableAssignments == null || !stableAssignments.nodes().contains(localMemberAssignment));

        // This is a set of assignments for nodes that are not the part of stable assignments, i.e. unstable part of the distribution.
        // For regular pending assignments we use (old) stable set, so that none of new nodes would be able to propose itself as a leader.
        // For forced assignments, we should do the same thing, but only for the subset of stable set that is alive right now. Dead nodes
        // are excluded. It is calculated precisely as an intersection between forced assignments and (old) stable assignments.
        Assignments computedStableAssignments;

        // TODO: https://issues.apache.org/jira/browse/IGNITE-22600 remove the second condition
        //  when we will have a proper handling of empty stable assignments
        if (stableAssignments == null || stableAssignments.nodes().isEmpty()) {
            // This condition can only pass if all stable nodes are dead, and we start new raft group from scratch.
            // In this case new initial configuration must match new forced assignments.
            computedStableAssignments = Assignments.forced(pendingAssignmentsNodes);
        } else if (pendingAssignmentsAreForced) {
            // In case of forced assignments we need to remove nodes that are present in the stable set but are missing from the
            // pending set. Such operation removes dead stable nodes from the resulting stable set, which guarantees that we will
            // have a live majority.
            computedStableAssignments = pendingAssignments;
        } else {
            computedStableAssignments = stableAssignments;
        }

        int partitionId = replicaGrpId.partitionId();
        int zoneId = replicaGrpId.zoneId();

        CompletableFuture<Void> localServicesStartFuture;

        if (shouldStartLocalGroupNode) {
            localServicesStartFuture = createZonePartitionReplicationNode(
                    zoneId,
                    partitionId,
                    localMemberAssignment,
                    computedStableAssignments,
                    revision
            );
        } else if (pendingAssignmentsAreForced && localMemberAssignment != null) {
            localServicesStartFuture = runAsync(() -> {
                inBusyLock(busyLock, () -> replicaMgr.resetPeers(replicaGrpId, fromAssignments(computedStableAssignments.nodes())));
            }, ioExecutor);
        } else {
            localServicesStartFuture = nullCompletedFuture();
        }

        return localServicesStartFuture.thenRunAsync(() -> inBusyLock(busyLock, () -> {
            if (!replicaMgr.isReplicaStarted(replicaGrpId)) {
                return;
            }

            // For forced assignments, we exclude dead stable nodes, and all alive stable nodes are already in pending assignments.
            // Union is not required in such a case.
            Set<Assignment> newAssignments = pendingAssignmentsAreForced || stableAssignments == null
                    ? pendingAssignmentsNodes
                    : RebalanceUtil.union(pendingAssignmentsNodes, stableAssignments.nodes());

            replicaMgr.replica(replicaGrpId).join().raftClient().updateConfiguration(fromAssignments(newAssignments));
        }), ioExecutor);
    }

    private CompletableFuture<Void> changePeersOnRebalance(
            ReplicaManager replicaMgr,
            ZonePartitionId replicaGrpId,
            Set<Assignment> pendingAssignments,
            Set<Assignment> stableAssignments,
            long revision
    ) {
        Set<Assignment> union = new HashSet<>();
        union.addAll(pendingAssignments);
        union.addAll(stableAssignments);

        if (!union.stream().map(Assignment::consistentId).collect(toSet()).contains(localNode().name())) {
            return nullCompletedFuture();
        }

        int partId = replicaGrpId.partitionId();

        RaftGroupService partGrpSvc = replicaMgr.replica(replicaGrpId).join().raftClient();

        return partGrpSvc.refreshAndGetLeaderWithTerm()
                .exceptionally(throwable -> {
                    throwable = unwrapCause(throwable);

                    if (throwable instanceof TimeoutException) {
                        LOG.info("Node couldn't get the leader within timeout so the changing peers is skipped [grp={}].", replicaGrpId);

                        return LeaderWithTerm.NO_LEADER;
                    }

                    throw new IgniteInternalException(
                            INTERNAL_ERR,
                            "Failed to get a leader for the RAFT replication group [get=" + replicaGrpId + "].",
                            throwable
                    );
                })
                .thenCompose(leaderWithTerm -> {
                    if (leaderWithTerm.isEmpty() || !isLocalPeer(leaderWithTerm.leader())) {
                        return nullCompletedFuture();
                    }

                    // run update of raft configuration if this node is a leader
                    LOG.info("Current node={} is the leader of partition raft group={}. "
                                    + "Initiate rebalance process for partition={}, zoneId={}",
                            leaderWithTerm.leader(), replicaGrpId, partId, replicaGrpId.zoneId());

                    return metaStorageMgr.get(pendingPartAssignmentsKey(replicaGrpId))
                            .thenCompose(latestPendingAssignmentsEntry -> {
                                // Do not change peers of the raft group if this is a stale event.
                                // Note that we start raft node before for the sake of the consistency in a
                                // starting and stopping raft nodes.
                                if (revision < latestPendingAssignmentsEntry.revision()) {
                                    return nullCompletedFuture();
                                }

                                PeersAndLearners newConfiguration = fromAssignments(pendingAssignments);

                                CompletableFuture<Void> voidCompletableFuture = partGrpSvc.changePeersAsync(newConfiguration,
                                        leaderWithTerm.term()).exceptionally(e -> {
                                            return null;
                                        });
                                return voidCompletableFuture;
                            });
                });
    }

    private boolean isLocalPeer(Peer peer) {
        return peer.consistentId().equals(localNode().name());
    }

    @Nullable
    private Assignment localMemberAssignment(Assignments assignments) {
        Assignment localMemberAssignment = Assignment.forPeer(localNode().name());

        return assignments.nodes().contains(localMemberAssignment) ? localMemberAssignment : null;
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!ENABLED) {
            return nullCompletedFuture();
        }

        return nullCompletedFuture();
    }

    private static String zoneInfo(CatalogZoneDescriptor zoneDescriptor) {
        return zoneDescriptor.id() + "/" + zoneDescriptor.name();
    }

    private @Nullable Assignments stableAssignments(ZonePartitionId zonePartitionId, long revision) {
        Entry entry = metaStorageMgr.getLocally(stablePartAssignmentsKey(zonePartitionId), revision);

        return Assignments.fromBytes(entry.value());
    }

    /**
     * Stops all resources associated with a given partition, like replicas and partition trackers.
     *
     * @param zonePartitionId Partition ID.
     * @return Future that will be completed after all resources have been closed.
     */
    private CompletableFuture<?> stopPartition(ReplicationGroupId zonePartitionId) {
        CompletableFuture<Boolean> stopReplicaFuture;

        try {
            stopReplicaFuture = replicaMgr.stopReplica(zonePartitionId);
        } catch (NodeStoppingException e) {
            // No-op.
            stopReplicaFuture = falseCompletedFuture();
        }

        return stopReplicaFuture;
    }

    /**
     * Stops resources that are related to provided zone partitions.
     *
     * @param partitionIds Partitions to stop.
     */
    private void cleanUpPartitionsResources(Set<ReplicationGroupId> partitionIds) {
        CompletableFuture<Void> future = runAsync(() -> {
            Stream.Builder<ManuallyCloseable> stopping = Stream.builder();

            stopping.add(() -> {
                var stopReplicaFutures = new CompletableFuture<?>[partitionIds.size()];

                int i = 0;

                for (ReplicationGroupId partitionId : partitionIds) {
                    stopReplicaFutures[i++] = stopPartition(partitionId);
                }

                allOf(stopReplicaFutures).get(10, TimeUnit.SECONDS);
            });

            try {
                IgniteUtils.closeAllManually(stopping.build());
            } catch (Throwable t) {
                LOG.error("Unable to stop partition");
            }
        }, ioExecutor);

        try {
            future.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Unable to clean zones resources", e);
        }
    }
}
