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
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.subtract;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.union;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceRaftGroupEventsListener.handleReduceChanged;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.PENDING_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.extractZonePartitionId;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zoneAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zonePartitionAssignmentsGetLocally;
import static org.apache.ignite.internal.hlc.HybridTimestamp.LOGICAL_TIME_BITS_SIZE;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.partitiondistribution.Assignments.assignmentListToString;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignments;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.utils.SystemDistributedConfigurationPropertyHolder;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.rebalance.PartitionMover;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceRaftGroupEventsListener;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.ClockService;
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
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.FailFastSnapshotStorageFactory;
import org.apache.ignite.internal.partition.replicator.raft.ZonePartitionRaftListener;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.ExecutorInclinedRaftCommandRunner;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaManager.WeakReplicaStopReason;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class that manages per-zone replicas.
 *
 * <p>Its responsibilities include:
 *
 * <ul>
 *     <li>Starting the appropriate replication nodes on the zone creation.</li>
 *     <li>Stopping the same nodes on the zone removing.</li>
 *     <li>Supporting the rebalance mechanism and starting the new replication nodes when the rebalance triggers occurred.</li>
 * </ul>
 */
public class PartitionReplicaLifecycleManager extends
        AbstractEventProducer<LocalPartitionReplicaEvent, LocalPartitionReplicaEventParameters> implements IgniteComponent {
    /* Feature flag for zone based collocation track */
    // TODO IGNITE-22115 remove it
    public static final String FEATURE_FLAG_NAME = "IGNITE_ZONE_BASED_REPLICATION";
    private final boolean enabledColocationFeature = getBoolean(FEATURE_FLAG_NAME, false);

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

    private final Set<ZonePartitionId> replicationGroupIds = ConcurrentHashMap.newKeySet();

    /** (zoneId -> lock) map to provide concurrent access to the zone replicas list. */
    private final Map<Integer, StampedLock> zonePartitionsLocks = new ConcurrentHashMap<>();

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

    /** Clock service for primary replica awaitings. */
    private final ClockService clockService;

    /** Placement driver for primary replicas checks. */
    private final PlacementDriver placementDriver;

    /** Schema sync service for waiting for catalog metadata. */
    private final SchemaSyncService schemaSyncService;

    /** A predicate that checks that the given assignment is corresponded to the local node. */
    private final Predicate<Assignment> isLocalNodeAssignment = assignment -> assignment.consistentId().equals(localNode().name());

    /** Configuration of rebalance retries delay. */
    private final SystemDistributedConfigurationPropertyHolder<Integer> rebalanceRetryDelayConfiguration;

    private final ConcurrentMap<ZonePartitionId, ZonePartitionRaftListener> zonePartitionRaftListeners = new ConcurrentHashMap<>();

    private final ZoneResourcesManager zoneResourcesManager;

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
     *         will be executed.
     * @param clockService Clock service.
     * @param placementDriver Placement driver.
     * @param schemaSyncService Schema synchronization service.
     * @param systemDistributedConfiguration System distributed configuration.
     * @param sharedTxStateStorage Shared tx state storage.
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
            Executor partitionOperationsExecutor,
            ClockService clockService,
            PlacementDriver placementDriver,
            SchemaSyncService schemaSyncService,
            SystemDistributedConfiguration systemDistributedConfiguration,
            TxStateRocksDbSharedStorage sharedTxStateStorage
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
        this.clockService = clockService;
        this.schemaSyncService = schemaSyncService;

        this.placementDriver = placementDriver;

        rebalanceRetryDelayConfiguration = new SystemDistributedConfigurationPropertyHolder<>(
                systemDistributedConfiguration,
                (v, r) -> {},
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_MS,
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_DEFAULT,
                Integer::parseInt
        );

        zoneResourcesManager = new ZoneResourcesManager(sharedTxStateStorage);

        pendingAssignmentsRebalanceListener = createPendingAssignmentsRebalanceListener();
        stableAssignmentsRebalanceListener = createStableAssignmentsRebalanceListener();
        assignmentsSwitchRebalanceListener = createAssignmentsSwitchRebalanceListener();
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        if (!enabledColocationFeature) {
            return nullCompletedFuture();
        }

        CompletableFuture<Revisions> recoveryFinishFuture = metaStorageMgr.recoveryFinishedFuture();

        assert recoveryFinishFuture.isDone();

        long recoveryRevision = recoveryFinishFuture.join().revision();

        cleanUpResourcesForDroppedZonesOnRecovery();

        CompletableFuture<Void> processZonesAndAssignmentsOnStart = processZonesOnStart(recoveryRevision, lowWatermark.getLowWatermark())
                .thenCompose(ignored -> processAssignmentsOnRecovery(recoveryRevision));

        metaStorageMgr.registerPrefixWatch(new ByteArray(PENDING_ASSIGNMENTS_PREFIX_BYTES), pendingAssignmentsRebalanceListener);
        metaStorageMgr.registerPrefixWatch(new ByteArray(STABLE_ASSIGNMENTS_PREFIX_BYTES), stableAssignmentsRebalanceListener);
        metaStorageMgr.registerPrefixWatch(new ByteArray(ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES), assignmentsSwitchRebalanceListener);

        catalogMgr.listen(ZONE_CREATE,
                (CreateZoneEventParameters parameters) ->
                        inBusyLock(busyLock, () -> onCreateZone(parameters).thenApply((ignored) -> false))
        );

        rebalanceRetryDelayConfiguration.init();

        return processZonesAndAssignmentsOnStart;
    }

    private CompletableFuture<Void> processZonesOnStart(long recoveryRevision, @Nullable HybridTimestamp lwm) {
        // If Catalog manager is empty, it gets initialized asynchronously and at this moment the initialization might not complete,
        // nevertheless everything works correctly.
        // All components execute the synchronous part of startAsync sequentially and only when they all complete,
        // we enable metastorage listeners (see IgniteImpl.joinClusterAsync: metaStorageMgr.deployWatches()).
        // Once the metastorage watches are deployed, all components start to receive callbacks, this chain of callbacks eventually
        // fires CatalogManager's ZONE_CREATE event, and the state of PartitionReplicaLifecycleManager becomes consistent
        // (calculateZoneAssignmentsAndCreateReplicationNodes() will be called).
        int earliestCatalogVersion = lwm == null ? catalogMgr.earliestCatalogVersion() : catalogMgr.activeCatalogVersion(lwm.longValue());

        int latestCatalogVersion = catalogMgr.latestCatalogVersion();

        var startedZones = new IntOpenHashSet();
        var startZoneFutures = new ArrayList<CompletableFuture<?>>();

        for (int ver = latestCatalogVersion; ver >= earliestCatalogVersion; ver--) {
            int ver0 = ver;
            catalogMgr.catalog(ver).zones().stream()
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
        var stableAssignmentsPrefix = new ByteArray(STABLE_ASSIGNMENTS_PREFIX_BYTES);
        var pendingAssignmentsPrefix = new ByteArray(PENDING_ASSIGNMENTS_PREFIX_BYTES);

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
                (entry, rev) -> handleChangePendingAssignmentEvent(entry, rev),
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
            int zoneId = zoneDescriptor.id();

            zoneResourcesManager.registerZonePartitionCount(zoneId, zoneDescriptor.partitions());

            return getOrCreateAssignments(zoneDescriptor, causalityToken, catalogVersion)
                    .thenCompose(assignments -> writeZoneAssignmentsToMetastore(zoneId, assignments))
                    .thenCompose(assignments -> createZoneReplicationNodes(zoneId, assignments, causalityToken));
        });
    }

    private CompletableFuture<Void> createZoneReplicationNodes(
            int zoneId,
            List<Assignments> assignments,
            long revision
    ) {
        return inBusyLockAsync(busyLock, () -> {
            assert assignments != null : IgniteStringFormatter.format("Zone has empty assignments [id={}].", zoneId);

            var partitionsStartFutures = new CompletableFuture<?>[assignments.size()];

            for (int partId = 0; partId < assignments.size(); partId++) {
                Assignments zoneAssignment = assignments.get(partId);

                Assignment localMemberAssignment = localMemberAssignment(zoneAssignment);

                var zonePartitionId = new ZonePartitionId(zoneId, partId);

                partitionsStartFutures[partId] = createZonePartitionReplicationNode(
                        zonePartitionId,
                        localMemberAssignment,
                        zoneAssignment,
                        revision
                );
            }

            return allOf(partitionsStartFutures);
        });
    }

    /**
     * Start a replica for the corresponding {@code zoneId} and {@code partId} on the local node if {@code localMemberAssignment} is not
     * null, meaning that the local node is part of the assignment.
     *
     * @param zonePartitionId Zone Partition ID.
     * @param localMemberAssignment Assignment of the local member, or null if local member is not part of the assignment.
     * @param stableAssignments Stable assignments.
     * @param revision Event's revision.
     * @return Future that completes when a replica is started.
     */
    private CompletableFuture<?> createZonePartitionReplicationNode(
            ZonePartitionId zonePartitionId,
            @Nullable Assignment localMemberAssignment,
            Assignments stableAssignments,
            long revision
    ) {
        if (localMemberAssignment == null) {
            return nullCompletedFuture();
        }

        // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 We need to integrate PartitionReplicatorNodeRecovery logic here when
        //  we in the recovery phase.

        Assignments forcedAssignments = stableAssignments.force() ? stableAssignments : null;

        PeersAndLearners stablePeersAndLearners = fromAssignments(stableAssignments.nodes());

        var raftGroupListener = new ZonePartitionRaftListener();

        zonePartitionRaftListeners.put(zonePartitionId, raftGroupListener);

        ZoneRebalanceRaftGroupEventsListener raftGroupEventsListener = new ZoneRebalanceRaftGroupEventsListener(
                metaStorageMgr,
                zonePartitionId,
                busyLock,
                createPartitionMover(zonePartitionId),
                rebalanceScheduler,
                this::calculateZoneAssignments,
                rebalanceRetryDelayConfiguration
        );

        Supplier<CompletableFuture<Boolean>> startReplicaSupplier = () -> {
            TxStatePartitionStorage txStatePartitionStorage = zoneResourcesManager.getOrCreatePartitionTxStateStorage(
                    zonePartitionId.zoneId(),
                    zonePartitionId.partitionId()
            );

            try {
                return replicaMgr.startReplica(
                                zonePartitionId,
                                raftClient -> new ZonePartitionReplicaListener(
                                        new ExecutorInclinedRaftCommandRunner(raftClient, partitionOperationsExecutor)),
                                new FailFastSnapshotStorageFactory(),
                                stablePeersAndLearners,
                                raftGroupListener,
                                raftGroupEventsListener,
                                // TODO: IGNITE-24371 - pass real isVolatile flag
                                false,
                                txStatePartitionStorage,
                                busyLock
                        ).thenCompose(replica -> executeUnderZoneWriteLock(zonePartitionId.zoneId(), () -> {
                            replicationGroupIds.add(zonePartitionId);

                            var eventParams = new LocalPartitionReplicaEventParameters(zonePartitionId, revision);

                            return fireEvent(LocalPartitionReplicaEvent.AFTER_REPLICA_STARTED, eventParams);
                        }))
                        .thenApply(unused -> false);
            } catch (NodeStoppingException e) {
                return failedFuture(e);
            }
        };

        return replicaMgr.weakStartReplica(zonePartitionId, startReplicaSupplier, forcedAssignments)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        LOG.warn("Unable to update raft groups on the node [zonePartitionId={}]", ex, zonePartitionId);
                    }
                });
    }

    private CompletableFuture<Set<Assignment>> calculateZoneAssignments(
            ZonePartitionId zonePartitionId,
            Long assignmentsTimestamp
    ) {
        return waitForMetadataCompleteness(assignmentsTimestamp).thenCompose(unused -> {
            Catalog catalog = catalogMgr.activeCatalog(assignmentsTimestamp);

            CatalogZoneDescriptor zoneDescriptor = catalog.zone(zonePartitionId.zoneId());

            int zoneId = zonePartitionId.zoneId();

            return distributionZoneMgr.dataNodes(zoneDescriptor.updateToken(), catalog.version(), zoneId)
                    .thenApply(dataNodes -> calculateAssignmentForPartition(
                            dataNodes,
                            zonePartitionId.partitionId(),
                            zoneDescriptor.partitions(),
                            zoneDescriptor.replicas()
                    ));
        });
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
     * Writes the set of assignments to meta storage. If there are some assignments already, gets them from meta storage. Returns the list
     * of assignments that really are in meta storage.
     *
     * @param zoneId Zone id.
     * @param newAssignments Assignments that should be written.
     * @return Real list of assignments.
     */
    private CompletableFuture<List<Assignments>> writeZoneAssignmentsToMetastore(int zoneId, List<Assignments> newAssignments) {
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
                .whenComplete((invokeResult, e) -> {
                    if (e != null) {
                        LOG.error(
                                "Couldn't write assignments [assignmentsList={}] to metastore during invoke.",
                                e,
                                assignmentListToString(newAssignments)
                        );
                    }
                })
                .thenCompose(invokeResult -> {
                    if (invokeResult) {
                        LOG.info(
                                "Assignments calculated from data nodes are successfully written to meta storage"
                                        + " [zoneId={}, assignments={}].",
                                zoneId,
                                assignmentListToString(newAssignments)
                        );

                        return completedFuture(newAssignments);
                    } else {
                        Set<ByteArray> partKeys = IntStream.range(0, newAssignments.size())
                                .mapToObj(p -> stablePartAssignmentsKey(new ZonePartitionId(zoneId, p)))
                                .collect(toSet());

                        return metaStorageMgr.getAll(partKeys)
                                .thenApply(metaStorageAssignments -> {
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
                                            assignmentListToString(realAssignments)
                                    );

                                    return realAssignments;
                                })
                                .whenComplete((realAssignments, e) -> {
                                    if (e != null) {
                                        LOG.error("Couldn't get assignments from metastore for zone [zoneId={}].", e, zoneId);
                                    }
                                });
                    }
                });
    }

    /**
     * Check if the zone already has assignments in the meta storage locally. So, it means, that it is a recovery process and we should use
     * the meta storage local assignments instead of calculation of the new ones.
     */
    private CompletableFuture<List<Assignments>> getOrCreateAssignments(
            CatalogZoneDescriptor zoneDescriptor,
            long causalityToken,
            int catalogVersion
    ) {
        if (zonePartitionAssignmentsGetLocally(metaStorageMgr, zoneDescriptor.id(), 0, causalityToken) != null) {
            return completedFuture(zoneAssignmentsGetLocally(
                    metaStorageMgr,
                    zoneDescriptor.id(),
                    zoneDescriptor.partitions(),
                    causalityToken
            ));
        } else {
            // Safe to get catalog by version here as the version either comes from iteration from the latest to earliest,
            // or from the handler of catalog version creation.
            Catalog catalog = catalogMgr.catalog(catalogVersion);
            long assignmentsTimestamp = catalog.time();

            return distributionZoneMgr.dataNodes(causalityToken, catalogVersion, zoneDescriptor.id())
                    .thenApply(dataNodes -> calculateAssignments(dataNodes, zoneDescriptor.partitions(), zoneDescriptor.replicas())
                            .stream()
                            .map(assignments -> Assignments.of(assignments, assignmentsTimestamp))
                            .collect(toList())
                    )
                    .whenComplete((assignments, e) -> {
                        if (e == null && LOG.isInfoEnabled()) {
                            LOG.info(
                                    "Assignments calculated from data nodes [zone={}, zoneId={}, assignments={}, revision={}]",
                                    zoneDescriptor.name(),
                                    zoneDescriptor.id(),
                                    assignmentListToString(assignments),
                                    causalityToken
                            );
                        }
                    });
        }
    }

    /**
     * Check if the current node has local replica for this {@link ZonePartitionId}.
     *
     * <p>Important: this method must be invoked always under the according stamped lock.
     *
     * @param zonePartitionId Zone partition id.
     * @return true if local replica exists, false otherwise.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22624 replace this method by the replicas await process.
    public boolean hasLocalPartition(ZonePartitionId zonePartitionId) {
        assert zonePartitionsLocks.get(zonePartitionId.zoneId()).tryWriteLock() == 0;

        return replicationGroupIds.contains(zonePartitionId);
    }

    /**
     * Creates meta storage listener for pending assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createPendingAssignmentsRebalanceListener() {
        return evt -> {
            if (!busyLock.enterBusy()) {
                return failedFuture(new NodeStoppingException());
            }

            try {
                Entry newEntry = evt.entryEvent().newEntry();

                return handleChangePendingAssignmentEvent(newEntry, evt.revision());
            } finally {
                busyLock.leaveBusy();
            }
        };
    }

    /**
     * Creates Meta storage listener for stable assignments updates.
     *
     * @return The watch listener.
     */
    private WatchListener createStableAssignmentsRebalanceListener() {
        return evt -> {
            if (!busyLock.enterBusy()) {
                return failedFuture(new NodeStoppingException());
            }

            try {
                return handleChangeStableAssignmentEvent(evt);
            } finally {
                busyLock.leaveBusy();
            }
        };
    }

    /** Creates Meta storage listener for switch reduce assignments updates. */
    private WatchListener createAssignmentsSwitchRebalanceListener() {
        return evt -> inBusyLockAsync(busyLock, () -> {
            byte[] key = evt.entryEvent().newEntry().key();

            ZonePartitionId replicaGrpId = extractZonePartitionId(key, ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES);

            Assignments assignments = Assignments.fromBytes(evt.entryEvent().newEntry().value());

            long assignmentsTimestamp = assignments.timestamp();

            return waitForMetadataCompleteness(assignmentsTimestamp).thenCompose(unused -> inBusyLockAsync(busyLock, () -> {
                Catalog catalog = catalogMgr.activeCatalog(assignmentsTimestamp);

                CatalogZoneDescriptor zoneDescriptor = catalog.zone(replicaGrpId.zoneId());

                long causalityToken = zoneDescriptor.updateToken();

                return distributionZoneMgr.dataNodes(causalityToken, catalog.version(), replicaGrpId.zoneId())
                        .thenCompose(dataNodes -> handleReduceChanged(
                                metaStorageMgr,
                                dataNodes,
                                zoneDescriptor.partitions(),
                                zoneDescriptor.replicas(),
                                replicaGrpId,
                                evt,
                                assignmentsTimestamp
                        ));
            }));
        });
    }

    /**
     * Handles the {@link org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil#STABLE_ASSIGNMENTS_PREFIX} update event.
     *
     * @param evt Event.
     */
    private CompletableFuture<Void> handleChangeStableAssignmentEvent(WatchEvent evt) {
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

    private CompletableFuture<Void> handleChangeStableAssignmentEvent(
            Entry stableAssignmentsWatchEvent,
            long revision,
            boolean isRecovery
    ) {
        ZonePartitionId zonePartitionId = extractZonePartitionId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX_BYTES);

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
                    isRecovery,
                    revision
            );
        }, ioExecutor).thenCompose(identity());
    }

    private CompletableFuture<Void> updatePartitionClients(
            ZonePartitionId zonePartitionId,
            Set<Assignment> stableAssignments
    ) {
        return isLocalNodeIsPrimary(zonePartitionId).thenCompose(isLeaseholder -> inBusyLock(busyLock, () -> {
            boolean isLocalInStable = isLocalNodeInAssignments(stableAssignments);

            if (!isLocalInStable && !isLeaseholder) {
                return nullCompletedFuture();
            }

            assert replicaMgr.isReplicaStarted(zonePartitionId)
                    : "The local node is outside of the replication group [groupId=" + zonePartitionId
                    + ", stable=" + stableAssignments
                    + ", isLeaseholder=" + isLeaseholder + "].";

            // Update raft client peers and learners according to the actual assignments.
            return replicaMgr.replica(zonePartitionId)
                    .thenAccept(replica -> replica.updatePeersAndLearners(fromAssignments(stableAssignments)));
        }));
    }

    private CompletableFuture<Void> stopAndDestroyPartitionAndUpdateClients(
            ZonePartitionId zonePartitionId,
            Set<Assignment> stableAssignments,
            Assignments pendingAssignments,
            boolean isRecovery,
            long revision
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
            return clientUpdateFuture.thenCompose(v -> stopAndDestroyPartition(zonePartitionId, revision));
        } else {
            return clientUpdateFuture;
        }
    }

    private CompletableFuture<Void> stopAndDestroyPartition(ZonePartitionId zonePartitionId, long revision) {
        return weakStopPartition(zonePartitionId, revision);
    }

    private CompletableFuture<Void> handleChangePendingAssignmentEvent(Entry pendingAssignmentsEntry, long revision) {
        if (pendingAssignmentsEntry.value() == null || pendingAssignmentsEntry.empty()) {
            return nullCompletedFuture();
        }

        ZonePartitionId zonePartitionId = extractZonePartitionId(pendingAssignmentsEntry.key(), PENDING_ASSIGNMENTS_PREFIX_BYTES);

        // Stable assignments from the meta store, which revision is bounded by the current pending event.
        Assignments stableAssignments = stableAssignments(zonePartitionId, revision);

        Assignments pendingAssignments = Assignments.fromBytes(pendingAssignmentsEntry.value());

        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            if (LOG.isInfoEnabled()) {
                var stringKey = new String(pendingAssignmentsEntry.key(), UTF_8);

                LOG.info(
                        "Received update on pending assignments. Check if new replication node should be started [key={}, "
                                + "partition={}, zoneId={}, localMemberAddress={}, pendingAssignments={}, revision={}]",
                        stringKey,
                        zonePartitionId.partitionId(),
                        zonePartitionId.zoneId(),
                        localNode().address(),
                        pendingAssignments,
                        revision
                );
            }

            return handleChangePendingAssignmentEvent(
                    zonePartitionId,
                    stableAssignments,
                    pendingAssignments,
                    revision
            ).thenCompose(v -> {
                boolean isLocalNodeInStableOrPending = isNodeInReducedStableOrPendingAssignments(
                        zonePartitionId,
                        stableAssignments,
                        pendingAssignments,
                        revision
                );

                if (!isLocalNodeInStableOrPending) {
                    return nullCompletedFuture();
                }

                return changePeersOnRebalance(
                        replicaMgr,
                        zonePartitionId,
                        pendingAssignments.nodes(),
                        revision);
            });
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
            computedStableAssignments = Assignments.forced(pendingAssignmentsNodes, pendingAssignments.timestamp());
        } else if (pendingAssignmentsAreForced) {
            // In case of forced assignments we need to remove nodes that are present in the stable set but are missing from the
            // pending set. Such operation removes dead stable nodes from the resulting stable set, which guarantees that we will
            // have a live majority.
            computedStableAssignments = pendingAssignments;
        } else {
            computedStableAssignments = stableAssignments;
        }

        CompletableFuture<?> localServicesStartFuture;

        if (shouldStartLocalGroupNode) {
            localServicesStartFuture = createZonePartitionReplicationNode(
                    replicaGrpId,
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

        return localServicesStartFuture
                .thenComposeAsync(v -> inBusyLock(busyLock, () -> isLocalNodeIsPrimary(replicaGrpId)), ioExecutor)
                .thenAcceptAsync(isLeaseholder -> inBusyLock(busyLock, () -> {
                    boolean isLocalNodeInStableOrPending = isNodeInReducedStableOrPendingAssignments(
                            replicaGrpId,
                            stableAssignments,
                            pendingAssignments,
                            revision
                    );

                    if (!isLocalNodeInStableOrPending && !isLeaseholder) {
                        return;
                    }

                    assert isLocalNodeInStableOrPending || isLeaseholder
                            : "The local node is outside of the replication group [inStableOrPending=" + isLocalNodeInStableOrPending
                            + ", isLeaseholder=" + isLeaseholder + "].";

                    // For forced assignments, we exclude dead stable nodes, and all alive stable nodes are already in pending assignments.
                    // Union is not required in such a case.
                    Set<Assignment> newAssignments = pendingAssignmentsAreForced || stableAssignments == null
                            ? pendingAssignmentsNodes
                            : union(pendingAssignmentsNodes, stableAssignments.nodes());

                    replicaMgr.replica(replicaGrpId)
                            .thenAccept(replica -> replica.updatePeersAndLearners(fromAssignments(newAssignments)));
                }), ioExecutor);
    }

    private CompletableFuture<Void> changePeersOnRebalance(
            ReplicaManager replicaMgr,
            ZonePartitionId replicaGrpId,
            Set<Assignment> pendingAssignments,
            long revision
    ) {
        return replicaMgr.replica(replicaGrpId)
                .thenApply(Replica::raftClient)
                .thenCompose(raftClient -> raftClient.refreshAndGetLeaderWithTerm()
                        .exceptionally(throwable -> {
                            throwable = unwrapCause(throwable);

                            if (throwable instanceof TimeoutException) {
                                LOG.info("Node couldn't get the leader within timeout so the changing peers is skipped [grp={}].",
                                        replicaGrpId);

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
                                    leaderWithTerm.leader(), replicaGrpId, replicaGrpId.partitionId(), replicaGrpId.zoneId());

                            return metaStorageMgr.get(pendingPartAssignmentsKey(replicaGrpId))
                                    .thenCompose(latestPendingAssignmentsEntry -> {
                                        // Do not change peers of the raft group if this is a stale event.
                                        // Note that we start raft node before for the sake of the consistency in a
                                        // starting and stopping raft nodes.
                                        if (revision < latestPendingAssignmentsEntry.revision()) {
                                            return nullCompletedFuture();
                                        }

                                        PeersAndLearners newConfiguration = fromAssignments(pendingAssignments);

                                        return raftClient.changePeersAndLearnersAsync(newConfiguration, leaderWithTerm.term())
                                                .exceptionally(e -> null);
                                    });
                        }));
    }

    private boolean isLocalPeer(Peer peer) {
        return peer.consistentId().equals(localNode().name());
    }

    private boolean isLocalNodeInAssignments(Collection<Assignment> assignments) {
        return assignments.stream().anyMatch(isLocalNodeAssignment);
    }

    private CompletableFuture<Void> waitForMetadataCompleteness(long ts) {
        return schemaSyncService.waitForMetadataCompleteness(HybridTimestamp.hybridTimestamp(ts));
    }

    /**
     * Checks that the local node is primary or not.
     * <br>
     * Internally we use there {@link PlacementDriver#getPrimaryReplica} with a penultimate safe time value, because metastore is waiting
     * for pending or stable assignments events handling over and only then metastore will increment the safe time. On the other hand
     * placement driver internally is waiting the metastore for given safe time plus {@link ClockService#maxClockSkewMillis}. So, if given
     * time is just {@link ClockService#now}, then there is a dead lock: metastore is waiting until assignments handling is over, but
     * internally placement driver is waiting for a non-applied safe time.
     * <br>
     * To solve this issue we pass to {@link PlacementDriver#getPrimaryReplica} current time minus the skew, so placement driver could
     * successfully get primary replica for the time stamp before the handling has began. Also there a corner case for tests that are using
     * {@code WatchListenerInhibitor#metastorageEventsInhibitor} and it leads to current time equals {@link HybridTimestamp#MIN_VALUE} and
     * the skew's subtraction will lead to {@link IllegalArgumentException} from {@link HybridTimestamp}. Then, if we got the minimal
     * possible timestamp, then we also couldn't have any primary replica, then return {@code false}.
     *
     * @param replicationGroupId Replication group ID for that we check is the local node a primary.
     * @return {@code true} is the local node is primary and {@code false} otherwise.
     */
    private CompletableFuture<Boolean> isLocalNodeIsPrimary(ReplicationGroupId replicationGroupId) {
        HybridTimestamp currentSafeTime = metaStorageMgr.clusterTime().currentSafeTime();

        if (HybridTimestamp.MIN_VALUE.equals(currentSafeTime)) {
            return falseCompletedFuture();
        }

        long skewMs = clockService.maxClockSkewMillis();

        try {
            HybridTimestamp previousMetastoreSafeTime = currentSafeTime.subtractPhysicalTime(skewMs);

            return placementDriver.getPrimaryReplica(replicationGroupId, previousMetastoreSafeTime)
                    .thenApply(replicaMeta -> replicaMeta != null
                            && replicaMeta.getLeaseholderId() != null
                            && replicaMeta.getLeaseholderId().equals(localNode().id()));
        } catch (IllegalArgumentException e) {
            long currentSafeTimeMs = currentSafeTime.longValue();

            throw new AssertionError("Got a negative time [currentSafeTime=" + currentSafeTime
                    + ", currentSafeTimeMs=" + currentSafeTimeMs
                    + ", skewMs=" + skewMs
                    + ", internal=" + (currentSafeTimeMs + ((-skewMs) << LOGICAL_TIME_BITS_SIZE)) + "]", e);
        }
    }

    private boolean isNodeInReducedStableOrPendingAssignments(
            ZonePartitionId replicaGrpId,
            @Nullable Assignments stableAssignments,
            Assignments pendingAssignments,
            long revision
    ) {
        Entry reduceEntry = metaStorageMgr.getLocally(ZoneRebalanceUtil.switchReduceKey(replicaGrpId), revision);

        Assignments reduceAssignments = reduceEntry != null
                ? Assignments.fromBytes(reduceEntry.value())
                : null;

        Set<Assignment> reducedStableAssignments = reduceAssignments != null
                ? subtract(stableAssignments.nodes(), reduceAssignments.nodes())
                : stableAssignments.nodes();

        if (!isLocalNodeInAssignments(union(reducedStableAssignments, pendingAssignments.nodes()))) {
            return false;
        }

        assert replicaMgr.isReplicaStarted(replicaGrpId) : "The local node is outside of the replication group ["
                + ", stable=" + stableAssignments
                + ", pending=" + pendingAssignments
                + ", reduce=" + reduceAssignments
                + ", localName=" + localNode().name() + "].";

        return true;
    }

    @Nullable
    private Assignment localMemberAssignment(Assignments assignments) {
        Assignment localMemberAssignment = Assignment.forPeer(localNode().name());

        return assignments.nodes().contains(localMemberAssignment) ? localMemberAssignment : null;
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!enabledColocationFeature) {
            return nullCompletedFuture();
        }

        try {
            IgniteUtils.closeAllManually(zoneResourcesManager);
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }

    private @Nullable Assignments stableAssignments(ZonePartitionId zonePartitionId, long revision) {
        Entry entry = metaStorageMgr.getLocally(stablePartAssignmentsKey(zonePartitionId), revision);

        return Assignments.fromBytes(entry.value());
    }

    private CompletableFuture<Void> weakStopPartition(ZonePartitionId zonePartitionId, long revision) {
        return replicaMgr.weakStopReplica(
                zonePartitionId,
                WeakReplicaStopReason.EXCLUDED_FROM_ASSIGNMENTS,
                () -> stopPartition(zonePartitionId, revision)
        );
    }

    /**
     * Stops all resources associated with a given partition, like replicas and partition trackers.
     *
     * @param zonePartitionId Partition ID.
     * @return Future that will be completed after all resources have been closed.
     */
    private CompletableFuture<Void> stopPartition(ZonePartitionId zonePartitionId, long revision) {
        return executeUnderZoneWriteLock(zonePartitionId.zoneId(), () -> {
            try {
                return replicaMgr.stopReplica(zonePartitionId)
                        .thenCompose((replicaWasStopped) -> {
                            if (replicaWasStopped) {
                                zonePartitionRaftListeners.remove(zonePartitionId);
                                replicationGroupIds.remove(zonePartitionId);

                                return fireEvent(
                                        LocalPartitionReplicaEvent.AFTER_REPLICA_STOPPED,
                                        new LocalPartitionReplicaEventParameters(zonePartitionId, revision)
                                );
                            } else {
                                return nullCompletedFuture();
                            }
                        });
            } catch (NodeStoppingException e) {
                // No-op.
                return nullCompletedFuture();
            }
        });
    }

    /**
     * Stops resources that are related to provided zone partitions.
     *
     * @param partitionIds Partitions to stop.
     */
    private void cleanUpPartitionsResources(Set<ZonePartitionId> partitionIds) {
        // TODO: Due to IGNITE-23741 we shouldn't destroy partitions on node stop thus the revision will be removed.
        long revision = catalogMgr.latestCatalogVersion();

        CompletableFuture<?>[] stopPartitionsFuture = partitionIds.stream()
                .map(partId -> stopPartition(partId, revision))
                .toArray(CompletableFuture[]::new);

        try {
            allOf(stopPartitionsFuture).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Unable to clean up zones resources", e);
        }
    }

    /**
     * Lock the zones replica list for any changes. {@link #hasLocalPartition(ZonePartitionId)} must be executed under this lock always.
     *
     * @param zoneId Zone id.
     * @return Stamp, which must be used for further unlock.
     */
    public long lockZoneForRead(int zoneId) {
        return zonePartitionsLocks.computeIfAbsent(zoneId, id -> new StampedLock()).readLock();
    }

    /**
     * Unlock zones replica list.
     *
     * @param zoneId Zone id.
     * @param stamp Stamp, produced by the according {@link #hasLocalPartition(ZonePartitionId) call.}
     */
    public void unlockZoneForRead(int zoneId, long stamp) {
        zonePartitionsLocks.get(zoneId).unlockRead(stamp);
    }

    /**
     * Load a new table partition listener to the zone replica.
     *
     * <p>Important: This method must be called only with the guarantee, that the replica exists at the current moment.
     *
     * @param zonePartitionId Zone partition id.
     * @param tablePartitionId Table partition id.
     * @param tablePartitionReplicaListenerFactory Factory for creating table-specific partition replicas.
     * @param tablePartitionRaftListener Raft group listener for the table-specific partition.
     */
    public void loadTableListenerToZoneReplica(
            ZonePartitionId zonePartitionId,
            TablePartitionId tablePartitionId,
            Function<RaftCommandRunner, ReplicaListener> tablePartitionReplicaListenerFactory,
            RaftGroupListener tablePartitionRaftListener
    ) {
        CompletableFuture<Replica> replicaFut = replicaMgr.replica(zonePartitionId);

        assert replicaFut != null && replicaFut.isDone();

        var zonePartitionReplicaListener = (ZonePartitionReplicaListener) replicaFut.join().listener();

        zonePartitionReplicaListener.addTableReplicaListener(tablePartitionId, tablePartitionReplicaListenerFactory);

        zonePartitionRaftListeners.get(zonePartitionId).addTablePartitionRaftListener(tablePartitionId, tablePartitionRaftListener);
    }

    private CompletableFuture<Void> executeUnderZoneWriteLock(int zoneId, Supplier<CompletableFuture<Void>> action) {
        StampedLock lock = zonePartitionsLocks.computeIfAbsent(zoneId, id -> new StampedLock());

        long stamp = lock.writeLock();

        try {
            return action.get()
                    .whenComplete((v, e) -> lock.unlockWrite(stamp));
        } catch (Throwable e) {
            lock.unlockWrite(stamp);

            return failedFuture(e);
        }
    }
}
