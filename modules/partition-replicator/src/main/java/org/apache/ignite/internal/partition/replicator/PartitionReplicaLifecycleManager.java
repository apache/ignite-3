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
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_DROP;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.subtract;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.union;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.assignmentsChainKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.extractZonePartitionId;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.plannedPartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.switchAppendKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.switchReduceKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zoneAssignmentsChainGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zoneAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zonePartitionAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zonePendingAssignmentsGetLocally;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zoneStableAssignmentsGetLocally;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.hlc.HybridTimestamp.LOGICAL_TIME_BITS_SIZE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.AFTER_REPLICA_DESTROYED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.AFTER_REPLICA_STOPPED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.BEFORE_REPLICA_DESTROYED;
import static org.apache.ignite.internal.partition.replicator.LocalPartitionReplicaEvent.BEFORE_REPLICA_STOPPED;
import static org.apache.ignite.internal.partitiondistribution.Assignments.assignmentListToString;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignmentForPartition;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignments;
import static org.apache.ignite.internal.raft.PeersAndLearners.fromAssignments;
import static org.apache.ignite.internal.raft.RaftGroupConfiguration.UNKNOWN_INDEX;
import static org.apache.ignite.internal.raft.RaftGroupConfiguration.UNKNOWN_TERM;
import static org.apache.ignite.internal.tostring.IgniteToStringBuilder.COLLECTION_LIMIT;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.utils.SystemDistributedConfigurationPropertyHolder;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceRaftGroupEventsListener;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.ComponentStoppingException;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.ZoneResourcesManager.ZonePartitionResources;
import org.apache.ignite.internal.partition.replicator.raft.RaftTableProcessor;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorageFactory;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.schema.CatalogValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schema.ExecutorInclinedSchemaSyncService;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsChain;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.placementdriver.wrappers.ExecutorInclinedPlacementDriver;
import org.apache.ignite.internal.raft.ExecutorInclinedRaftCommandRunner;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.rebalance.ChangePeersAndLearnersWithRetry;
import org.apache.ignite.internal.raft.rebalance.RaftStaleUpdateException;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaManager.WeakReplicaStopReason;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.thread.ThreadUtils;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageRebalanceException;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.LongPriorityQueue;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.TrackerClosedException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

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
    private final CatalogService catalogService;

    private final ReplicaManager replicaMgr;

    private final DistributionZoneManager distributionZoneMgr;

    private final MetaStorageManager metaStorageMgr;

    private final TopologyService topologyService;

    private final LowWatermark lowWatermark;

    private final FailureProcessor failureProcessor;

    private final NodeProperties nodeProperties;

    /** Meta storage listener for pending assignments. */
    private final WatchListener pendingAssignmentsRebalanceListener;

    /** Meta storage listener for stable assignments. */
    private final WatchListener stableAssignmentsRebalanceListener;

    /** Meta storage listener for switch reduce assignments. */
    private final WatchListener assignmentsSwitchRebalanceListener;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PartitionReplicaLifecycleManager.class);

    /** Tracks starting and started replicas. */
    private final StartedReplicationGroups startedReplicationGroups = new StartedReplicationGroups();

    // TODO https://issues.apache.org/jira/browse/IGNITE-25347
    /** (zoneId -> lock) map to provide concurrent access to the zone replicas list. */
    private final Map<Integer, NaiveAsyncReadWriteLock> zonePartitionsLocks = new ConcurrentHashMap<>();

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
    private final PlacementDriver executorInclinedPlacementDriver;

    /** Schema sync service for waiting for catalog metadata. */
    private final SchemaSyncService executorInclinedSchemaSyncService;

    private final TxManager txManager;

    private final SchemaManager schemaManager;

    /** A predicate that checks that the given assignment is corresponded to the local node. */
    private final Predicate<Assignment> isLocalNodeAssignment = assignment -> assignment.consistentId().equals(localNode().name());

    /** Configuration of rebalance retries delay. */
    private final SystemDistributedConfigurationPropertyHolder<Integer> rebalanceRetryDelayConfiguration;

    private final DataStorageManager dataStorageManager;

    private final ZoneResourcesManager zoneResourcesManager;

    private final MetricManager metricManager;

    private final ReliableCatalogVersions reliableCatalogVersions;

    private final EventListener<CreateZoneEventParameters> onCreateZoneListener = this::onCreateZone;
    private final EventListener<PrimaryReplicaEventParameters> onPrimaryReplicaExpiredListener = this::onPrimaryReplicaExpired;
    private final EventListener<DropZoneEventParameters> onZoneDropListener = fromConsumer(this::onZoneDrop);

    private final EventListener<ChangeLowWatermarkEventParameters> onLowWatermarkChangedListener = this::onLwmChanged;

    /**
     * This future completes on {@link #beforeNodeStop()} with {@link NodeStoppingException} before the {@link #busyLock} is blocked.
     */
    private final CompletableFuture<Void> stopReplicaLifecycleFuture = new CompletableFuture<>();

    private final LongPriorityQueue<DestroyZoneEvent> destructionEventsQueue =
            new LongPriorityQueue<>(DestroyZoneEvent::catalogVersion);

    /**
     * The constructor.
     *
     * @param catalogService Catalog service.
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
     * @param txManager Transaction manager.
     * @param schemaManager Schema manager.
     * @param dataStorageManager Data storage manager.
     * @param outgoingSnapshotsManager Outgoing snapshots manager.
     * @param metricManager Metric manager.
     */
    public PartitionReplicaLifecycleManager(
            CatalogService catalogService,
            ReplicaManager replicaMgr,
            DistributionZoneManager distributionZoneMgr,
            MetaStorageManager metaStorageMgr,
            TopologyService topologyService,
            LowWatermark lowWatermark,
            FailureProcessor failureProcessor,
            NodeProperties nodeProperties,
            ExecutorService ioExecutor,
            ScheduledExecutorService rebalanceScheduler,
            Executor partitionOperationsExecutor,
            ClockService clockService,
            PlacementDriver placementDriver,
            SchemaSyncService schemaSyncService,
            SystemDistributedConfiguration systemDistributedConfiguration,
            TxStateRocksDbSharedStorage sharedTxStateStorage,
            TxManager txManager,
            SchemaManager schemaManager,
            DataStorageManager dataStorageManager,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            MetricManager metricManager
    ) {
        this(
                catalogService,
                replicaMgr,
                distributionZoneMgr,
                metaStorageMgr,
                topologyService,
                lowWatermark,
                failureProcessor,
                nodeProperties,
                ioExecutor,
                rebalanceScheduler,
                partitionOperationsExecutor,
                clockService,
                placementDriver,
                schemaSyncService,
                systemDistributedConfiguration,
                txManager,
                schemaManager,
                dataStorageManager,
                new ZoneResourcesManager(
                        sharedTxStateStorage,
                        txManager,
                        outgoingSnapshotsManager,
                        topologyService,
                        catalogService,
                        failureProcessor,
                        partitionOperationsExecutor,
                        replicaMgr
                ),
                metricManager
        );
    }

    @VisibleForTesting
    PartitionReplicaLifecycleManager(
            CatalogService catalogService,
            ReplicaManager replicaMgr,
            DistributionZoneManager distributionZoneMgr,
            MetaStorageManager metaStorageMgr,
            TopologyService topologyService,
            LowWatermark lowWatermark,
            FailureProcessor failureProcessor,
            NodeProperties nodeProperties,
            ExecutorService ioExecutor,
            ScheduledExecutorService rebalanceScheduler,
            Executor partitionOperationsExecutor,
            ClockService clockService,
            PlacementDriver placementDriver,
            SchemaSyncService schemaSyncService,
            SystemDistributedConfiguration systemDistributedConfiguration,
            TxManager txManager,
            SchemaManager schemaManager,
            DataStorageManager dataStorageManager,
            ZoneResourcesManager zoneResourcesManager,
            MetricManager metricManager
    ) {
        this.catalogService = catalogService;
        this.replicaMgr = replicaMgr;
        this.distributionZoneMgr = distributionZoneMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.topologyService = topologyService;
        this.lowWatermark = lowWatermark;
        this.failureProcessor = failureProcessor;
        this.nodeProperties = nodeProperties;
        this.ioExecutor = ioExecutor;
        this.rebalanceScheduler = rebalanceScheduler;
        this.partitionOperationsExecutor = partitionOperationsExecutor;
        this.clockService = clockService;
        this.executorInclinedSchemaSyncService = new ExecutorInclinedSchemaSyncService(schemaSyncService, partitionOperationsExecutor);
        this.executorInclinedPlacementDriver = new ExecutorInclinedPlacementDriver(placementDriver, partitionOperationsExecutor);
        this.txManager = txManager;
        this.schemaManager = schemaManager;
        this.dataStorageManager = dataStorageManager;
        this.zoneResourcesManager = zoneResourcesManager;
        this.metricManager = metricManager;

        rebalanceRetryDelayConfiguration = new SystemDistributedConfigurationPropertyHolder<>(
                systemDistributedConfiguration,
                (v, r) -> {},
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_MS,
                DistributionZonesUtil.REBALANCE_RETRY_DELAY_DEFAULT,
                Integer::parseInt
        );

        pendingAssignmentsRebalanceListener = createPendingAssignmentsRebalanceListener();
        stableAssignmentsRebalanceListener = createStableAssignmentsRebalanceListener();
        assignmentsSwitchRebalanceListener = createAssignmentsSwitchRebalanceListener();

        reliableCatalogVersions = new ReliableCatalogVersions(schemaSyncService, catalogService);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        if (!nodeProperties.colocationEnabled()) {
            return nullCompletedFuture();
        }

        CompletableFuture<Revisions> recoveryFinishFuture = metaStorageMgr.recoveryFinishedFuture();
        assert recoveryFinishFuture.isDone();
        long recoveryRevision = recoveryFinishFuture.join().revision();

        handleResourcesForDroppedZonesOnRecovery();

        CompletableFuture<Void> processZonesAndAssignmentsOnStart = processZonesOnStart(recoveryRevision, lowWatermark.getLowWatermark())
                .thenCompose(ignored -> processAssignmentsOnRecovery(recoveryRevision));

        metaStorageMgr.registerPrefixWatch(new ByteArray(PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES), pendingAssignmentsRebalanceListener);
        metaStorageMgr.registerPrefixWatch(new ByteArray(STABLE_ASSIGNMENTS_PREFIX_BYTES), stableAssignmentsRebalanceListener);
        metaStorageMgr.registerPrefixWatch(new ByteArray(ASSIGNMENTS_SWITCH_REDUCE_PREFIX_BYTES), assignmentsSwitchRebalanceListener);

        catalogService.listen(ZONE_CREATE, onCreateZoneListener);
        catalogService.listen(ZONE_DROP, onZoneDropListener);

        lowWatermark.listen(LowWatermarkEvent.LOW_WATERMARK_CHANGED, onLowWatermarkChangedListener);

        rebalanceRetryDelayConfiguration.init();

        executorInclinedPlacementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, onPrimaryReplicaExpiredListener);

        metricManager.registerSource(zoneResourcesManager.snapshotsMetricsSource());
        metricManager.enable(zoneResourcesManager.snapshotsMetricsSource());

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
        int earliestCatalogVersion = lwm == null ? catalogService.earliestCatalogVersion()
                : catalogService.activeCatalogVersion(lwm.longValue());

        int latestCatalogVersion = catalogService.latestCatalogVersion();

        var startedZones = new IntOpenHashSet();
        var startZoneFutures = new ArrayList<CompletableFuture<?>>();

        Catalog nextCatalog = null;

        for (int ver = latestCatalogVersion; ver >= earliestCatalogVersion; ver--) {
            Catalog catalog = catalogService.catalog(ver);
            Catalog nextCatalog0 = nextCatalog;

            int ver0 = ver;
            catalogService.catalog(ver).zones().stream()
                    .filter(zone -> startedZones.add(zone.id()))
                    .forEach(zoneDescriptor -> {
                        // Handle missed zone drop event.
                        int zoneId = zoneDescriptor.id();

                        startZoneFutures.add(
                                calculateZoneAssignmentsAndCreateReplicationNodes(recoveryRevision, ver0, zoneDescriptor, true));

                        if (nextCatalog0 != null && nextCatalog0.zone(zoneId) == null) {
                            destructionEventsQueue.enqueue(
                                    new DestroyZoneEvent(
                                            nextCatalog0.version(),
                                            zoneId,
                                            zoneDescriptor.partitions()
                                    )
                            );
                        }
                    });

            nextCatalog = catalog;
        }

        return allOf(startZoneFutures.toArray(CompletableFuture[]::new))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null && !hasCause(throwable, NodeStoppingException.class)) {
                        failureProcessor.process(new FailureContext(throwable, "Error starting zones"));
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
        return recoverStableAssignments(recoveryRevision).thenCompose(v -> recoverPendingAssignments(recoveryRevision));
    }

    private CompletableFuture<Void> recoverStableAssignments(long recoveryRevision) {
        return handleAssignmentsOnRecovery(
                new ByteArray(STABLE_ASSIGNMENTS_PREFIX_BYTES),
                recoveryRevision,
                (entry, rev) -> handleChangeStableAssignmentEvent(entry, rev, true),
                "stable"
        );
    }

    private CompletableFuture<Void> recoverPendingAssignments(long recoveryRevision) {
        return handleAssignmentsOnRecovery(
                new ByteArray(PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES),
                recoveryRevision,
                (pendingAssignmentsEntry, revision) -> handleChangePendingAssignmentEvent(pendingAssignmentsEntry, revision, true),
                "pending"
        );
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
                        if (e != null && !hasCause(e, NodeStoppingException.class)) {
                            failureProcessor.process(new FailureContext(e, "Error when performing assignments recovery"));
                        }
                    });
        }
    }

    private void handleResourcesForDroppedZonesOnRecovery() {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-27466 Handle abandoned resources for dropped zones from vault and metastore
    }

    private CompletableFuture<Boolean> onCreateZone(CreateZoneEventParameters createZoneEventParameters) {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-22535 start replica must be moved from metastore thread
        return inBusyLock(busyLock, () -> {
            return calculateZoneAssignmentsAndCreateReplicationNodes(
                    createZoneEventParameters.causalityToken(),
                    createZoneEventParameters.catalogVersion(),
                    createZoneEventParameters.zoneDescriptor(),
                    false
            ).thenApply(unused -> false);
        });
    }

    private CompletableFuture<Void> calculateZoneAssignmentsAndCreateReplicationNodes(
            long causalityToken,
            int catalogVersion,
            CatalogZoneDescriptor zoneDescriptor,
            boolean onNodeRecovery
    ) {
        return inBusyLockAsync(busyLock, () -> {
            int zoneId = zoneDescriptor.id();

            return getOrCreateAssignments(zoneDescriptor, causalityToken, catalogVersion)
                    .thenCompose(
                            stableAssignments -> createZoneReplicationNodes(
                                    zoneId,
                                    stableAssignments,
                                    zonePendingAssignmentsGetLocally(metaStorageMgr, zoneId, zoneDescriptor.partitions(), causalityToken),
                                    zoneAssignmentsChainGetLocally(metaStorageMgr, zoneId, zoneDescriptor.partitions(), causalityToken),
                                    causalityToken,
                                    catalogVersion,
                                    zoneDescriptor.partitions(),
                                    onNodeRecovery
                            )
                    );
        });
    }

    private CompletableFuture<Void> createZoneReplicationNodes(
            int zoneId,
            List<Assignments> stableAssignmentsForZone,
            List<@Nullable Assignments> pendingAssignmentsForZone,
            List<AssignmentsChain> assignmentsChains,
            long causalityToken,
            int catalogVersion,
            int partitionCount,
            boolean onNodeRecovery
    ) {
        assert stableAssignmentsForZone != null : format("Zone has empty assignments [id={}].", zoneId);

        Supplier<CompletableFuture<Void>> createZoneReplicationNodes = () -> inBusyLockAsync(busyLock, () -> {
            var partitionsStartFutures = new CompletableFuture<?>[stableAssignmentsForZone.size()];

            for (int partId = 0; partId < stableAssignmentsForZone.size(); partId++) {
                Assignments stableAssignments = stableAssignmentsForZone.get(partId);

                Assignments pendingAssignments = pendingAssignmentsForZone.get(partId);

                Assignment localAssignmentInStable = localAssignment(stableAssignments);

                AssignmentsChain assignmentsChain = assignmentsChains.get(partId);

                boolean shouldStartPartition;

                if (onNodeRecovery) {
                    if (lastRebalanceWasGraceful(assignmentsChain)) {
                        // The condition to start the replica is
                        // `pending.contains(node) || (stable.contains(node) && !pending.isForce())`.
                        // However we check only the right part of this condition here
                        // since after `startTables` we have a call to `processAssignmentsOnRecovery`,
                        // which executes pending assignments update and will start required partitions there.
                        shouldStartPartition = localAssignmentInStable != null
                                && (pendingAssignments == null || !pendingAssignments.force());
                    } else {
                        // TODO: Use logic from https://issues.apache.org/jira/browse/IGNITE-23874
                        LOG.warn("Recovery after a forced rebalance for zone is not supported yet [zoneId={}, partitionId={}].",
                                zoneId, partId);
                        shouldStartPartition = localAssignmentInStable != null
                                && (pendingAssignments == null || !pendingAssignments.force());
                    }
                } else {
                    shouldStartPartition = localAssignmentInStable != null;
                }

                if (shouldStartPartition) {
                    var zonePartitionId = new ZonePartitionId(zoneId, partId);

                    partitionsStartFutures[partId] = createZonePartitionReplicationNode(
                            zonePartitionId,
                            stableAssignments,
                            causalityToken,
                            partitionCount,
                            isVolatileZoneForCatalogVersion(zoneId, catalogVersion),
                            onNodeRecovery,
                            !onNodeRecovery
                    );
                } else {
                    partitionsStartFutures[partId] = nullCompletedFuture();
                }

            }

            return allOf(partitionsStartFutures);
        });

        // Zone nodes might be created in the same catalog version as a table in this zone, so there could be a race
        // between storage creation due to replica start and storage creation due to table addition. To eliminate the race,
        // we acquire a write lock on the zone (table addition acquires a read lock).

        // The mutual exclusion mechanism described above is used during normal functioning (i.e. not during node recovery).
        // On node recovery, there is another mechanism that guarantees mutual exclusion between replica starts and table additions:
        // namely, TableManager makes sure that it starts all tables before it allows the replicas to initiate their starts.
        // That's why during node recovery we DON'T acquire write locks for zones here: that would create a deadlock with the mechanism
        // in TableManager.
        if (onNodeRecovery) {
            return createZoneReplicationNodes.get();
        } else {
            return inBusyLockAsync(busyLock, () -> executeUnderZoneWriteLock(zoneId, createZoneReplicationNodes));
        }
    }

    private boolean isVolatileZoneForCatalogVersion(int zoneId, int catalogVersion) {
        CatalogZoneDescriptor zoneDescriptor = catalogService.catalog(catalogVersion).zone(zoneId);

        assert zoneDescriptor != null;

        return isVolatileZone(zoneDescriptor);
    }

    private boolean isVolatileZone(CatalogZoneDescriptor zoneDescriptor) {
        return zoneDescriptor.storageProfiles()
                .profiles()
                .stream()
                .map(CatalogStorageProfileDescriptor::storageProfile)
                .allMatch(storageProfile -> {
                    StorageEngine storageEngine = dataStorageManager.engineByStorageProfile(storageProfile);

                    return storageEngine != null && storageEngine.isVolatile();
                });
    }

    /**
     * Start a replica for the corresponding {@code zoneId} and {@code partId} on the local node if {@code localAssignment} is not null,
     * meaning that the local node is part of the assignment.
     *
     * @param zonePartitionId Zone Partition ID.
     * @param stableAssignments Stable assignments.
     * @param revision Event's revision.
     * @param partitionCount Number of partitions on the zone.
     * @param onRecovery Flag indicating whether this replication node is started on node recovery.
     * @return Future that completes when a replica is started.
     */
    private CompletableFuture<?> createZonePartitionReplicationNode(
            ZonePartitionId zonePartitionId,
            Assignments stableAssignments,
            long revision,
            int partitionCount,
            boolean isVolatileZone,
            boolean onRecovery,
            boolean holdingZoneWriteLock
    ) {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 We need to integrate PartitionReplicatorNodeRecovery logic here when
        //  we in the recovery phase.
        Assignments forcedAssignments = stableAssignments.force() ? stableAssignments : null;

        PeersAndLearners stablePeersAndLearners = fromAssignments(stableAssignments.nodes());

        RaftGroupEventsListener raftGroupEventsListener = createRaftGroupEventsListener(zonePartitionId);

        Supplier<CompletableFuture<Boolean>> startReplicaSupplier = () -> {
            var storageIndexTracker = new PendingComparableValuesTracker<Long, Void>(0L);

            ZonePartitionResources zoneResources = zoneResourcesManager.allocateZonePartitionResources(
                    zonePartitionId,
                    partitionCount,
                    storageIndexTracker
            );

            var eventParams = new LocalBeforeReplicaStartEventParameters(
                    zonePartitionId,
                    revision,
                    onRecovery,
                    zoneResources,
                    zoneResources.txStatePartitionStorageIsInRebalanceState()
            );

            startedReplicationGroups.beforeStartingGroup(zonePartitionId);

            return fireEvent(LocalPartitionReplicaEvent.BEFORE_REPLICA_STARTED, eventParams)
                    .thenCompose(v -> {
                        if (eventParams.anyStorageIsInRebalanceState()) {
                            // We must destroy protocol storages first. If we do so, then, as MV and TX state storages sync Raft log
                            // before being flushed, there is a guarantee that, after a possible crash, we will either see some storage
                            // still in the rebalance state (and hence we'll repeat the destruction on the next start), or the Raft log
                            // destruction will be persisted (and we'll just recover normally).
                            try {
                                replicaMgr.destroyReplicationProtocolStorages(zonePartitionId, isVolatileZone);
                            } catch (NodeStoppingException e) {
                                return failedFuture(e);
                            }

                            CompletableFuture<Void> clearTxStateStorage = zoneResources.txStatePartitionStorage().clear();

                            CompletableFuture<?>[] registeredCleanupFutures = eventParams.cleanupActions().stream()
                                    .map(Supplier::get)
                                    .toArray(CompletableFuture[]::new);
                            CompletableFuture<Void> clearMvStorages = allOf(registeredCleanupFutures);

                            return allOf(clearTxStateStorage, clearMvStorages);
                        } else {
                            return nullCompletedFuture();
                        }
                    })
                    .thenCompose(v -> {
                        try {
                            return replicaMgr.startReplica(
                                    zonePartitionId,
                                    raftClient -> {
                                        var replicaListener = new ZonePartitionReplicaListener(
                                                zoneResources.txStatePartitionStorage(),
                                                clockService,
                                                txManager,
                                                new CatalogValidationSchemasSource(catalogService, schemaManager),
                                                executorInclinedSchemaSyncService,
                                                catalogService,
                                                executorInclinedPlacementDriver,
                                                topologyService,
                                                new ExecutorInclinedRaftCommandRunner(raftClient, partitionOperationsExecutor),
                                                failureProcessor,
                                                topologyService.localMember(),
                                                zonePartitionId
                                        );

                                        zoneResources.replicaListenerFuture().complete(replicaListener);

                                        return replicaListener;
                                    },
                                    new PartitionSnapshotStorageFactory(zoneResources.snapshotStorage()),
                                    stablePeersAndLearners,
                                    zoneResources.raftListener(),
                                    raftGroupEventsListener,
                                    isVolatileZone,
                                    busyLock,
                                    storageIndexTracker
                            );
                        } catch (NodeStoppingException e) {
                            return failedFuture(e);
                        }
                    })
                    .whenComplete((replica, throwable) -> {
                        if (throwable != null) {
                            startedReplicationGroups.startingFailed(zonePartitionId);
                        }
                    })
                    .thenCompose(v -> {
                        Supplier<CompletableFuture<Void>> addReplicationGroupIdFuture = () -> {
                            startedReplicationGroups.startingCompleted(zonePartitionId);

                            return nullCompletedFuture();
                        };

                        if (holdingZoneWriteLock) {
                            return addReplicationGroupIdFuture.get();
                        } else {
                            return executeUnderZoneWriteLock(zonePartitionId.zoneId(), addReplicationGroupIdFuture);
                        }
                    })
                    .thenApply(unused -> true);
        };

        return replicaMgr.weakStartReplica(zonePartitionId, startReplicaSupplier, forcedAssignments, revision)
                .whenComplete((res, ex) -> {
                    if (ex != null && !hasCause(ex, NodeStoppingException.class)) {
                        String errorMessage = String.format(
                                "Unable to update raft groups on the node [zonePartitionId=%s]",
                                zonePartitionId
                        );
                        failureProcessor.process(new FailureContext(ex, errorMessage));
                    }
                });
    }

    private CompletableFuture<Set<Assignment>> calculateZoneAssignments(ZonePartitionId zonePartitionId, Long assignmentsTimestamp) {
        requireNonNull(assignmentsTimestamp, "assignmentsTimestamp must be not null");
        HybridTimestamp assignmentsHybridTimestamp = hybridTimestamp(assignmentsTimestamp);

        CompletableFuture<Set<Assignment>> assignmentsFuture =
                reliableCatalogVersions.safeReliableCatalogFor(assignmentsHybridTimestamp)
                        .thenCompose(catalog -> calculateZoneAssignments(zonePartitionId, catalog, assignmentsHybridTimestamp));

        return CompletableFuture.anyOf(stopReplicaLifecycleFuture, assignmentsFuture).thenApply(a -> (Set<Assignment>) a);
    }

    private CompletableFuture<Set<Assignment>> calculateZoneAssignments(
            ZonePartitionId zonePartitionId,
            Catalog catalog,
            HybridTimestamp assignmentsTimestamp
    ) {
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zonePartitionId.zoneId());

        return distributionZoneMgr.dataNodes(assignmentsTimestamp, catalog.version(), zoneDescriptor.id())
                .thenApply(dataNodes -> calculateAssignmentForPartition(
                        dataNodes,
                        zonePartitionId.partitionId(),
                        zoneDescriptor.partitions(),
                        zoneDescriptor.replicas(),
                        zoneDescriptor.consensusGroupSize()
                ));
    }

    private ChangePeersAndLearnersWithRetry createChangePeersAndLearnersWithRetry(ZonePartitionId replicaGrpId) {
        return new ChangePeersAndLearnersWithRetry(
                busyLock,
                rebalanceScheduler,
                () -> partitionRaftClient(replicaGrpId)
        );
    }

    private CompletableFuture<RaftGroupService> partitionRaftClient(ZonePartitionId replicaGrpId) {
        CompletableFuture<Replica> replicaFut = replicaMgr.replica(replicaGrpId);
        if (replicaFut == null) {
            return failedFuture(new IgniteInternalException("No such replica for partition " + replicaGrpId.partitionId()
                    + " in zone " + replicaGrpId.zoneId()));
        }
        return replicaFut.thenApply(Replica::raftClient);
    }

    private RaftGroupEventsListener createRaftGroupEventsListener(ZonePartitionId zonePartitionId) {
        return new ZoneRebalanceRaftGroupEventsListener(
                metaStorageMgr,
                failureProcessor,
                zonePartitionId,
                busyLock,
                createChangePeersAndLearnersWithRetry(zonePartitionId),
                rebalanceScheduler,
                this::calculateZoneAssignments,
                rebalanceRetryDelayConfiguration
        );
    }

    private InternalClusterNode localNode() {
        return topologyService.localMember();
    }

    @Override
    public void beforeNodeStop() {
        stopReplicaLifecycleFuture.completeExceptionally(new NodeStoppingException());

        busyLock.block();

        executorInclinedPlacementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_EXPIRED, onPrimaryReplicaExpiredListener);

        lowWatermark.removeListener(LowWatermarkEvent.LOW_WATERMARK_CHANGED, onLowWatermarkChangedListener);

        catalogService.removeListener(ZONE_CREATE, onCreateZoneListener);
        catalogService.removeListener(ZONE_DROP, onZoneDropListener);

        metaStorageMgr.unregisterWatch(pendingAssignmentsRebalanceListener);
        metaStorageMgr.unregisterWatch(stableAssignmentsRebalanceListener);
        metaStorageMgr.unregisterWatch(assignmentsSwitchRebalanceListener);

        startedReplicationGroups.waitForStartingReplicas();
        cleanUpPartitionsResources(startedReplicationGroups.streamStartedReplicationGroups());
    }

    /**
     * Writes the set of assignments to meta storage. If there are some assignments already, gets them from meta storage. Returns the list
     * of assignments that really are in meta storage.
     *
     * @param zoneId Zone id.
     * @param newAssignments Assignments that should be written.
     * @return Real list of assignments.
     */
    private CompletableFuture<List<Assignments>> writeZoneAssignmentsToMetastore(
            int zoneId,
            ConsistencyMode consistencyMode,
            List<Assignments> newAssignments
    ) {
        assert !newAssignments.isEmpty();

        boolean haMode = consistencyMode == ConsistencyMode.HIGH_AVAILABILITY;

        List<Operation> partitionAssignments = new ArrayList<>(newAssignments.size());

        for (int i = 0; i < newAssignments.size(); i++) {
            ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, i);

            ByteArray stableAssignmentsKey = stablePartAssignmentsKey(zonePartitionId);
            byte[] anAssignment = newAssignments.get(i).toBytes();
            Operation op = put(stableAssignmentsKey, anAssignment);
            partitionAssignments.add(op);

            if (haMode) {
                ByteArray assignmentsChainKey = assignmentsChainKey(zonePartitionId);
                byte[] assignmentChain = AssignmentsChain.of(UNKNOWN_TERM, UNKNOWN_INDEX, newAssignments.get(i)).toBytes();
                Operation chainOp = put(assignmentsChainKey, assignmentChain);
                partitionAssignments.add(chainOp);
            }
        }

        Condition condition = notExists(new ByteArray(toByteArray(partitionAssignments.get(0).key())));

        return metaStorageMgr
                .invoke(condition, partitionAssignments, Collections.emptyList())
                .whenComplete((invokeResult, e) -> {
                    if (e != null && !hasCause(e, NodeStoppingException.class)) {
                        String errorMessage = String.format(
                                "Couldn't write assignments [assignmentsList=%s] to metastore during invoke.",
                                assignmentListToString(newAssignments)
                        );
                        failureProcessor.process(new FailureContext(e, errorMessage));
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
                                    if (e != null && !hasCause(e, NodeStoppingException.class)) {
                                        String errorMessage = String.format(
                                                "Couldn't get assignments from metastore for zone [zoneId=%s].",
                                                zoneId
                                        );
                                        failureProcessor.process(new FailureContext(e, errorMessage));
                                    }
                                });
                    }
                });
    }

    /**
     * Check if the zone already has assignments in the meta storage locally. So, it means, that it is a recovery process and we should use
     * the meta storage local assignments instead of calculation of the new ones. If assignments do not exist, calculates new assignments
     * and attempts to write them to metastore.
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
            Catalog catalog = catalogService.catalog(catalogVersion);
            long assignmentsTimestamp = catalog.time();

            return distributionZoneMgr.dataNodes(zoneDescriptor.updateTimestamp(), catalogVersion, zoneDescriptor.id())
                    .thenApply(dataNodes -> calculateAssignments(
                                    dataNodes,
                                    zoneDescriptor.partitions(),
                                    zoneDescriptor.replicas(),
                                    zoneDescriptor.consensusGroupSize()
                            )
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
                    }).thenCompose(assignments ->
                            writeZoneAssignmentsToMetastore(zoneDescriptor.id(), zoneDescriptor.consistencyMode(), assignments)
                    );
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
    // TODO: https://issues.apache.org/jira/browse/IGNITE-25107 replace this method by the replicas await process.
    public boolean hasLocalPartition(ZonePartitionId zonePartitionId) {
        assert Optional.ofNullable(zonePartitionsLocks.get(zonePartitionId.zoneId()))
                .map(NaiveAsyncReadWriteLock::isReadLocked)
                .orElse(false)
                : zonePartitionId;

        return startedReplicationGroups.hasReplicationGroupStarted(zonePartitionId);
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

                return handleChangePendingAssignmentEvent(newEntry, evt.revision(), false);
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

            return reliableCatalogVersions.safeReliableCatalogFor(hybridTimestamp(assignmentsTimestamp))
                    .thenCompose(catalog ->
                            inBusyLockAsync(busyLock, () -> handleReduceChanged(evt, catalog, replicaGrpId, assignmentsTimestamp))
                    );
        });
    }

    private CompletableFuture<Void> handleReduceChanged(
            WatchEvent evt,
            Catalog catalog,
            ZonePartitionId replicaGrpId,
            long assignmentsTimestamp
    ) {
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(replicaGrpId.zoneId());

        return distributionZoneMgr.dataNodes(hybridTimestamp(assignmentsTimestamp), catalog.version(), replicaGrpId.zoneId())
                .thenCompose(dataNodes -> ZoneRebalanceRaftGroupEventsListener.handleReduceChanged(
                        metaStorageMgr,
                        dataNodes,
                        zoneDescriptor.partitions(),
                        zoneDescriptor.replicas(),
                        zoneDescriptor.consensusGroupSize(),
                        replicaGrpId,
                        evt,
                        assignmentsTimestamp
                ));
    }

    /**
     * Handles the {@link ZoneRebalanceUtil#STABLE_ASSIGNMENTS_PREFIX} update event.
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
            boolean onNodeRecovery
    ) {
        ZonePartitionId zonePartitionId = extractZonePartitionId(stableAssignmentsWatchEvent.key(), STABLE_ASSIGNMENTS_PREFIX_BYTES);

        Set<Assignment> stableAssignments = stableAssignmentsWatchEvent.value() == null
                ? emptySet()
                : Assignments.fromBytes(stableAssignmentsWatchEvent.value()).nodes();

        return supplyAsync(() -> {
            Entry pendingAssignmentsEntry = metaStorageMgr.getLocally(pendingPartAssignmentsQueueKey(zonePartitionId), revision);

            byte[] pendingAssignmentsFromMetaStorage = pendingAssignmentsEntry.value();

            Assignments pendingAssignments = pendingAssignmentsFromMetaStorage == null
                    ? Assignments.EMPTY
                    : AssignmentsQueue.fromBytes(pendingAssignmentsFromMetaStorage).poll();

            return stopAndMaybeDestroyPartitionAndUpdateClients(
                    zonePartitionId,
                    stableAssignments,
                    pendingAssignments,
                    onNodeRecovery,
                    revision
            );
        }, ioExecutor).thenCompose(identity());
    }

    private CompletableFuture<Void> updatePartitionClients(
            ZonePartitionId zonePartitionId,
            Set<Assignment> stableAssignments,
            Set<Assignment> pendingAssignments
    ) {
        return isLocalNodeIsPrimary(zonePartitionId).thenCompose(isLeaseholder -> inBusyLock(busyLock, () -> {
            CompletableFuture<Replica> replicaFuture = replicaMgr.replica(zonePartitionId);

            boolean isLocalInStable = isLocalNodeInAssignments(stableAssignments);

            if (!(isLocalInStable || isLeaseholder)) {
                return nullCompletedFuture();
            }

            if (replicaFuture == null) {
                return failedFuture(new ReplicaUnavailableException(zonePartitionId, localNode()));
            }

            // Update raft client peers and learners according to the actual assignments.
            return replicaFuture
                    .thenAccept(replica -> replica.updatePeersAndLearners(fromAssignments(union(stableAssignments, pendingAssignments))));
        }));
    }

    private CompletableFuture<Void> stopAndMaybeDestroyPartitionAndUpdateClients(
            ZonePartitionId zonePartitionId,
            Set<Assignment> stableAssignments,
            Assignments pendingAssignments,
            boolean onNodeRecovery,
            long revision
    ) {
        CompletableFuture<Void> clientUpdateFuture = onNodeRecovery
                // Updating clients is not needed on recovery.
                ? nullCompletedFuture()
                : updatePartitionClients(zonePartitionId, stableAssignments, pendingAssignments.nodes());

        boolean shouldStopLocalServices = (pendingAssignments.force()
                ? pendingAssignments.nodes().stream()
                : Stream.concat(stableAssignments.stream(), pendingAssignments.nodes().stream())
        )
                .noneMatch(assignment -> assignment.consistentId().equals(localNode().name()));

        if (!shouldStopLocalServices) {
            return clientUpdateFuture;
        }

        return clientUpdateFuture.thenCompose(v -> replicaMgr.weakStopReplica(
                zonePartitionId,
                WeakReplicaStopReason.EXCLUDED_FROM_ASSIGNMENTS,
                () -> stopAndDestroyPartition(zonePartitionId, revision)
        ));
    }

    /**
     * Stops all resources associated with a given partition, like replicas and partition trackers. Calls
     * {@link ReplicaManager#weakStopReplica} in order to change the replica state.
     *
     * @param zonePartitionId Partition ID.
     * @return Future that will be completed after all resources have been closed.
     */
    private CompletableFuture<Void> stopPartitionForRestart(ZonePartitionId zonePartitionId, long revision) {
        return replicaMgr.weakStopReplica(
                zonePartitionId,
                WeakReplicaStopReason.RESTART,
                () -> stopPartitionInternal(zonePartitionId, BEFORE_REPLICA_STOPPED, AFTER_REPLICA_STOPPED, revision, replica -> {})
        );
    }

    /**
     * Stops all resources associated with a given partition and destroys its storage, like replicas and partition trackers.
     * Calls {@link ReplicaManager#weakStopReplica} in order to change the replica state.
     *
     * @param zonePartitionId Partition ID.
     * @param revision Revision.
     * @return Future that will be completed after all resources have been closed and destroyed.
     */
    private CompletableFuture<Void> stopPartitionAndDestroyForRestart(ZonePartitionId zonePartitionId, long revision) {
        return replicaMgr.weakStopReplica(
                zonePartitionId,
                WeakReplicaStopReason.RESTART,
                () -> stopAndDestroyPartition(zonePartitionId, revision)
        );
    }

    private CompletableFuture<Void> handleChangePendingAssignmentEvent(Entry pendingAssignmentsEntry, long revision, boolean isRecovery) {
        if (pendingAssignmentsEntry.value() == null || pendingAssignmentsEntry.empty()) {
            return nullCompletedFuture();
        }

        ZonePartitionId zonePartitionId = extractZonePartitionId(pendingAssignmentsEntry.key(), PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES);

        // Stable assignments from the meta store, which revision is bounded by the current pending event.
        Assignments stableAssignments = stableAssignments(zonePartitionId, revision);

        AssignmentsQueue pendingAssignmentsQueue = AssignmentsQueue.fromBytes(pendingAssignmentsEntry.value());

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
                        pendingAssignmentsQueue,
                        revision
                );
            }

            Assignments pendingAssignments = pendingAssignmentsQueue == null ? Assignments.EMPTY : pendingAssignmentsQueue.poll();

            return handleChangePendingAssignmentEvent(
                    zonePartitionId,
                    stableAssignments,
                    pendingAssignments,
                    revision,
                    isRecovery
            ).thenRun(() -> {
                boolean isLocalNodeInStableOrPending = isNodeInReducedStableOrPendingAssignments(
                        zonePartitionId,
                        stableAssignments,
                        pendingAssignments,
                        revision
                );

                if (!isLocalNodeInStableOrPending) {
                    return;
                }

                // A node might exist in stable yet we don't want to start the replica
                // The case is: nodes A, B and C hold a replication group,
                // nodes A and B die.
                // Reset adds C into force pending and user writes data onto C.
                // Then A and B go back online. In this case
                // stable = [A, B, C], pending = [C, force] and only C should be started.
                if (isRecovery && !replicaMgr.isReplicaStarted(zonePartitionId)) {
                    return;
                }

                changePeersOnRebalance(
                        zonePartitionId,
                        pendingAssignments.nodes(),
                        revision);
            }).whenComplete((v, ex) -> maybeRunFailHandler(ex, zonePartitionId));
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Void> handleChangePendingAssignmentEvent(
            ZonePartitionId replicaGrpId,
            @Nullable Assignments stableAssignments,
            Assignments pendingAssignments,
            long revision,
            boolean isRecovery
    ) {
        boolean pendingAssignmentsAreForced = pendingAssignments.force();
        Set<Assignment> pendingAssignmentsNodes = pendingAssignments.nodes();

        // Start a new Raft node and Replica if this node has appeared in the new assignments.
        Assignment localAssignmentInPending = localAssignment(pendingAssignments);
        Assignment localAssignmentInStable = localAssignment(stableAssignments);

        AssignmentsChain assignmentsChain = ZoneRebalanceUtil.assignmentsChainGetLocally(metaStorageMgr, replicaGrpId, revision);

        boolean shouldStartLocalGroupNode;
        if (isRecovery) {
            // The condition to start the replica is
            // `pending.contains(node) || (stable.contains(node) && !pending.isForce())`.
            // This condition covers the left part of the OR expression.
            // The right part of it is covered in `startLocalPartitionsAndClients`.
            if (lastRebalanceWasGraceful(assignmentsChain)) {
                shouldStartLocalGroupNode = localAssignmentInPending != null;
            } else {
                // TODO: Use logic from https://issues.apache.org/jira/browse/IGNITE-23874.
                LOG.warn("Recovery after a forced rebalance for zone is not supported yet [zonePartitionId={}].",
                        replicaGrpId);
                shouldStartLocalGroupNode = localAssignmentInPending != null;
            }
        } else {
            shouldStartLocalGroupNode = localAssignmentInPending != null && localAssignmentInStable == null;
        }

        // This is a set of assignments for nodes that are not the part of stable assignments, i.e. unstable part of the distribution.
        // For regular pending assignments we use (old) stable set, so that none of new nodes would be able to propose itself as a leader.
        // For forced assignments, we should do the same thing, but only for the subset of stable set that is alive right now. Dead nodes
        // are excluded. It is calculated precisely as an intersection between forced assignments and (old) stable assignments.
        Assignments computedStableAssignments = getComputedStableAssignments(stableAssignments, pendingAssignments);

        CompletableFuture<?> localServicesStartFuture;

        if (shouldStartLocalGroupNode) {
            // We can safely access the Catalog at the timestamp because:
            // 1. It is guaranteed that Catalog update bringing the Catalog version has been applied (as we are now handling
            // a Metastorage event that was caused by that same Catalog version we need, so the Catalog version update is already
            // handled), so no Schema sync is needed.
            // 2. It is guaranteed that Catalog compactor cannot remove Catalog version corresponding to pending assignments timestamp.
            CatalogZoneDescriptor zoneDescriptor = zoneDescriptorAt(replicaGrpId.zoneId(), pendingAssignments.timestamp());

            localServicesStartFuture = createZonePartitionReplicationNode(
                    replicaGrpId,
                    computedStableAssignments,
                    revision,
                    zoneDescriptor.partitions(),
                    isVolatileZone(zoneDescriptor),
                    isRecovery,
                    false
            );
        } else if (pendingAssignmentsAreForced && localAssignmentInPending != null) {
            localServicesStartFuture =
                    replicaMgr.resetWithRetry(replicaGrpId, fromAssignments(computedStableAssignments.nodes()), revision);
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

                    // A node might exist in stable yet we don't want to start the replica
                    // The case is: nodes A, B and C hold a replication group,
                    // nodes A and B die.
                    // Reset adds C into force pending and user writes data onto C.
                    // Then A and B go back online. In this case
                    // stable = [A, B, C], pending = [C, force] and only C should be started.
                    if (isRecovery && !replicaMgr.isReplicaStarted(replicaGrpId)) {
                        return;
                    }

                    assert replicaMgr.isReplicaStarted(replicaGrpId) : "The local node is outside of the replication group ["
                            + ", groupId=" + replicaGrpId
                            + ", stable=" + stableAssignments
                            + ", pending=" + pendingAssignments
                            + ", localName=" + localNode().name() + "].";

                    // For forced assignments, we exclude dead stable nodes, and all alive stable nodes are already in pending assignments.
                    // Union is not required in such a case.
                    Set<Assignment> newAssignments = pendingAssignmentsAreForced || stableAssignments == null
                            ? pendingAssignmentsNodes
                            : union(pendingAssignmentsNodes, stableAssignments.nodes());

                    replicaMgr.replica(replicaGrpId)
                            .thenAccept(replica -> replica.updatePeersAndLearners(fromAssignments(newAssignments)));
                }), ioExecutor);
    }

    private static Assignments getComputedStableAssignments(@Nullable Assignments stableAssignments, Assignments pendingAssignments) {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-22600 remove the second condition
        //  when we will have a proper handling of empty stable assignments
        if (stableAssignments == null || stableAssignments.nodes().isEmpty()) {
            // This condition can only pass if all stable nodes are dead, and we start new raft group from scratch.
            // In this case new initial configuration must match new forced assignments.
            return Assignments.forced(pendingAssignments.nodes(), pendingAssignments.timestamp());
        } else if (pendingAssignments.force()) {
            // In case of forced assignments we need to remove nodes that are present in the stable set but are missing from the
            // pending set. Such operation removes dead stable nodes from the resulting stable set, which guarantees that we will
            // have a live majority.
            return pendingAssignments;
        } else {
            return stableAssignments;
        }
    }

    private CatalogZoneDescriptor zoneDescriptorAt(int zoneId, long timestamp) {
        Catalog catalog = catalogService.activeCatalog(timestamp);
        assert catalog != null : "Catalog is not available at " + nullableHybridTimestamp(timestamp);

        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);
        assert zoneDescriptor != null : "Zone descriptor is not available at " + nullableHybridTimestamp(timestamp) + " for zone " + zoneId;

        return zoneDescriptor;
    }

    private void changePeersOnRebalance(
            ZonePartitionId replicaGrpId,
            Set<Assignment> pendingAssignments,
            long revision
    ) {
        // According to the rebalance logic, it's safe to react to pending assignments change in async manner.
        PeersAndLearners newConfiguration = fromAssignments(pendingAssignments);

        ChangePeersAndLearnersWithRetry mover = createChangePeersAndLearnersWithRetry(replicaGrpId);

        mover.execute(
                        newConfiguration,
                        revision,
                        raftClient -> ensureLeader(replicaGrpId, raftClient)
                                .whenComplete((raftWithTerm, ex) -> {
                                    if (raftWithTerm != null) {
                                        LOG.info("Current node={} is the leader of partition raft group={}. "
                                                        + "Initiate rebalance process for partition={}, zoneId={}",
                                                localNode().name(), replicaGrpId, replicaGrpId.partitionId(), replicaGrpId.zoneId());
                                    }
                                })
                )
                .whenComplete((res, ex) -> maybeRunFailHandler(ex, replicaGrpId));
    }

    private void maybeRunFailHandler(Throwable ex, ZonePartitionId replicaGrpId) {
        if (ex != null && !hasCause(ex, NodeStoppingException.class, ComponentStoppingException.class, RaftStaleUpdateException.class)) {
            String errorMessage = String.format("Failure while moving partition [partId=%s]", replicaGrpId);
            failureProcessor.process(new FailureContext(ex, errorMessage));
        }
    }

    private CompletableFuture<@Nullable IgniteBiTuple<RaftGroupService, Long>> ensureLeader(
            ZonePartitionId replicaGrpId,
            RaftGroupService raftClient
    ) {
        return raftClient.refreshAndGetLeaderWithTerm()
                .exceptionally(throwable -> {
                    if (hasCause(throwable, TimeoutException.class)) {
                        LOG.info(
                                "Node couldn't get the leader within timeout so the changing peers is skipped [grp={}].",
                                replicaGrpId
                        );
                    } else if (hasCause(throwable, ComponentStoppingException.class)) {
                        LOG.info("Replica is being stopped so the changing peers is skipped [grp={}].", replicaGrpId);
                    } else {
                        LOG.info("Failed to get a leader for the RAFT replication group [grp={}].", throwable, replicaGrpId);
                    }

                    return LeaderWithTerm.NO_LEADER;
                }).thenApply(leaderWithTerm -> {
                    if (leaderWithTerm.isEmpty() || !isLocalPeer(leaderWithTerm.leader())) {
                        return null;
                    }

                    return new IgniteBiTuple<>(raftClient, leaderWithTerm.term());
                });
    }

    private boolean isLocalPeer(Peer peer) {
        return peer.consistentId().equals(localNode().name());
    }

    private boolean isLocalNodeInAssignments(Collection<Assignment> assignments) {
        return assignments.stream().anyMatch(isLocalNodeAssignment);
    }

    private CompletableFuture<Void> waitForMetadataCompleteness(long ts) {
        return executorInclinedSchemaSyncService.waitForMetadataCompleteness(hybridTimestamp(ts));
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

            return executorInclinedPlacementDriver.getPrimaryReplica(replicationGroupId, previousMetastoreSafeTime)
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
        Entry reduceEntry = metaStorageMgr.getLocally(switchReduceKey(replicaGrpId), revision);

        Assignments reduceAssignments = reduceEntry != null
                ? Assignments.fromBytes(reduceEntry.value())
                : null;

        Set<Assignment> reducedStableAssignments = reduceAssignments != null
                ? subtract(stableAssignments.nodes(), reduceAssignments.nodes())
                : stableAssignments.nodes();

        return isLocalNodeInAssignments(union(reducedStableAssignments, pendingAssignments.nodes()));
    }

    @Nullable
    private Assignment localAssignment(@Nullable Assignments assignments) {
        if (assignments != null) {
            for (Assignment assignment : assignments.nodes()) {
                if (isLocalNodeAssignment.test(assignment)) {
                    return assignment;
                }
            }
        }
        return null;
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!nodeProperties.colocationEnabled()) {
            return nullCompletedFuture();
        }

        try {
            metricManager.unregisterSource(zoneResourcesManager.snapshotsMetricsSource());

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

    /**
     * Stops zone replica, executes a given action and fires given local events.
     *
     * @return Future that will be completed after zone replica was stopped and event handling was finished.
     */
    @VisibleForTesting
    public CompletableFuture<Void> stopPartitionInternal(
            ZonePartitionId zonePartitionId,
            LocalPartitionReplicaEvent beforeReplicaStoppedEvent,
            LocalPartitionReplicaEvent afterReplicaStoppedEvent,
            long eventRevision,
            Consumer<Boolean> afterReplicaStopAction
    ) {
        // Not using the busy lock here, because this method is called on component stop.
        return executeUnderZoneWriteLock(zonePartitionId.zoneId(), () -> {
            var eventParameters = new LocalPartitionReplicaEventParameters(zonePartitionId, eventRevision, false);

            return fireEvent(beforeReplicaStoppedEvent, eventParameters)
                    .thenCompose(v -> {
                        try {
                            return replicaMgr.stopReplica(zonePartitionId)
                                    .thenComposeAsync(replicaWasStopped -> {
                                        closeTrackers(zonePartitionId);

                                        afterReplicaStopAction.accept(replicaWasStopped);

                                        if (!replicaWasStopped) {
                                            return nullCompletedFuture();
                                        }

                                        startedReplicationGroups.afterStoppingGroup(zonePartitionId);

                                        return fireEvent(afterReplicaStoppedEvent, eventParameters);
                                    }, ioExecutor);
                        } catch (NodeStoppingException e) {
                            return nullCompletedFuture();
                        }
                    });
        });
    }

    private void closeTrackers(ZonePartitionId zonePartitionId) {
        // Resources can be null if the replica was never started (or was already stopped) on this node and we are just stopping
        // the stopped replica.
        ZonePartitionResources replicaResources = zonePartitionResourcesOrNull(zonePartitionId);

        if (replicaResources != null) {
            replicaResources.closeTrackers();
        }
    }

    /**
     * Stops resources that are related to provided zone partitions.
     *
     * @param partitionIds Partitions to stop.
     */
    private void cleanUpPartitionsResources(Stream<ZonePartitionId> partitionIds) {
        CompletableFuture<?>[] stopPartitionsFuture = partitionIds
                .map(zonePartitionId -> stopPartitionInternal(
                        zonePartitionId,
                        BEFORE_REPLICA_STOPPED,
                        AFTER_REPLICA_STOPPED,
                        -1L,
                        replicaWasStopped -> {}
                ))
                .toArray(CompletableFuture[]::new);

        try {
            CompletableFuture<Void> fut = allOf(stopPartitionsFuture);

            delayedExecutor(30, TimeUnit.SECONDS, Runnable::run)
                    .execute(() -> {
                        if (!fut.isDone()) {
                            printPartitionState(partitionIds);
                        }
                    });

            fut.get();
        } catch (Throwable e) {
            if (!isExpectedThrowableDuringResourcesStop(e)) {
                failureProcessor.process(new FailureContext(e, "Unable to clean up zones resources"));
            }
        }
    }

    private static boolean isExpectedThrowableDuringResourcesStop(Throwable throwable) {
        return hasCause(
                throwable,
                NodeStoppingException.class,
                ComponentStoppingException.class,
                TrackerClosedException.class,
                CancellationException.class,
                // Is possible during cluster stop due to "stale" nodes (nodes that already left the cluster).
                RecipientLeftException.class
        );
    }

    private void printPartitionState(Stream<ZonePartitionId> partitionIds) {
        List<ZonePartitionId> nonStoppedPartitions = partitionIds
                .filter(partId -> replicaMgr.replica(partId) != null)
                .collect(toList());

        int exceedLimit = nonStoppedPartitions.size() - COLLECTION_LIMIT;

        String partitionsStr = "There are still some partitions that are being stopped: "
                + S.toString(nonStoppedPartitions, (sb, e, i) -> sb.app(e).app(i < nonStoppedPartitions.size() - 1 ? ", " : ""))
                + (exceedLimit > 0 ? format(" and {} more; ", exceedLimit) : "; ");

        ThreadUtils.dumpThreads(LOG, partitionsStr, false);
    }

    /**
     * Lock the zones replica list for any changes. {@link #hasLocalPartition(ZonePartitionId)} must be executed under this lock always.
     *
     * @param zoneId Zone id.
     * @return Future completing with a stamp which must be used for further unlock.
     */
    public CompletableFuture<Long> lockZoneForRead(int zoneId) {
        NaiveAsyncReadWriteLock lock = zonePartitionsLocks.computeIfAbsent(zoneId, id -> newZoneLock());
        return lock.readLock();
    }

    private static NaiveAsyncReadWriteLock newZoneLock() {
        return new NaiveAsyncReadWriteLock();
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
     * @param zonePartitionId Zone partition id.
     * @param tableId Table id.
     * @param tablePartitionReplicaProcessorFactory Factory for creating table-specific partition replicas.
     * @param raftTableProcessor Raft table processor for the table-specific partition.
     */
    public void loadTableListenerToZoneReplica(
            ZonePartitionId zonePartitionId,
            int tableId,
            Function<RaftCommandRunner, ReplicaTableProcessor> tablePartitionReplicaProcessorFactory,
            RaftTableProcessor raftTableProcessor,
            PartitionMvStorageAccess partitionMvStorageAccess,
            boolean onNodeRecovery
    ) {
        ZonePartitionResources resources = zonePartitionResources(zonePartitionId);

        // Register an intent to register a table-wide replica listener. On recovery this method is called before the replica is started,
        // so the listeners will be registered by the thread completing the "replicaListenerFuture". On normal operation (where there is
        // a HB relationship between zone and table creation) zone-wide replica must already be started, this future will always be
        // completed and the listeners will be registered immediately.
        resources.replicaListenerFuture().thenAccept(zoneReplicaListener -> zoneReplicaListener.addTableReplicaProcessor(
                tableId,
                tablePartitionReplicaProcessorFactory
        ));

        if (onNodeRecovery) {
            resources.raftListener().addTableProcessorOnRecovery(tableId, raftTableProcessor);
        } else {
            resources.raftListener().addTableProcessor(tableId, raftTableProcessor);
        }

        resources.snapshotStorage().addMvPartition(tableId, partitionMvStorageAccess);
    }

    /**
     * Load a new table partition listener to the zone replica.
     *
     * @param zonePartitionId Zone partition id.
     * @param tableId Table's identifier.
     * @return A future that gets completed after all resources have been unloaded.
     */
    public CompletableFuture<Void> unloadTableResourcesFromZoneReplica(ZonePartitionId zonePartitionId, int tableId) {
        return zoneResourcesManager.removeTableResources(zonePartitionId, tableId);
    }

    /**
     * Restarts the zone's partition including the replica and raft node with storage cleanup.
     *
     * @param zonePartitionId Zone's partition that needs to be restarted.
     * @param revision Metastore revision.
     * @param assignmentsTimestamp Timestamp of the assignments.
     * @return Operation future.
     */
    public CompletableFuture<?> restartPartitionWithCleanUp(ZonePartitionId zonePartitionId, long revision, long assignmentsTimestamp) {
        return inBusyLockAsync(busyLock, () -> stopPartitionAndDestroyForRestart(zonePartitionId, revision).thenComposeAsync(unused -> {
            Assignments stableAssignments = zoneStableAssignmentsGetLocally(metaStorageMgr, zonePartitionId, revision);

            assert stableAssignments != null : "zonePartitionId=" + zonePartitionId + ", revision=" + revision;

            return waitForMetadataCompleteness(assignmentsTimestamp).thenCompose(unused2 -> inBusyLockAsync(busyLock, () -> {
                CatalogZoneDescriptor zoneDescriptor = zoneDescriptorAt(zonePartitionId.zoneId(), assignmentsTimestamp);

                return createZonePartitionReplicationNode(
                        zonePartitionId,
                        stableAssignments,
                        revision,
                        zoneDescriptor.partitions(),
                        isVolatileZone(zoneDescriptor),
                        false,
                        false
                );
            }));
        }, ioExecutor));
    }

    /**
     * Restarts the zone's partition including the replica and raft node.
     *
     * @param zonePartitionId Zone's partition that needs to be restarted.
     * @param revision Metastore revision.
     * @param assignmentsTimestamp Timestamp of the assignments.
     * @return Operation future.
     */
    public CompletableFuture<?> restartPartition(ZonePartitionId zonePartitionId, long revision, long assignmentsTimestamp) {
        return inBusyLockAsync(busyLock, () -> stopPartitionForRestart(zonePartitionId, revision).thenComposeAsync(unused -> {
            Assignments stableAssignments = zoneStableAssignmentsGetLocally(metaStorageMgr, zonePartitionId, revision);

            assert stableAssignments != null : "zonePartitionId=" + zonePartitionId + ", revision=" + revision;

            return waitForMetadataCompleteness(assignmentsTimestamp).thenCompose(unused2 -> inBusyLockAsync(busyLock, () -> {
                Assignment localAssignment = localAssignment(stableAssignments);

                if (localAssignment == null) {
                    // (0) in case if node not in the assignments
                    return nullCompletedFuture();
                }

                CatalogZoneDescriptor zoneDescriptor = zoneDescriptorAt(zonePartitionId.zoneId(), assignmentsTimestamp);

                return createZonePartitionReplicationNode(
                        zonePartitionId,
                        stableAssignments,
                        revision,
                        zoneDescriptor.partitions(),
                        isVolatileZone(zoneDescriptor),
                        false,
                        false
                );
            }));
        }, ioExecutor));
    }

    private <T> CompletableFuture<T> executeUnderZoneWriteLock(int zoneId, Supplier<CompletableFuture<T>> action) {
        NaiveAsyncReadWriteLock lock = zonePartitionsLocks.computeIfAbsent(zoneId, id -> newZoneLock());

        return lock.writeLock().thenCompose(stamp -> {
            try {
                return action.get()
                        .whenComplete((v, e) -> lock.unlockWrite(stamp));
            } catch (Throwable e) {
                lock.unlockWrite(stamp);

                return failedFuture(e);
            }
        });
    }

    private CompletableFuture<Boolean> onPrimaryReplicaExpired(PrimaryReplicaEventParameters parameters) {
        if (topologyService.localMember().id().equals(parameters.leaseholderId())) {
            ZonePartitionId groupId = (ZonePartitionId) parameters.groupId();

            // We do not wait future in order not to block meta storage updates.
            replicaMgr.weakStopReplica(
                    groupId,
                    WeakReplicaStopReason.PRIMARY_EXPIRED,
                    () -> stopAndDestroyPartition(groupId, parameters.causalityToken())
            );
        }

        return falseCompletedFuture();
    }

    private CompletableFuture<Void> stopAndDestroyPartition(ZonePartitionId zonePartitionId, long revision) {
        return stopPartitionInternal(
                zonePartitionId,
                BEFORE_REPLICA_DESTROYED,
                AFTER_REPLICA_DESTROYED,
                revision,
                replicaWasStopped -> {
                    if (replicaWasStopped) {
                        zoneResourcesManager.destroyZonePartitionResources(zonePartitionId);

                        try {
                            replicaMgr.destroyReplicationProtocolStoragesDurably(zonePartitionId, false);
                        } catch (NodeStoppingException e) {
                            throw new IgniteInternalException(NODE_STOPPING_ERR, e);
                        }
                    }
                }
        );
    }

    @TestOnly
    public @Nullable TxStatePartitionStorage txStatePartitionStorage(int zoneId, int partitionId) {
        return zoneResourcesManager.txStatePartitionStorage(zoneId, partitionId);
    }

    @TestOnly
    public HybridTimestamp currentSafeTimeForZonePartition(int zoneId, int partId) {
        return zonePartitionResources(new ZonePartitionId(zoneId, partId)).raftListener().currentSafeTime();
    }

    /**
     * Returns resources for the given zone partition.
     */
    public ZonePartitionResources zonePartitionResources(ZonePartitionId zonePartitionId) {
        ZonePartitionResources resources = zonePartitionResourcesOrNull(zonePartitionId);

        assert resources != null : String.format("Missing resources for zone partition [zonePartitionId=%s]", zonePartitionId);

        return resources;
    }

    /**
     * Returns resources for the given zone partition or {@code null} if not available (because the replica is stopped/destroyed).
     */
    public @Nullable ZonePartitionResources zonePartitionResourcesOrNull(ZonePartitionId zonePartitionId) {
        return zoneResourcesManager.getZonePartitionResources(zonePartitionId);
    }

    private void onZoneDrop(DropZoneEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            int eventCatalogVersion = parameters.catalogVersion();
            int catalogVersionWithZonePresent = eventCatalogVersion - 1;
            int zoneId = parameters.zoneId();

            CatalogZoneDescriptor zoneDescriptor = catalogService.catalog(catalogVersionWithZonePresent).zone(zoneId);

            assert zoneDescriptor != null : "Unexpected null zone descriptor for zoneId=" + zoneId + ", catalogVersion "
                    + catalogVersionWithZonePresent;

            destructionEventsQueue.enqueue(
                new DestroyZoneEvent(
                        eventCatalogVersion,
                        zoneId,
                        zoneDescriptor.partitions()
                )
        );});
    }

    private CompletableFuture<Boolean> onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        if (!busyLock.enterBusy()) {
            return falseCompletedFuture();
        }

        try {
            int newEarliestCatalogVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());

            // Run zone destruction fully asynchronously.
            destructionEventsQueue.drainUpTo(newEarliestCatalogVersion)
                    .forEach(this::removeZonePartitionsIfPossible);

            return falseCompletedFuture();
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * For each partition performs following actions.
     *
     * <ol>
     *     <li>Check whether it's started or await if it's starting.</li>
     *     <li>Check whether the partition is eligible for removal, it is if it has zero table resources and if txStateStorage is empty.</li>
     *     <li>Stop partition, drop zone partition resources and unregister it from within startedReplicationGroups.</li>
     *     <li>Remove partition assignments from meta storage.</li>
     * </ol>
     */
    private void removeZonePartitionsIfPossible(DestroyZoneEvent event) {
        int zoneId = event.zoneId();
        int partitionsCount = event.partitions();

        List<CompletableFuture<Boolean>> partitionsEligibilityForRemovalFutures = new ArrayList<>();
        for (int partitionIndex = 0; partitionIndex < partitionsCount; partitionIndex++) {
            ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, partitionIndex);

            CompletableFuture<Boolean> partitionRemovalFuture =
                    startedReplicationGroups.hasReplicationGroupStartedOrAwaitIfStarting(zonePartitionId)
                            .thenComposeAsync(started -> {
                                if (!started) {
                                    return falseCompletedFuture();
                                }

                                return inBusyLockAsync(busyLock, () ->
                                        isEligibleForDrop(zonePartitionId)
                                                .thenCompose(eligible -> {
                                                    if (!eligible) {
                                                        return falseCompletedFuture();
                                                    }

                                                    // It's safe to use -1 as revision id here, since we only stop partitions that do not
                                                    // active table-related resources.
                                                    return stopAndDestroyPartition(zonePartitionId, -1)
                                                            .thenCompose(v -> dropAssignments(zonePartitionId))
                                                            .thenApply(v -> true);
                                                })
                                );
                            }, partitionOperationsExecutor)
                            .exceptionally(e -> {
                                if (!hasCause(e, NodeStoppingException.class)) {
                                    LOG.error(
                                            "Unable to destroy zone partition [zonePartitionId={}]",
                                            e,
                                            zonePartitionId);
                                }

                                // In case of false, event will be returned to the destructionEventsQueue and thus removal will be retried
                                // on next iteration of LWM change.
                                return false;
                            });

            partitionsEligibilityForRemovalFutures.add(partitionRemovalFuture);
        }

        // If there's a partition that still have non empty recourses e.g. non-empty txnStateStorage, event is returned
        // back to destructionEventsQueue and thus will be re-processed on next lwm change.
        allOf(partitionsEligibilityForRemovalFutures.toArray(new CompletableFuture[0]))
                .thenApply(fs -> partitionsEligibilityForRemovalFutures.stream().anyMatch(f -> !f.join()))
                .thenAccept(anyFalse -> {
                    if (anyFalse) {
                        destructionEventsQueue.enqueue(event);
                    }
                });
    }

    /**
     * Checks whether partition could be removed. In order to match the condition zone partition should have zero table resources and
     * empty txStateStorage.
     */
    private CompletableFuture<Boolean> isEligibleForDrop(ZonePartitionId zonePartitionId) {
        ZonePartitionResources zonePartitionResources = zoneResourcesManager.getZonePartitionResources(zonePartitionId);

        if (zonePartitionResources == null) {
            return trueCompletedFuture();
        }

        try (var cursor = zonePartitionResources.txStatePartitionStorage().scan()) {
            if (cursor.hasNext()) {
                return falseCompletedFuture();
            }
        } catch (TxStateStorageRebalanceException e) {
            return falseCompletedFuture();
        }

        return zoneResourcesManager.areTableResourcesEmpty(zonePartitionId);
    }

    /**
     * Removes all zone partition assignment keys from metastorage.
     */
    private CompletableFuture<Void> dropAssignments(ZonePartitionId zonePartitionId) {
        Set<ByteArray> assignmentKeys = Set.of(
                stablePartAssignmentsKey(zonePartitionId),
                pendingPartAssignmentsQueueKey(zonePartitionId),
                pendingChangeTriggerKey(zonePartitionId),
                plannedPartAssignmentsKey(zonePartitionId),
                switchAppendKey(zonePartitionId),
                switchReduceKey(zonePartitionId),
                assignmentsChainKey(zonePartitionId)
        );

        return metaStorageMgr.removeAll(assignmentKeys)
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Failed to remove assignments from metastorage [zonePartitionId={}]", e, zonePartitionId);
                    }
                });
    }

    // TODO sanpwc create jira for thread safety of drop on lwm and concurrent rebalances along and/or partition restarts.
    /**
     * For HA zones: Check that last rebalance was graceful (caused by common rebalance triggers, like data nodes change, replica factor
     * change, etc.) rather than forced (caused by a disaster recovery reset after losing the majority of nodes).
     */
    private static boolean lastRebalanceWasGraceful(@Nullable AssignmentsChain assignmentsChain) {
        // Assignments chain is either empty (when there have been no stable switch yet) or contains a single element in chain.
        return assignmentsChain == null || assignmentsChain.size() == 1;
    }

    /** Internal event. */
    private static class DestroyZoneEvent {
        final int catalogVersion;
        final int zoneId;
        final int partitions;

        DestroyZoneEvent(
                int catalogVersion,
                int zoneId,
                int partitions
        ) {
            this.catalogVersion = catalogVersion;
            this.zoneId = zoneId;
            this.partitions = partitions;
        }

        int catalogVersion() {
            return catalogVersion;
        }

        int zoneId() {
            return zoneId;
        }

        int partitions() {
            return partitions;
        }
    }
}
