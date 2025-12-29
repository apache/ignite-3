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

package org.apache.ignite.internal.table.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.deriveUuidFrom;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.AbstractMessagingService;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterNodeResolver;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.SingleClusterNodeResolver;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.serialization.MessageSerializer;
import org.apache.ignite.internal.partition.replicator.ZonePartitionReplicaListener;
import org.apache.ignite.internal.partition.replicator.raft.ZonePartitionRaftListener;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionDataStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionSnapshots;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionsSnapshots;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimePropagatingCommand;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TimestampAwareReplicaResponse;
import org.apache.ignite.internal.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.TablePartitionProcessor;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.TransactionStateResolver;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.metrics.TableMetricSource;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.QualifiedNameHelper;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Dummy table storage implementation.
 */
@TestOnly
public class DummyInternalTableImpl extends InternalTableImpl {
    public static final IgniteLogger LOG = Loggers.forClass(DummyInternalTableImpl.class);

    public static final NetworkAddress ADDR = new NetworkAddress("127.0.0.1", 2004);

    public static final InternalClusterNode LOCAL_NODE = new ClusterNodeImpl(new UUID(1, 2), "node", ADDR);

    // 2000 was picked to avoid negative time that we get when building read timestamp
    // in TxManagerImpl.currentReadTimestamp.
    // We subtract (ReplicaManager.IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS + HybridTimestamp.CLOCK_SKEW) = (1000 + 7) = 1007
    // from the current time.
    // Any value greater than that will work, hence 2000.
    public static final HybridClock CLOCK = new TestHybridClock(() -> 2000);

    private static final ClockService CLOCK_SERVICE = new TestClockService(CLOCK);

    /** ID of the zone to which the corresponding table belongs. */
    public static final int ZONE_ID = 2;

    private static final int PART_ID = 0;

    private static final ReplicationGroupId crossTableGroupId = new TablePartitionId(333, 0);

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private RaftGroupListener partitionListener;

    private ReplicaListener replicaListener;

    private final ReplicationGroupId groupId;

    /** The thread updates safe time on the dummy replica. */
    private final SafeTimeValuesTracker safeTime;

    private final Object raftServiceMutex = new Object();

    private final TxStateStorage txStateStorage;

    private static final AtomicInteger nextTableId = new AtomicInteger(10_001);

    private static final ScheduledExecutorService COMMON_SCHEDULER = Executors.newSingleThreadScheduledExecutor(
            IgniteThreadFactory.create("node", "DummyInternalTable-common-scheduler-", true, LOG)
    );

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     * @param schema Schema.
     * @param txConfiguration Transaction configuration.
     * @param systemCfg System configuration.
     * @param replicationConfiguration Replication configuration.
     */
    public DummyInternalTableImpl(
            ReplicaService replicaSvc,
            SchemaDescriptor schema,
            TransactionConfiguration txConfiguration,
            SystemDistributedConfiguration systemCfg,
            ReplicationConfiguration replicationConfiguration
    ) {
        this(
                replicaSvc,
                new TestPlacementDriver(LOCAL_NODE),
                new TestMvPartitionStorage(0),
                schema,
                txConfiguration,
                systemCfg,
                replicationConfiguration
        );
    }

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     * @param placementDriver Placement driver.
     * @param storage Storage.
     * @param schema Schema.
     * @param txConfiguration Transaction configuration.
     * @param systemCfg System configuration.
     * @param replicationConfiguration Replication Configuration.
     */
    public DummyInternalTableImpl(
            ReplicaService replicaSvc,
            PlacementDriver placementDriver,
            MvPartitionStorage storage,
            SchemaDescriptor schema,
            TransactionConfiguration txConfiguration,
            SystemDistributedConfiguration systemCfg,
            ReplicationConfiguration replicationConfiguration
    ) {
        this(
                replicaSvc,
                storage,
                false,
                null,
                schema,
                HybridTimestampTracker.atomicTracker(null),
                placementDriver,
                replicationConfiguration,
                txConfiguration,
                systemCfg,
                new RemotelyTriggeredResourceRegistry(),
                new TransactionInflights(placementDriver, CLOCK_SERVICE)
        );
    }

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     * @param mvPartStorage Multi version partition storage.
     * @param crossTableUsage If this dummy table is going to be used in cross-table tests, it won't mock the calls of
     *         ReplicaService by itself.
     * @param transactionStateResolver Transaction state resolver.
     * @param schema Schema descriptor.
     * @param tracker Observable timestamp tracker.
     * @param placementDriver Placement driver.
     * @param replicationConfiguration Replication configuration.
     * @param txConfiguration Transaction configuration.
     * @param systemCfg System configuration.
     * @param resourcesRegistry Resource registry.
     * @param transactionInflights Inflights.
     */
    public DummyInternalTableImpl(
            ReplicaService replicaSvc,
            MvPartitionStorage mvPartStorage,
            boolean crossTableUsage,
            @Nullable TransactionStateResolver transactionStateResolver,
            SchemaDescriptor schema,
            HybridTimestampTracker tracker,
            PlacementDriver placementDriver,
            ReplicationConfiguration replicationConfiguration,
            TransactionConfiguration txConfiguration,
            SystemDistributedConfiguration systemCfg,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            TransactionInflights transactionInflights
    ) {
        super(
                QualifiedNameHelper.fromNormalized(SqlCommon.DEFAULT_SCHEMA_NAME, "test"),
                ZONE_ID, // zone id.
                nextTableId.getAndIncrement(), // table id.
                1, // number of partitions.
                new SingleClusterNodeResolver(LOCAL_NODE),
                txManager(replicaSvc, placementDriver, txConfiguration, systemCfg, resourcesRegistry),
                mock(MvTableStorage.class),
                replicaSvc,
                CLOCK_SERVICE,
                tracker,
                placementDriver,
                transactionInflights,
                null,
                mock(StreamerReceiverRunner.class),
                () -> 10_000L,
                () -> 10_000L,
                new TableMetricSource(QualifiedName.fromSimple("test"))
        );

        txStateStorage = new TestTxStateStorage();

        RaftGroupService svc = mock(RaftGroupService.class);

        groupId = crossTableUsage ? new TablePartitionId(tableId(), PART_ID) : crossTableGroupId;

        lenient().doReturn(groupId).when(svc).groupId();
        Peer leaderPeer = new Peer(UUID.randomUUID().toString());
        lenient().doReturn(leaderPeer).when(svc).leader();
        lenient().doReturn(completedFuture(new LeaderWithTerm(leaderPeer, 1L))).when(svc).refreshAndGetLeaderWithTerm();

        if (!crossTableUsage) {
            // Delegate replica requests directly to replica listener.
            lenient()
                    .doAnswer(invocationOnMock -> {
                        InternalClusterNode node = invocationOnMock.getArgument(0);

                        return replicaListener.invoke(invocationOnMock.getArgument(1), node.id()).thenApply(ReplicaResult::result);
                    })
                    .when(replicaSvc).invoke(any(InternalClusterNode.class), any());

            lenient()
                    .doAnswer(invocationOnMock -> {
                        String nodeConsistenId = invocationOnMock.getArgument(0);
                        UUID nodeId = deriveUuidFrom(nodeConsistenId);

                        return replicaListener.invoke(invocationOnMock.getArgument(1), nodeId).thenApply(ReplicaResult::result);
                    })
                    .when(replicaSvc).invoke(anyString(), any());

            lenient()
                    .doAnswer(invocationOnMock -> {
                        InternalClusterNode node = invocationOnMock.getArgument(0);

                        return replicaListener.invoke(invocationOnMock.getArgument(1), node.id())
                                .thenApply(DummyInternalTableImpl::dummyTimestampAwareResponse);
                    })
                    .when(replicaSvc).invokeRaw(any(InternalClusterNode.class), any());

            lenient()
                    .doAnswer(invocationOnMock -> {
                        String nodeConsistenId = invocationOnMock.getArgument(0);
                        UUID nodeId = deriveUuidFrom(nodeConsistenId);

                        return replicaListener.invoke(invocationOnMock.getArgument(1), nodeId)
                                .thenApply(DummyInternalTableImpl::dummyTimestampAwareResponse);
                    })
                    .when(replicaSvc).invokeRaw(anyString(), any());
        }

        AtomicLong raftIndex = new AtomicLong(1);

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    synchronized (raftServiceMutex) {
                        Command cmd = invocationClose.getArgument(0);

                        long commandIndex = raftIndex.incrementAndGet();

                        HybridTimestamp safeTs = cmd instanceof SafeTimePropagatingCommand ? CLOCK.now() : null;

                        CompletableFuture<Serializable> res = new CompletableFuture<>();

                        // All read commands are handled directly throw partition replica listener.
                        CommandClosure<WriteCommand> clo = new CommandClosure<>() {
                            /** {@inheritDoc} */
                            @Override
                            public long index() {
                                return commandIndex;
                            }

                            /** {@inheritDoc} */
                            @Override
                            public HybridTimestamp safeTimestamp() {
                                return safeTs;
                            }

                            /** {@inheritDoc} */
                            @Override
                            public @Nullable WriteCommand command() {
                                return (WriteCommand) cmd;
                            }

                            /** {@inheritDoc} */
                            @Override
                            public void result(@Nullable Serializable r) {
                                if (r instanceof Throwable) {
                                    res.completeExceptionally((Throwable) r);
                                } else {
                                    res.complete(r);
                                }
                            }
                        };

                        try {
                            partitionListener.onWrite(List.of(clo).iterator());
                        } catch (Throwable e) {
                            res.completeExceptionally(new TransactionException(INTERNAL_ERR, e));
                        }

                        return res;
                    }
                }
        ).when(svc).run(any());

        int tableId = tableId();
        int indexId = 1;

        ColumnsExtractor row2Tuple = BinaryRowConverter.keyExtractor(schema);

        StorageHashIndexDescriptor pkIndexDescriptor = mock(StorageHashIndexDescriptor.class);

        when(pkIndexDescriptor.columns()).then(
                invocation -> Collections.nCopies(schema.keyColumns().size(), mock(StorageHashIndexColumnDescriptor.class))
        );

        Lazy<TableSchemaAwareIndexStorage> pkStorage = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                indexId,
                new TestHashIndexStorage(PART_ID, pkIndexDescriptor),
                row2Tuple
        ));

        IndexLocker pkLocker = new HashIndexLocker(indexId, true, this.txManager.lockManager(), row2Tuple);

        safeTime = new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE);

        PartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(tableId, PART_ID, mvPartStorage);
        TableIndexStoragesSupplier indexes = createTableIndexStoragesSupplier(Map.of(pkStorage.get().id(), pkStorage.get()));

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexes);

        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                PART_ID,
                partitionDataStorage,
                indexUpdateHandler,
                replicationConfiguration,
                TableTestUtils.NOOP_PARTITION_MODIFICATION_COUNTER
        );

        DummySchemaManagerImpl schemaManager = new DummySchemaManagerImpl(schema);

        Catalog catalog = mock(Catalog.class);
        CatalogService catalogService = mock(CatalogService.class);
        CatalogTableDescriptor tableDescriptor = mock(CatalogTableDescriptor.class);

        lenient().when(catalogService.catalog(anyInt())).thenReturn(catalog);
        lenient().when(catalogService.activeCatalog(anyLong())).thenReturn(catalog);
        lenient().when(catalog.table(anyInt())).thenReturn(tableDescriptor);
        lenient().when(tableDescriptor.latestSchemaVersion()).thenReturn(1);

        CatalogIndexDescriptor indexDescriptor = mock(CatalogIndexDescriptor.class);
        lenient().when(indexDescriptor.id()).thenReturn(pkStorage.get().id());

        lenient().when(catalog.indexes(anyInt())).thenReturn(List.of(indexDescriptor));

        ZonePartitionId zonePartitionId = new ZonePartitionId(ZONE_ID, PART_ID);

        var tableReplicaListener = new PartitionReplicaListener(
                mvPartStorage,
                svc,
                this.txManager,
                this.txManager.lockManager(),
                Runnable::run,
                zonePartitionId,
                tableId,
                () -> Map.of(pkLocker.id(), pkLocker),
                pkStorage,
                Map::of,
                CLOCK_SERVICE,
                safeTime,
                transactionStateResolver,
                storageUpdateHandler,
                new DummyValidationSchemasSource(schemaManager),
                new AlwaysSyncedSchemaSyncService(),
                catalogService,
                mock(ClusterNodeResolver.class),
                resourcesRegistry,
                schemaManager,
                mock(IndexMetaStorage.class),
                new TestLowWatermark(),
                mock(FailureProcessor.class),
                new TableMetricSource(QualifiedName.fromSimple("dummy_table"))
        );

        ZonePartitionReplicaListener zoneReplicaListener = new ZonePartitionReplicaListener(
                txStateStorage.getOrCreatePartitionStorage(PART_ID),
                CLOCK_SERVICE,
                this.txManager,
                new DummyValidationSchemasSource(schemaManager),
                new AlwaysSyncedSchemaSyncService(),
                catalogService,
                placementDriver,
                mock(ClusterNodeResolver.class),
                svc,
                mock(FailureProcessor.class),
                LOCAL_NODE,
                zonePartitionId
        );

        zoneReplicaListener.addTableReplicaProcessor(tableId, raftClient -> tableReplicaListener);

        replicaListener = zoneReplicaListener;

        HybridClock clock = new HybridClockImpl();
        ClockService clockService = mock(ClockService.class);
        lenient().when(clockService.current()).thenReturn(clock.current());

        PendingComparableValuesTracker<Long, Void> storageIndexTracker = new PendingComparableValuesTracker<>(0L);
        var tablePartitionListener = new TablePartitionProcessor(
                this.txManager,
                new TestPartitionDataStorage(tableId, PART_ID, mvPartStorage),
                storageUpdateHandler,
                safeTime,
                catalogService,
                schemaManager,
                mock(IndexMetaStorage.class),
                LOCAL_NODE.id(),
                mock(MinimumRequiredTimeCollectorService.class),
                mock(Executor.class),
                placementDriver,
                clockService,
                zonePartitionId
        );

        ZonePartitionRaftListener zoneRaftListener = new ZonePartitionRaftListener(
                zonePartitionId,
                txStateStorage.getOrCreatePartitionStorage(PART_ID),
                this.txManager,
                safeTime,
                storageIndexTracker,
                new NoOpPartitionsSnapshots(),
                mock(Executor.class)
        );

        zoneRaftListener.addTableProcessor(tableId, tablePartitionListener);

        partitionListener = zoneRaftListener;

        // Update(All)Command handling requires both information about raft group topology and the primary replica,
        // thus onConfigurationCommited and primaryReplicaChangeCommand are called.
        {
            partitionListener.onConfigurationCommitted(
                    new RaftGroupConfiguration(
                            1,
                            1,
                            111L,
                            110L,
                            List.of(LOCAL_NODE.name()),
                            Collections.emptyList(),
                            null,
                            null
                    ),
                    1,
                    1
            );

            CompletableFuture<ReplicaMeta> primaryMetaFuture = placementDriver.getPrimaryReplica(groupId, CLOCK.now());

            assertThat(primaryMetaFuture, willCompleteSuccessfully());

            ReplicaMeta primary = primaryMetaFuture.join();

            PrimaryReplicaChangeCommand primaryReplicaChangeCommand = REPLICA_MESSAGES_FACTORY.primaryReplicaChangeCommand()
                    .leaseStartTime(primary.getStartTime().longValue())
                    .primaryReplicaNodeId(primary.getLeaseholderId())
                    .primaryReplicaNodeName(primary.getLeaseholder())
                    .build();

            assertThat(svc.run(primaryReplicaChangeCommand), willCompleteSuccessfully());
        }
    }

    private static TimestampAwareReplicaResponse dummyTimestampAwareResponse(ReplicaResult r) {
        return new TimestampAwareReplicaResponse() {
            @Override
            public @Nullable Object result() {
                return r.result();
            }

            @Override
            public @Nullable HybridTimestamp timestamp() {
                return CLOCK.now();
            }

            @Override
            public MessageSerializer<NetworkMessage> serializer() {
                return null;
            }

            @Override
            public short messageType() {
                return 0;
            }

            @Override
            public short groupType() {
                return 0;
            }

            @Override
            public NetworkMessage clone() {
                return null;
            }
        };
    }

    /**
     * Replica listener.
     *
     * @return Replica listener.
     */
    public ReplicaListener getReplicaListener() {
        return replicaListener;
    }

    /**
     * Group id of single partition of this table.
     *
     * @return Group id.
     */
    public ReplicationGroupId groupId() {
        return groupId;
    }

    /**
     * Gets the transaction manager that is bound to the table.
     *
     * @return Transaction manager.
     */
    public TxManager txManager() {
        return txManager;
    }

    /**
     * Creates a {@link TxManager}.
     *
     * @param replicaSvc Replica service to use.
     * @param placementDriver Placement driver.
     * @param txConfiguration Transaction configuration.
     * @param systemCfg System configuration.
     * @param resourcesRegistry Resources registry.
     */
    public static TxManagerImpl txManager(
            ReplicaService replicaSvc,
            PlacementDriver placementDriver,
            TransactionConfiguration txConfiguration,
            SystemDistributedConfiguration systemCfg,
            RemotelyTriggeredResourceRegistry resourcesRegistry
    ) {
        TopologyService topologyService = mock(TopologyService.class);
        when(topologyService.localMember()).thenReturn(LOCAL_NODE);

        ClusterService clusterService = mock(ClusterService.class);

        when(clusterService.messagingService()).thenReturn(new DummyMessagingService(LOCAL_NODE));
        when(clusterService.topologyService()).thenReturn(topologyService);

        TransactionInflights transactionInflights = new TransactionInflights(placementDriver, CLOCK_SERVICE);

        var txManager = new TxManagerImpl(
                txConfiguration,
                systemCfg,
                clusterService,
                replicaSvc,
                HeapLockManager.smallInstance(),
                CLOCK_SERVICE,
                new TransactionIdGenerator(0xdeadbeef),
                placementDriver,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                new TestLocalRwTxCounter(),
                resourcesRegistry,
                transactionInflights,
                new TestLowWatermark(),
                COMMON_SCHEDULER,
                new NoOpMetricManager()
        );

        assertThat(txManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        return txManager;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, InternalTransaction tx) {
        return super.get(keyRow, tx);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<InternalClusterNode> evaluateReadOnlyRecipientNode(int partId, @Nullable HybridTimestamp readTimestamp) {
        return completedFuture(LOCAL_NODE);
    }

    /**
     * Returns dummy table index storages supplier.
     *
     * @param indexes Index storage by ID.
     */
    public static TableIndexStoragesSupplier createTableIndexStoragesSupplier(Map<Integer, TableSchemaAwareIndexStorage> indexes) {
        return () -> indexes;
    }

    /**
     * Dummy messaging service for tests purposes. It does not provide any messaging functionality, but allows to trigger events.
     */
    private static class DummyMessagingService extends AbstractMessagingService {
        private final InternalClusterNode localNode;

        private final AtomicLong correlationIdGenerator = new AtomicLong();

        DummyMessagingService(InternalClusterNode localNode) {
            this.localNode = localNode;
        }

        /** {@inheritDoc} */
        @Override
        public void weakSend(InternalClusterNode recipient, NetworkMessage msg) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> send(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public CompletableFuture<Void> send(String recipientConsistentId, ChannelType channelType, NetworkMessage msg) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public CompletableFuture<Void> send(NetworkAddress recipientNetworkAddress, ChannelType channelType, NetworkMessage msg) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> respond(InternalClusterNode recipient, ChannelType type, NetworkMessage msg, long correlationId) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> respond(String recipientConsistentId, ChannelType type, NetworkMessage msg, long correlationId) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<NetworkMessage> invoke(InternalClusterNode recipient, ChannelType type, NetworkMessage msg, long timeout) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<NetworkMessage> invoke(String recipientNodeId, ChannelType type, NetworkMessage msg, long timeout) {
            getMessageHandlers(msg.groupType()).forEach(h -> h.onReceived(msg, localNode, correlationIdGenerator.getAndIncrement()));

            return nullCompletedFuture();
        }
    }

    private static class NoOpPartitionsSnapshots implements PartitionsSnapshots {
        @Override
        public PartitionSnapshots partitionSnapshots(PartitionKey partitionKey) {
            return mock(PartitionSnapshots.class);
        }

        @Override
        public void cleanupOutgoingSnapshots(PartitionKey partitionKey) {

        }

        @Override
        public void finishOutgoingSnapshot(UUID snapshotId) {

        }
    }
}
