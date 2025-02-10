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

package org.apache.ignite.internal.table.distributed;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils.calculateAssignments;
import static org.apache.ignite.internal.table.TableTestUtils.createHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.NodeConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageRevisionListenerRegistry;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.raft.MinimumRequiredTimeCollectorServiceImpl;
import org.apache.ignite.internal.partitiondistribution.PartitionDistributionUtils;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.IgniteSql;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Table manager recovery scenarios.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class, ExecutorServiceExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class TableManagerRecoveryTest extends IgniteAbstractTest {
    private static final String NODE_NAME = "testNode1";
    private static final String ZONE_NAME = "zone1";
    private static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;
    private static final String TABLE_NAME = "testTable";
    private static final String INDEX_NAME = "testIndex1";
    private static final String INDEXED_COLUMN_NAME = "columnName";
    private static final int PARTITIONS = 8;
    private static final ClusterNode node = new ClusterNodeImpl(UUID.randomUUID(), NODE_NAME, new NetworkAddress("127.0.0.1", 2245));
    private static final long WAIT_TIMEOUT = SECONDS.toMillis(10);

    // Configuration
    @InjectConfiguration("mock.profiles.default = {engine = aipersist}")
    private StorageConfiguration storageConfiguration;
    @InjectConfiguration
    private GcConfiguration gcConfig;
    @InjectConfiguration
    private TransactionConfiguration txConfig;
    @InjectConfiguration
    private StorageUpdateConfiguration storageUpdateConfiguration;
    @InjectConfiguration
    private SystemDistributedConfiguration systemDistributedConfiguration;

    // Table manager dependencies.
    private SchemaManager sm;
    private CatalogManager catalogManager;
    private MetaStorageManager metaStorageManager;
    private TxStateRocksDbSharedStorage sharedTxStateStorage;
    private TableManager tableManager;
    @InjectExecutorService(threadCount = 4, allowedOperations = {STORAGE_READ, STORAGE_WRITE})
    private ExecutorService partitionOperationsExecutor;
    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutor;
    private DataStorageManager dsm;
    private HybridClockImpl clock;
    private TestLowWatermark lowWatermark;

    private IndexMetaStorage indexMetaStorage;

    // Table internal components
    @Mock
    private ReplicaManager replicaMgr;
    @Mock
    private LogSyncer logSyncer;
    private volatile MvTableStorage mvTableStorage;
    private volatile TxStateStorage txStateStorage;

    private volatile HybridTimestamp savedWatermark;

    private final DataStorageModule dataStorageModule = createDataStorageModule();

    @AfterEach
    void after() throws Exception {
        stopComponents();
    }

    @Test
    public void testTableIgnoredOnRecovery() throws Exception {
        startComponents();

        createZone(ZONE_NAME);
        createTable(TABLE_NAME);
        createIndex(TABLE_NAME, INDEX_NAME);

        verify(mvTableStorage, timeout(WAIT_TIMEOUT).times(PARTITIONS)).createMvPartition(anyInt());
        verify(txStateStorage, timeout(WAIT_TIMEOUT).times(PARTITIONS)).getOrCreatePartitionStorage(anyInt());
        clearInvocations(mvTableStorage);
        clearInvocations(txStateStorage);

        int tableId = getTableIdStrict(catalogManager, TABLE_NAME, clock.nowLong());

        // Drop table and save watermark without triggering LWM events.
        dropTable(TABLE_NAME);

        savedWatermark = clock.now();

        stopComponents();
        startComponents();

        // Table below LWM shouldn't started.
        assertEquals(0, tableManager.startedTables().size());

        verify(mvTableStorage, never()).createMvPartition(anyInt());
        verify(txStateStorage, never()).getOrCreatePartitionStorage(anyInt());

        // Let's check that the table was deleted.
        verify(dsm.engineByStorageProfile(DEFAULT_STORAGE_PROFILE)).dropMvTable(eq(tableId));
    }

    @Test
    public void testTableStartedOnRecovery() throws Exception {
        startComponents();

        createZone(ZONE_NAME);
        createTable(TABLE_NAME);
        createIndex(TABLE_NAME, INDEX_NAME);

        int tableId = catalogManager.activeCatalog(clock.nowLong()).table(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME).id();

        verify(mvTableStorage, timeout(WAIT_TIMEOUT).times(PARTITIONS)).createMvPartition(anyInt());
        verify(txStateStorage, timeout(WAIT_TIMEOUT).times(PARTITIONS)).getOrCreatePartitionStorage(anyInt());
        clearInvocations(mvTableStorage);
        clearInvocations(txStateStorage);

        // Drop table.
        dropTable(TABLE_NAME);

        stopComponents();
        startComponents();

        // Table is available after restart.
        assertThat(tableManager.startedTables().keySet(), contains(tableId));

        verify(mvTableStorage, timeout(WAIT_TIMEOUT).times(PARTITIONS)).createMvPartition(anyInt());
        verify(txStateStorage, timeout(WAIT_TIMEOUT).times(PARTITIONS)).getOrCreatePartitionStorage(anyInt());
    }

    /**
     * Creates and starts TableManage and dependencies.
     */
    private void startComponents() throws Exception {
        var readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

        var storage = new RocksDbKeyValueStorage(
                NODE_NAME,
                workDir,
                new NoOpFailureManager(),
                readOperationForCompactionTracker,
                scheduledExecutor
        );

        clock = new HybridClockImpl();

        ClusterService clusterService = mock(ClusterService.class);
        TopologyService topologyService = mock(TopologyService.class);
        DistributionZoneManager distributionZoneManager = mock(DistributionZoneManager.class);
        TxManager tm = mock(TxManager.class);
        Loza rm = mock(Loza.class);
        RaftGroupService raftGrpSrvcMock = mock(TopologyAwareRaftGroupService.class);

        when(raftGrpSrvcMock.leader()).thenReturn(new Peer("node0"));
        when(rm.startRaftGroupService(any(), any(), any(), any())).thenAnswer(mock -> raftGrpSrvcMock);

        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class));
        when(clusterService.topologyService()).thenReturn(topologyService);
        when(topologyService.localMember()).thenReturn(node);
        when(distributionZoneManager.dataNodes(anyLong(), anyInt(), anyInt())).thenReturn(completedFuture(Set.of(NODE_NAME)));

        when(replicaMgr.startReplica(any(), any(), anyBoolean(), any(), any(), any(), any(), any()))
                .thenReturn(nullCompletedFuture());
        when(replicaMgr.stopReplica(any())).thenReturn(trueCompletedFuture());
        when(replicaMgr.weakStartReplica(any(), any(), any())).thenReturn(trueCompletedFuture());
        when(replicaMgr.weakStopReplica(any(), any(), any())).thenReturn(nullCompletedFuture());

        try (MockedStatic<SchemaUtils> schemaServiceMock = mockStatic(SchemaUtils.class)) {
            schemaServiceMock.when(() -> SchemaUtils.prepareSchemaDescriptor(any()))
                    .thenReturn(mock(SchemaDescriptor.class));
        }

        try (MockedStatic<PartitionDistributionUtils> partitionDistributionServiceMock = mockStatic(PartitionDistributionUtils.class)) {
            ArrayList<List<ClusterNode>> assignment = new ArrayList<>(PARTITIONS);

            for (int part = 0; part < PARTITIONS; part++) {
                assignment.add(new ArrayList<>(Collections.singleton(node)));
            }

            partitionDistributionServiceMock.when(() -> calculateAssignments(any(), anyInt(), anyInt()))
                    .thenReturn(assignment);
        }

        metaStorageManager = StandaloneMetaStorageManager.create(storage, clock, readOperationForCompactionTracker);
        catalogManager = createTestCatalogManager(NODE_NAME, clock, metaStorageManager);

        var revisionUpdater = new MetaStorageRevisionListenerRegistry(metaStorageManager);

        PlacementDriver placementDriver = new TestPlacementDriver(node);

        lowWatermark = new TestLowWatermark();
        lowWatermark.updateWithoutNotify(savedWatermark);
        ClockService clockService = new TestClockService(clock);

        indexMetaStorage = new IndexMetaStorage(catalogManager, lowWatermark, metaStorageManager);

        dsm = createDataStorageManager();

        AlwaysSyncedSchemaSyncService schemaSyncService = new AlwaysSyncedSchemaSyncService();

        MinimumRequiredTimeCollectorServiceImpl minTimeCollectorService = new MinimumRequiredTimeCollectorServiceImpl();

        sharedTxStateStorage = new TxStateRocksDbSharedStorage(
                workDir.resolve("tx-state"),
                scheduledExecutor,
                partitionOperationsExecutor,
                logSyncer
        );

        tableManager = new TableManager(
                NODE_NAME,
                revisionUpdater,
                gcConfig,
                txConfig,
                storageUpdateConfiguration,
                clusterService.messagingService(),
                clusterService.topologyService(),
                clusterService.serializationRegistry(),
                replicaMgr,
                null,
                null,
                tm,
                dsm,
                sharedTxStateStorage,
                metaStorageManager,
                sm = new SchemaManager(revisionUpdater, catalogManager),
                partitionOperationsExecutor,
                partitionOperationsExecutor,
                scheduledExecutor,
                scheduledExecutor,
                clockService,
                new OutgoingSnapshotsManager(clusterService.messagingService()),
                distributionZoneManager,
                schemaSyncService,
                catalogManager,
                HybridTimestampTracker.atomicTracker(null),
                placementDriver,
                () -> mock(IgniteSql.class),
                new RemotelyTriggeredResourceRegistry(),
                lowWatermark,
                new TransactionInflights(placementDriver, clockService),
                indexMetaStorage,
                logSyncer,
                new PartitionReplicaLifecycleManager(
                        catalogManager,
                        replicaMgr,
                        distributionZoneManager,
                        metaStorageManager,
                        topologyService,
                        lowWatermark,
                        ForkJoinPool.commonPool(),
                        mock(ScheduledExecutorService.class),
                        partitionOperationsExecutor,
                        clockService,
                        placementDriver,
                        schemaSyncService,
                        systemDistributedConfiguration,
                        sharedTxStateStorage,
                        minTimeCollectorService
                ),
                minTimeCollectorService,
                systemDistributedConfiguration
        ) {

            @Override
            protected MvTableStorage createTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
                mvTableStorage = spy(super.createTableStorage(tableDescriptor, zoneDescriptor));

                return mvTableStorage;
            }

            @Override
            protected TxStateStorage createTxStateTableStorage(
                    CatalogTableDescriptor tableDescriptor,
                    CatalogZoneDescriptor zoneDescriptor
            ) {
                txStateStorage = spy(super.createTxStateTableStorage(tableDescriptor, zoneDescriptor));

                return txStateStorage;
            }
        };

        tableManager.setStreamerReceiverRunner(mock(StreamerReceiverRunner.class));

        var componentContext = new ComponentContext();

        assertThat(
                metaStorageManager.startAsync(componentContext)
                        .thenCompose(unused -> metaStorageManager.recoveryFinishedFuture())
                        .thenCompose(unused -> startAsync(
                                componentContext,
                                catalogManager,
                                sm,
                                indexMetaStorage,
                                sharedTxStateStorage,
                                tableManager
                        ))
                        .thenCompose(unused -> ((MetaStorageManagerImpl) metaStorageManager).notifyRevisionUpdateListenerOnStart())
                        .thenCompose(unused -> metaStorageManager.deployWatches()),
                willCompleteSuccessfully()
        );
    }

    /** Stops TableManager and dependencies. */
    private void stopComponents() throws Exception {
        closeAll(
                tableManager == null ? null : tableManager::beforeNodeStop,
                dsm == null ? null : dsm::beforeNodeStop,
                sm == null ? null : sm::beforeNodeStop,
                indexMetaStorage == null ? null : indexMetaStorage::beforeNodeStop,
                catalogManager == null ? null : catalogManager::beforeNodeStop,
                metaStorageManager == null ? null : metaStorageManager::beforeNodeStop,
                () -> assertThat(
                        stopAsync(
                                new ComponentContext(),
                                tableManager,
                                sharedTxStateStorage,
                                dsm,
                                sm,
                                indexMetaStorage,
                                catalogManager,
                                metaStorageManager
                        ),
                        willCompleteSuccessfully()
                )
        );
    }

    private DataStorageManager createDataStorageManager() {
        ConfigurationRegistry mockedRegistry = mock(ConfigurationRegistry.class);

        StorageExtensionConfiguration mock = mock(StorageExtensionConfiguration.class);
        when(mockedRegistry.getConfiguration(NodeConfiguration.KEY)).thenReturn(mock);
        when(mock.storage()).thenReturn(storageConfiguration);

        DataStorageModules dataStorageModules = new DataStorageModules(List.of(dataStorageModule));

        DataStorageManager manager = new DataStorageManager(
                dataStorageModules.createStorageEngines(
                        NODE_NAME,
                        mockedRegistry,
                        workDir,
                        null,
                        mock(FailureManager.class),
                        mock(LogSyncer.class),
                        clock,
                        scheduledExecutor
                ),
                storageConfiguration
        );

        assertThat(manager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        return manager;
    }

    private void createTable(String tableName) {
        TableTestUtils.createTable(
                catalogManager,
                SqlCommon.DEFAULT_SCHEMA_NAME,
                ZONE_NAME,
                tableName,
                List.of(
                        ColumnParams.builder().name("key").type(INT64).build(),
                        ColumnParams.builder().name(INDEXED_COLUMN_NAME).type(INT64).nullable(true).build()
                ),
                List.of("key")
        );
    }

    private void createZone(String zoneName) {
        DistributionZonesTestUtil.createZone(catalogManager, zoneName, PARTITIONS, 1);
    }

    private void dropTable(String tableName) {
        TableTestUtils.dropTable(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, tableName);
    }

    private void createIndex(String tableName, String indexName) {
        createHashIndex(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, tableName, indexName, List.of(INDEXED_COLUMN_NAME), false);
    }

    private void dropIndex(String indexName) {
        TableTestUtils.dropIndex(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, indexName);
    }

    private static PersistentPageMemoryDataStorageModule createDataStorageModule() {
        return new PersistentPageMemoryDataStorageModule() {
            @Override
            public StorageEngine createEngine(
                    String igniteInstanceName,
                    ConfigurationRegistry configRegistry,
                    Path storagePath,
                    @Nullable LongJvmPauseDetector longJvmPauseDetector,
                    FailureManager failureManager,
                    LogSyncer logSyncer,
                    HybridClock clock,
                    ScheduledExecutorService commonScheduler
            ) throws StorageException {
                return spy(super.createEngine(
                        igniteInstanceName,
                        configRegistry,
                        storagePath,
                        longJvmPauseDetector,
                        failureManager,
                        logSyncer,
                        clock,
                        commonScheduler
                ));
            }
        };
    }
}
