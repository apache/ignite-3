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
import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_DROP;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.apache.ignite.sql.ColumnType.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.causality.RevisionListenerRegistry;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.NodeConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.impl.MetaStorageRevisionListenerRegistry;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.AlwaysSyncedSchemaSyncService;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorServiceImpl;
import org.apache.ignite.internal.table.distributed.storage.BrokenTxStateStorage;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.testframework.failure.FailureManagerExtension;
import org.apache.ignite.internal.testframework.failure.MuteFailureManagerLogging;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.util.CursorUtils;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Tests scenarios for table manager. */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class, ExecutorServiceExtension.class, FailureManagerExtension.class})
public class TableManagerTest extends IgniteAbstractTest {
    private static final long VERIFICATION_TIMEOUT = TimeUnit.SECONDS.toMillis(10);

    /** The name of the table which is preconfigured. */
    private static final String PRECONFIGURED_TABLE_NAME = "T1";

    /** The name of the table which will be configured dynamically. */
    private static final String DYNAMIC_TABLE_NAME = "T2";

    /** The name of table to drop it. */
    private static final String DYNAMIC_TABLE_FOR_DROP_NAME = "T3";

    /** Table partitions. */
    private static final int PARTITIONS = 32;

    /** Node name. */
    private static final String NODE_NAME = "node1";

    /** Count of replicas. */
    private static final int REPLICAS = 1;

    /** Zone name. */
    private static final String ZONE_NAME = "zone1";

    /** Replica manager. */
    @Mock
    private ReplicaManager replicaMgr;

    /** Raft log syncer. */
    @Mock
    private LogSyncer logSyncer;

    /** TX manager. */
    @Mock
    private TxManager tm;

    /** Meta storage manager. */
    @Mock
    private MetaStorageManager msm;

    /** Mock cluster service. */
    @Mock
    private ClusterService clusterService;

    private volatile MvTableStorage mvTableStorage;

    private volatile TxStateStorage txStateStorage;

    /** Revision updater. */
    private RevisionListenerRegistry revisionUpdater;

    /** Garbage collector configuration. */
    @InjectConfiguration
    private GcConfiguration gcConfig;

    @InjectConfiguration
    private TransactionConfiguration txConfig;

    /** Storage update configuration. */
    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    @InjectConfiguration
    private SystemDistributedConfiguration systemDistributedConfiguration;

    @InjectConfiguration("mock.storage = {profiles.default = {engine = \"aipersist\"}}")
    private NodeConfiguration nodeConfiguration;

    @Mock
    private ConfigurationRegistry configRegistry;

    private DataStorageManager dsm;

    private SchemaManager sm;

    @Mock
    private DistributionZoneManager distributionZoneManager;

    /** Test node. */
    private final ClusterNode node = new ClusterNodeImpl(
            UUID.randomUUID(),
            NODE_NAME,
            new NetworkAddress("127.0.0.1", 2245)
    );

    /** The future will be completed after each tests of this class. */
    private CompletableFuture<TableManager> tblManagerFut;

    /** Hybrid clock. */
    private final HybridClock clock = new HybridClockImpl();

    /** Catalog metastore. */
    private MetaStorageManager catalogMetastore;

    /** Catalog manager. */
    private CatalogManager catalogManager;

    @InjectExecutorService(threadCount = 5, allowedOperations = {STORAGE_READ, STORAGE_WRITE})
    private ExecutorService partitionOperationsExecutor;

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutor;

    private TestLowWatermark lowWatermark;

    private IndexMetaStorage indexMetaStorage;

    /** Partition replica lifecycle manager. */
    @Mock
    private PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    @BeforeEach
    void before() {
        lowWatermark = new TestLowWatermark();
        catalogMetastore = StandaloneMetaStorageManager.create(NODE_NAME, clock);
        catalogManager = CatalogTestUtils.createTestCatalogManager(NODE_NAME, clock, catalogMetastore);
        indexMetaStorage = new IndexMetaStorage(catalogManager, lowWatermark, catalogMetastore);

        ComponentContext context = new ComponentContext();
        assertThat(startAsync(context, catalogMetastore), willCompleteSuccessfully());
        assertThat(catalogMetastore.recoveryFinishedFuture(), willCompleteSuccessfully());

        assertThat(startAsync(context, catalogManager, indexMetaStorage), willCompleteSuccessfully());

        revisionUpdater = new MetaStorageRevisionListenerRegistry(catalogMetastore);

        assertThat(catalogMetastore.deployWatches(), willCompleteSuccessfully());

        assertThat("Catalog initialization", catalogManager.catalogInitializationFuture(), willCompleteSuccessfully());

        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class));

        tblManagerFut = new CompletableFuture<>();

        mockMetastore();
    }

    @AfterEach
    void after() throws Exception {
        var componentContext = new ComponentContext();

        closeAll(
                () -> {
                    assertTrue(tblManagerFut.isDone());

                    tblManagerFut.join().beforeNodeStop();
                    assertThat(tblManagerFut.join().stopAsync(componentContext), willCompleteSuccessfully());
                },
                dsm == null ? null : dsm::beforeNodeStop,
                sm == null ? null : sm::beforeNodeStop,
                indexMetaStorage == null ? null : indexMetaStorage::beforeNodeStop,
                catalogManager == null ? null : catalogManager::beforeNodeStop,
                catalogMetastore == null ? null : catalogMetastore::beforeNodeStop,
                () -> assertThat(
                        stopAsync(componentContext, dsm, sm, indexMetaStorage, catalogManager, catalogMetastore),
                        willCompleteSuccessfully()
                )
        );
    }

    /**
     * Tests a table which was preconfigured.
     */
    @Test
    public void testPreconfiguredTable() throws Exception {
        if (colocationEnabled()) {
            mockZoneLockForRead();
        } else {
            when(distributionZoneManager.dataNodes(any(), anyInt(), anyInt())).thenReturn(emptySetCompletedFuture());

            when(msm.invoke(any(), anyList(), anyList())).thenReturn(trueCompletedFuture());

            when(replicaMgr.stopReplica(any())).thenReturn(trueCompletedFuture());
        }

        TableManager tableManager = createTableManager(tblManagerFut);

        tblManagerFut.complete(tableManager);

        createZone(PARTITIONS, REPLICAS);

        createTable(PRECONFIGURED_TABLE_NAME);

        assertEquals(1, tableManager.tables().size());

        Table table = tableManager.table(PRECONFIGURED_TABLE_NAME);
        assertNotNull(table);

        if (colocationEnabled()) {
            InternalTable internalTable = Wrappers.unwrap(table, TableImpl.class).internalTable();
            assertThat(internalTable.txStateStorage(), isA(BrokenTxStateStorage.class));
        }
    }

    /**
     * Tests create a table through public API.
     *
     */
    @Test
    public void testCreateTable() throws Exception {
        if (colocationEnabled()) {
            mockZoneLockForRead();
        } else {
            mockReplicaServicesExtended();
        }

        Table table = mockManagersAndCreateTable(DYNAMIC_TABLE_NAME, tblManagerFut);

        assertNotNull(table);

        assertSame(table, tblManagerFut.join().table(DYNAMIC_TABLE_NAME));

        if (colocationEnabled()) {
            InternalTable internalTable = Wrappers.unwrap(table, TableImpl.class).internalTable();
            assertThat(internalTable.txStateStorage(), isA(BrokenTxStateStorage.class));
        }
    }

    /**
     * Testing TableManager#writeTableAssignmentsToMetastore for 2 exceptional scenarios:
     * 1. the method was interrupted in outer future before invoke calling completion.
     * 2. the method was interrupted in inner metastore's future when the result of invocation had gotten, but after error happens;
     *
     */
    @Test
    @MuteFailureManagerLogging
    public void testWriteTableAssignmentsToMetastoreExceptionally() throws Exception {
        if (colocationEnabled()) {
            mockZoneLockForRead();
        } else {
            mockReplicaServicesExtended();
        }

        when(msm.invoke(any(), anyList(), anyList())).thenReturn(trueCompletedFuture());

        TableViewInternal table = mockManagersAndCreateTable(DYNAMIC_TABLE_NAME, tblManagerFut);
        int tableId = table.tableId();

        assertThat(tblManagerFut, willCompleteSuccessfully());

        var assignmentsService = new TableAssignmentsService(msm, catalogManager, distributionZoneManager, new NoOpFailureManager());
        long assignmentsTimestamp = catalogManager.catalog(catalogManager.latestCatalogVersion()).time();
        List<Assignments> assignmentsList = List.of(Assignments.of(assignmentsTimestamp, Assignment.forPeer(node.name())));

        // the first case scenario
        CompletableFuture<List<Assignments>> assignmentsFuture = new CompletableFuture<>();
        var outerExceptionMsg = "Outer future is interrupted";
        assignmentsFuture.completeExceptionally(new TimeoutException(outerExceptionMsg));
        CompletableFuture<List<Assignments>> writtenAssignmentsFuture = assignmentsService
                .writeTableAssignmentsToMetastore(tableId, ConsistencyMode.STRONG_CONSISTENCY, assignmentsFuture);
        assertTrue(writtenAssignmentsFuture.isCompletedExceptionally());
        assertThrowsWithCause(writtenAssignmentsFuture::get, TimeoutException.class, outerExceptionMsg);

        // the second case scenario
        assignmentsFuture = completedFuture(assignmentsList);
        CompletableFuture<Boolean> invokeTimeoutFuture = new CompletableFuture<>();
        var innerExceptionMsg = "Inner future is interrupted";
        invokeTimeoutFuture.completeExceptionally(new TimeoutException(innerExceptionMsg));
        when(msm.invoke(any(), anyList(), anyList())).thenReturn(invokeTimeoutFuture);
        writtenAssignmentsFuture =
                assignmentsService.writeTableAssignmentsToMetastore(tableId, ConsistencyMode.STRONG_CONSISTENCY, assignmentsFuture);
        assertTrue(writtenAssignmentsFuture.isCompletedExceptionally());
        assertThrowsWithCause(writtenAssignmentsFuture::get, TimeoutException.class, innerExceptionMsg);
    }

    /**
     * Tests drop a table through public API.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropTable() throws Exception {
        if (colocationEnabled()) {
            when(partitionReplicaLifecycleManager.unloadTableResourcesFromZoneReplica(any(), anyInt())).thenReturn(nullCompletedFuture());

            mockZoneLockForRead();
        } else {
            mockReplicaServicesExtended();

            when(msm.removeAll(any())).thenReturn(nullCompletedFuture());
        }

        mockManagersAndCreateTable(DYNAMIC_TABLE_FOR_DROP_NAME, tblManagerFut);

        TableManager tableManager = tblManagerFut.join();

        dropTable(DYNAMIC_TABLE_FOR_DROP_NAME);

        assertNull(tableManager.table(DYNAMIC_TABLE_FOR_DROP_NAME));
        assertEquals(0, tableManager.tables().size());

        verify(mvTableStorage, never()).destroy();
        verify(txStateStorage, never()).destroy();
        verify(replicaMgr, never()).stopReplica(any());

        assertThat(fireDestroyEvent(), willCompleteSuccessfully());

        verify(mvTableStorage, timeout(VERIFICATION_TIMEOUT)).destroy();
        verify(txStateStorage, timeout(VERIFICATION_TIMEOUT)).destroy();

        if (colocationEnabled()) {
            verify(replicaMgr, never()).stopReplica(any());
        } else {
            verify(replicaMgr, timeout(VERIFICATION_TIMEOUT).times(PARTITIONS)).stopReplica(any());
        }
    }

    /**
     * Tests create a table through public API right after another table with the same name was dropped.
     *
     */
    @Test
    public void testReCreateTableWithSameName() throws Exception {
        if (colocationEnabled()) {
            when(partitionReplicaLifecycleManager.lockZoneForRead(anyInt()))
                    .thenReturn(completedFuture(1L));
        } else {
            mockReplicaServicesExtended();
        }

        mockManagersAndCreateTable(DYNAMIC_TABLE_NAME, tblManagerFut);

        TableManager tableManager = tblManagerFut.join();

        TableViewInternal table = (TableViewInternal) tableManager.table(DYNAMIC_TABLE_NAME);

        assertNotNull(table);

        int oldTableId = table.tableId();

        dropTable(DYNAMIC_TABLE_NAME);
        createTable(DYNAMIC_TABLE_NAME);

        table = tableManager.tableView(table.qualifiedName());

        assertNotNull(table);
        assertNotEquals(oldTableId, table.tableId());

        assertNotNull(tableManager.cachedTable(oldTableId));
        assertNotNull(tableManager.cachedTable(table.tableId()));
        assertNotSame(tableManager.cachedTable(oldTableId), tableManager.cachedTable(table.tableId()));
    }

    /**
     * Tests a work of the public API for Table manager {@see org.apache.ignite.table.manager.IgniteTables} when the manager is stopping.
     */
    @Test
    public void testApiTableManagerOnStop() {
        createTableManager(tblManagerFut);

        TableManager tableManager = tblManagerFut.join();

        tableManager.beforeNodeStop();
        assertThat(tableManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        assertThrowsWithCause(tableManager::tables, NodeStoppingException.class);
        assertThrowsWithCause(() -> tableManager.table(DYNAMIC_TABLE_FOR_DROP_NAME), NodeStoppingException.class);

        assertThat(tableManager.tablesAsync(), willThrow(NodeStoppingException.class));
        assertThat(tableManager.tableAsync(DYNAMIC_TABLE_FOR_DROP_NAME), willThrow(NodeStoppingException.class));
    }

    /**
     * Tests a work of the public API for Table manager {@see org.apache.ignite.internal.table.IgniteTablesInternal} when the manager is
     * stopping.
     */
    @Test
    public void testInternalApiTableManagerOnStop() {
        createTableManager(tblManagerFut);

        TableManager tableManager = tblManagerFut.join();

        tableManager.beforeNodeStop();
        assertThat(tableManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
        int fakeTblId = 1;

        assertThrowsWithCause(() -> tableManager.table(fakeTblId), NodeStoppingException.class);
        assertThat(tableManager.tableAsync(fakeTblId), willThrow(NodeStoppingException.class));
    }

    /**
     * Checks that all RAFT nodes will be stopped when Table manager is stopping and an exception that was thrown by one of the
     * components will not prevent stopping other components.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "false")
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove this test when zone colocation will be the only implementation.
    public void tableManagerStopTest1() throws Exception {
        IgniteBiTuple<TableViewInternal, TableManager> tblAndMnr = startTableManagerStopTest();

        endTableManagerStopTest(tblAndMnr.get1(), tblAndMnr.get2(), () -> {});
    }

    /**
     * Checks that all RAFT nodes will be stopped when Table manager is stopping and an exception that was thrown by one of the
     * components will not prevent stopping other components.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "false")
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove this test when zone colocation will be the only implementation.
    public void tableManagerStopTest2() throws Exception {
        IgniteBiTuple<TableViewInternal, TableManager> tblAndMnr = startTableManagerStopTest();

        endTableManagerStopTest(tblAndMnr.get1(), tblAndMnr.get2(),
                () -> {
                    try {
                        when(replicaMgr.stopReplica(any())).thenThrow(NodeStoppingException.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Checks that all RAFT nodes will be stopped when Table manager is stopping and an exception that was thrown by one of the
     * components will not prevent stopping other components.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "false")
    @MuteFailureManagerLogging
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove this test when zone colocation will be the only implementation.
    public void tableManagerStopTest3() throws Exception {
        IgniteBiTuple<TableViewInternal, TableManager> tblAndMnr = startTableManagerStopTest();

        endTableManagerStopTest(tblAndMnr.get1(), tblAndMnr.get2(),
                () -> {
                    try {
                        doThrow(new RuntimeException("Test exception")).when(tblAndMnr.get1().internalTable().storage()).close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Checks that all RAFT nodes will be stopped when Table manager is stopping and an exception that was thrown by one of the
     * components will not prevent stopping other components.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "false")
    @MuteFailureManagerLogging
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove this test when zone colocation will be the only implementation.
    public void tableManagerStopTest4() throws Exception {
        IgniteBiTuple<TableViewInternal, TableManager> tblAndMnr = startTableManagerStopTest();

        endTableManagerStopTest(tblAndMnr.get1(), tblAndMnr.get2(),
                () -> doThrow(new RuntimeException()).when(tblAndMnr.get1().internalTable().txStateStorage()).close());
    }

    private IgniteBiTuple<TableViewInternal, TableManager> startTableManagerStopTest() throws Exception {
        mockReplicaServicesExtended();

        TableViewInternal table = mockManagersAndCreateTable(DYNAMIC_TABLE_FOR_DROP_NAME, tblManagerFut);

        TableManager tableManager = tblManagerFut.join();

        return new IgniteBiTuple<>(table, tableManager);
    }

    private void endTableManagerStopTest(TableViewInternal table, TableManager tableManager, Runnable mockDoThrow) throws Exception {
        mockDoThrow.run();

        tableManager.beforeNodeStop();
        assertThat(tableManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        verify(replicaMgr, times(PARTITIONS)).stopReplica(any());

        verify(table.internalTable().storage()).close();
        verify(table.internalTable().txStateStorage()).close();
    }

    /**
     * Instantiates a table and prepares Table manager.
     */
    @Test
    public void testGetTableDuringCreation() throws Exception {
        if (colocationEnabled()) {
            mockZoneLockForRead();
        } else {
            mockReplicaServicesExtended();
        }

        Phaser phaser = new Phaser(2);

        CompletableFuture<Table> createFut = CompletableFuture.supplyAsync(() -> {
            try {
                return mockManagersAndCreateTableWithDelay(DYNAMIC_TABLE_FOR_DROP_NAME, tblManagerFut, phaser);
            } catch (Exception e) {
                fail(e.getMessage());
            }

            return null;
        });

        CompletableFuture<Table> getFut = CompletableFuture.supplyAsync(() -> {
            phaser.awaitAdvance(0);

            return tblManagerFut.join().table(DYNAMIC_TABLE_FOR_DROP_NAME);
        });

        CompletableFuture<Collection<Table>> getAllTablesFut = CompletableFuture.supplyAsync(() -> {
            phaser.awaitAdvance(0);

            return tblManagerFut.join().tables();
        });

        assertFalse(createFut.isDone());
        assertFalse(getFut.isDone());
        assertFalse(getAllTablesFut.isDone());

        phaser.arrive();

        assertSame(createFut.join(), getFut.join());

        assertEquals(1, getAllTablesFut.join().size());
    }

    @Test
    @WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "false")
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove this test when zone colocation will be the only implementation.
    void testStoragesGetClearedInMiddleOfFailedTxStorageRebalance() throws Exception {
        testStoragesGetClearedInMiddleOfFailedRebalance(true);
    }

    @Test
    @WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "false")
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove this test when zone colocation will be the only implementation.
    void testStoragesGetClearedInMiddleOfFailedPartitionStorageRebalance() throws Exception {
        testStoragesGetClearedInMiddleOfFailedRebalance(false);
    }

    /**
     * Emulates a situation, when either a TX state storage or partition storage were stopped in a middle of a rebalance. We then expect
     * that these storages get cleared upon startup.
     *
     * @param isTxStorageUnderRebalance When {@code true} - TX state storage is emulated as being under rebalance, when {@code false} -
     *         partition storage is emulated instead.
     */
    private void testStoragesGetClearedInMiddleOfFailedRebalance(boolean isTxStorageUnderRebalance) throws Exception {
        if (!colocationEnabled()) {
            mockReplicaServicesExtended();
        }

        when(distributionZoneManager.dataNodes(any(), anyInt(), anyInt()))
                .thenReturn(completedFuture(Set.of(NODE_NAME)));

        when(msm.invoke(any(), anyList(), anyList())).thenReturn(trueCompletedFuture());

        var txStateStorage = mock(TxStatePartitionStorage.class);
        var mvPartitionStorage = mock(MvPartitionStorage.class);

        if (isTxStorageUnderRebalance) {
            // Emulate a situation when TX state storage was stopped in a middle of rebalance.
            when(txStateStorage.lastAppliedIndex()).thenReturn(TxStatePartitionStorage.REBALANCE_IN_PROGRESS);
        } else {
            // Emulate a situation when partition storage was stopped in a middle of rebalance.
            when(mvPartitionStorage.lastAppliedIndex()).thenReturn(MvPartitionStorage.REBALANCE_IN_PROGRESS);
        }

        when(txStateStorage.clear()).thenReturn(nullCompletedFuture());

        when(msm.recoveryFinishedFuture()).thenReturn(completedFuture(new Revisions(2, -1)));

        // For some reason, "when(something).thenReturn" does not work on spies, but this notation works.
        createTableManager(tblManagerFut, (mvTableStorage) -> {
            doReturn(completedFuture(mvPartitionStorage)).when(mvTableStorage).createMvPartition(anyInt());
            doReturn(mvPartitionStorage).when(mvTableStorage).getMvPartition(anyInt());
            doReturn(nullCompletedFuture()).when(mvTableStorage).clearPartition(anyInt());
        }, (txStateTableStorage) -> {
            doReturn(txStateStorage).when(txStateTableStorage).getOrCreatePartitionStorage(anyInt());
            doReturn(txStateStorage).when(txStateTableStorage).getPartitionStorage(anyInt());
        });

        createZone(1, 1);

        createTable(PRECONFIGURED_TABLE_NAME);

        verify(txStateStorage, timeout(1000)).clear();
        verify(mvTableStorage, timeout(1000)).clearPartition(anyInt());
    }

    /**
     * Instantiates Table manager and creates a table in it.
     *
     * @param tableName Table name.
     * @param tblManagerFut Future for table manager.
     * @return Table.
     */
    private TableViewInternal mockManagersAndCreateTable(String tableName, CompletableFuture<TableManager> tblManagerFut) {
        return mockManagersAndCreateTableWithDelay(tableName, tblManagerFut, null);
    }

    /** Dummy metastore activity mock. */
    private void mockMetastore() {
        when(msm.recoveryFinishedFuture()).thenReturn(completedFuture(new Revisions(2, -1)));

        when(msm.prefixLocally(any(), anyLong())).thenReturn(CursorUtils.emptyCursor());
    }

    private void mockZoneLockForRead() {
        when(partitionReplicaLifecycleManager.lockZoneForRead(anyInt())).thenReturn(completedFuture(1L));
        when(partitionReplicaLifecycleManager.lockZoneForRead(anyInt())).thenReturn(completedFuture(100L));
    }

    private void mockReplicaServicesExtended() throws Exception {
        mockReplicaServices();

        when(replicaMgr.startReplica(any(), any(), anyBoolean(), any(), any(), any(), any(), any()))
                .thenReturn(nullCompletedFuture());

        when(replicaMgr.weakStartReplica(any(), any(), any())).thenAnswer(inv -> {
            Supplier<CompletableFuture<Void>> startOperation = inv.getArgument(1);
            return startOperation.get();
        });
    }

    private void mockReplicaServices() throws Exception {
        TopologyService topologyService = mock(TopologyService.class);

        when(clusterService.topologyService()).thenReturn(topologyService);
        when(topologyService.localMember()).thenReturn(node);

        when(replicaMgr.stopReplica(any())).thenReturn(trueCompletedFuture());
    }

    /**
     * Instantiates a table and prepares Table manager. When the latch would open, the method completes.
     *
     * @param tableName Table name.
     * @param tblManagerFut Future for table manager.
     * @param phaser Phaser for the wait.
     * @return Table manager.
     */
    private TableViewInternal mockManagersAndCreateTableWithDelay(
            String tableName,
            CompletableFuture<TableManager> tblManagerFut,
            @Nullable Phaser phaser
    ) {
        if (!colocationEnabled()) {
            when(distributionZoneManager.dataNodes(any(), anyInt(), anyInt()))
                    .thenReturn(completedFuture(Set.of(NODE_NAME)));

            when(msm.invoke(any(), anyList(), anyList())).thenReturn(trueCompletedFuture());
        }

        TableManager tableManager = createTableManager(tblManagerFut);

        int tablesBeforeCreation = tableManager.tables().size();

        if (phaser != null) {
            catalogManager.listen(TABLE_CREATE, parameters -> {
                phaser.arriveAndAwaitAdvance();

                return falseCompletedFuture();
            });

            catalogManager.listen(TABLE_DROP, parameters -> {
                phaser.arriveAndAwaitAdvance();

                return falseCompletedFuture();
            });
        }

        createZone(PARTITIONS, REPLICAS);

        createTable(tableName);

        TableViewInternal tbl2 = tableManager.tableView(QualifiedName.fromSimple(tableName));

        assertNotNull(tbl2);

        assertEquals(tablesBeforeCreation + 1, tableManager.tables().size());

        return tbl2;
    }

    private TableManager createTableManager(CompletableFuture<TableManager> tblManagerFut) {
        return createTableManager(tblManagerFut, unused -> {}, unused -> {});
    }

    /**
     * Creates Table manager.
     *
     * @param tblManagerFut Future to wrap Table manager.
     * @param tableStorageDecorator Table storage spy decorator.
     * @param txStateTableStorageDecorator Tx state table storage spy decorator.
     *
     * @return Table manager.
     */
    private TableManager createTableManager(
            CompletableFuture<TableManager> tblManagerFut,
            Consumer<MvTableStorage> tableStorageDecorator,
            Consumer<TxStateStorage> txStateTableStorageDecorator
    ) {
        var failureProcessor = new NoOpFailureManager();

        var sharedTxStateStorage = new TxStateRocksDbSharedStorage(
                NODE_NAME,
                workDir.resolve("tx-state"),
                scheduledExecutor,
                partitionOperationsExecutor,
                logSyncer,
                failureProcessor
        );

        dsm = createDataStorageManager(configRegistry, workDir);

        sm = new SchemaManager(revisionUpdater, catalogManager);

        var tableManager = new TableManager(
                NODE_NAME,
                revisionUpdater,
                gcConfig,
                txConfig,
                replicationConfiguration,
                clusterService.messagingService(),
                clusterService.topologyService(),
                clusterService.serializationRegistry(),
                replicaMgr,
                mock(LockManager.class),
                mock(ReplicaService.class),
                tm,
                dsm,
                sharedTxStateStorage,
                msm,
                sm,
                partitionOperationsExecutor,
                partitionOperationsExecutor,
                scheduledExecutor,
                scheduledExecutor,
                new TestClockService(clock),
                new OutgoingSnapshotsManager(node.name(), clusterService.messagingService(), failureProcessor),
                distributionZoneManager,
                new AlwaysSyncedSchemaSyncService(),
                catalogManager,
                failureProcessor,
                HybridTimestampTracker.atomicTracker(null),
                new TestPlacementDriver(node),
                () -> mock(IgniteSql.class),
                new RemotelyTriggeredResourceRegistry(),
                lowWatermark,
                mock(TransactionInflights.class),
                indexMetaStorage,
                logSyncer,
                partitionReplicaLifecycleManager,
                new SystemPropertiesNodeProperties(),
                new MinimumRequiredTimeCollectorServiceImpl(),
                systemDistributedConfiguration
        ) {

            @Override
            protected MvTableStorage createTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
                mvTableStorage = spy(super.createTableStorage(tableDescriptor, zoneDescriptor));

                tableStorageDecorator.accept(mvTableStorage);

                return mvTableStorage;
            }

            @Override
            protected TxStateStorage createTxStateTableStorage(
                    CatalogTableDescriptor tableDescriptor,
                    CatalogZoneDescriptor zoneDescriptor
            ) {
                txStateStorage = spy(super.createTxStateTableStorage(tableDescriptor, zoneDescriptor));

                txStateTableStorageDecorator.accept(txStateStorage);

                return txStateStorage;
            }

            @Override
            public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                return sharedTxStateStorage.startAsync(componentContext)
                        .thenCompose(unused -> super.startAsync(componentContext));
            }

            @Override
            public void beforeNodeStop() {
                super.beforeNodeStop();
                sharedTxStateStorage.beforeNodeStop();
            }

            @Override
            public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                return super.stopAsync(componentContext)
                        .thenCompose(unused -> sharedTxStateStorage.stopAsync(componentContext));
            }
        };

        tableManager.setStreamerReceiverRunner(mock(StreamerReceiverRunner.class));

        assertThat(startAsync(new ComponentContext(), sm, tableManager), willCompleteSuccessfully());

        tblManagerFut.complete(tableManager);

        return tableManager;
    }

    private DataStorageManager createDataStorageManager(
            ConfigurationRegistry mockedRegistry,
            Path storagePath
    ) {
        when(mockedRegistry.getConfiguration(NodeConfiguration.KEY)).thenReturn(nodeConfiguration);

        DataStorageModules dataStorageModules = new DataStorageModules(
                List.of(new PersistentPageMemoryDataStorageModule())
        );

        Map<String, StorageEngine> storageEngines = dataStorageModules.createStorageEngines(
                NODE_NAME,
                mock(MetricManager.class),
                mockedRegistry,
                storagePath,
                null,
                mock(FailureManager.class),
                mock(LogSyncer.class),
                clock,
                scheduledExecutor
        );

        DataStorageManager manager = new DataStorageManager(
                storageEngines,
                ((StorageExtensionConfiguration) nodeConfiguration).storage()
        );

        assertThat(manager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        return manager;
    }

    private void createZone(int partitions, int replicas) {
        DistributionZonesTestUtil.createZone(catalogManager, ZONE_NAME, partitions, replicas);
    }

    private void createTable(String tableName) {
        TableTestUtils.createTable(
                catalogManager,
                SqlCommon.DEFAULT_SCHEMA_NAME,
                ZONE_NAME,
                tableName,
                List.of(
                        ColumnParams.builder().name("key").type(INT64).build(),
                        ColumnParams.builder().name("val").type(INT64).nullable(true).build()
                ),
                List.of("key")
        );
    }

    private void dropTable(String tableName) {
        TableTestUtils.dropTable(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, tableName);
    }

    private CompletableFuture<Void> fireDestroyEvent() {
        return lowWatermark.updateAndNotify(clock.now());
    }
}
