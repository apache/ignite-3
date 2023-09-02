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

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.stream.Stream;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListenerHolder;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectRevisionListenerHolder;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesView;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.configuration.ExtendedTableChange;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageView;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests scenarios for table manager.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class TableManagerTest extends IgniteAbstractTest {
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

    /** Default zone id. */
    private static final int DEFAULT_ZONE_ID = 0;

    /** Zone name. */
    private static final String ZONE_NAME = "zone1";

    /** Zone id. */
    private static final int ZONE_ID = 1;

    /** Schema manager. */
    @Mock
    private BaselineManager bm;

    /** Topology service. */
    @Mock
    private TopologyService ts;

    /** Raft manager. */
    @Mock
    private Loza rm;

    /** Replica manager. */
    @Mock
    private ReplicaManager replicaMgr;

    /** TX manager. */
    @Mock
    private TxManager tm;

    /** Meta storage manager. */
    @Mock
    MetaStorageManager msm;

    /** Mock cluster service. */
    @Mock
    private ClusterService clusterService;

    private volatile MvTableStorage mvTableStorage;

    private volatile TxStateTableStorage txStateTableStorage;

    /**
     * Revision listener holder. It uses for the test configurations:
     * <ul>
     * <li>{@link TableManagerTest#fieldRevisionListenerHolder},</li>
     * <li>{@link TableManagerTest#tblsCfg}.</li>
     * </ul>
     */
    @InjectRevisionListenerHolder
    private ConfigurationStorageRevisionListenerHolder fieldRevisionListenerHolder;

    /** Revision updater. */
    private Consumer<LongFunction<CompletableFuture<?>>> revisionUpdater;

    /** Tables configuration. */
    @InjectConfiguration
    private TablesConfiguration tblsCfg;

    /** Distribution zones configuration. */
    @InjectConfiguration
    private DistributionZonesConfiguration distributionZonesConfiguration;

    /** Garbage collector configuration. */
    @InjectConfiguration
    private GcConfiguration gcConfig;

    @InjectConfiguration
    private PersistentPageMemoryStorageEngineConfiguration storageEngineConfig;

    @Mock
    private ConfigurationRegistry configRegistry;

    private DataStorageManager dsm;

    private SchemaManager sm;

    private ClusterManagementGroupManager cmgMgr;

    private DistributionZoneManager distributionZoneManager;

    /** Test node. */
    private final ClusterNode node = new ClusterNodeImpl(
            UUID.randomUUID().toString(),
            NODE_NAME,
            new NetworkAddress("127.0.0.1", 2245)
    );

    /** The future will be completed after each tests of this class. */
    private CompletableFuture<TableManager> tblManagerFut;

    @BeforeEach
    void before() throws NodeStoppingException {
        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class));

        TopologyService topologyService = mock(TopologyService.class);

        when(clusterService.topologyService()).thenReturn(topologyService);
        when(topologyService.localMember()).thenReturn(node);

        revisionUpdater = (LongFunction<CompletableFuture<?>> function) -> {
            assertThat(function.apply(0L), willCompleteSuccessfully());

            fieldRevisionListenerHolder.listenUpdateStorageRevision(newStorageRevision -> {
                log.info("Notify about revision: {}", newStorageRevision);

                return function.apply(newStorageRevision);
            });
        };

        cmgMgr = mock(ClusterManagementGroupManager.class);

        LogicalTopologySnapshot logicalTopologySnapshot = new LogicalTopologySnapshot(0, emptySet());

        when(cmgMgr.logicalTopology()).thenReturn(completedFuture(logicalTopologySnapshot));

        distributionZoneManager = mock(DistributionZoneManager.class);

        when(distributionZoneManager.zoneIdAsyncInternal(anyString()))
                .then(invocation -> completedFuture(getZoneId(invocation.getArgument(0))));

        when(distributionZoneManager.dataNodes(anyLong(), anyInt())).thenReturn(completedFuture(emptySet()));

        when(replicaMgr.stopReplica(any())).thenReturn(completedFuture(true));

        tblManagerFut = new CompletableFuture<>();
    }

    @AfterEach
    void after() throws Exception {
        IgniteUtils.closeAll(
                () -> {
                    assertTrue(tblManagerFut.isDone());

                    tblManagerFut.join().beforeNodeStop();
                    tblManagerFut.join().stop();
                },
                dsm == null ? null : dsm::stop,
                sm == null ? null : sm::stop
        );
    }

    /**
     * Tests a table which was preconfigured.
     */
    @Test
    public void testPreconfiguredTable() throws Exception {
        when(rm.startRaftGroupService(any(), any(), any())).thenAnswer(mock -> completedFuture(mock(TopologyAwareRaftGroupService.class)));

        mockMetastore();

        TableManager tableManager = createTableManager(tblManagerFut);

        tblManagerFut.complete(tableManager);

        createZone(PARTITIONS, REPLICAS);

        TableDefinition scmTbl = createTableDefinition(PRECONFIGURED_TABLE_NAME);

        CompletableFuture<Void> createTableConfigFuture = tblsCfg.tables().change(tablesChange ->
                tablesChange.create(scmTbl.name(), tableChange -> {
                    (SchemaConfigurationConverter.convert(scmTbl, tableChange)).changeZoneId(getZoneId(ZONE_NAME));

                    var extConfCh = ((ExtendedTableChange) tableChange);

                    extConfCh.changeSchemaId(1);
                }));

        assertThat(createTableConfigFuture, willCompleteSuccessfully());

        assertEquals(1, tableManager.tables().size());

        assertNotNull(tableManager.table(scmTbl.name()));

        checkTableDataStorage(tblsCfg.tables().value(), PersistentPageMemoryStorageEngine.ENGINE_NAME);
    }

    /**
     * Tests create a table through public API.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateTable() throws Exception {
        Table table = mockManagersAndCreateTable(DYNAMIC_TABLE_NAME, tblManagerFut);

        assertNotNull(table);

        assertSame(table, tblManagerFut.join().table(DYNAMIC_TABLE_NAME));

        checkTableDataStorage(tblsCfg.tables().value(), PersistentPageMemoryStorageEngine.ENGINE_NAME);
    }

    /**
     * Tests drop a table through public API.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropTable() throws Exception {
        mockManagersAndCreateTable(DYNAMIC_TABLE_FOR_DROP_NAME, tblManagerFut);

        TableManager tableManager = tblManagerFut.join();

        await(tableManager.dropTableAsync(DYNAMIC_TABLE_FOR_DROP_NAME));

        verify(mvTableStorage).destroy();
        verify(txStateTableStorage).destroy();
        verify(replicaMgr, times(PARTITIONS)).stopReplica(any());

        assertNull(tableManager.table(DYNAMIC_TABLE_FOR_DROP_NAME));

        assertEquals(0, tableManager.tables().size());
    }

    /**
     * Tests a work of the public API for Table manager {@see org.apache.ignite.table.manager.IgniteTables} when the manager is stopping.
     */
    @Test
    public void testApiTableManagerOnStop() {
        createTableManager(tblManagerFut);

        TableManager tableManager = tblManagerFut.join();

        tableManager.beforeNodeStop();
        tableManager.stop();

        createZone(PARTITIONS, REPLICAS);

        Consumer<TableChange> createTableChange = (TableChange change) ->
                SchemaConfigurationConverter.convert(createTableDefinition(DYNAMIC_TABLE_FOR_DROP_NAME), change);

        Function<TableChange, Boolean> addColumnChange = (TableChange change) -> {
            change.changeColumns(cols -> {
                int colIdx = change.columns().namedListKeys().stream().mapToInt(Integer::parseInt).max().getAsInt() + 1;

                cols.create(String.valueOf(colIdx),
                        colChg -> SchemaConfigurationConverter.convert(SchemaBuilders.column("name", ColumnType.string()).build(),
                                colChg));

            });

            return true;
        };

        TableManager igniteTables = tableManager;

        assertThrows(IgniteException.class,
                () -> igniteTables.createTableAsync(DYNAMIC_TABLE_FOR_DROP_NAME, ZONE_NAME, createTableChange));

        assertThrows(IgniteException.class, () -> igniteTables.alterTableAsync(DYNAMIC_TABLE_FOR_DROP_NAME, addColumnChange));

        assertThrows(IgniteException.class, () -> igniteTables.dropTableAsync(DYNAMIC_TABLE_FOR_DROP_NAME));

        assertThrowsWithCause(tableManager::tables, NodeStoppingException.class);
        assertThat(tableManager.tablesAsync(), willThrow(NodeStoppingException.class));

        assertThrowsWithCause(() -> tableManager.table(DYNAMIC_TABLE_FOR_DROP_NAME), NodeStoppingException.class);
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
        tableManager.stop();

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
    public void tableManagerStopTest1() throws Exception {
        IgniteBiTuple<TableImpl, TableManager> tblAndMnr = startTableManagerStopTest();

        endTableManagerStopTest(tblAndMnr.get1(), tblAndMnr.get2(),
                () -> {
                    try {
                        doThrow(new NodeStoppingException()).when(rm).stopRaftNodes(any());
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
    public void tableManagerStopTest2() throws Exception {
        IgniteBiTuple<TableImpl, TableManager> tblAndMnr = startTableManagerStopTest();

        endTableManagerStopTest(tblAndMnr.get1(), tblAndMnr.get2(),
                () -> {
                    try {
                        doThrow(new NodeStoppingException()).when(replicaMgr).stopReplica(any());
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
    public void tableManagerStopTest3() throws Exception {
        IgniteBiTuple<TableImpl, TableManager> tblAndMnr = startTableManagerStopTest();

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
    public void tableManagerStopTest4() throws Exception {
        IgniteBiTuple<TableImpl, TableManager> tblAndMnr = startTableManagerStopTest();

        endTableManagerStopTest(tblAndMnr.get1(), tblAndMnr.get2(),
                () -> doThrow(new RuntimeException()).when(tblAndMnr.get1().internalTable().txStateStorage()).close());
    }

    private IgniteBiTuple<TableImpl, TableManager> startTableManagerStopTest() throws Exception {
        TableImpl table = mockManagersAndCreateTable(DYNAMIC_TABLE_FOR_DROP_NAME, tblManagerFut);

        verify(rm, times(PARTITIONS)).startRaftGroupService(any(), any(), any());

        TableManager tableManager = tblManagerFut.join();

        return new IgniteBiTuple<>(table, tableManager);
    }

    private void endTableManagerStopTest(TableImpl table, TableManager tableManager, Runnable mockDoThrow) throws Exception {
        mockDoThrow.run();

        tableManager.stop();

        verify(rm, times(PARTITIONS)).stopRaftNodes(any());
        verify(replicaMgr, times(PARTITIONS)).stopReplica(any());

        verify(table.internalTable().storage()).close();
        verify(table.internalTable().txStateStorage()).close();
    }

    /**
     * Instantiates a table and prepares Table manager.
     */
    @Test
    public void testGetTableDuringCreation() {
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

    /**
     * Tries to create a table that already exists.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDoubledCreateTable() throws Exception {
        Table table = mockManagersAndCreateTable(DYNAMIC_TABLE_NAME, tblManagerFut);

        assertNotNull(table);

        assertThrows(RuntimeException.class,
                () -> await(tblManagerFut.join().createTableAsync(DYNAMIC_TABLE_NAME, ZONE_NAME,
                        tblCh -> SchemaConfigurationConverter.convert(createTableDefinition(DYNAMIC_TABLE_NAME), tblCh))));

        assertSame(table, tblManagerFut.join().table(DYNAMIC_TABLE_NAME));
    }

    @Test
    void testStoragesGetClearedInMiddleOfFailedTxStorageRebalance() throws Exception {
        testStoragesGetClearedInMiddleOfFailedRebalance(true);
    }

    @Test
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
    private void testStoragesGetClearedInMiddleOfFailedRebalance(boolean isTxStorageUnderRebalance) throws NodeStoppingException {
        when(rm.startRaftGroupService(any(), any(), any())).thenAnswer(mock -> completedFuture(mock(TopologyAwareRaftGroupService.class)));
        when(rm.raftNodeReadyFuture(any())).thenReturn(completedFuture(1L));
        when(bm.nodes()).thenReturn(Set.of(node));

        createZone(1, 1);

        var txStateStorage = mock(TxStateStorage.class);
        var mvPartitionStorage = mock(MvPartitionStorage.class);

        if (isTxStorageUnderRebalance) {
            // Emulate a situation when TX state storage was stopped in a middle of rebalance.
            when(txStateStorage.lastAppliedIndex()).thenReturn(TxStateStorage.REBALANCE_IN_PROGRESS);
        } else {
            // Emulate a situation when partition storage was stopped in a middle of rebalance.
            when(mvPartitionStorage.lastAppliedIndex()).thenReturn(MvPartitionStorage.REBALANCE_IN_PROGRESS);
        }

        doReturn(mock(PartitionTimestampCursor.class)).when(mvPartitionStorage).scan(any());
        when(txStateStorage.clear()).thenReturn(completedFuture(null));

        mockMetastore();

        // For some reason, "when(something).thenReturn" does not work on spies, but this notation works.
        createTableManager(tblManagerFut, (mvTableStorage) -> {
            doReturn(completedFuture(mvPartitionStorage)).when(mvTableStorage).createMvPartition(anyInt());
            doReturn(mvPartitionStorage).when(mvTableStorage).getMvPartition(anyInt());
            doReturn(completedFuture(null)).when(mvTableStorage).clearPartition(anyInt());
        }, (txStateTableStorage) -> {
            doReturn(txStateStorage).when(txStateTableStorage).getOrCreateTxStateStorage(anyInt());
            doReturn(txStateStorage).when(txStateTableStorage).getTxStateStorage(anyInt());
        });

        TableDefinition scmTbl = createTableDefinition(PRECONFIGURED_TABLE_NAME);

        CompletableFuture<Void> cfgChangeFuture = tblsCfg.tables()
                .change(namedListChange -> namedListChange.create(scmTbl.name(),
                        tableChange -> {
                            SchemaConfigurationConverter.convert(scmTbl, tableChange);

                            ((ExtendedTableChange) tableChange)
                                    .changeSchemaId(1)
                                    .changeZoneId(getZoneId(ZONE_NAME));
                        }));

        assertThat(cfgChangeFuture, willCompleteSuccessfully());

        verify(txStateStorage, timeout(1000)).clear();
        verify(mvTableStorage, timeout(1000)).clearPartition(anyInt());
    }

    /**
     * Instantiates Table manager and creates a table in it.
     *
     * @param tableName Table name.
     * @param tblManagerFut Future for table manager.
     * @return Table.
     * @throws Exception If something went wrong.
     */
    private TableImpl mockManagersAndCreateTable(String tableName, CompletableFuture<TableManager> tblManagerFut) throws Exception {
        return mockManagersAndCreateTableWithDelay(tableName, tblManagerFut, null);
    }

    /** Dummy metastore activity mock. */
    private void mockMetastore() {
        when(msm.prefix(any())).thenReturn(subscriber -> {
            subscriber.onSubscribe(mock(Subscription.class));

            subscriber.onComplete();
        });

        when(msm.invoke(any(), any(Operation.class), any(Operation.class))).thenReturn(completedFuture(null));
        when(msm.invoke(any(), any(List.class), any(List.class))).thenReturn(completedFuture(null));
    }

    /**
     * Instantiates a table and prepares Table manager. When the latch would open, the method completes.
     *
     * @param tableName Table name.
     * @param tblManagerFut Future for table manager.
     * @param phaser Phaser for the wait.
     * @return Table manager.
     * @throws Exception If something went wrong.
     */
    private TableImpl mockManagersAndCreateTableWithDelay(
            String tableName,
            CompletableFuture<TableManager> tblManagerFut,
            @Nullable Phaser phaser
    ) throws Exception {
        String consistentId = "node0";

        when(rm.startRaftGroupService(any(), any(), any())).thenAnswer(mock -> {
            RaftGroupService raftGrpSrvcMock = mock(TopologyAwareRaftGroupService.class);

            when(raftGrpSrvcMock.leader()).thenReturn(new Peer(consistentId));

            return completedFuture(raftGrpSrvcMock);
        });

        when(ts.getByConsistentId(any())).thenReturn(new ClusterNodeImpl(
                UUID.randomUUID().toString(),
                consistentId,
                new NetworkAddress("localhost", 47500)
        ));

        try (MockedStatic<SchemaUtils> schemaServiceMock = mockStatic(SchemaUtils.class)) {
            schemaServiceMock.when(() -> SchemaUtils.prepareSchemaDescriptor(anyInt(), any()))
                    .thenReturn(mock(SchemaDescriptor.class));
        }

        try (MockedStatic<AffinityUtils> affinityServiceMock = mockStatic(AffinityUtils.class)) {
            ArrayList<List<ClusterNode>> assignment = new ArrayList<>(PARTITIONS);

            for (int part = 0; part < PARTITIONS; part++) {
                assignment.add(new ArrayList<>(Collections.singleton(node)));
            }

            affinityServiceMock.when(() -> AffinityUtils.calculateAssignments(any(), anyInt(), anyInt()))
                    .thenReturn(assignment);
        }

        mockMetastore();

        TableManager tableManager = createTableManager(tblManagerFut);

        int tablesBeforeCreation = tableManager.tables().size();

        TableDefinition tableDefinition = createTableDefinition(tableName);

        CountDownLatch createTblLatch = new CountDownLatch(1);

        tableManager.listen(TableEvent.CREATE, (parameters, exception) -> {
            TableImpl table = tableManager.getTable(parameters.tableId());

            if (phaser != null && table != null && table.name().equals(tableDefinition.name())) {
                phaser.arriveAndAwaitAdvance();
            }

            createTblLatch.countDown();

            return completedFuture(true);
        });
        tableManager.listen(TableEvent.DROP, (parameters, exception) -> {
            TableImpl table = tableManager.latestTables().get(parameters.tableId());

            if (phaser != null && table != null && table.name().equals(tableDefinition.name())) {
                phaser.arriveAndAwaitAdvance();
            }

            return completedFuture(true);
        });

        createZone(PARTITIONS, REPLICAS);

        CompletableFuture<Table> tbl2Fut = tableManager.createTableAsync(tableDefinition.name(), ZONE_NAME,
                tblCh -> SchemaConfigurationConverter.convert(tableDefinition, tblCh)
        );

        assertTrue(createTblLatch.await(10, TimeUnit.SECONDS));

        TableImpl tbl2 = (TableImpl) tbl2Fut.get();

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
    private TableManager createTableManager(CompletableFuture<TableManager> tblManagerFut, Consumer<MvTableStorage> tableStorageDecorator,
            Consumer<TxStateTableStorage> txStateTableStorageDecorator) {
        VaultManager vaultManager = mock(VaultManager.class);

        when(vaultManager.get(any(ByteArray.class))).thenReturn(completedFuture(null));
        when(vaultManager.put(any(ByteArray.class), any(byte[].class))).thenReturn(completedFuture(null));

        TableManager tableManager = new TableManager(
                NODE_NAME,
                revisionUpdater,
                tblsCfg,
                distributionZonesConfiguration,
                gcConfig,
                clusterService,
                rm,
                replicaMgr,
                null,
                null,
                bm,
                ts,
                tm,
                dsm = createDataStorageManager(configRegistry, workDir, storageEngineConfig),
                workDir,
                msm,
                sm = new SchemaManager(revisionUpdater, tblsCfg, msm),
                budgetView -> new LocalLogStorageFactory(),
                new HybridClockImpl(),
                new OutgoingSnapshotsManager(clusterService.messagingService()),
                mock(TopologyAwareRaftGroupServiceFactory.class),
                vaultManager,
                cmgMgr,
                distributionZoneManager,
                mock(SchemaSyncService.class, invocation -> completedFuture(null)),
                mock(CatalogService.class),
                new HybridTimestampTracker()
        ) {

            @Override
            protected MvTableStorage createTableStorage(CatalogTableDescriptor tableDescriptor, CatalogZoneDescriptor zoneDescriptor) {
                mvTableStorage = spy(super.createTableStorage(tableDescriptor, zoneDescriptor));

                tableStorageDecorator.accept(mvTableStorage);

                return mvTableStorage;
            }

            @Override
            protected TxStateTableStorage createTxStateTableStorage(
                    CatalogTableDescriptor tableDescriptor,
                    CatalogZoneDescriptor zoneDescriptor
            ) {
                txStateTableStorage = spy(super.createTxStateTableStorage(tableDescriptor, zoneDescriptor));

                txStateTableStorageDecorator.accept(txStateTableStorage);

                return txStateTableStorage;
            }
        };

        sm.start();

        tableManager.start();

        tblManagerFut.complete(tableManager);

        return tableManager;
    }

    private DataStorageManager createDataStorageManager(
            ConfigurationRegistry mockedRegistry,
            Path storagePath,
            PersistentPageMemoryStorageEngineConfiguration config
    ) {
        when(mockedRegistry.getConfiguration(PersistentPageMemoryStorageEngineConfiguration.KEY)).thenReturn(config);

        DataStorageModules dataStorageModules = new DataStorageModules(List.of(new PersistentPageMemoryDataStorageModule()));

        DataStorageManager manager = new DataStorageManager(
                distributionZonesConfiguration,
                dataStorageModules.createStorageEngines(NODE_NAME, mockedRegistry, storagePath, null)
        );

        manager.start();

        return manager;
    }

    private void checkTableDataStorage(NamedListView<TableView> tables, String expDataStorage) {
        for (TableView table : tables) {
            assertEquals(getZoneDataStorage(table.zoneId()), expDataStorage, table.name());
        }
    }

    private void createZone(int partitions, int replicas) {
        CompletableFuture<Void> createZoneFuture = distributionZonesConfiguration.distributionZones().change(zones ->
                zones.create(ZONE_NAME, ch -> {
                    ch.changeZoneId(ZONE_ID);
                    ch.changePartitions(partitions);
                    ch.changeReplicas(replicas);
                }));

        assertThat(createZoneFuture, willCompleteSuccessfully());
    }

    private int getZoneId(String zoneName) {
        switch (zoneName) {
            case DEFAULT_ZONE_NAME:
                return DEFAULT_ZONE_ID;
            case ZONE_NAME:
                return ZONE_ID;
            default:
                throw new IllegalArgumentException(zoneName);
        }
    }

    private @Nullable String getZoneDataStorage(int zoneId) {
        DistributionZonesView zonesView = distributionZonesConfiguration.value();

        return Stream.concat(Stream.of(zonesView.defaultDistributionZone()), zonesView.distributionZones().stream())
                .filter(zoneView -> zoneId == zoneView.zoneId())
                .findFirst()
                .map(DistributionZoneView::dataStorage)
                .map(DataStorageView::name)
                .orElse(null);
    }

    private static TableDefinition createTableDefinition(String tableName) {
        return SchemaBuilders.tableBuilder(DEFAULT_SCHEMA_NAME, tableName).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build()
        ).withPrimaryKey("key").build();
    }
}
