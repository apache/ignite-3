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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListenerHolder;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectRevisionListenerHolder;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.configuration.ExtendedTableChange;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataStorageModule;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryDataStorageChange;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
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
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests scenarios for table manager.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class TableManagerTest extends IgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(TableManagerTest.class);

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

    /** Schema manager. */
    @Mock
    private BaselineManager bm;

    /** Topology service. */
    @Mock
    private TopologyService ts;

    /** Raft manager. */
    @Mock
    private RaftManager rm;

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
    private Consumer<Function<Long, CompletableFuture<?>>> revisionUpdater;

    /** Tables configuration. */
    @InjectConfiguration
    private TablesConfiguration tblsCfg;

    @InjectConfiguration
    private DistributionZonesConfiguration distributionZonesConfiguration;

    @InjectConfiguration
    private PersistentPageMemoryStorageEngineConfiguration storageEngineConfig;

    @Mock
    private ConfigurationRegistry configRegistry;

    private DataStorageManager dsm;

    private SchemaManager sm;

    /** Test node. */
    private final ClusterNode node = new ClusterNode(
            UUID.randomUUID().toString(),
            NODE_NAME,
            new NetworkAddress("127.0.0.1", 2245)
    );

    /** The future will be completed after each tests of this class. */
    private CompletableFuture<TableManager> tblManagerFut;

    /** Before all test scenarios. */
    @BeforeEach
    void before() {
        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class));

        TopologyService topologyService = mock(TopologyService.class);

        when(clusterService.topologyService()).thenReturn(topologyService);
        when(topologyService.localMember()).thenReturn(node);

        revisionUpdater = (Function<Long, CompletableFuture<?>> function) -> {
            function.apply(0L).join();

            fieldRevisionListenerHolder.listenUpdateStorageRevision(newStorageRevision -> {
                log.info("Notify about revision: {}", newStorageRevision);

                return function.apply(newStorageRevision);
            });
        };

        tblManagerFut = new CompletableFuture<>();
    }

    /** Stop configuration manager. */
    @AfterEach
    void after() throws Exception {
        assertTrue(tblManagerFut.isDone());

        tblManagerFut.join().beforeNodeStop();
        tblManagerFut.join().stop();

        if (dsm != null) {
            dsm.stop();
        }

        sm.stop();
    }

    /**
     * Tests a table which was preconfigured.
     */
    @Test
    public void testPreconfiguredTable() throws Exception {
        when(rm.startRaftGroupService(any(), any())).thenAnswer(mock -> completedFuture(mock(RaftGroupService.class)));

        mockMetastore();

        TableManager tableManager = createTableManager(tblManagerFut, false);

        tblManagerFut.complete(tableManager);

        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", PRECONFIGURED_TABLE_NAME).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build()
        ).withPrimaryKey("key").build();

        createDistributionZone(1, REPLICAS, PARTITIONS);

        tblsCfg.tables().change(tablesChange -> {

            tablesChange.create(scmTbl.name(), tableChange -> {
                (SchemaConfigurationConverter.convert(scmTbl, tableChange))
                        .changeZoneId(1);

                tableChange.changeDataStorage(c -> c.convert(PersistentPageMemoryDataStorageChange.class));

                var extConfCh = ((ExtendedTableChange) tableChange);

                var assignment = new ArrayList<Set<Assignment>>(PARTITIONS);

                for (int part = 0; part < PARTITIONS; part++) {
                    assignment.add(new HashSet<>(Collections.singleton(Assignment.forPeer(node.name()))));
                }

                extConfCh.changeAssignments(ByteUtils.toBytes(assignment)).changeSchemaId(1);
            });
        }).join();

        assertEquals(1, tableManager.tables().size());

        assertNotNull(tableManager.table(scmTbl.name()));

        checkTableDataStorage(tblsCfg.tables().value(), PersistentPageMemoryStorageEngine.ENGINE_NAME);
    }

    private void createDistributionZone(int zoneId, int replicas, int partitions) {
        distributionZonesConfiguration.distributionZones().change(zones -> {
            zones.create("zone1", ch -> {
                ch.changeZoneId(zoneId);
                ch.changePartitions(partitions);
                ch.changeReplicas(replicas);
            });
        }).join();
    }

    /**
     * Tests create a table through public API.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateTable() throws Exception {
        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_NAME).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build()
        ).withPrimaryKey("key").build();

        Table table = mockManagersAndCreateTable(scmTbl, tblManagerFut);

        assertNotNull(table);

        assertSame(table, tblManagerFut.join().table(scmTbl.name()));

        checkTableDataStorage(tblsCfg.tables().value(), PersistentPageMemoryStorageEngine.ENGINE_NAME);
    }

    /**
     * Tests drop a table through public API.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropTable() throws Exception {
        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_FOR_DROP_NAME).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build()
        ).withPrimaryKey("key").build();

        TableImpl table = mockManagersAndCreateTable(scmTbl, tblManagerFut);

        TableManager tableManager = tblManagerFut.join();

        await(tableManager.dropTableAsync(DYNAMIC_TABLE_FOR_DROP_NAME));

        verify(table.internalTable().storage()).destroy();
        verify(table.internalTable().txStateStorage()).destroy();

        assertNull(tableManager.table(scmTbl.name()));

        assertEquals(0, tableManager.tables().size());
    }

    /**
     * Tests a work of the public API for Table manager {@see org.apache.ignite.table.manager.IgniteTables} when the manager is stopping.
     */
    @Test
    public void testApiTableManagerOnStop() {
        createTableManager(tblManagerFut, false);

        TableManager tableManager = tblManagerFut.join();

        tableManager.beforeNodeStop();
        tableManager.stop();

        createDistributionZone(1, REPLICAS, PARTITIONS);

        Consumer<TableChange> createTableChange = (TableChange change) ->
                SchemaConfigurationConverter.convert(SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_FOR_DROP_NAME).columns(
                                SchemaBuilders.column("key", ColumnType.INT64).build(),
                                SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build()
                        ).withPrimaryKey("key").build(), change)
                        .changeZoneId(1);

        final Function<TableChange, Boolean> addColumnChange = (TableChange change) -> {
            change.changeColumns(cols -> {
                int colIdx = change.columns().namedListKeys().stream().mapToInt(Integer::parseInt).max().getAsInt() + 1;

                cols.create(String.valueOf(colIdx),
                        colChg -> SchemaConfigurationConverter.convert(SchemaBuilders.column("name", ColumnType.string()).build(),
                                colChg));

            });

            return true;
        };

        TableManager igniteTables = tableManager;

        assertThrows(IgniteException.class, () -> igniteTables.createTableAsync(DYNAMIC_TABLE_FOR_DROP_NAME, createTableChange));

        assertThrows(IgniteException.class, () -> igniteTables.alterTableAsync(DYNAMIC_TABLE_FOR_DROP_NAME, addColumnChange));

        assertThrows(IgniteException.class, () -> igniteTables.dropTableAsync(DYNAMIC_TABLE_FOR_DROP_NAME));

        assertThrows(IgniteException.class, () -> igniteTables.tables());
        assertThrows(IgniteException.class, () -> igniteTables.tablesAsync());

        assertThrows(IgniteException.class, () -> igniteTables.table(DYNAMIC_TABLE_FOR_DROP_NAME));
        assertThrows(IgniteException.class, () -> igniteTables.tableAsync(DYNAMIC_TABLE_FOR_DROP_NAME));
    }

    /**
     * Tests a work of the public API for Table manager {@see org.apache.ignite.internal.table.IgniteTablesInternal} when the manager is
     * stopping.
     */
    @Test
    public void testInternalApiTableManagerOnStop() {
        createTableManager(tblManagerFut, false);

        TableManager tableManager = tblManagerFut.join();

        tableManager.beforeNodeStop();
        tableManager.stop();

        UUID fakeTblId = UUID.randomUUID();

        assertThrows(IgniteException.class, () -> tableManager.table(fakeTblId));
        assertThrows(IgniteException.class, () -> tableManager.tableAsync(fakeTblId));
    }

    /**
     * Checks that the all RAFT nodes will be stopped when Table manager is stopping and
     * an exception that was thrown by one of the component will not prevent stopping other components.
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
     * Checks that the all RAFT nodes will be stopped when Table manager is stopping and
     * an exception that was thrown by one of the component will not prevent stopping other components.
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
     * Checks that the all RAFT nodes will be stopped when Table manager is stopping and
     * an exception that was thrown by one of the component will not prevent stopping other components.
     *
     * @throws Exception If failed.
     */
    @Test
    public void tableManagerStopTest3() throws Exception {
        IgniteBiTuple<TableImpl, TableManager> tblAndMnr = startTableManagerStopTest();

        endTableManagerStopTest(tblAndMnr.get1(), tblAndMnr.get2(),
                () -> {
                    try {
                        doThrow(new RuntimeException()).when(tblAndMnr.get1().internalTable().storage()).close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Checks that the all RAFT nodes will be stopped when Table manager is stopping and
     * an exception that was thrown by one of the component will not prevent stopping other components.
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
        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_FOR_DROP_NAME).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build()
        ).withPrimaryKey("key").build();

        TableImpl table = mockManagersAndCreateTable(scmTbl, tblManagerFut);

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
        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_FOR_DROP_NAME).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build()
        ).withPrimaryKey("key").build();

        Phaser phaser = new Phaser(2);

        CompletableFuture<Table> createFut = CompletableFuture.supplyAsync(() -> {
            try {
                return mockManagersAndCreateTableWithDelay(scmTbl, tblManagerFut, phaser);
            } catch (Exception e) {
                fail(e.getMessage());
            }

            return null;
        });

        CompletableFuture<Table> getFut = CompletableFuture.supplyAsync(() -> {
            phaser.awaitAdvance(0);

            return tblManagerFut.join().table(scmTbl.name());
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
        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_NAME)
                .columns(
                        SchemaBuilders.column("key", ColumnType.INT64).build(),
                        SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build())
                .withPrimaryKey("key")
                .build();

        Table table = mockManagersAndCreateTable(scmTbl, tblManagerFut);

        assertNotNull(table);

        assertThrows(RuntimeException.class,
                () -> await(tblManagerFut.join().createTableAsync(DYNAMIC_TABLE_NAME,
                        tblCh -> SchemaConfigurationConverter.convert(scmTbl, tblCh)
                                .changeZoneId(1))));

        assertSame(table, tblManagerFut.join().table(scmTbl.name()));
    }

    /**
     * Instantiates Table manager and creates a table in it.
     *
     * @param tableDefinition Configuration schema for a table.
     * @param tblManagerFut Future for table manager.
     * @return Table.
     * @throws Exception If something went wrong.
     */
    private TableImpl mockManagersAndCreateTable(
            TableDefinition tableDefinition,
            CompletableFuture<TableManager> tblManagerFut
    ) throws Exception {
        return mockManagersAndCreateTableWithDelay(tableDefinition, tblManagerFut, null);
    }

    /** Dummy metastore activity mock. */
    private void mockMetastore() {
        when(msm.prefix(any())).thenReturn(subscriber -> {
            subscriber.onSubscribe(mock(Subscription.class));

            subscriber.onComplete();
        });

        when(msm.invoke(any(), any(Operation.class), any(Operation.class))).thenReturn(completedFuture(null));
    }

    /**
     * Instantiates a table and prepares Table manager. When the latch would open, the method completes.
     *
     * @param tableDefinition Configuration schema for a table.
     * @param tblManagerFut Future for table manager.
     * @param phaser Phaser for the wait.
     * @return Table manager.
     * @throws Exception If something went wrong.
     */
    private TableImpl mockManagersAndCreateTableWithDelay(
            TableDefinition tableDefinition,
            CompletableFuture<TableManager> tblManagerFut,
            @Nullable Phaser phaser
    ) throws Exception {
        String consistentId = "node0";

        when(rm.startRaftGroupService(any(), any())).thenAnswer(mock -> {
            RaftGroupService raftGrpSrvcMock = mock(RaftGroupService.class);

            when(raftGrpSrvcMock.leader()).thenReturn(new Peer(consistentId));

            return completedFuture(raftGrpSrvcMock);
        });

        when(ts.getByConsistentId(any())).thenReturn(new ClusterNode(
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

        TableManager tableManager = createTableManager(tblManagerFut, true);

        final int tablesBeforeCreation = tableManager.tables().size();

        tblsCfg.tables().listen(ctx -> {
            boolean createTbl = ctx.newValue().get(tableDefinition.name()) != null
                    && ctx.oldValue().get(tableDefinition.name()) == null;

            boolean dropTbl = ctx.oldValue().get(tableDefinition.name()) != null
                    && ctx.newValue().get(tableDefinition.name()) == null;

            if (!createTbl && !dropTbl) {
                return completedFuture(null);
            }

            if (phaser != null) {
                phaser.arriveAndAwaitAdvance();
            }

            return completedFuture(null);
        });

        CountDownLatch createTblLatch = new CountDownLatch(1);

        tableManager.listen(TableEvent.CREATE, (parameters, exception) -> {
            createTblLatch.countDown();

            return completedFuture(true);
        });

        createDistributionZone(1, REPLICAS, PARTITIONS);

        CompletableFuture<Table> tbl2Fut = tableManager.createTableAsync(tableDefinition.name(),
                tblCh -> SchemaConfigurationConverter.convert(tableDefinition, tblCh)
                        .changeZoneId(1)
        );

        assertTrue(createTblLatch.await(10, TimeUnit.SECONDS));

        TableImpl tbl2 = (TableImpl) tbl2Fut.get();

        assertNotNull(tbl2);

        assertEquals(tablesBeforeCreation + 1, tableManager.tables().size());

        return tbl2;
    }

    /**
     * Creates Table manager.
     *
     * @param tblManagerFut    Future to wrap Table manager.
     * @param waitingSqlSchema If the flag is true, a table will wait of {@link TableManager#onSqlSchemaReady(long)} invocation before
     *                         create otherwise, the waiting will not be.
     * @return Table manager.
     */
    private TableManager createTableManager(CompletableFuture<TableManager> tblManagerFut, boolean waitingSqlSchema) {
        TableManager tableManager = new TableManager(
                "test",
                revisionUpdater,
                tblsCfg,
                distributionZonesConfiguration,
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
                mock(TopologyAwareRaftGroupServiceFactory.class)
        ) {

            @Override
            protected MvTableStorage createTableStorage(
                    TableConfiguration tableCfg, TablesConfiguration tablesCfg, DistributionZoneConfiguration distributionZonesCfg) {
                return Mockito.spy(super.createTableStorage(tableCfg, tablesCfg, distributionZonesCfg));
            }

            @Override
            protected TxStateTableStorage createTxStateTableStorage(TableConfiguration tableCfg,
                    DistributionZoneConfiguration distributionZoneCfg) {
                return Mockito.spy(super.createTxStateTableStorage(tableCfg, distributionZoneCfg));
            }
        };

        sm.start();

        if (!waitingSqlSchema) {
            tableManager.listen(TableEvent.CREATE, (parameters, exception) -> completedFuture(false));
        }

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
                tblsCfg,
                dataStorageModules.createStorageEngines(NODE_NAME, mockedRegistry, storagePath, null)
        );

        manager.start();

        return manager;
    }

    private void checkTableDataStorage(NamedListView<TableView> tables, String expDataStorage) {
        for (String tableName : tables.namedListKeys()) {
            assertThat(tables.get(tableName).dataStorage().name(), equalTo(expDataStorage));
        }
    }
}
