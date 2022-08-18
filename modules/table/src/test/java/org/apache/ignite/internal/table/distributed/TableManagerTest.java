/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.raft.jraft.test.TestUtils.peersToIds;
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListenerHolder;
import org.apache.ignite.internal.configuration.schema.ExtendedTableChange;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfigurationSchema;
import org.apache.ignite.internal.configuration.schema.ExtendedTableView;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectRevisionListenerHolder;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.rocksdb.RocksDbDataStorageModule;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageChange;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
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
 * TODO: to change storage from "rocksdb" to "aimem" https://issues.apache.org/jira/browse/IGNITE-17197
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class TableManagerTest extends IgniteAbstractTest {
    private static IgniteLogger LOG = Loggers.forClass(TableManagerTest.class);

    /** The name of the table which is preconfigured. */
    private static final String PRECONFIGURED_TABLE_NAME = "t1";

    /** The name of the table which will be configured dynamically. */
    private static final String DYNAMIC_TABLE_NAME = "t2";

    /** The name of table to drop it. */
    private static final String DYNAMIC_TABLE_FOR_DROP_NAME = "t3";

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
    private Loza rm;

    /** Replica manager. */
    @Mock
    private ReplicaManager replicaMgr;

    /** TX manager. */
    @Mock(lenient = true)
    private TxManager tm;

    /** TX manager. */
    @Mock(lenient = true)
    private LockManager lm;

    /** Meta storage manager. */
    @Mock
    MetaStorageManager msm;

    /** Mock messaging service. */
    @Mock
    private MessagingService messagingService;

    /** Mock cluster. */
    @Mock
    private ClusterService cluster;

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
    @InjectConfiguration(
            internalExtensions = ExtendedTableConfigurationSchema.class,
            polymorphicExtensions = {
                    HashIndexConfigurationSchema.class,
                    SortedIndexConfigurationSchema.class,
                    UnknownDataStorageConfigurationSchema.class,
                    RocksDbDataStorageConfigurationSchema.class,
                    ConstantValueDefaultConfigurationSchema.class,
                    FunctionCallDefaultConfigurationSchema.class,
                    NullValueDefaultConfigurationSchema.class
            }
    )
    private TablesConfiguration tblsCfg;

    @InjectConfiguration(polymorphicExtensions = UnsafeMemoryAllocatorConfigurationSchema.class)
    private RocksDbStorageEngineConfiguration rocksDbEngineConfig;

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
        revisionUpdater = (Function<Long, CompletableFuture<?>> function) -> {
            function.apply(0L).join();

            fieldRevisionListenerHolder.listenUpdateStorageRevision(newStorageRevision -> {
                log.info("Notify about revision: {}", newStorageRevision);

                return function.apply(newStorageRevision);
            });
        };

        when(msm.registerWatch(any(ByteArray.class), any())).thenReturn(CompletableFuture.completedFuture(1L));

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
        when(rm.startRaftGroupService(any(), any())).thenAnswer(mock ->
                CompletableFuture.completedFuture(mock(RaftGroupService.class)));

        TableManager tableManager = createTableManager(tblManagerFut, false);

        tblManagerFut.complete(tableManager);

        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", PRECONFIGURED_TABLE_NAME).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build()
        ).withPrimaryKey("key").build();

        tblsCfg.tables().change(tablesChange -> {
            tablesChange.create(scmTbl.canonicalName(), tableChange -> {
                (SchemaConfigurationConverter.convert(scmTbl, tableChange))
                        .changeReplicas(REPLICAS)
                        .changePartitions(PARTITIONS);

                tableChange.changeDataStorage(c -> c.convert(RocksDbDataStorageChange.class));

                var extConfCh = ((ExtendedTableChange) tableChange);

                ArrayList<List<ClusterNode>> assignment = new ArrayList<>(PARTITIONS);

                for (int part = 0; part < PARTITIONS; part++) {
                    assignment.add(new ArrayList<>(Collections.singleton(node)));
                }

                extConfCh.changeAssignments(ByteUtils.toBytes(assignment))
                        .changeSchemas(schemasCh -> schemasCh.create(
                                String.valueOf(1),
                                schemaCh -> {
                                    SchemaDescriptor schemaDesc = SchemaUtils.prepareSchemaDescriptor(
                                            ((ExtendedTableView) tableChange).schemas().size(),
                                            tableChange);

                                    schemaCh.changeSchema(SchemaSerializerImpl.INSTANCE.serialize(schemaDesc));
                                }
                        ));
            });
        }).join();

        assertEquals(1, tableManager.tables().size());

        assertNotNull(tableManager.table(scmTbl.canonicalName()));

        checkTableDataStorage(tblsCfg.tables().value(), RocksDbStorageEngine.ENGINE_NAME);
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

        assertSame(table, tblManagerFut.join().table(scmTbl.canonicalName()));

        checkTableDataStorage(tblsCfg.tables().value(), RocksDbStorageEngine.ENGINE_NAME);
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

        mockManagersAndCreateTable(scmTbl, tblManagerFut);

        TableManager tableManager = tblManagerFut.join();

        tableManager.dropTable(scmTbl.canonicalName());

        assertNull(tableManager.table(scmTbl.canonicalName()));

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

        String tblFullName = "PUBLIC." + DYNAMIC_TABLE_FOR_DROP_NAME;

        Consumer<TableChange> createTableChange = (TableChange change) ->
                SchemaConfigurationConverter.convert(SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_FOR_DROP_NAME).columns(
                                SchemaBuilders.column("key", ColumnType.INT64).build(),
                                SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build()
                        ).withPrimaryKey("key").build(), change)
                        .changeReplicas(REPLICAS)
                        .changePartitions(PARTITIONS);

        final Consumer<TableChange> addColumnChange = (TableChange change) ->
                change.changeColumns(cols -> {
                    int colIdx = change.columns().namedListKeys().stream().mapToInt(Integer::parseInt).max().getAsInt() + 1;

                    cols.create(String.valueOf(colIdx),
                            colChg -> SchemaConfigurationConverter.convert(SchemaBuilders.column("name", ColumnType.string()).build(),
                                    colChg));

                });

        TableManager igniteTables = tableManager;

        assertThrows(IgniteException.class, () -> igniteTables.createTable(tblFullName, createTableChange));
        assertThrows(IgniteException.class, () -> igniteTables.createTableAsync(tblFullName, createTableChange));

        assertThrows(IgniteException.class, () -> igniteTables.alterTable(tblFullName, addColumnChange));
        assertThrows(IgniteException.class, () -> igniteTables.alterTableAsync(tblFullName, addColumnChange));

        assertThrows(IgniteException.class, () -> igniteTables.dropTable(tblFullName));
        assertThrows(IgniteException.class, () -> igniteTables.dropTableAsync(tblFullName));

        assertThrows(IgniteException.class, () -> igniteTables.tables());
        assertThrows(IgniteException.class, () -> igniteTables.tablesAsync());

        assertThrows(IgniteException.class, () -> igniteTables.table(tblFullName));
        assertThrows(IgniteException.class, () -> igniteTables.tableAsync(tblFullName));
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
     * Cheks that the all RAFT nodes will be stopped when Table manager is stopping.
     *
     * @throws Exception If failed.
     */
    @Test
    public void tableManagerStopTest() throws Exception {
        TableDefinition scmTbl = SchemaBuilders.tableBuilder("PUBLIC", DYNAMIC_TABLE_FOR_DROP_NAME).columns(
                SchemaBuilders.column("key", ColumnType.INT64).build(),
                SchemaBuilders.column("val", ColumnType.INT64).asNullable(true).build()
        ).withPrimaryKey("key").build();

        mockManagersAndCreateTable(scmTbl, tblManagerFut);

        verify(rm, times(PARTITIONS)).startRaftGroupService(anyString(), any());

        TableManager tableManager = tblManagerFut.join();

        tableManager.stop();

        verify(rm, times(PARTITIONS)).stopRaftGroup(anyString());
        verify(replicaMgr, times(PARTITIONS)).stopReplica(anyString());
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

            return tblManagerFut.join().table(scmTbl.canonicalName());
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
                () -> tblManagerFut.join().createTable(scmTbl.canonicalName(), tblCh -> SchemaConfigurationConverter.convert(scmTbl, tblCh)
                        .changeReplicas(REPLICAS)
                        .changePartitions(PARTITIONS)));

        assertSame(table, tblManagerFut.join().table(scmTbl.canonicalName()));
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
            Phaser phaser
    ) throws Exception {
        when(rm.startRaftGroupService(any(), any())).thenAnswer(mock -> {
            RaftGroupService raftGrpSrvcMock = mock(RaftGroupService.class);

            when(raftGrpSrvcMock.leader()).thenReturn(new Peer(new NetworkAddress("localhost", 47500)));

            return completedFuture(raftGrpSrvcMock);
        });

        when(ts.getByAddress(any(NetworkAddress.class))).thenReturn(new ClusterNode(
                UUID.randomUUID().toString(),
                "node0",
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

        TableManager tableManager = createTableManager(tblManagerFut, true);

        final int tablesBeforeCreation = tableManager.tables().size();

        tblsCfg.tables().listen(ctx -> {
            boolean createTbl = ctx.newValue().get(tableDefinition.canonicalName()) != null
                    && ctx.oldValue().get(tableDefinition.canonicalName()) == null;

            boolean dropTbl = ctx.oldValue().get(tableDefinition.canonicalName()) != null
                    && ctx.newValue().get(tableDefinition.canonicalName()) == null;

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

        CompletableFuture<Table> tbl2Fut = tableManager.createTableAsync(tableDefinition.canonicalName(),
                tblCh -> SchemaConfigurationConverter.convert(tableDefinition, tblCh)
                        .changeReplicas(REPLICAS)
                        .changePartitions(PARTITIONS)
        );

        assertTrue(createTblLatch.await(10, TimeUnit.SECONDS));

        TableImpl tbl2 = (TableImpl) tbl2Fut.get();

        assertNotNull(tbl2);

        assertEquals(tablesBeforeCreation + 1, tableManager.tables().size());

        return tbl2;
    }

    /**
     * Tests that {@link RaftGroupServiceImpl#changePeersAsync(java.util.List, long)} was retried after some exceptions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testChangePeersAsyncRetryLogic() throws Exception {
        RaftMessagesFactory factory = new RaftMessagesFactory();

        List<Peer> nodes = Stream.of(20000, 20001, 20002)
                .map(port -> new NetworkAddress("localhost", port))
                .map(Peer::new)
                .collect(Collectors.toUnmodifiableList());

        int timeout = 1000;

        int delay = 200;

        Peer leader = nodes.get(0);

        when(cluster.messagingService()).thenReturn(messagingService);

        TableManager tableManager = createTableManager(tblManagerFut, false);

        ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory(Loza.CLIENT_POOL_NAME, LOG));

        String groupId = "test";

        List<String> shrunkPeers = peersToIds(nodes.subList(0, 1));

        List<String> extendedPeers = peersToIds(nodes);

        AtomicLong firstInvocationOfChangePeersAsync = new AtomicLong(0L);

        AtomicInteger counter = new AtomicInteger(0);

        when(messagingService.invoke(any(NetworkAddress.class),
                eq(factory.changePeersAsyncRequest()
                        .newPeersList(shrunkPeers)
                        .term(1L)
                        .groupId(groupId).build()), anyLong()))
                .then(invocation -> {
                    if (firstInvocationOfChangePeersAsync.get() == 0) {
                        firstInvocationOfChangePeersAsync.set(System.currentTimeMillis());
                        return failedFuture(new TimeoutException());
                    } else {
                        if (firstInvocationOfChangePeersAsync.get() + timeout < System.currentTimeMillis()) {
                            //retry happened, new changePeersAsync was called
                            counter.incrementAndGet();

                            return completedFuture(factory.changePeersAsyncResponse().newPeersList(extendedPeers).build());
                        }
                    }

                    return failedFuture(new TimeoutException());
                });

        when(messagingService.invoke(any(NetworkAddress.class), any(CliRequests.GetLeaderRequest.class), anyLong()))
                .then(invocation -> {
                    PeerId leader0 = PeerId.fromPeer(leader);

                    Object resp = leader0 == null
                            ? factory.errorResponse().errorCode(RaftError.EPERM.getNumber()).build()
                            : factory.getLeaderResponse().leaderId(leader0.toString()).currentTerm(1L).build();

                    return completedFuture(resp);
                });

        RaftGroupService service = RaftGroupServiceImpl.start(groupId, cluster, factory, timeout, nodes.subList(0, 2),
                true, delay, executor).get(3, TimeUnit.SECONDS);

        tableManager.movePartition(() -> service).apply(nodes.subList(0, 1), 1L).join();

        assertEquals(counter.get(), 1);

        AtomicLong secondInvocationOfChangePeersAsync = new AtomicLong(0L);

        when(messagingService.invoke(any(NetworkAddress.class),
                eq(factory.changePeersAsyncRequest()
                        .newPeersList(shrunkPeers)
                        .term(1L)
                        .groupId(groupId).build()), anyLong()))
                .then(invocation -> {
                    if (secondInvocationOfChangePeersAsync.get() == 0) {
                        secondInvocationOfChangePeersAsync.set(System.currentTimeMillis());

                        return failedFuture(new NullPointerException());
                    } else {
                        if (secondInvocationOfChangePeersAsync.get() + timeout < System.currentTimeMillis()) {
                            //retry happened, new changePeersAsync was called
                            counter.incrementAndGet();

                            return completedFuture(factory.changePeersAsyncResponse().newPeersList(extendedPeers).build());
                        }
                    }

                    return failedFuture(new NullPointerException());
                });

        tableManager.movePartition(() -> service).apply(nodes.subList(0, 1), 1L).join();

        assertEquals(2, counter.get());

        shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
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
                revisionUpdater,
                tblsCfg,
                rm,
                replicaMgr,
                null,
                null,
                bm,
                ts,
                tm,
                dsm = createDataStorageManager(configRegistry, workDir, rocksDbEngineConfig),
                msm,
                sm = new SchemaManager(revisionUpdater, tblsCfg)
        );

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
            RocksDbStorageEngineConfiguration config
    ) {
        when(mockedRegistry.getConfiguration(RocksDbStorageEngineConfiguration.KEY)).thenReturn(config);

        DataStorageModules dataStorageModules = new DataStorageModules(List.of(new RocksDbDataStorageModule()));

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
