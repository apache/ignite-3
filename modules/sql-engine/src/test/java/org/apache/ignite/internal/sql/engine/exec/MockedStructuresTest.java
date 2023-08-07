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

package org.apache.ignite.internal.sql.engine.exec;

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine.ENGINE_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListenerHolder;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectRevisionListenerHolder;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.configuration.MetricConfiguration;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.impl.TestDataStorageModule;
import org.apache.ignite.internal.storage.impl.schema.TestDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.RocksDbDataStorageModule;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageChange;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.sql.SqlException;
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

/** Mock ddl usage. */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class MockedStructuresTest extends IgniteAbstractTest {
    /** Node name. */
    private static final String NODE_NAME = "node1";

    /** Zone name. */
    private static final String ZONE_NAME = "zone1";

    /** Zone id. */
    private static final int ZONE_ID = 1;

    /** Empty logical topology snapshot. */
    private static final LogicalTopologySnapshot logicalTopologySnapshot = new LogicalTopologySnapshot(0, emptySet());

    /** Schema manager. */
    @Mock
    private BaselineManager bm;

    /** Topology service. */
    @Mock
    private TopologyService ts;

    /** Cluster service. */
    @Mock(lenient = true)
    private ClusterService cs;

    /** Raft manager. */
    @Mock
    private RaftManager rm;

    /** TX manager. */
    @Mock(lenient = true)
    private TxManager tm;

    /** Meta storage manager. */
    @Mock
    MetaStorageManager msm;

    /** Replica manager. */
    @Mock
    ReplicaManager replicaManager;

    @Mock
    HybridClock clock;

    @Mock
    private VaultManager vaultManager;

    /**
     * Revision listener holder. It uses for the test configurations:
     * <ul>
     * <li>{@link MockedStructuresTest#tblsCfg},</li>
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
    @InjectConfiguration("mock.distributionZones." + ZONE_NAME + "{dataStorage.name = " + ENGINE_NAME + ", zoneId = " + ZONE_ID + "}")
    private DistributionZonesConfiguration dstZnsCfg;

    /** Garbage collector configuration. */
    @InjectConfiguration
    private GcConfiguration gcConfig;

    TableManager tblManager;

    IndexManager idxManager;

    ClusterManagementGroupManager cmgMgr;

    SqlQueryProcessor queryProc;

    /** Test node. */
    private final ClusterNode node = new ClusterNodeImpl(
            UUID.randomUUID().toString(),
            NODE_NAME,
            new NetworkAddress("127.0.0.1", 2245)
    );

    @InjectConfiguration
    private RocksDbStorageEngineConfiguration rocksDbEngineConfig;

    @InjectConfiguration
    private MetricConfiguration metricConfiguration;

    @Mock
    private ConfigurationRegistry configRegistry;

    private DistributionZoneManager distributionZoneManager;

    private DataStorageManager dataStorageManager;

    private SchemaManager schemaManager;

    private CatalogManager catalogManager;

    private MetricManager metricManager;

    /** Returns current method name. */
    private static String getCurrentMethodName() {
        return StackWalker.getInstance()
                .walk(s -> s.skip(1).findFirst())
                .get()
                .getMethodName();
    }

    /** Stop configuration manager. */
    @AfterEach
    void after() {
        try {
            Objects.requireNonNull(queryProc).stop();
        } catch (Exception e) {
            fail(e);
        }

        try {
            Objects.requireNonNull(dataStorageManager).stop();
        } catch (Exception e) {
            fail(e);
        }

        Objects.requireNonNull(tblManager).stop();
    }

    /** Inner initialisation. */
    @BeforeEach
    void before() throws Exception {
        mockVault();
        mockMetastore();

        revisionUpdater = (LongFunction<CompletableFuture<?>> function) -> {
            await(function.apply(0L));

            fieldRevisionListenerHolder.listenUpdateStorageRevision(newStorageRevision -> {
                log.info("Notify about revision: {}", newStorageRevision);

                return function.apply(newStorageRevision);
            });
        };

        when(configRegistry.getConfiguration(RocksDbStorageEngineConfiguration.KEY)).thenReturn(rocksDbEngineConfig);

        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                new RocksDbDataStorageModule(),
                new TestDataStorageModule()
        ));

        dataStorageManager = new DataStorageManager(
                dstZnsCfg,
                dataStorageModules.createStorageEngines(NODE_NAME, configRegistry, workDir, null)
        );

        dataStorageManager.start();

        schemaManager = new SchemaManager(revisionUpdater, tblsCfg, msm);

        schemaManager.start();

        catalogManager = mock(CatalogManager.class);
        when(catalogManager.createTable(any())).thenReturn(completedFuture(null));
        when(catalogManager.dropTable(any())).thenReturn(completedFuture(null));

        cmgMgr = mock(ClusterManagementGroupManager.class);

        when(cmgMgr.logicalTopology()).thenReturn(completedFuture(logicalTopologySnapshot));

        distributionZoneManager = mock(DistributionZoneManager.class);

        when(distributionZoneManager.getZoneId(DEFAULT_ZONE_NAME)).thenReturn(0);
        when(distributionZoneManager.zoneIdAsyncInternal(DEFAULT_ZONE_NAME)).thenReturn(completedFuture(0));

        when(distributionZoneManager.getZoneId(ZONE_NAME)).thenReturn(ZONE_ID);
        when(distributionZoneManager.zoneIdAsyncInternal(ZONE_NAME)).thenReturn(completedFuture(ZONE_ID));

        when(distributionZoneManager.dataNodes(anyLong(), anyInt())).thenReturn(completedFuture(emptySet()));

        tblManager = mockManagers();

        idxManager = new IndexManager(tblsCfg, schemaManager, tblManager);

        idxManager.start();

        metricManager = new MetricManager();
        metricManager.configure(metricConfiguration);

        metricManager.start();

        queryProc = new SqlQueryProcessor(
                revisionUpdater,
                cs,
                tblManager,
                idxManager,
                schemaManager,
                dataStorageManager,
                tm,
                distributionZoneManager,
                () -> dataStorageModules.collectSchemasFields(
                        List.of(
                                RocksDbDataStorageConfigurationSchema.class,
                                TestDataStorageConfigurationSchema.class
                        )
                ),
                mock(ReplicaService.class),
                clock,
                catalogManager,
                metricManager
        );

        queryProc.start();

        dstZnsCfg.defaultDistributionZone().change(ch -> ch.changeDataStorage(d -> d.convert(RocksDbDataStorageChange.class)))
                .get(1, TimeUnit.SECONDS);

        rocksDbEngineConfig.regions()
                .change(c -> c.create("test_region", rocksDbDataRegionChange -> {}))
                .get(1, TimeUnit.SECONDS);
    }

    private void mockVault() {
        when(vaultManager.get(any(ByteArray.class))).thenReturn(completedFuture(null));
        when(vaultManager.put(any(ByteArray.class), any(byte[].class))).thenReturn(completedFuture(null));
    }

    /** Dummy metastore activity mock. */
    private void mockMetastore() {
        when(msm.prefix(any())).thenReturn(subscriber -> {
            subscriber.onSubscribe(mock(Subscription.class));

            subscriber.onComplete();
        });

        when(msm.invoke(any(), any(Operation.class), any(Operation.class))).thenReturn(completedFuture(null));

        //noinspection unchecked
        when(msm.invoke(any(), any(Collection.class), any(Collection.class))).thenReturn(completedFuture(null));
    }

    /**
     * Tests create a table through public API.
     */
    @Test
    public void testCreateTable() {
        SqlQueryProcessor finalQueryProc = queryProc;

        SessionId sessionId = queryProc.createSession(PropertiesHelper.emptyHolder());
        QueryContext context = QueryContext.create(SqlQueryType.ALL);

        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) "
                + "with primary_zone='%s'", curMethodName, ZONE_NAME);

        readFirst(queryProc.querySingleAsync(sessionId, context, newTblSql));

        assertTrue(tblManager.tables().stream().anyMatch(t -> t.name()
                .equalsIgnoreCase(curMethodName)));

        String finalNewTblSql1 = newTblSql;

        assertThrows(TableAlreadyExistsException.class,
                () -> readFirst(finalQueryProc.querySingleAsync(sessionId, context, finalNewTblSql1)));

        String finalNewTblSql2 = String.format("CREATE TABLE \"PUBLIC\".%s (c1 int PRIMARY KEY, c2 varbinary(255)) "
                + "with primary_zone='%s'", curMethodName, ZONE_NAME);

        assertThrows(TableAlreadyExistsException.class,
                () -> readFirst(finalQueryProc.querySingleAsync(sessionId, context, finalNewTblSql2)));

        assertThrows(SqlException.class, () -> readFirst(finalQueryProc.querySingleAsync(sessionId, context,
                "CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with partitions__wrong=1,primary_zone='"
                        + ZONE_NAME + "'")));

        assertThrows(SqlException.class, () -> readFirst(finalQueryProc.querySingleAsync(sessionId, context,
                "CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with replicas__wrong=1,primary_zone='"
                        + ZONE_NAME + "'")));

        assertThrows(SqlException.class, () -> readFirst(finalQueryProc.querySingleAsync(sessionId, context,
                "CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with primary_zone__wrong='"
                        + ZONE_NAME + "'")));

        newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))",
                " IF NOT EXISTS " + curMethodName);

        String finalNewTblSql3 = newTblSql;

        assertDoesNotThrow(() -> readFirst(finalQueryProc.querySingleAsync(sessionId, context, finalNewTblSql3)));
    }

    /**
     * Tests create a table with distribution zone through public API.
     */
    @Test
    public void testCreateTableWithDistributionZone() {
        String tableName = getCurrentMethodName().toUpperCase();

        SessionId sessionId = queryProc.createSession(PropertiesHelper.emptyHolder());
        QueryContext context = QueryContext.create(SqlQueryType.ALL);

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) ",
                 tableName);

        readFirst(queryProc.querySingleAsync(sessionId, context, newTblSql));

        assertEquals(DistributionZoneManager.DEFAULT_ZONE_ID, tblsCfg.tables().get(tableName).zoneId().value());

        readFirst(queryProc.querySingleAsync(sessionId, context, "DROP TABLE " + tableName));

        int zoneId = dstZnsCfg.distributionZones().get(ZONE_NAME).zoneId().value();

        newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) "
                + "with primary_zone='%s'", tableName, ZONE_NAME);

        readFirst(queryProc.querySingleAsync(sessionId, context, newTblSql));

        assertEquals(zoneId, tblsCfg.tables().get(tableName).zoneId().value());

        readFirst(queryProc.querySingleAsync(sessionId, context, "DROP TABLE " + tableName));

        log.info("Creating a table with a non-existent distribution zone.");

        String nonExistZone = "non-exist-zone";

        when(distributionZoneManager.zoneIdAsyncInternal(nonExistZone)).thenReturn(completedFuture(null));

        Throwable exception = assertThrows(
                Throwable.class,
                () -> readFirst(queryProc.querySingleAsync(sessionId, context,
                        String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) "
                                + "with primary_zone='%s'", tableName, nonExistZone)))
        );

        assertTrue(IgniteTestUtils.hasCause(exception.getCause(), DistributionZoneNotFoundException.class, null));
    }

    /**
     * Tests create and drop table through public API.
     */
    @Test
    public void testDropTable() {
        String curMethodName = getCurrentMethodName();

        SessionId sessionId = queryProc.createSession(PropertiesHelper.emptyHolder());
        QueryContext context = QueryContext.create(SqlQueryType.ALL);

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))", curMethodName);

        readFirst(queryProc.querySingleAsync(sessionId, context, newTblSql));

        readFirst(queryProc.querySingleAsync(sessionId, context, "DROP TABLE " + curMethodName));

        SqlQueryProcessor finalQueryProc = queryProc;

        assertThrows(TableNotFoundException.class, () -> readFirst(finalQueryProc.querySingleAsync(sessionId, context,
                "DROP TABLE " + curMethodName + "_not_exist")));

        assertThrows(TableNotFoundException.class, () -> readFirst(finalQueryProc.querySingleAsync(sessionId, context,
                "DROP TABLE " + curMethodName)));

        assertThrows(TableNotFoundException.class, () -> readFirst(finalQueryProc.querySingleAsync(sessionId, context,
                "DROP TABLE PUBLIC." + curMethodName)));

        readFirst(queryProc.querySingleAsync(sessionId, context, "DROP TABLE IF EXISTS PUBLIC." + curMethodName + "_not_exist"));

        readFirst(queryProc.querySingleAsync(sessionId, context, "DROP TABLE IF EXISTS PUBLIC." + curMethodName));

        assertTrue(tblManager.tables().stream().noneMatch(t -> t.name()
                .equalsIgnoreCase("PUBLIC." + curMethodName)));
    }

    @Test
    void createTableWithTableOptions() {
        String method = getCurrentMethodName();

        SessionId sessionId = queryProc.createSession(PropertiesHelper.emptyHolder());
        QueryContext context = QueryContext.create(SqlQueryType.ALL);

        assertDoesNotThrow(() -> readFirst(queryProc.querySingleAsync(
                sessionId,
                context,
                String.format(
                        "CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with primary_zone='%s'",
                        method + 4,
                        ZONE_NAME
                )
        )));

        IgniteException exception = assertThrows(
                IgniteException.class,
                () -> readFirst(queryProc.querySingleAsync(
                        sessionId,
                        context,
                        String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) WITH %s='%s'", method + 6, method, method)
                ))
        );

        assertThat(exception.getMessage(), containsString("Unexpected table option"));
    }

    // todo copy-paste from TableManagerTest will be removed after https://issues.apache.org/jira/browse/IGNITE-16050

    /**
     * Instantiates a table and prepares Table manager.
     *
     * @return Table manager.
     */
    private TableManager mockManagers() throws NodeStoppingException {
        when(rm.startRaftGroupNodeAndWaitNodeReadyFuture(any(), any(), any(), any())).thenAnswer(mock -> {
            RaftGroupService raftGrpSrvcMock = mock(RaftGroupService.class);

            when(raftGrpSrvcMock.leader()).thenReturn(new Peer("test"));

            return completedFuture(raftGrpSrvcMock);
        });

        when(rm.startRaftGroupService(any(), any(), any())).thenAnswer(mock -> {
            RaftGroupService raftGrpSrvcMock = mock(TopologyAwareRaftGroupService.class);

            when(raftGrpSrvcMock.leader()).thenReturn(new Peer("test"));

            return completedFuture(raftGrpSrvcMock);
        });

        when(ts.getByAddress(any(NetworkAddress.class))).thenReturn(new ClusterNodeImpl(
                UUID.randomUUID().toString(),
                "node0",
                new NetworkAddress("localhost", 47500)
        ));

        try (MockedStatic<SchemaUtils> schemaServiceMock = mockStatic(SchemaUtils.class)) {
            schemaServiceMock.when(() -> SchemaUtils.prepareSchemaDescriptor(anyInt(), any()))
                    .thenReturn(mock(SchemaDescriptor.class));
        }

        when(cs.messagingService()).thenAnswer(invocation -> {
            MessagingService ret = mock(MessagingService.class);

            return ret;
        });

        when(cs.nodeName()).thenAnswer(invocation -> "node1");

        when(cs.topologyService()).thenAnswer(invocation -> {
            TopologyService ret = mock(TopologyService.class);

            when(ret.localMember()).thenReturn(new ClusterNodeImpl("1", "node1", null));

            return ret;
        });

        when(replicaManager.stopReplica(any())).thenReturn(completedFuture(true));

        return createTableManager();
    }

    private TableManager createTableManager() {
        TableManager tableManager = new TableManager(
                "",
                revisionUpdater,
                tblsCfg,
                dstZnsCfg,
                gcConfig,
                cs,
                rm,
                replicaManager,
                null,
                null,
                bm,
                ts,
                tm,
                dataStorageManager,
                workDir,
                msm,
                schemaManager,
                null,
                clock,
                mock(OutgoingSnapshotsManager.class),
                mock(TopologyAwareRaftGroupServiceFactory.class),
                vaultManager,
                cmgMgr,
                distributionZoneManager
        );

        tableManager.start();

        return tableManager;
    }

    private <T> BatchedResult<T> readFirst(CompletableFuture<AsyncSqlCursor<List<Object>>> cursors) {
        return (BatchedResult<T>) await(await(cursors).requestNextAsync(512));
    }

    private @Nullable TableView tableView(String tableName) {
        return tblsCfg.tables().value().get(tableName.toUpperCase());
    }
}
