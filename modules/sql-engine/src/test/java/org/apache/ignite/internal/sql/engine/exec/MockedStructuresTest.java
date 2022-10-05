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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.schema.configuration.storage.UnknownDataStorageConfigurationSchema.UNKNOWN_DATA_STORAGE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine.ENGINE_NAME;
import static org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.notifications.ConfigurationStorageRevisionListenerHolder;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectRevisionListenerHolder;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.impl.TestDataStorageModule;
import org.apache.ignite.internal.storage.impl.TestStorageEngine;
import org.apache.ignite.internal.storage.impl.schema.TestDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.impl.schema.TestDataStorageView;
import org.apache.ignite.internal.storage.rocksdb.RocksDbDataStorageModule;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageView;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
    private Loza rm;

    /** TX manager. */
    @Mock(lenient = true)
    private TxManager tm;

    /** Meta storage manager. */
    @Mock
    MetaStorageManager msm;

    /**
     * Revision listener holder. It uses for the test configurations:
     * <ul>
     * <li>{@link MockedStructuresTest#tblsCfg},</li>
     * </ul>
     */
    @InjectRevisionListenerHolder
    private ConfigurationStorageRevisionListenerHolder fieldRevisionListenerHolder;

    /** Revision updater. */
    private Consumer<Function<Long, CompletableFuture<?>>> revisionUpdater;

    /** Tables configuration. */
    @InjectConfiguration
    private TablesConfiguration tblsCfg;

    TableManager tblManager;

    IndexManager idxManager;

    SqlQueryProcessor queryProc;

    /** Test node. */
    private final ClusterNode node = new ClusterNode(
            UUID.randomUUID().toString(),
            NODE_NAME,
            new NetworkAddress("127.0.0.1", 2245)
    );

    @InjectConfiguration
    private RocksDbStorageEngineConfiguration rocksDbEngineConfig;

    @Mock
    private ConfigurationRegistry configRegistry;

    DataStorageManager dataStorageManager;

    SchemaManager schemaManager;

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
        when(rm.messagingService()).thenReturn(mock(MessagingService.class));
        when(rm.topologyService()).thenReturn(mock(TopologyService.class));

        mockMetastore();

        revisionUpdater = (Function<Long, CompletableFuture<?>> function) -> {
            function.apply(0L).join();

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
                tblsCfg,
                dataStorageModules.createStorageEngines(NODE_NAME, configRegistry, workDir, null)
        );

        dataStorageManager.start();

        schemaManager = new SchemaManager(revisionUpdater, tblsCfg, msm);

        schemaManager.start();

        tblManager = mockManagers();

        idxManager = new IndexManager(tblsCfg);

        idxManager.start();

        queryProc = new SqlQueryProcessor(
                revisionUpdater,
                cs,
                tblManager,
                idxManager,
                schemaManager,
                dataStorageManager,
                tm,
                () -> dataStorageModules.collectSchemasFields(List.of(
                        RocksDbDataStorageConfigurationSchema.class,
                        TestDataStorageConfigurationSchema.class
                ))
        );

        queryProc.start();

        tblsCfg.defaultDataStorage().update(ENGINE_NAME).get(1, TimeUnit.SECONDS);

        rocksDbEngineConfig.regions()
                .change(c -> c.create("test_region", rocksDbDataRegionChange -> {}))
                .get(1, TimeUnit.SECONDS);
    }

    /**
     * Checks inner transactions are initialized correctly.
     */
    @Test
    public void testInnerTxInitiated() throws Exception {
        //mockMetastore();
        SessionId sesId = queryProc.createSession(1000, PropertiesHolder.fromMap(Map.of()));

        InternalTransaction tx = mock(InternalTransaction.class);

        when(tm.begin()).thenReturn(tx);

        String sql = "CREATE TABLE TEST (c1 int PRIMARY KEY, c2 varbinary(255))";

        CompletableFuture<AsyncSqlCursor<List<Object>>> f = queryProc.querySingleAsync(sesId, QueryContext.of(), sql);

        AsyncSqlCursor<List<Object>> asyncRes = f.get();

        asyncRes.closeAsync();

        verify(tm, never()).begin();
    }

    /** Dummy metastore activity mock. */
    private void mockMetastore() throws Exception {
        Cursor cursorMocked = mock(Cursor.class);
        Iterator itMock = mock(Iterator.class);
        when(itMock.hasNext()).thenReturn(false);
        when(msm.prefix(any())).thenReturn(cursorMocked);
        when(cursorMocked.iterator()).thenReturn(itMock);
    }

    /**
     * Tests create a table through public API.
     */
    @Test
    public void testCreateTable() throws Exception {
        SqlQueryProcessor finalQueryProc = queryProc;

        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) "
                + "with partitions=1,replicas=1", curMethodName);

        readFirst(queryProc.queryAsync("PUBLIC", newTblSql));

        assertTrue(tblManager.tables().stream().anyMatch(t -> t.name()
                .equalsIgnoreCase(curMethodName)));

        String finalNewTblSql1 = newTblSql;

        assertThrows(TableAlreadyExistsException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC", finalNewTblSql1)));

        String finalNewTblSql2 = String.format("CREATE TABLE \"PUBLIC\".%s (c1 int PRIMARY KEY, c2 varbinary(255)) "
                + "with partitions=1,replicas=1", curMethodName);

        assertThrows(TableAlreadyExistsException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC", finalNewTblSql2)));

        // todo: correct exception need to be thrown https://issues.apache.org/jira/browse/IGNITE-16084
        assertThrows(SqlException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC",
                "CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with partitions__wrong=1,replicas=1")));

        assertThrows(SqlException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC",
                "CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with partitions=1,replicas__wrong=1")));

        newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))",
                " IF NOT EXISTS " + curMethodName);

        String finalNewTblSql3 = newTblSql;

        assertDoesNotThrow(() -> await(finalQueryProc.queryAsync("PUBLIC", finalNewTblSql3).get(0)));
    }

    /**
     * Tests create a table with multiple pk through public API.
     */
    @Test
    public void testCreateTableMultiplePk() {
        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int, c2 int NOT NULL DEFAULT 1, c3 int, primary key(c1, c2))", curMethodName);

        readFirst(queryProc.queryAsync("PUBLIC", newTblSql));

        assertTrue(tblManager.tables().stream().anyMatch(t -> t.name()
                .equalsIgnoreCase(curMethodName)));
    }

    /**
     * Tests create and drop table through public API.
     */
    @Test
    public void testDropTable() {
        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))", curMethodName);

        readFirst(queryProc.queryAsync("PUBLIC", newTblSql));

        readFirst(queryProc.queryAsync("PUBLIC", "DROP TABLE " + curMethodName));

        SqlQueryProcessor finalQueryProc = queryProc;

        assertThrows(TableNotFoundException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC",
                "DROP TABLE " + curMethodName + "_not_exist")));

        assertThrows(TableNotFoundException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC",
                "DROP TABLE " + curMethodName)));

        assertThrows(TableNotFoundException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC",
                "DROP TABLE PUBLIC." + curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", "DROP TABLE IF EXISTS PUBLIC." + curMethodName + "_not_exist"));

        readFirst(queryProc.queryAsync("PUBLIC", "DROP TABLE IF EXISTS PUBLIC." + curMethodName));

        assertTrue(tblManager.tables().stream().noneMatch(t -> t.name()
                .equalsIgnoreCase("PUBLIC." + curMethodName)));
    }

    /**
     * Tests alter and drop columns through public API.
     */
    @Test
    public void testAlterAndDropSimpleCase() {
        SqlQueryProcessor finalQueryProc = queryProc;

        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))", curMethodName);

        readFirst(queryProc.queryAsync("PUBLIC", newTblSql));

        String alterCmd = String.format("ALTER TABLE %s ADD COLUMN (c3 varchar, c4 int)", curMethodName);

        readFirst(queryProc.queryAsync("PUBLIC", alterCmd));

        String alterCmd1 = String.format("ALTER TABLE %s ADD COLUMN c5 int NOT NULL DEFAULT 1", curMethodName);

        readFirst(queryProc.queryAsync("PUBLIC", alterCmd1));

        assertThrows(ColumnAlreadyExistsException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC", alterCmd)));

        String alterCmdNoTbl = String.format("ALTER TABLE %s ADD COLUMN (c3 varchar, c4 int)", curMethodName + "_notExist");

        assertThrows(TableNotFoundException.class, () -> readFirst(queryProc.queryAsync("PUBLIC", alterCmdNoTbl)));

        String alterIfExistsCmd = String.format("ALTER TABLE IF EXISTS %s ADD COLUMN (c3 varchar, c4 int)", curMethodName + "NotExist");

        readFirst(queryProc.queryAsync("PUBLIC", alterIfExistsCmd));

        assertThrows(ColumnAlreadyExistsException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC", alterCmd)));

        readFirst(finalQueryProc.queryAsync("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN c4", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS c3 varchar", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN c3", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN IF EXISTS c3", curMethodName)));

        assertThrows(ColumnNotFoundException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC",
                String.format("ALTER TABLE %s DROP COLUMN (c3, c4)", curMethodName))));

        assertThrows(IgniteException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC",
                String.format("ALTER TABLE %s DROP COLUMN c1", curMethodName))));
    }

    /**
     * Tests alter add multiple columns through public API.
     */
    @Test
    public void testAlterColumnsAddBatch() {
        String curMethodName = getCurrentMethodName();

        readFirst(queryProc.queryAsync("PUBLIC", String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("ALTER TABLE %s ADD COLUMN (c3 varchar, c4 varchar)", curMethodName)));

        readFirst(queryProc
                .queryAsync("PUBLIC", String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS (c3 varchar, c4 varchar)", curMethodName)));

        readFirst(
                queryProc.queryAsync("PUBLIC", String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS (c3 varchar, c4 varchar, c5 varchar)",
                        curMethodName)));

        SqlQueryProcessor finalQueryProc = queryProc;

        assertThrows(ColumnAlreadyExistsException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC",
                String.format("ALTER TABLE %s ADD COLUMN (c5 varchar)", curMethodName))));
    }

    /**
     * Tests alter drop multiple columns through public API.
     */
    @Test
    public void testAlterColumnsDropBatch() {
        String curMethodName = getCurrentMethodName();

        readFirst(queryProc.queryAsync("PUBLIC", String.format("CREATE TABLE %s "
                + "(c1 int PRIMARY KEY, c2 decimal(10), c3 varchar, c4 varchar, c5 varchar)", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN c4", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN IF EXISTS (c3, c4, c5)", curMethodName)));

        SqlQueryProcessor finalQueryProc = queryProc;

        assertThrows(ColumnNotFoundException.class, () -> readFirst(finalQueryProc.queryAsync("PUBLIC",
                String.format("ALTER TABLE %s DROP COLUMN c4", curMethodName))));
    }

    /**
     * Tests create a table through public API.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16032")
    @Test
    public void testCreateDropIndex() {
        SqlQueryProcessor finalQueryProc = queryProc;

        String curMethodName = getCurrentMethodName();

        String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with partitions=1", curMethodName);

        readFirst(queryProc.queryAsync("PUBLIC", newTblSql));

        assertTrue(tblManager.tables().stream().anyMatch(t -> t.name()
                .equalsIgnoreCase("PUBLIC." + curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("CREATE INDEX index1 ON %s (c1)", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("CREATE INDEX IF NOT EXISTS index1 ON %s (c1)", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("CREATE INDEX index2 ON %s (c1)", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("CREATE INDEX index3 ON %s (c2)", curMethodName)));

        assertThrows(IndexAlreadyExistsException.class, () ->
                readFirst(finalQueryProc.queryAsync("PUBLIC", String.format("CREATE INDEX index3 ON %s (c1)", curMethodName))));

        assertThrows(IgniteException.class, () ->
                readFirst(finalQueryProc
                        .queryAsync("PUBLIC", String.format("CREATE INDEX index_3 ON %s (c1)", curMethodName + "_nonExist"))));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("CREATE INDEX index4 ON %s (c2 desc, c1 asc)", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("DROP INDEX index4 ON %s", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("CREATE INDEX index4 ON %s (c2 desc, c1 asc)", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("DROP INDEX index4 ON %s", curMethodName)));

        readFirst(queryProc.queryAsync("PUBLIC", String.format("DROP INDEX IF EXISTS index4 ON %s", curMethodName)));
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17197")
    @Test
    void createTableWithEngine() throws Exception {
        String method = getCurrentMethodName();

        // Without engine.
        assertDoesNotThrow(() -> readFirst(queryProc.queryAsync(
                "PUBLIC",
                String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255))", method + 0)
        )));

        assertThat(tableView(method + 0).dataStorage(), instanceOf(RocksDbDataStorageView.class));

        // With existing engine.
        assertDoesNotThrow(() -> readFirst(queryProc.queryAsync(
                "PUBLIC",
                String.format(
                        "CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) engine %s",
                        method + 1,
                        TestStorageEngine.ENGINE_NAME
                )
        )));

        assertThat(tableView(method + 1).dataStorage(), instanceOf(TestDataStorageView.class));

        // With existing engine in mixed case
        assertDoesNotThrow(() -> readFirst(queryProc.queryAsync(
                "PUBLIC",
                String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) engine %s", method + 2, "\"RocksDb\"")
        )));

        assertThat(tableView(method + 2).dataStorage(), instanceOf(RocksDbDataStorageView.class));

        IgniteException exception = assertThrows(
                IgniteException.class,
                () -> readFirst(queryProc.queryAsync(
                        "PUBLIC",
                        String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) engine %s", method + 3, method)
                ))
        );

        assertThat(exception.getMessage(), startsWith("Unexpected data storage engine"));

        tblsCfg.defaultDataStorage().update(UNKNOWN_DATA_STORAGE).get(1, TimeUnit.SECONDS);

        exception = assertThrows(
                IgniteException.class,
                () -> readFirst(queryProc.queryAsync(
                        "PUBLIC",
                        String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255))", method + 4)
                ))
        );

        assertThat(exception.getMessage(), startsWith("Default data storage is not defined"));
    }

    @Test
    void createTableWithTableOptions() {
        String method = getCurrentMethodName();

        assertDoesNotThrow(() -> readFirst(queryProc.queryAsync(
                "PUBLIC",
                String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with replicas=1", method + 0)
        )));

        assertDoesNotThrow(() -> readFirst(queryProc.queryAsync(
                "PUBLIC",
                String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with REPLICAS=1", method + 1)
        )));

        assertDoesNotThrow(() -> readFirst(queryProc.queryAsync(
                "PUBLIC",
                String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with \"replicas\"=1", method + 2)
        )));

        assertDoesNotThrow(() -> readFirst(queryProc.queryAsync(
                "PUBLIC",
                String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with \"replICAS\"=1", method + 3)
        )));

        assertDoesNotThrow(() -> readFirst(queryProc.queryAsync(
                "PUBLIC",
                String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with replicas=1, partitions=1", method + 4)
        )));

        IgniteException exception = assertThrows(
                IgniteException.class,
                () -> readFirst(queryProc.queryAsync(
                        "PUBLIC",
                        String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with replicas='%s'", method + 5, method)
                ))
        );

        assertThat(exception.getMessage(), containsString("Unsuspected table option type"));

        exception = assertThrows(
                IgniteException.class,
                () -> readFirst(queryProc.queryAsync(
                        "PUBLIC",
                        String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with %s='%s'", method + 6, method, method)
                ))
        );

        assertThat(exception.getMessage(), containsString("Unexpected table option"));

        exception = assertThrows(
                IgniteException.class,
                () -> readFirst(queryProc.queryAsync(
                        "PUBLIC",
                        String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with replicas=-1", method + 7)
                ))
        );

        assertThat(exception.getMessage(), containsString("Table option validation failed"));
    }

    @Test
    void createTableWithDataStorageOptions() {
        String method = getCurrentMethodName();

        assertDoesNotThrow(() -> readFirst(queryProc.queryAsync(
                "PUBLIC",
                String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with dataRegion='default'", method + 0)
        )));

        assertThat(
                ((RocksDbDataStorageView) tableView(method + 0).dataStorage()).dataRegion(),
                equalTo(DEFAULT_DATA_REGION_NAME)
        );

        assertDoesNotThrow(() -> readFirst(queryProc.queryAsync(
                "PUBLIC",
                String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with DATAREGION='test_region'", method + 1)
        )));

        assertThat(
                ((RocksDbDataStorageView) tableView(method + 1).dataStorage()).dataRegion(),
                equalTo("test_region")
        );
    }

    /**
     * Tests that schema that is not applied yet is accessible after some time.
     *
     * @throws Exception If test has failed.
     */
    @Test
    public void testSchemaForTheFutureUpdate() throws Exception {
        String curMethodName = getCurrentMethodName();

        readFirst(queryProc.queryAsync("PUBLIC", String.format("CREATE TABLE %s "
                + "(c1 int PRIMARY KEY, c2 decimal(10), c3 varchar, c4 varchar, c5 varchar)", curMethodName)));

        SchemaRegistry schemaRegistry = (((TableImpl) tblManager.tables().get(0)).schemaView());

        int lastSchemaVersion = schemaRegistry.lastSchemaVersion();

        CountDownLatch alter = new CountDownLatch(1);
        CountDownLatch watcher = new CountDownLatch(1);

        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("id".toUpperCase(), NativeTypes.INT64, false)},
                new Column[]{new Column("val".toUpperCase(), NativeTypes.INT64, false)}
        );

        byte[] ser = SchemaSerializerImpl.INSTANCE.serialize(schema);

        try (MockedStatic<SchemaUtils> schemaServiceMock = mockStatic(SchemaUtils.class)) {
            schemaServiceMock.when(() -> SchemaUtils.schemaById(any(), any(), eq(1)))
                    .thenReturn(ser);
        }

        runAsync(() -> {
            watcher.await();
            readFirst(queryProc.queryAsync("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN c4", curMethodName)));
            alter.countDown();
        });

        //Ensure that we can get schema for the future update
        watcher.countDown();
        alter.await();
        assertEquals(schemaRegistry.schema(lastSchemaVersion + 1).version(), lastSchemaVersion + 1);
    }

    // todo copy-paste from TableManagerTest will be removed after https://issues.apache.org/jira/browse/IGNITE-16050

    /**
     * Instantiates a table and prepares Table manager.
     *
     * @return Table manager.
     */
    private TableManager mockManagers() throws NodeStoppingException {
        when(rm.prepareRaftGroup(any(), any(), any(), any())).thenAnswer(mock -> {
            RaftGroupService raftGrpSrvcMock = mock(RaftGroupService.class);

            when(raftGrpSrvcMock.leader()).thenReturn(new Peer(new NetworkAddress("localhost", 47500)));

            return completedFuture(raftGrpSrvcMock);
        });

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

        when(cs.messagingService()).thenAnswer(invocation -> {
            MessagingService ret = mock(MessagingService.class);

            return ret;
        });

        when(cs.localConfiguration()).thenAnswer(invocation -> {
            ClusterLocalConfiguration ret = mock(ClusterLocalConfiguration.class);

            when(ret.getName()).thenReturn("node1");

            return ret;
        });

        when(cs.topologyService()).thenAnswer(invocation -> {
            TopologyService ret = mock(TopologyService.class);

            when(ret.localMember()).thenReturn(new ClusterNode("1", "node1", null));

            return ret;
        });

        when(msm.registerWatch(any(ByteArray.class), any())).thenReturn(CompletableFuture.completedFuture(1L));

        TableManager tableManager = createTableManager();

        return tableManager;
    }

    private TableManager createTableManager() {
        TableManager tableManager = new TableManager(
                "",
                revisionUpdater,
                tblsCfg,
                rm,
                null,
                null,
                null,
                bm,
                ts,
                tm,
                dataStorageManager,
                workDir,
                msm,
                schemaManager,
                view -> new LocalLogStorageFactory(),
                null
        );

        tableManager.start();

        return tableManager;
    }

    private <T> BatchedResult<T> readFirst(List<CompletableFuture<AsyncSqlCursor<T>>> cursors) {
        return await(await(cursors.get(0)).requestNextAsync(512));
    }

    private @Nullable TableView tableView(String tableName) {
        return tblsCfg.tables().value().get(tableName.toUpperCase());
    }
}
