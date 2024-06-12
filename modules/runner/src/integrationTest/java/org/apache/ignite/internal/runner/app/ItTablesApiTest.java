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

package org.apache.ignite.internal.runner.app;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.IndexTestUtils.waitForIndexToAppearInAnyState;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteTablesInternal;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.test.WatchListenerInhibitor.metastorageEventsInhibitor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.EmbeddedNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.IndexExistsValidationException;
import org.apache.ignite.internal.catalog.TableExistsValidationException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests to check consistent of java API on different nodes.
 */
@SuppressWarnings("ThrowableNotThrown")
public class ItTablesApiTest extends IgniteAbstractTest {
    /** Table name. */
    public static final String TABLE_NAME = "TBL1";

    private static final String INDEX_NAME = "testHI".toUpperCase(Locale.ROOT);

    /** Nodes bootstrap configuration. */
    private final List<String> nodesBootstrapCfg = List.of(
            "{\n"
                    + "  network.port :3344, clientConnector.port: 10800,\n"
                    + "  network.nodeFinder.netClusterNodes:[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ],\n"
                    + "  rest.port: 10300\n"
                    + "}",

            "{\n"
                    + "  network.port :3345, clientConnector.port: 10801,\n"
                    + "  network.nodeFinder.netClusterNodes:[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ],\n"
                    + "  rest.port: 10301\n"
                    + "}",

            "{\n"
                    + "  network.port :3346, clientConnector.port: 10802,\n"
                    + "  network.nodeFinder.netClusterNodes:[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ],\n"
                    + "  rest.port: 10302\n"
                    + "}"
    );

    private final List<EmbeddedNode> nodes = new ArrayList<>();

    /** Cluster nodes. */
    private final List<Ignite> clusterNodes = new ArrayList<>();

    /**
     * Before each.
     */
    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        for (int i = 0; i < nodesBootstrapCfg.size(); i++) {
            String nodeName = testNodeName(testInfo, i);

            nodes.add(TestIgnitionManager.start(nodeName, nodesBootstrapCfg.get(i), workDir.resolve(nodeName)));
        }

        EmbeddedNode metaStorageNode = nodes.get(0);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(metaStorageNode)
                .clusterName("cluster")
                .build();
        TestIgnitionManager.init(metaStorageNode, initParameters);

        for (EmbeddedNode node : nodes) {
            CompletableFuture<Ignite> future = node.joinClusterAsync();

            assertThat(future, willCompleteSuccessfully());

            clusterNodes.add(future.join());
        }
    }

    /**
     * After each.
     */
    @AfterEach
    void afterEach(TestInfo testInfo) throws Exception {
        IgniteUtils.closeAll(nodes.stream().map(node -> node::stop));
    }

    /**
     * Tries to create a table which is already created.
     */
    @Test
    public void testTableAlreadyCreated() {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        Table tbl = createTable(ignite0, TABLE_NAME);

        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Table with name 'PUBLIC.TBL1' already exists",
                () -> createTable(ignite0, TABLE_NAME));

        assertEquals(unwrapTableImpl(tbl), unwrapTableImpl(createTableIfNotExists(ignite0, TABLE_NAME)));
    }

    /**
     * Tries to create a table which is already created from lagged node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTableAlreadyCreatedFromLaggedNode() {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        Ignite ignite1 = clusterNodes.get(1);

        WatchListenerInhibitor ignite1Inhibitor = metastorageEventsInhibitor(ignite1);

        ignite1Inhibitor.startInhibit();

        createTable(ignite0, TABLE_NAME);

        CompletableFuture<Void> createTblFut = runAsync(() -> createTable(ignite1, TABLE_NAME));
        CompletableFuture<Table> createTblIfNotExistsFut = supplyAsync(() -> createTableIfNotExists(ignite1, TABLE_NAME));

        for (Ignite ignite : clusterNodes) {
            if (ignite != ignite1) {
                assertThrowsSqlException(
                        Sql.STMT_VALIDATION_ERR,
                        "Table with name 'PUBLIC.TBL1' already exists",
                        () -> createTable(ignite, TABLE_NAME));

                assertNotNull(createTableIfNotExists(ignite, TABLE_NAME));
            }
        }

        assertFalse(createTblFut.isDone());
        assertFalse(createTblIfNotExistsFut.isDone());

        ignite1Inhibitor.stopInhibit();

        assertThat(createTblFut, willThrowWithCauseOrSuppressed(TableExistsValidationException.class));
        assertThat(createTblIfNotExistsFut, willCompleteSuccessfully());
    }

    /**
     * Test scenario when we have lagged node, and tables with the same name are deleted and created again.
     */
    @Test
    public void testGetTableFromLaggedNode() {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        Ignite ignite1 = clusterNodes.get(1);

        Table tbl = createTable(ignite0, TABLE_NAME);

        Tuple tableKey = Tuple.create()
                .set("key", 123L);

        Tuple value = Tuple.create()
                .set("valInt", 1234)
                .set("valStr", "some string row");

        tbl.keyValueView().put(null, tableKey, value);

        assertEquals(value, tbl.keyValueView().get(null, tableKey));

        assertEquals(value, ignite1.tables().table(TABLE_NAME).keyValueView().get(null, tableKey));

        WatchListenerInhibitor ignite1Inhibitor = metastorageEventsInhibitor(ignite1);

        ignite1Inhibitor.startInhibit();

        Tuple otherValue = Tuple.create()
                .set("valInt", 12345)
                .set("valStr", "some other string row");

        tbl.keyValueView().put(null, tableKey, otherValue);

        assertEquals(otherValue, tbl.keyValueView().get(null, tableKey));

        ignite1Inhibitor.stopInhibit();

        assertEquals(otherValue, ignite1.tables().table(TABLE_NAME).keyValueView().get(null, tableKey));
    }

    /**
     * Tries to create an index which is already created.
     */
    @Test
    public void testAddIndex() {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        createTable(ignite0, TABLE_NAME);

        tryToCreateIndex(ignite0, TABLE_NAME, true);

        assertThrowsWithCause(() -> tryToCreateIndex(ignite0, TABLE_NAME, true), IndexExistsValidationException.class);

        tryToCreateIndex(ignite0, TABLE_NAME, false);
    }

    /**
     * Tries to create an index which is already created from lagged node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAddIndexFromLaggedNode() throws Exception {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        IgniteImpl ignite0 = (IgniteImpl) clusterNodes.get(0);

        createTable(ignite0, TABLE_NAME);

        Ignite ignite1 = clusterNodes.get(1);

        CompletableFuture<Void> addIndexFut;
        CompletableFuture<Void> addIndexIfNotExistsFut;

        WatchListenerInhibitor ignite1Inhibitor = metastorageEventsInhibitor(ignite1);

        ignite1Inhibitor.startInhibit();

        try {
            runAsync(() -> tryToCreateIndex(ignite0, TABLE_NAME, true));
            waitForIndexToAppearInAnyState(INDEX_NAME, ignite0);

            addIndexFut = runAsync(() -> tryToCreateIndex(ignite1, TABLE_NAME, true));
            addIndexIfNotExistsFut = runAsync(() -> addIndexIfNotExists(ignite1, TABLE_NAME));

            for (Ignite ignite : clusterNodes) {
                if (ignite != ignite1) {
                    assertThrowsWithCause(() -> tryToCreateIndex(ignite, TABLE_NAME, true), IndexExistsValidationException.class);

                    addIndexIfNotExists(ignite, TABLE_NAME);
                }
            }

            assertFalse(addIndexFut.isDone());
            assertFalse(addIndexIfNotExistsFut.isDone());
        } finally {
            ignite1Inhibitor.stopInhibit();
        }

        assertThat(addIndexFut, willThrowWithCauseOrSuppressed(IndexExistsValidationException.class));

        addIndexIfNotExistsFut.get(10, TimeUnit.SECONDS);
    }

    /**
     * Tries to create a column which is already created.
     */
    @Test
    public void testAddColumn() {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        createTable(ignite0, TABLE_NAME);

        addColumn(ignite0, TABLE_NAME);

        assertThrowsWithCause(() -> addColumn(ignite0, TABLE_NAME), CatalogValidationException.class);
    }

    /** Tries to create a column which is already created from lagged node. */
    @Test
    public void testAddColumnFromLaggedNode() {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        createTable(ignite0, TABLE_NAME);

        Ignite ignite1 = clusterNodes.get(1);

        WatchListenerInhibitor ignite1Inhibitor = metastorageEventsInhibitor(ignite1);

        ignite1Inhibitor.startInhibit();

        addColumn(ignite0, TABLE_NAME);

        CompletableFuture<Void> addColFut = runAsync(() -> addColumn(ignite1, TABLE_NAME));

        for (Ignite ignite : clusterNodes) {
            if (ignite != ignite1) {
                assertThrowsSqlException(
                        Sql.STMT_VALIDATION_ERR,
                        "Failed to validate query. Column with name 'VALINT3' already exists",
                        () -> addColumn(ignite, TABLE_NAME));
            }
        }

        assertFalse(addColFut.isDone());

        ignite1Inhibitor.stopInhibit();

        assertThat(addColFut, willThrowWithCauseOrSuppressed(CatalogValidationException.class));
    }

    /**
     * Checks that if a table would be created/dropped in any cluster node, this action reflects on all others. Table management operations
     * should pass in linearize order: if an action completed in one node, the result has to be visible to another one.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateDropTable() throws Exception {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite1 = clusterNodes.get(1);

        WatchListenerInhibitor ignite1Inhibitor = metastorageEventsInhibitor(ignite1);

        ignite1Inhibitor.startInhibit();

        Table table = createTable(clusterNodes.get(0), TABLE_NAME);

        int tblId = unwrapTableViewInternal(table).tableId();

        CompletableFuture<Table> tableByNameFut = supplyAsync(() -> ignite1.tables().table(TABLE_NAME));

        CompletableFuture<Table> tableByIdFut = supplyAsync(() -> {
            try {
                return unwrapIgniteTablesInternal(ignite1.tables()).table(tblId);
            } catch (NodeStoppingException e) {
                throw new AssertionError(e.getMessage());
            }
        });

        // Because the event inhibitor was started, last metastorage updates do not reach to one node.
        // Therefore the table still doesn't exists locally, but API prevents getting null and waits events.
        for (Ignite ignite : clusterNodes) {
            if (ignite != ignite1) {
                assertNotNull(ignite.tables().table(TABLE_NAME));

                assertNotNull(unwrapIgniteTablesInternal(ignite.tables()).table(tblId));
            }
        }

        assertFalse(tableByNameFut.isDone());
        assertFalse(tableByIdFut.isDone());

        ignite1Inhibitor.stopInhibit();

        assertNotNull(tableByNameFut.get(10, TimeUnit.SECONDS));
        assertNotNull(tableByIdFut.get(10, TimeUnit.SECONDS));

        ignite1Inhibitor.startInhibit();

        dropTable(clusterNodes.get(0), TABLE_NAME);

        // Because the event inhibitor was started, last metastorage updates do not reach to one node.
        // Therefore the table still exists locally, but API prevents getting it.
        for (Ignite ignite : clusterNodes) {
            assertNull(ignite.tables().table(TABLE_NAME));

            assertNull(unwrapIgniteTablesInternal(ignite.tables()).table(tblId));

            assertThrowsSqlException(
                    Sql.STMT_VALIDATION_ERR,
                    "Table with name 'PUBLIC.TBL1' not found",
                    () -> dropTable(ignite, TABLE_NAME));

            dropTableIfExists(ignite, TABLE_NAME);
        }

        ignite1Inhibitor.stopInhibit();
    }

    @Test
    public void usingTableAfterDrop() {
        Ignite ignite0 = clusterNodes.get(0);
        Table tbl = createTable(ignite0, TABLE_NAME);
        RecordView<Tuple> view = tbl.recordView();

        sql(ignite0, "DROP TABLE " + TABLE_NAME);

        assertThrows(
                TableNotFoundException.class,
                () -> view.insert(null, Tuple.create().set("key", 1L).set("valInt", 1).set("valStr", "1")),
                "Table does not exist or was dropped concurrently"
        );
    }

    /**
     * Creates table.
     *
     * @param node Cluster node.
     * @param tableName Table name.
     */
    protected static Table createTable(Ignite node, String tableName) {
        sql(node, String.format("CREATE TABLE %s (key BIGINT PRIMARY KEY, valInt INT, valStr VARCHAR)", tableName));

        return node.tables().table(tableName);
    }

    /**
     * Adds an index if it does not exist.
     *
     * @param node Cluster node.
     * @param tableName Table name.
     */
    private static Table createTableIfNotExists(Ignite node, String tableName) {
        sql(node, String.format("CREATE TABLE IF NOT EXISTS %s (key BIGINT PRIMARY KEY, valInt INT, valStr VARCHAR)", tableName));

        return node.tables().table(tableName);
    }

    /**
     * Drops the table which name is specified. If the table does not exist, an exception will be thrown.
     *
     * @param node Cluster node.
     * @param tableName Table name.
     */
    private static void dropTable(Ignite node, String tableName) {
        sql(node, String.format("DROP TABLE %s", tableName));
    }

    /**
     * Drops the table which name is specified. If the table did not exist, a dropping would ignore.
     *
     * @param node Cluster node.
     * @param tableName Table name.
     */
    private static void dropTableIfExists(Ignite node, String tableName) {
        sql(node, String.format("DROP TABLE IF EXISTS %s", tableName));
    }

    /**
     * Adds an index.
     *
     * @param node Cluster node.
     * @param tableName Table name.
     */
    private static void addColumn(Ignite node, String tableName) {
        sql(node, String.format("ALTER TABLE %s ADD COLUMN valint3 INT", tableName));
    }

    /**
     * Adds a column.
     *
     * @param node Cluster node.
     * @param tableName Table name.
     */
    protected void tryToCreateIndex(Ignite node, String tableName, boolean failIfNotExist) {
        sql(
                node,
                String.format("CREATE INDEX %s %s ON %s (valInt, valStr)", failIfNotExist ? "" : "IF NOT EXISTS", INDEX_NAME, tableName)
        );
    }

    /**
     * Creates a table if it does not exist.
     *
     * @param node Cluster node.
     * @param tableName Table name.
     */
    protected void addIndexIfNotExists(Ignite node, String tableName) {
        sql(node, String.format("CREATE INDEX IF NOT EXISTS %s ON %s (valInt)", INDEX_NAME, tableName));
    }

    private static void sql(Ignite node, String sql) {
        node.sql().execute(null, sql);
    }
}
