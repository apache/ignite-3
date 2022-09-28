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
import static org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.test.WatchListenerInhibitor.metastorageEventsInhibitor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnDefinition;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests to check consistent of java API on different nodes.
 */
public class ItTablesApiTest extends IgniteAbstractTest {
    /** Schema name. */
    public static final String SCHEMA = "PUBLIC";

    /** Short table name. */
    public static final String SHORT_TABLE_NAME = "tbl1";

    /** Table name. */
    public static final String TABLE_NAME = SchemaUtils.canonicalName(SCHEMA, SHORT_TABLE_NAME);

    /** Nodes bootstrap configuration. */
    private final List<String> nodesBootstrapCfg = List.of(
            "{\n"
                    + "  network.port :3344,\n"
                    + "  network.nodeFinder.netClusterNodes:[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                    + "}",

            "{\n"
                    + "  network.port :3345,\n"
                    + "  network.nodeFinder.netClusterNodes:[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                    + "}",

            "{\n"
                    + "  network.port :3346,\n"
                    + "  network.nodeFinder.netClusterNodes:[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                    + "}"
    );

    /** Cluster nodes. */
    private final List<Ignite> clusterNodes = new ArrayList<>();

    /**
     * Before each.
     */
    @BeforeEach
    void beforeEach(TestInfo testInfo) throws Exception {
        List<CompletableFuture<Ignite>> futures = new ArrayList<>();

        for (int i = 0; i < nodesBootstrapCfg.size(); i++) {
            String nodeName = testNodeName(testInfo, i);

            futures.add(IgnitionManager.start(nodeName, nodesBootstrapCfg.get(i), workDir.resolve(nodeName)));
        }

        String metaStorageNodeName = testNodeName(testInfo, 0);

        IgnitionManager.init(metaStorageNodeName, List.of(metaStorageNodeName), "cluster");

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            clusterNodes.add(future.join());
        }
    }

    /**
     * After each.
     */
    @AfterEach
    void afterEach(TestInfo testInfo) throws Exception {
        List<AutoCloseable> closeables = IntStream.range(0, nodesBootstrapCfg.size())
                .mapToObj(i -> testNodeName(testInfo, i))
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .collect(Collectors.toList());

        IgniteUtils.closeAll(closeables);
    }

    /**
     * Tries to create a table which is already created.
     */
    @Test
    public void testTableAlreadyCreated() {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        Table tbl = createTable(ignite0, SCHEMA, SHORT_TABLE_NAME);

        assertThrows(TableAlreadyExistsException.class,
                () -> createTable(ignite0, SCHEMA, SHORT_TABLE_NAME));

        assertEquals(tbl, createTableIfNotExists(ignite0, SCHEMA, SHORT_TABLE_NAME));
    }

    /**
     * Tries to create a table which is already created from lagged node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTableAlreadyCreatedFromLaggedNode() throws Exception {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        Ignite ignite1 = clusterNodes.get(1);

        WatchListenerInhibitor ignite1Inhibitor = metastorageEventsInhibitor(ignite1);

        ignite1Inhibitor.startInhibit();

        createTable(ignite0, SCHEMA, SHORT_TABLE_NAME);

        CompletableFuture<Void> createTblFut = runAsync(() -> createTable(ignite1, SCHEMA, SHORT_TABLE_NAME));
        CompletableFuture<Table> createTblIfNotExistsFut = supplyAsync(() -> createTableIfNotExists(ignite1, SCHEMA, SHORT_TABLE_NAME));

        for (Ignite ignite : clusterNodes) {
            if (ignite != ignite1) {
                assertThrows(TableAlreadyExistsException.class,
                        () -> createTable(ignite, SCHEMA, SHORT_TABLE_NAME));

                assertNotNull(createTableIfNotExists(ignite, SCHEMA, SHORT_TABLE_NAME));
            }
        }

        assertFalse(createTblFut.isDone());
        assertFalse(createTblIfNotExistsFut.isDone());

        ignite1Inhibitor.stopInhibit();

        assertThrows(TableAlreadyExistsException.class, () -> {
            try {
                createTblFut.get(10, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        });

        assertNotNull(createTblIfNotExistsFut.get(10, TimeUnit.SECONDS));
    }

    /**
     * Test scenario when we have lagged node, and tables with the same name are deleted and created again.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testGetTableFromLaggedNode() throws Exception {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        Ignite ignite1 = clusterNodes.get(1);

        Table tbl = createTable(ignite0, SCHEMA, SHORT_TABLE_NAME);

        final Tuple tableKey = Tuple.create()
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

        createTable(ignite0, SCHEMA, SHORT_TABLE_NAME);

        tryToCreateIndex(ignite0, SCHEMA, SHORT_TABLE_NAME, true);

        try {
            tryToCreateIndex(ignite0, SCHEMA, SHORT_TABLE_NAME, true);
        } catch (Throwable e) {
            IgniteTestUtils.hasCause(e, IndexAlreadyExistsException.class, null);
        }

        tryToCreateIndex(ignite0, SCHEMA, SHORT_TABLE_NAME, false);
    }

    /**
     * Tries to create an index which is already created from lagged node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAddIndexFromLaggedNode() throws Exception {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        createTable(ignite0, SCHEMA, SHORT_TABLE_NAME);

        Ignite ignite1 = clusterNodes.get(1);

        WatchListenerInhibitor ignite1Inhibitor = metastorageEventsInhibitor(ignite1);

        ignite1Inhibitor.startInhibit();

        tryToCreateIndex(ignite0, SCHEMA, SHORT_TABLE_NAME, true);

        CompletableFuture<Void> addIndesFut = runAsync(() -> tryToCreateIndex(ignite1, SCHEMA, SHORT_TABLE_NAME, true));
        CompletableFuture<Void> addIndesIfNotExistsFut = runAsync(() -> addIndexIfNotExists(ignite1, SCHEMA, SHORT_TABLE_NAME));

        for (Ignite ignite : clusterNodes) {
            if (ignite != ignite1) {
                try {
                    tryToCreateIndex(ignite, SCHEMA, SHORT_TABLE_NAME, true);

                    fail("Should not reach here");
                } catch (Throwable e) {
                    IgniteTestUtils.hasCause(e, IndexAlreadyExistsException.class, null);
                }

                addIndexIfNotExists(ignite, SCHEMA, SHORT_TABLE_NAME);
            }
        }

        assertFalse(addIndesFut.isDone());
        assertFalse(addIndesIfNotExistsFut.isDone());

        ignite1Inhibitor.stopInhibit();

        try {
            addIndesFut.get(10, TimeUnit.SECONDS);
        } catch (Throwable e) {
            IgniteTestUtils.hasCause(e, IndexAlreadyExistsException.class, null);
        }

        addIndesIfNotExistsFut.get(10, TimeUnit.SECONDS);
    }

    /**
     * Tries to create a column which is already created.
     */
    @Test
    public void testAddColumn() {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        createTable(ignite0, SCHEMA, SHORT_TABLE_NAME);

        addColumn(ignite0, SCHEMA, SHORT_TABLE_NAME);

        assertThrows(ColumnAlreadyExistsException.class,
                () -> addColumn(ignite0, SCHEMA, SHORT_TABLE_NAME));

        addColumnIfNotExists(ignite0, SCHEMA, SHORT_TABLE_NAME);
    }

    /**
     * Tries to create a column which is already created from lagged node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAddColumnFromLaggedNode() throws Exception {
        clusterNodes.forEach(ign -> assertNull(ign.tables().table(TABLE_NAME)));

        Ignite ignite0 = clusterNodes.get(0);

        createTable(ignite0, SCHEMA, SHORT_TABLE_NAME);

        Ignite ignite1 = clusterNodes.get(1);

        WatchListenerInhibitor ignite1Inhibitor = metastorageEventsInhibitor(ignite1);

        ignite1Inhibitor.startInhibit();

        addColumn(ignite0, SCHEMA, SHORT_TABLE_NAME);

        CompletableFuture<Void> addColFut = runAsync(() -> addColumn(ignite1, SCHEMA, SHORT_TABLE_NAME));
        CompletableFuture<Void> addColIfNotExistsFut = runAsync(() -> addColumnIfNotExists(ignite1, SCHEMA, SHORT_TABLE_NAME));

        for (Ignite ignite : clusterNodes) {
            if (ignite != ignite1) {
                assertThrows(ColumnAlreadyExistsException.class,
                        () -> addColumn(ignite, SCHEMA, SHORT_TABLE_NAME));

                addColumnIfNotExists(ignite, SCHEMA, SHORT_TABLE_NAME);
            }
        }

        assertFalse(addColFut.isDone());
        assertFalse(addColIfNotExistsFut.isDone());

        ignite1Inhibitor.stopInhibit();

        assertThrows(ColumnAlreadyExistsException.class, () -> {
            try {
                addColFut.get(10, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        });

        addColIfNotExistsFut.get(10, TimeUnit.SECONDS);
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

        Table table = createTable(clusterNodes.get(0), SCHEMA, SHORT_TABLE_NAME);

        UUID tblId = ((TableImpl) table).tableId();

        CompletableFuture<Table> tableByNameFut = supplyAsync(() -> ignite1.tables().table(TABLE_NAME));

        CompletableFuture<Table> tableByIdFut = supplyAsync(() -> {
            try {
                return ((IgniteTablesInternal) ignite1.tables()).table(tblId);
            } catch (NodeStoppingException e) {
                throw new AssertionError(e.getMessage());
            }
        });

        // Because the event inhibitor was started, last metastorage updates do not reach to one node.
        // Therefore the table still doesn't exists locally, but API prevents getting null and waits events.
        for (Ignite ignite : clusterNodes) {
            if (ignite != ignite1) {
                assertNotNull(ignite.tables().table(TABLE_NAME));

                assertNotNull(((IgniteTablesInternal) ignite.tables()).table(tblId));
            }
        }

        assertFalse(tableByNameFut.isDone());
        assertFalse(tableByIdFut.isDone());

        ignite1Inhibitor.stopInhibit();

        assertNotNull(tableByNameFut.get(10, TimeUnit.SECONDS));
        assertNotNull(tableByIdFut.get(10, TimeUnit.SECONDS));

        ignite1Inhibitor.startInhibit();

        clusterNodes.get(0).tables().dropTable(TABLE_NAME);

        // Because the event inhibitor was started, last metastorage updates do not reach to one node.
        // Therefore the table still exists locally, but API prevents getting it.
        for (Ignite ignite : clusterNodes) {
            assertNull(ignite.tables().table(TABLE_NAME));

            assertNull(((IgniteTablesInternal) ignite.tables()).table(tblId));

            assertThrows(TableNotFoundException.class, () -> dropTable(ignite, SCHEMA, SHORT_TABLE_NAME));

            dropTableIfExists(ignite, SCHEMA, SHORT_TABLE_NAME);
        }

        ignite1Inhibitor.stopInhibit();
    }

    /**
     * Creates table.
     *
     * @param node Cluster node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     */
    protected Table createTable(Ignite node, String schemaName, String shortTableName) {
        List<ColumnDefinition> cols = new ArrayList<>();
        cols.add(SchemaBuilders.column("key", ColumnType.INT64).build());
        cols.add(SchemaBuilders.column("valInt", ColumnType.INT32).asNullable(true).build());
        cols.add(SchemaBuilders.column("valStr", ColumnType.string()).withDefaultValue("default").build());

        return node.tables().createTable(
                schemaName + "." + shortTableName,
                tblCh -> convert(SchemaBuilders.tableBuilder(schemaName, shortTableName).columns(
                        cols).withPrimaryKey("key").build(), tblCh).changeReplicas(2).changePartitions(10)
        );
    }

    /**
     * Adds an index if it does not exist.
     *
     * @param node Cluster node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     */
    protected Table createTableIfNotExists(Ignite node, String schemaName, String shortTableName) {
        try {
            return node.tables().createTable(
                    schemaName + "." + shortTableName,
                    tblCh -> convert(SchemaBuilders.tableBuilder(schemaName, shortTableName).columns(Arrays.asList(
                                    SchemaBuilders.column("key", ColumnType.INT64).build(),
                                    SchemaBuilders.column("valInt", ColumnType.INT32).asNullable(true).build(),
                                    SchemaBuilders.column("valStr", ColumnType.string())
                                            .withDefaultValue("default").build()
                            )).withPrimaryKey("key").build(),
                            tblCh).changeReplicas(2).changePartitions(10)
            );
        } catch (TableAlreadyExistsException ex) {
            return node.tables().table(schemaName + "." + shortTableName);
        }
    }

    /**
     * Drops the table which name is specified. If the table does not exist, an exception will be thrown.
     *
     * @param node Cluster node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     */
    protected void dropTable(Ignite node, String schemaName, String shortTableName) {
        node.tables().dropTable(schemaName + "." + shortTableName);
    }

    /**
     * Drops the table which name is specified. If the table did not exist, a dropping would ignore.
     *
     * @param node Cluster node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     */
    protected void dropTableIfExists(Ignite node, String schemaName, String shortTableName) {
        try {
            node.tables().dropTable(schemaName + "." + shortTableName);
        } catch (TableNotFoundException ex) {
            log.info("Dropping the table ignored.", ex);
        }
    }

    /**
     * Adds an index.
     *
     * @param node Cluster node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     */
    protected void addColumn(Ignite node, String schemaName, String shortTableName) {
        ColumnDefinition col = SchemaBuilders.column("valStrNew", ColumnType.string()).asNullable(true)
                .withDefaultValue("default").build();

        addColumnInternal(node, schemaName, shortTableName, col);
    }

    /**
     * Adds a column according to the column definition.
     *
     * @param node Ignite node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     * @param colDefinition Column defenition.
     */
    private void addColumnInternal(Ignite node, String schemaName, String shortTableName, ColumnDefinition colDefinition) {
        node.tables().alterTable(
                schemaName + "." + shortTableName,
                chng -> chng.changeColumns(cols -> {
                    try {
                        cols.create(colDefinition.name(), colChg -> convert(colDefinition, colChg));
                    } catch (IllegalArgumentException e) {
                        throw new ColumnAlreadyExistsException(colDefinition.name());
                    }
                }));
    }

    /**
     * Adds a column if it does not exist.
     *
     * @param node Ignite node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     */
    protected void addColumnIfNotExists(Ignite node, String schemaName, String shortTableName) {
        ColumnDefinition col = SchemaBuilders.column("valStrNew", ColumnType.string()).asNullable(true)
                .withDefaultValue("default").build();

        try {
            addColumnInternal(node, schemaName, shortTableName, col);
        } catch (ColumnAlreadyExistsException ex) {
            log.info("Column already exists [naem={}]", col.name());
        }
    }

    /**
     * Adds a column.
     *
     * @param node Cluster node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     */
    protected void tryToCreateIndex(Ignite node, String schemaName, String shortTableName, boolean failIfNotExist) {
        String tblName = SchemaUtils.canonicalName(schemaName, shortTableName);

        var tmpl  = "CREATE INDEX %s testHI ON %s (valInt, valStr)";
        var sql = String.format(tmpl, failIfNotExist ? "" : "IF NOT EXISTS", tblName);

        try (Session ses = node.sql().createSession()) {
            ses.execute(null, sql);
        }
    }

    /**
     * Creates a table if it does not exist.
     *
     * @param node Cluster node.
     * @param schemaName Schema name.
     * @param shortTableName Table name.
     */
    protected void addIndexIfNotExists(Ignite node, String schemaName, String shortTableName) {
        String tblName = SchemaUtils.canonicalName(schemaName, shortTableName);

        try (Session ses = node.sql().createSession()) {
            ses.execute(null, String.format("CREATE INDEX IF NOT EXISTS testHI ON %s (CAT_ID)", tblName));
        }
    }
}
