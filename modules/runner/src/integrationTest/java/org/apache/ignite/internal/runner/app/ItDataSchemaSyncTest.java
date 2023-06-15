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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * There is a test of table schema synchronization.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItDataSchemaSyncTest extends IgniteAbstractTest {
    /**
     * Table name.
     */
    public static final String TABLE_NAME = "tbl1";

    /**
     * Nodes bootstrap configuration.
     */
    private static final Map<String, String> nodesBootstrapCfg = Map.of(
            "node0", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3344,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}",

            "node1", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3345,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                    + "    }\n"
                    + "  },\n"
                    + "  clientConnector: { port:10801 }\n"
                    + "}",

            "node2", "{\n"
                    + "  \"network\": {\n"
                    + "    \"port\":3346,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                    + "    }\n"
                    + "  },\n"
                    + "  clientConnector: { port:10802 }\n"
                    + "}"
    );

    /**
     * Cluster nodes.
     */
    private final List<Ignite> clusterNodes = new ArrayList<>();

    /**
     * Starts a cluster before every test started.
     */
    @BeforeEach
    void beforeEach() {
        List<CompletableFuture<Ignite>> futures = nodesBootstrapCfg.entrySet().stream()
                .map(e -> TestIgnitionManager.start(e.getKey(), e.getValue(), workDir.resolve(e.getKey())))
                .collect(toList());

        String metaStorageNode = nodesBootstrapCfg.keySet().iterator().next();

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNode)
                .metaStorageNodeNames(List.of(metaStorageNode))
                .clusterName("cluster")
                .build();

        IgnitionManager.init(initParameters);

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            clusterNodes.add(future.join());
        }
    }

    /**
     * Stops a cluster after every test finished.
     */
    @AfterEach
    void afterEach() throws Exception {
        List<AutoCloseable> closeables = nodesBootstrapCfg.keySet().stream()
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .collect(toList());

        IgniteUtils.closeAll(closeables);
    }

    /**
     * Test correctness of schema updates on lagged node.
     */
    @Test
    public void checkSchemasCorrectUpdate() throws Exception {
        Ignite ignite0 = clusterNodes.get(0);
        IgniteImpl ignite1 = (IgniteImpl) clusterNodes.get(1);
        IgniteImpl ignite2 = (IgniteImpl) clusterNodes.get(2);

        createTable(ignite0, TABLE_NAME);

        TableImpl table = (TableImpl) ignite0.tables().table(TABLE_NAME);

        assertEquals(1, table.schemaView().schema().version());

        WatchListenerInhibitor listenerInhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(ignite1);

        listenerInhibitor.startInhibit();

        alterTable(ignite0, TABLE_NAME);

        table = (TableImpl) ignite2.tables().table(TABLE_NAME);

        TableImpl table0 = table;
        assertTrue(waitForCondition(() -> table0.schemaView().schema().version() == 2, 5_000));

        table = (TableImpl) ignite1.tables().table(TABLE_NAME);

        assertEquals(1, table.schemaView().schema().version());

        String nodeToStop = ignite1.name();

        IgnitionManager.stop(nodeToStop);

        listenerInhibitor.stopInhibit();

        CompletableFuture<Ignite> ignite1Fut = nodesBootstrapCfg.entrySet().stream()
                .filter(k -> k.getKey().equals(nodeToStop))
                .map(e -> TestIgnitionManager.start(e.getKey(), e.getValue(), workDir.resolve(e.getKey())))
                .findFirst().get();

        ignite1 = (IgniteImpl) ignite1Fut.get();

        table = (TableImpl) ignite1.tables().table(TABLE_NAME);

        TableImpl table1 = table;
        assertTrue(waitForCondition(() -> table1.schemaView().schema().version() == 2, 5_000));
    }

    /**
     * Check that sql query will wait until appropriate schema is not propagated into all nodes.
     */
    @Test
    public void queryWaitAppropriateSchema() throws Exception {
        Ignite ignite0 = clusterNodes.get(0);
        IgniteImpl ignite1 = (IgniteImpl) clusterNodes.get(1);

        createTable(ignite0, TABLE_NAME);

        WatchListenerInhibitor listenerInhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(ignite1);

        listenerInhibitor.startInhibit();

        sql(ignite0, "CREATE INDEX idx1 ON " + TABLE_NAME + "(valint)");

        CompletableFuture<Void> fut = CompletableFuture.runAsync(() -> sql(ignite0, "SELECT * FROM "
                + TABLE_NAME + " WHERE valint > 0"));

        try {
            // wait a timeout to observe that query can`t be executed.
            fut.get(1, TimeUnit.SECONDS);

            fail();
        } catch (TimeoutException e) {
            // Expected, no op.
        }

        listenerInhibitor.stopInhibit();

        // only check that request is executed without timeout.
        ResultSet<SqlRow> rs = sql(ignite0, "SELECT * FROM " + TABLE_NAME + " WHERE valint > 0");

        assertNotNull(rs);

        rs.close();
    }

    /**
     * Test correctness of schemes recovery after node restart.
     */
    @Test
    public void checkSchemasCorrectlyRestore() throws Exception {
        Ignite ignite1 = clusterNodes.get(1);

        sql(ignite1, "CREATE TABLE " + TABLE_NAME + "(key BIGINT PRIMARY KEY, valint1 INT, valint2 INT)");

        for (int i = 0; i < 10; ++i) {
            sql(ignite1, String.format("INSERT INTO " + TABLE_NAME + " VALUES(%d, %d, %d)", i, i, 2 * i));
        }

        sql(ignite1, "ALTER TABLE " + TABLE_NAME + " DROP COLUMN valint1");

        sql(ignite1, "ALTER TABLE " + TABLE_NAME + " ADD COLUMN valint3 INT");

        sql(ignite1, "ALTER TABLE " + TABLE_NAME + " ADD COLUMN valint4 INT");

        String nodeToStop = ignite1.name();

        IgnitionManager.stop(nodeToStop);

        CompletableFuture<Ignite> ignite1Fut = nodesBootstrapCfg.entrySet().stream()
                .filter(k -> k.getKey().equals(nodeToStop))
                .map(e -> TestIgnitionManager.start(e.getKey(), e.getValue(), workDir.resolve(e.getKey())))
                .findFirst().get();

        ignite1 = ignite1Fut.get();

        try (Session ses = ignite1.sql().createSession()) {
            ResultSet<SqlRow> res = ses.execute(null, "SELECT valint2 FROM tbl1");

            for (int i = 0; i < 10; ++i) {
                assertNotNull(res.next().iterator().next());
            }

            for (int i = 10; i < 20; ++i) {
                sql(ignite1, String.format("INSERT INTO " + TABLE_NAME + " VALUES(%d, %d, %d, %d)", i, i, i, i));
            }

            sql(ignite1, "ALTER TABLE " + TABLE_NAME + " DROP COLUMN valint3");

            sql(ignite1, "ALTER TABLE " + TABLE_NAME + " ADD COLUMN valint5 INT");

            res = ses.execute(null, "SELECT sum(valint4) FROM tbl1");

            assertEquals(10L * (10 + 19) / 2, res.next().iterator().next());

            res.close();
        }
    }

    /**
     * The test executes various operation over the lagging node. The operations can be executed only the node overtakes a distributed
     * cluster state.
     */
    @Test
    public void testExpectReplicationTimeout() throws Exception {
        Ignite ignite0 = clusterNodes.get(0);
        IgniteImpl ignite1 = (IgniteImpl) clusterNodes.get(1);

        createTable(ignite0, TABLE_NAME);

        TableImpl table = (TableImpl) ignite0.tables().table(TABLE_NAME);

        assertEquals(1, table.schemaView().schema().version());

        for (int i = 0; i < 10; i++) {
            table.recordView().insert(
                    null,
                    Tuple.create()
                            .set("key", (long) i)
                            .set("valInt", i)
                            .set("valStr", "str_" + i)
            );
        }

        WatchListenerInhibitor listenerInhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(ignite1);

        listenerInhibitor.startInhibit();

        alterTable(ignite0, TABLE_NAME);

        for (Ignite node : clusterNodes) {
            if (node == ignite1) {
                continue;
            }

            TableImpl tableOnNode = (TableImpl) node.tables().table(TABLE_NAME);

            waitForCondition(() -> tableOnNode.schemaView().lastSchemaVersion() == 2, 10_000);
        }

        CompletableFuture<?> insertFut = IgniteTestUtils.runAsync(() -> {
                    for (int i = 10; i < 20; i++) {
                        table.recordView().insert(
                                null,
                                Tuple.create()
                                        .set("key", (long) i)
                                        .set("valInt", i)
                                        .set("valStr", "str_" + i)
                                        .set("valStr2", "str2_" + i)
                        );
                    }
                }
        );

        IgniteException ex = assertThrows(IgniteException.class, () -> await(insertFut));
        assertThat(ex.getMessage(), containsString("Replication is timed out"));
    }

    /**
     * Creates a table with the passed name.
     *
     * @param node Cluster node.
     * @param tableName Table name.
     */
    protected void createTable(Ignite node, String tableName) {
        sql(node, "CREATE TABLE " + tableName + "(key BIGINT PRIMARY KEY, valint INT, valstr VARCHAR)");
    }

    protected void alterTable(Ignite node, String tableName) {
        sql(node, "ALTER TABLE " + tableName + " ADD COLUMN valstr2 VARCHAR NOT NULL DEFAULT 'default'");
    }

    protected ResultSet<SqlRow> sql(Ignite node, String query, Object... args) {
        ResultSet<SqlRow> rs = null;
        try (Session session = node.sql().createSession()) {
            rs = session.execute(null, query, args);
        }
        return rs;
    }
}
