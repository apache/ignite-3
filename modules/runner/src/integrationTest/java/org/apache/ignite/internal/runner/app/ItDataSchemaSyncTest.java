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
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutIn;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * There is a test of table schema synchronization.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItDataSchemaSyncTest extends ClusterPerTestIntegrationTest {
    public static final String TABLE_NAME = "tbl1";

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration("replication.rpcTimeout: 3000");
    }

    /**
     * Test correctness of schema updates on lagged node.
     */
    @Test
    public void checkSchemasCorrectUpdate() throws Exception {
        Ignite ignite0 = cluster.node(0);
        IgniteImpl ignite1 = unwrapIgniteImpl(cluster.node(1));
        IgniteImpl ignite2 = unwrapIgniteImpl(cluster.node(2));

        createTable(ignite0, TABLE_NAME);

        TableViewInternal table = tableView(ignite0, TABLE_NAME);

        assertEquals(1, table.schemaView().lastKnownSchemaVersion());

        WatchListenerInhibitor listenerInhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(ignite1);

        listenerInhibitor.startInhibit();

        alterTable(ignite0, TABLE_NAME);

        table = tableView(ignite2, TABLE_NAME);

        TableViewInternal table0 = table;
        assertTrue(waitForCondition(() -> table0.schemaView().lastKnownSchemaVersion() == 2, 5_000));

        // Should not receive the table because we are waiting for the synchronization of schemas.
        assertThat(ignite1.tables().tableAsync(TABLE_NAME), willTimeoutFast());

        cluster.stopNode(1);

        listenerInhibitor.stopInhibit();

        cluster.startNode(1);

        ignite1 = unwrapIgniteImpl(cluster.node(1));

        table = tableView(ignite1, TABLE_NAME);

        TableViewInternal table1 = table;
        assertTrue(waitForCondition(() -> table1.schemaView().lastKnownSchemaVersion() == 2, 5_000));
    }

    /**
     * Check that sql query will wait until appropriate schema is not propagated into all nodes.
     */
    @Test
    public void queryWaitAppropriateSchema() {
        Ignite ignite0 = cluster.node(0);
        Ignite ignite1 = cluster.node(1);

        createTable(ignite0, TABLE_NAME);

        WatchListenerInhibitor node1Inhibitor = WatchListenerInhibitor.metastorageEventsInhibitor(ignite1);

        CompletableFuture<ResultSet<SqlRow>> createIndexFuture;

        node1Inhibitor.startInhibit();
        try {
            createIndexFuture = runAsync(() -> sql(ignite0, "CREATE INDEX idx1 ON " + TABLE_NAME + "(valint)"));

            assertThat(
                    runAsync(() -> sql(ignite0, "SELECT * FROM " + TABLE_NAME + " WHERE valint > 0")),
                    willTimeoutIn(1, TimeUnit.SECONDS)
            );
        } finally {
            node1Inhibitor.stopInhibit();
        }

        assertThat(createIndexFuture, willCompleteSuccessfully());

        // only check that request is executed without timeout.
        try (ResultSet<SqlRow> rs = sql(ignite0, "SELECT * FROM " + TABLE_NAME + " WHERE valint > 0")) {
            assertNotNull(rs);
        }
    }

    /**
     * Test correctness of schemes recovery after node restart.
     */
    @Test
    public void checkSchemasCorrectlyRestore() {
        Ignite ignite1 = cluster.node(1);

        sql(ignite1, "CREATE TABLE " + TABLE_NAME + "(key BIGINT PRIMARY KEY, valint1 INT, valint2 INT)");

        for (int i = 0; i < 10; ++i) {
            sql(ignite1, String.format("INSERT INTO " + TABLE_NAME + " VALUES(%d, %d, %d)", i, i, 2 * i));
        }

        sql(ignite1, "ALTER TABLE " + TABLE_NAME + " DROP COLUMN valint1");

        sql(ignite1, "ALTER TABLE " + TABLE_NAME + " ADD COLUMN valint3 INT");

        sql(ignite1, "ALTER TABLE " + TABLE_NAME + " ADD COLUMN valint4 INT");

        cluster.restartNode(1);

        ignite1 = cluster.node(1);

        IgniteSql sql = ignite1.sql();

        ResultSet<SqlRow> res = sql.execute(null, "SELECT valint2 FROM tbl1");

        for (int i = 0; i < 10; ++i) {
            assertNotNull(res.next().iterator().next());
        }

        for (int i = 10; i < 20; ++i) {
            sql(ignite1, String.format("INSERT INTO " + TABLE_NAME + " VALUES(%d, %d, %d, %d)", i, i, i, i));
        }

        sql(ignite1, "ALTER TABLE " + TABLE_NAME + " DROP COLUMN valint3");

        sql(ignite1, "ALTER TABLE " + TABLE_NAME + " ADD COLUMN valint5 INT");

        res.close();

        res = sql.execute(null, "SELECT sum(valint4) FROM tbl1");

        assertEquals(10L * (10 + 19) / 2, res.next().iterator().next());

        res.close();
    }

    /**
     * The test executes various operation over the lagging node. The operations can be executed only the node overtakes a distributed
     * cluster state.
     */
    @Test
    public void testExpectReplicationTimeout() throws Exception {
        Ignite ignite0 = cluster.node(0);
        Ignite ignite1 = cluster.node(1);

        createTable(ignite0, TABLE_NAME);

        TableViewInternal table = tableView(ignite0, TABLE_NAME);

        assertEquals(1, table.schemaView().lastKnownSchemaVersion());

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

        for (Ignite node : cluster.runningNodes().collect(toList())) {
            if (node.name().equals(ignite1.name())) {
                continue;
            }

            TableViewInternal tableOnNode = tableView(node, TABLE_NAME);

            assertTrue(waitForCondition(() -> tableOnNode.schemaView().lastKnownSchemaVersion() == 2, 10_000));
        }

        CompletableFuture<?> insertFut = runAsync(() -> {
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

        assertThat(
                insertFut,
                willThrow(IgniteException.class, 30, TimeUnit.SECONDS, "Replication is timed out")
        );
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
        ResultSet<SqlRow> rs = node.sql().execute(null, query, args);
        return rs;
    }

    private static TableViewInternal tableView(Ignite ignite, String tableName) {
        CompletableFuture<Table> tableFuture = ignite.tables().tableAsync(tableName);

        assertThat(tableFuture, willCompleteSuccessfully());

        return unwrapTableViewInternal(tableFuture.join());
    }
}
