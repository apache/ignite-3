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

package org.apache.ignite.internal;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

/**
 * Abstract basic integration test that starts a cluster once for all the tests it runs.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ClusterPerClassIntegrationTest extends IgniteIntegrationTest {
    /** Test default table name. */
    protected static final String DEFAULT_TABLE_NAME = "person";

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} },\n"
            + "  rest.port: {}\n"
            + "}";

    /** Cluster nodes. */
    protected static Cluster CLUSTER;

    /** Work directory. */
    @WorkDirectory
    private static Path WORK_DIR;

    /**
     * Before all.
     *
     * @param testInfo Test information object.
     */
    @BeforeAll
    protected void beforeAll(TestInfo testInfo) {
        CLUSTER = new Cluster(testInfo, WORK_DIR, getNodeBootstrapConfigTemplate());

        if (initialNodes() > 0) {
            CLUSTER.startAndInit(initialNodes(), cmgMetastoreNodes());
        }
    }

    /**
     * Get a count of nodes in the Ignite cluster.
     *
     * @return Count of nodes.
     */
    protected int initialNodes() {
        return 3;
    }

    protected int[] cmgMetastoreNodes() {
        return new int[] { 0 };
    }

    /**
     * Returns node bootstrap config template.
     *
     * @return Node bootstrap config template.
     */
    protected String getNodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    /**
     * After all.
     */
    @AfterAll
    void afterAll() throws Exception {
        CLUSTER.shutdown();
    }

    /** Drops all visible tables. */
    protected static void dropAllTables() {
        for (Table t : CLUSTER.aliveNode().tables().tables()) {
            sql("DROP TABLE " + t.name());
        }
    }

    /**
     * Creates a table.
     *
     * @param name Table name.
     * @param replicas Replica factor.
     * @param partitions Partitions count.
     */
    protected static Table createTable(String name, int replicas, int partitions) {
        return createZoneAndTable(zoneName(name), name, replicas, partitions);
    }

    /**
     * Creates zone and table.
     *
     * @param zoneName Zone name.
     * @param tableName Table name.
     * @param replicas Replica factor.
     * @param partitions Partitions count.
     */
    protected static Table createZoneAndTable(String zoneName, String tableName, int replicas, int partitions) {
        sql(format(
                "CREATE ZONE IF NOT EXISTS {} WITH REPLICAS={}, PARTITIONS={};",
                zoneName, replicas, partitions
        ));

        sql(format(
                "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, name VARCHAR, salary DOUBLE) WITH PRIMARY_ZONE='{}'",
                tableName, zoneName
        ));

        return CLUSTER.node(0).tables().table(tableName);
    }

    /**
     * Inserts data into the table created by {@link #createZoneAndTable(String, String, int, int)}.
     *
     * @param tableName Table name.
     * @param people People to insert into the table.
     */
    protected static void insertPersons(String tableName, Person... people) {
        insertData(
                tableName,
                List.of("ID", "NAME", "SALARY"),
                Stream.of(people).map(person -> new Object[]{person.id, person.name, person.salary}).toArray(Object[][]::new)
        );
    }

    /**
     * Creates an index for the table created by {@link #createZoneAndTable(String, String, int, int)}..
     *
     * @param tableName Table name.
     * @param indexName Index name.
     * @param columnName Column name.
     */
    protected static void createIndex(String tableName, String indexName, String columnName) {
        sql(format("CREATE INDEX {} ON {} ({})", indexName, tableName, columnName));
    }

    protected static void insertData(String tblName, List<String> columnNames, Object[]... tuples) {
        Transaction tx = CLUSTER.node(0).transactions().begin();

        insertDataInTransaction(tx, tblName, columnNames, tuples);

        tx.commit();
    }

    protected static void insertDataInTransaction(Transaction tx, String tblName, List<String> columnNames, Object[]... tuples) {
        String insertStmt = "INSERT INTO " + tblName + "(" + String.join(", ", columnNames) + ")"
                + " VALUES (" + ", ?".repeat(columnNames.size()).substring(2) + ")";

        for (Object[] args : tuples) {
            sql(tx, insertStmt, args);
        }
    }

    protected static List<List<Object>> sql(String sql, Object... args) {
        return sql(null, sql, args);
    }

    protected static List<List<Object>> sql(@Nullable Transaction tx, String sql, Object... args) {
        return SqlTestUtils.sql(CLUSTER.node(0), tx, sql, args);
    }

    /**
     * Looks up a node by a consistent ID, {@code null} if absent.
     *
     * @param consistentId Node consistent ID.
     */
    protected static @Nullable IgniteImpl findByConsistentId(String consistentId) {
        return CLUSTER.runningNodes()
                .filter(Objects::nonNull)
                .map(IgniteImpl.class::cast)
                .filter(ignite -> consistentId.equals(ignite.name()))
                .findFirst()
                .orElse(null);
    }

    protected static String zoneName(String tableName) {
        return "ZONE_" + tableName.toUpperCase();
    }

    /** Class for inserting into a table using {@link #insertPersons(String, Person...)}. */
    protected static class Person {
        final int id;

        final String name;

        final double salary;

        public Person(int id, String name, double salary) {
            this.id = id;
            this.name = name;
            this.salary = salary;
        }
    }
}
