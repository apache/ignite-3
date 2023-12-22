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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

    private static final boolean DEFAULT_WAIT_FOR_INDEX_AVAILABLE = true;

    /** Whether to wait for indexes to become available or not. Default is {@code true}. */
    private static final AtomicBoolean AWAIT_INDEX_AVAILABILITY = new AtomicBoolean(DEFAULT_WAIT_FOR_INDEX_AVAILABLE);

    /** Work directory. */
    @WorkDirectory
    private static Path WORK_DIR;

    /** Reset {@link #AWAIT_INDEX_AVAILABILITY}. */
    @BeforeEach
    @AfterEach
    void resetIndexAvailabilityFlag() {
        setAwaitIndexAvailability(DEFAULT_WAIT_FOR_INDEX_AVAILABLE);
    }

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
    protected static void insertPeople(String tableName, Person... people) {
        insertData(
                tableName,
                List.of("ID", "NAME", "SALARY"),
                Stream.of(people).map(person -> new Object[]{person.id, person.name, person.salary}).toArray(Object[][]::new)
        );
    }

    /**
     * Updates data in the table created by {@link #createZoneAndTable(String, String, int, int)}.
     *
     * @param tableName Table name.
     * @param people People to update in the table.
     */
    protected static void updatePeople(String tableName, Person... people) {
        Transaction tx = CLUSTER.node(0).transactions().begin();

        String sql = String.format("UPDATE %s SET NAME=?, SALARY=? WHERE ID=?", tableName);

        for (Person person : people) {
            sql(tx, sql, person.name, person.salary, person.id);
        }

        tx.commit();
    }

    /**
     * Deletes data in the table created by {@link #createZoneAndTable(String, String, int, int)}.
     *
     * @param tableName Table name.
     * @param personIds Person IDs to delete.
     */
    protected static void deletePeople(String tableName, int... personIds) {
        Transaction tx = CLUSTER.node(0).transactions().begin();

        String sql = String.format("DELETE FROM %s WHERE ID=?", tableName);

        for (int personId : personIds) {
            sql(tx, sql, personId);
        }

        tx.commit();
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

    /**
     * Drops an index for the table created by {@link #createZoneAndTable(String, String, int, int)}.
     *
     * @param indexName Index name.
     */
    protected static void dropIndex(String indexName) {
        sql(format("DROP INDEX {}", indexName));
    }

    /**
     * Sets whether to wait for indexes to become available.
     *
     * @param value Whether to wait for indexes to become available.
     */
    protected static void setAwaitIndexAvailability(boolean value) {
        AWAIT_INDEX_AVAILABILITY.set(value);
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

    protected static List<List<Object>> sql(int nodeIndex, String sql, Object... args) {
        return sql(nodeIndex, null, sql, args);
    }

    /**
     * Run SQL on given Ignite instance with given transaction and parameters.
     *
     * @param node Ignite instance to run a query.
     * @param tx Transaction to run a given query. Can be {@code null} to run within implicit transaction.
     * @param sql Query to be run.
     * @param args Dynamic parameters for a given query.
     * @return List of lists, where outer list represents a rows, internal lists represents a columns.
     */
    public static List<List<Object>> sql(Ignite node, @Nullable Transaction tx, String sql, Object... args) {
        try (
                Session session = node.sql().createSession();
                ResultSet<SqlRow> rs = session.execute(tx, sql, args)
        ) {
            return getAllResultSet(rs);
        }
    }

    protected static List<List<Object>> sql(@Nullable Transaction tx, String sql, Object... args) {
        return sql(0, tx, sql, args);
    }

    protected static List<List<Object>> sql(int nodeIndex, @Nullable Transaction tx, String sql, Object[] args) {
        IgniteImpl node = CLUSTER.node(nodeIndex);

        if (!AWAIT_INDEX_AVAILABILITY.get()) {
            return sql(node, tx, sql, args);
        } else {
            return executeAwaitingIndexes(node, (n) -> sql(n, tx, sql, args));
        }
    }

    private static List<List<Object>> getAllResultSet(ResultSet<SqlRow> resultSet) {
        List<List<Object>> res = new ArrayList<>();

        while (resultSet.hasNext()) {
            SqlRow sqlRow = resultSet.next();

            ArrayList<Object> row = new ArrayList<>(sqlRow.columnCount());
            for (int i = 0; i < sqlRow.columnCount(); i++) {
                row.add(sqlRow.value(i));
            }

            res.add(row);
        }

        return res;
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

    /**
     * Class for updating table in {@link #insertPeople(String, Person...)}, {@link #updatePeople(String, Person...)}. You can use
     * {@link #deletePeople(String, int...)} to remove people.
     */
    protected static class Person {
        int id;

        String name;

        double salary;

        public Person() {
            //No-op.
        }

        public Person(int id, String name, double salary) {
            this.id = id;
            this.name = name;
            this.salary = salary;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Person person = (Person) o;
            return id == person.id && Double.compare(salary, person.salary) == 0 && Objects.equals(name, person.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, salary);
        }
    }

    /**
     * Waits for some amount of time so that read-only transactions can observe the most recent version of the catalog.
     */
    protected static void waitForReadTimestampThatObservesMostRecentCatalog()  {
        // See TxManagerImpl::currentReadTimestamp.
        long delay = HybridTimestamp.CLOCK_SKEW + TestIgnitionManager.DEFAULT_PARTITION_IDLE_SYNC_TIME_INTERVAL_MS;
        try {
            TimeUnit.MILLISECONDS.sleep(delay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<List<Object>> executeAwaitingIndexes(IgniteImpl node, Function<IgniteImpl, List<List<Object>>> statement) {
        CatalogManager catalogManager = node.catalogManager();

        // Get existing indexes
        Set<Integer> existing = catalogManager.indexes(catalogManager.latestCatalogVersion())
                .stream().map(CatalogObjectDescriptor::id)
                .collect(Collectors.toSet());

        List<List<Object>> result = statement.apply(node);

        // Get indexes after a statement and compute the difference
        Set<Integer> difference = catalogManager.indexes(catalogManager.latestCatalogVersion()).stream()
                .map(CatalogObjectDescriptor::id)
                .collect(Collectors.toSet());

        difference.removeAll(existing);

        if (difference.isEmpty()) {
            return result;
        }

        // If there are new indexes, wait for them to become available.

        try {
            assertTrue(waitForCondition(() -> {
                int latestVersion = catalogManager.latestCatalogVersion();
                int notAvailable = 0;

                for (CatalogIndexDescriptor index : catalogManager.indexes(latestVersion)) {
                    if (!index.available() && difference.contains(index.id())) {
                        notAvailable++;
                    }
                }

                return notAvailable == 0;
            }, 10_000));

            // We have no knowledge whether the next transaction is readonly or not,
            // so we have to assume that the next transaction is read only transaction.
            // otherwise the statements in that transaction may not observe
            // the latest catalog version.
            waitForReadTimestampThatObservesMostRecentCatalog();

            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns {@code true} if the index exists and is available in the latest catalog version.
     *
     * @param ignite Node.
     * @param indexName Index name that is being checked.
     */
    protected static boolean isIndexAvailable(IgniteImpl ignite, String indexName) {
        CatalogManager catalogManager = ignite.catalogManager();
        HybridClock clock = ignite.clock();

        CatalogIndexDescriptor indexDescriptor = catalogManager.index(indexName, clock.nowLong());

        return indexDescriptor != null && indexDescriptor.available();
    }

    /**
     * Awaits for all requested indexes to become available in the latest catalog version.
     *
     * @param ignite Node.
     * @param indexNames Names of indexes that are of interest.
     */
    protected static void awaitIndexesBecomeAvailable(IgniteImpl ignite, String... indexNames) throws Exception {
        assertTrue(waitForCondition(
                () -> Arrays.stream(indexNames).allMatch(indexName -> isIndexAvailable(ignite, indexName)),
                10,
                30_000L
        ));
    }
}
