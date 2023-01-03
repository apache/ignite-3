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

package org.apache.ignite.internal.sql.engine;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract basic integration test.
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractBasicIntegrationTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(AbstractBasicIntegrationTest.class);

    /** Timeout should be big enough to prevent premature session expiration. */
    private static final long SESSION_IDLE_TIMEOUT = TimeUnit.SECONDS.toMillis(60);

    /** Test default table name. */
    protected static final String DEFAULT_TABLE_NAME = "person";

    /** Base port number. */
    private static final int BASE_PORT = 3344;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Cluster nodes. */
    protected static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    /** Work directory. */
    @WorkDirectory
    private static Path WORK_DIR;

    /** Information object that is initialised on the test startup. */
    private TestInfo testInfo;

    /**
     * Before all.
     *
     * @param testInfo Test information object.
     */
    @BeforeAll
    void beforeAll(TestInfo testInfo) {
        LOG.info("Start beforeAll()");

        this.testInfo = testInfo;

        startCluster();

        LOG.info("End beforeAll()");
    }

    /**
     * Starts and initializes a test cluster.
     */
    protected void startCluster() {
        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        List<CompletableFuture<Ignite>> futures = new ArrayList<>();

        for (int i = 0; i < nodes(); i++) {
            String nodeName = testNodeName(testInfo, i);

            String config = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, BASE_PORT + i, connectNodeAddr);

            futures.add(IgnitionManager.start(nodeName, config, WORK_DIR.resolve(nodeName)));
        }

        String metaStorageNodeName = testNodeName(testInfo, 0);

        IgnitionManager.init(metaStorageNodeName, List.of(metaStorageNodeName), "cluster");

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            CLUSTER_NODES.add(future.join());
        }
    }

    /**
     * Get a count of nodes in the Ignite cluster.
     *
     * @return Count of nodes.
     */
    protected int nodes() {
        return 3;
    }

    /**
     * After all.
     */
    @AfterAll
    void afterAll() throws Exception {
        LOG.info("Start afterAll()");

        stopNodes();

        LOG.info("End afterAll()");
    }

    /**
     * Stops all started nodes.
     */
    protected void stopNodes() throws Exception {
        List<AutoCloseable> closeables = IntStream.range(0, nodes())
                .mapToObj(i -> testNodeName(testInfo, i))
                .map(nodeName -> (AutoCloseable) () -> IgnitionManager.stop(nodeName))
                .collect(toList());

        IgniteUtils.closeAll(closeables);

        CLUSTER_NODES.clear();
    }

    /** Drops all visible tables. */
    protected void dropAllTables() {
        for (Table t : CLUSTER_NODES.get(0).tables().tables()) {
            sql("DROP TABLE " + t.name());
        }
    }

    /**
     * Appends indexes.
     *
     * @param node Execution cluster node.
     * @param idxs Map with index representation.
     * @param tblCanonicalName Canonical table name to create index in.
     */
    protected static void addIndexes(Ignite node, Map<String, List<String>> idxs, String tblCanonicalName) {
        try (Session ses = node.sql().createSession()) {
            for (Map.Entry<String, List<String>> idx : idxs.entrySet()) {
                ses.execute(null, String.format("CREATE INDEX %s ON %s (%s)", idx.getKey(), tblCanonicalName,
                        String.join(",", idx.getValue())));
            }
        }
    }

    /**
     * Invokes before the test will start.
     *
     * @param testInfo Test information object.
     * @throws Exception If failed.
     */
    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        setupBase(testInfo, WORK_DIR);
    }

    /**
     * Invokes after the test has finished.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        tearDownBase(testInfo);
    }

    protected static QueryChecker assertQuery(String qry) {
        return new QueryChecker(qry) {
            @Override
            protected QueryProcessor getEngine() {
                return ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();
            }
        };
    }

    /**
     * Creates a table.
     *
     * @param name Table name.
     * @param replicas Replica factor.
     * @param partitions Partitions count.
     */
    protected static Table createTable(String name, int replicas, int partitions) {
        sql(IgniteStringFormatter.format("CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, name VARCHAR, salary DOUBLE) "
                + "WITH replicas={}, partitions={}", name, replicas, partitions));

        return CLUSTER_NODES.get(0).tables().table(name);
    }

    /**
     * Used for join checks, disables other join rules for executing exact join algo.
     *
     * @param qry Query for check.
     * @param joinType Type of join algo.
     * @param rules Additional rules need to be disabled.
     */
    static QueryChecker assertQuery(String qry, JoinType joinType, String... rules) {
        return AbstractBasicIntegrationTest.assertQuery(qry.replaceAll("(?i)^select", "select "
                + Stream.concat(Arrays.stream(joinType.disabledRules), Arrays.stream(rules))
                .collect(Collectors.joining("','", "/*+ DISABLE_RULE('", "') */"))));
    }

    enum JoinType {
        NESTED_LOOP(
                "CorrelatedNestedLoopJoin",
                "JoinCommuteRule",
                "MergeJoinConverter"
        ),

        MERGE(
                "CorrelatedNestedLoopJoin",
                "JoinCommuteRule",
                "NestedLoopJoinConverter"
        ),

        CORRELATED(
                "MergeJoinConverter",
                "JoinCommuteRule",
                "NestedLoopJoinConverter"
        );

        private final String[] disabledRules;

        JoinType(String... disabledRules) {
            this.disabledRules = disabledRules;
        }
    }

    protected static void createAndPopulateTable() {
        createTable(DEFAULT_TABLE_NAME, 1, 8);

        int idx = 0;

        insertData("person", List.of("ID", "NAME", "SALARY"), new Object[][]{
                {idx++, "Igor", 10d},
                {idx++, null, 15d},
                {idx++, "Ilya", 15d},
                {idx++, "Roma", 10d},
                {idx, "Roma", 10d}
        });
    }

    protected static Table table(String canonicalName) {
        return CLUSTER_NODES.get(0).tables().table(canonicalName);
    }

    protected static void insertData(String tblName, List<String> columnNames, Object[]... tuples) {
        Transaction tx = CLUSTER_NODES.get(0).transactions().begin();

        insertDataInTransaction(tx, tblName, columnNames, tuples);

        tx.commit();
    }

    protected static void insertDataInTransaction(Transaction tx, String tblName, List<String> columnNames, Object[][] tuples) {
        String insertStmt = "INSERT INTO " + tblName + "(" + String.join(", ", columnNames) + ")"
                + " VALUES (" + ", ?".repeat(columnNames.size()).substring(2) + ")";

        for (Object[] args : tuples) {
            sql(tx, insertStmt, args);
        }
    }

    protected static void checkData(Table table, String[] columnNames, Object[]... tuples) {
        RecordView<Tuple> view = table.recordView();

        for (Object[] tuple : tuples) {
            assert tuple != null && tuple.length == columnNames.length;

            Object id = tuple[0];

            assert id != null : "Primary key cannot be null";

            Tuple row  = view.get(null, Tuple.create().set(columnNames[0], id));

            assertNotNull(row);

            for (int i = 0; i < columnNames.length; i++) {
                assertEquals(tuple[i], row.value(columnNames[i]));
            }
        }
    }

    protected static List<List<Object>> sql(String sql, Object... args) {
        return sql(null, sql, args);
    }

    protected static List<List<Object>> sql(@Nullable Transaction tx, String sql, Object... args) {
        var queryEngine = ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();

        SessionId sessionId = queryEngine.createSession(SESSION_IDLE_TIMEOUT, PropertiesHolder.fromMap(
                Map.of(QueryProperty.DEFAULT_SCHEMA, "PUBLIC")
        ));

        try {
            var context = tx != null ? QueryContext.of(tx) : QueryContext.of();

            return getAllFromCursor(
                    await(queryEngine.querySingleAsync(sessionId, context, sql, args))
            );
        } finally {
            queryEngine.closeSession(sessionId);
        }
    }

    protected static void checkMetadata(ColumnMetadata expectedMeta, ColumnMetadata actualMeta) {
        assertAll("Missmatch:\n expected = " + expectedMeta + ",\n actual = " + actualMeta,
                () -> assertEquals(expectedMeta.name(), actualMeta.name(), "name"),
                () -> assertEquals(expectedMeta.nullable(), actualMeta.nullable(), "nullable"),
                () -> assertSame(expectedMeta.type(), actualMeta.type(), "type"),
                () -> assertEquals(expectedMeta.precision(), actualMeta.precision(), "precision"),
                () -> assertEquals(expectedMeta.scale(), actualMeta.scale(), "scale"),
                () -> assertSame(expectedMeta.valueClass(), actualMeta.valueClass(), "value class"),
                () -> {
                    if (expectedMeta.origin() == null) {
                        assertNull(actualMeta.origin(), "origin");

                        return;
                    }

                    assertNotNull(actualMeta.origin(), "origin");
                    assertEquals(expectedMeta.origin().schemaName(), actualMeta.origin().schemaName(), " origin schema");
                    assertEquals(expectedMeta.origin().tableName(), actualMeta.origin().tableName(), " origin table");
                    assertEquals(expectedMeta.origin().columnName(), actualMeta.origin().columnName(), " origin column");
                }
        );
    }
}
