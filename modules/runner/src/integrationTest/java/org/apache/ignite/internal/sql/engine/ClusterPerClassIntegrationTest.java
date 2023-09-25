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
import static org.apache.ignite.internal.table.TableTestUtils.getTable;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.sql.engine.util.InjectQueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract basic integration test that starts a cluster once for all the tests it runs.
 */
@ExtendWith(QueryCheckerExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ClusterPerClassIntegrationTest extends IgniteIntegrationTest {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterPerClassIntegrationTest.class);

    /** Test default table name. */
    protected static final String DEFAULT_TABLE_NAME = "person";

    /** Base port number. */
    private static final int BASE_PORT = 3344;

    /** Base client port number. */
    private static final int BASE_CLIENT_PORT = 10800;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} }\n"
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

    @InjectQueryCheckerFactory
    protected static QueryCheckerFactory queryCheckerFactory;

    /**
     * Starts and initializes a test cluster.
     */
    protected void startCluster() {
        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        List<CompletableFuture<Ignite>> futures = new ArrayList<>();

        for (int i = 0; i < nodes(); i++) {
            String nodeName = testNodeName(testInfo, i);

            String config = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, BASE_PORT + i, connectNodeAddr, BASE_CLIENT_PORT + i);

            futures.add(TestIgnitionManager.start(nodeName, config, WORK_DIR.resolve(nodeName)));
        }

        String metaStorageNodeName = testNodeName(testInfo, 0);

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNodeName)
                .metaStorageNodeNames(List.of(metaStorageNodeName))
                .clusterName("cluster")
                .build();
        TestIgnitionManager.init(initParameters);

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            CLUSTER_NODES.add(await(future));
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
     * Returns table index descriptor of the given index at the given node, or {@code null} if no such index exists.
     *
     * @param node Node.
     * @param indexName Index name.
     */
    public static @Nullable CatalogIndexDescriptor getIndexDescriptor(Ignite node, String indexName) {
        IgniteImpl nodeImpl = (IgniteImpl) node;

        return nodeImpl.catalogManager().index(indexName, nodeImpl.clock().nowLong());
    }

    /**
     * Executes the query and validates any asserts passed to the builder.
     *
     * @param qry Query to execute.
     * @return Instance of QueryChecker.
     */
    protected static QueryChecker assertQuery(String qry) {
        return assertQuery(null, qry);
    }

    /**
     * Executes the query with the given transaction and validates any asserts passed to the builder.
     *
     * @param tx Transaction.
     * @param qry Query to execute.
     * @return Instance of QueryChecker.
     */
    protected static QueryChecker assertQuery(Transaction tx, String qry) {
        IgniteImpl node = (IgniteImpl) CLUSTER_NODES.get(0);

        return queryCheckerFactory.create(node.queryEngine(), node.transactions(), tx, qry);
    }

    /**
     * Used for join checks, disables other join rules for executing exact join algo.
     *
     * @param qry Query for check.
     * @param joinType Type of join algo.
     * @param rules Additional rules need to be disabled.
     */
    static QueryChecker assertQuery(String qry, JoinType joinType, String... rules) {
        return assertQuery(qry)
                .disableRules(joinType.disabledRules)
                .disableRules(rules);
    }

    /**
     * Used for query with aggregates checks, disables other aggregate rules for executing exact agregate algo.
     *
     * @param qry Query for check.
     * @param aggregateType Type of aggregate algo.
     * @param rules Additional rules need to be disabled.
     */
    static QueryChecker assertQuery(String qry, AggregateType aggregateType, String... rules) {
        return assertQuery(qry)
                .disableRules(aggregateType.disabledRules)
                .disableRules(rules);
    }

    /**
     * Creates a table.
     *
     * @param name Table name.
     * @param replicas Replica factor.
     * @param partitions Partitions count.
     */
    protected static Table createTable(String name, int replicas, int partitions) {
        sql(IgniteStringFormatter.format("CREATE ZONE IF NOT EXISTS {} WITH REPLICAS={}, PARTITIONS={};",
                "ZONE_" + name.toUpperCase(), replicas, partitions));
        sql(IgniteStringFormatter.format("CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, name VARCHAR, salary DOUBLE) "
                + "WITH PRIMARY_ZONE='{}'", name, "ZONE_" + name.toUpperCase()));

        return CLUSTER_NODES.get(0).tables().table(name);
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

    enum AggregateType {
        SORT(
                "ColocatedHashAggregateConverterRule",
                "ColocatedSortAggregateConverterRule",
                "MapReduceHashAggregateConverterRule"
        ),

        HASH(
                "ColocatedHashAggregateConverterRule",
                "ColocatedSortAggregateConverterRule",
                "MapReduceSortAggregateConverterRule"
        );

        private final String[] disabledRules;

        AggregateType(String... disabledRules) {
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

            Tuple row = view.get(null, Tuple.create().set(columnNames[0], id));

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
        Ignite ignite = CLUSTER_NODES.get(0);

        return SqlTestUtils.sql(ignite, tx, sql, args);
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

    /**
     * Waits for the index to be built on all nodes.
     *
     * @param tableName Table name.
     * @param indexName Index name.
     * @return Nodes on which the partition index was built.
     * @throws Exception If failed.
     */
    protected static Map<Integer, List<Ignite>> waitForIndexBuild(String tableName, String indexName) throws Exception {
        Map<Integer, List<Ignite>> partitionIdToNodes = new HashMap<>();

        // TODO: IGNITE-18733 We are waiting for the synchronization of schemes
        for (Ignite clusterNode : CLUSTER_NODES) {
            TableImpl tableImpl = getTableImpl(clusterNode, tableName);

            assertNotNull(tableImpl, clusterNode.name() + " : " + tableName);

            InternalTable internalTable = tableImpl.internalTable();

            assertTrue(
                    waitForCondition(() -> getIndexDescriptor(clusterNode, indexName) != null, 10, TimeUnit.SECONDS.toMillis(10)),
                    String.format("node=%s, tableName=%s, indexName=%s", clusterNode.name(), tableName, indexName)
            );

            for (int partitionId = 0; partitionId < internalTable.partitions(); partitionId++) {
                RaftGroupService raftGroupService = internalTable.partitionRaftGroupService(partitionId);

                Stream<Peer> allPeers = Stream.concat(Stream.of(raftGroupService.leader()), raftGroupService.peers().stream());

                // Let's check if there is a node in the partition assignments.
                if (allPeers.map(Peer::consistentId).noneMatch(clusterNode.name()::equals)) {
                    continue;
                }

                CatalogTableDescriptor tableDescriptor = getTableDescriptor(clusterNode, tableName);
                CatalogIndexDescriptor indexDescriptor = getIndexDescriptor(clusterNode, indexName);

                IndexStorage index = internalTable.storage().getOrCreateIndex(
                        partitionId,
                        StorageIndexDescriptor.create(tableDescriptor, indexDescriptor)
                );

                assertTrue(waitForCondition(() -> index.getNextRowIdToBuild() == null, 10, TimeUnit.SECONDS.toMillis(10)));

                partitionIdToNodes.computeIfAbsent(partitionId, p -> new ArrayList<>()).add(clusterNode);
            }
        }

        return partitionIdToNodes;
    }

    /**
     * Returns internal  {@code SqlQueryProcessor} for first cluster node.
     */
    protected SqlQueryProcessor queryProcessor() {
        return (SqlQueryProcessor) ((IgniteImpl) CLUSTER_NODES.get(0)).queryEngine();
    }

    /**
     * Returns internal {@code TxManager} for first cluster node.
     */
    protected TxManager txManager() {
        return ((IgniteImpl) CLUSTER_NODES.get(0)).txManager();
    }

    /**
     * Returns transaction manager for first cluster node.
     */
    protected IgniteTransactions igniteTx() {
        return CLUSTER_NODES.get(0).transactions();
    }

    /**
     * Gets the SQL API.
     *
     * @return SQL API.
     */
    protected IgniteSql igniteSql() {
        return CLUSTER_NODES.get(0).sql();
    }

    /**
     * Looks up a node by a consistent ID, {@code null} if absent.
     *
     * @param consistentId Node consistent ID.
     */
    static @Nullable IgniteImpl findByConsistentId(String consistentId) {
        return CLUSTER_NODES.stream()
                .map(IgniteImpl.class::cast)
                .filter(ignite -> consistentId.equals(ignite.node().name()))
                .findFirst()
                .orElse(null);
    }

    /**
     * Returns the table by name, {@code null} if absent.
     *
     * @param node Node.
     * @param tableName Table name.
     */
    static @Nullable TableImpl getTableImpl(Ignite node, String tableName) {
        CompletableFuture<Table> tableFuture = node.tables().tableAsync(tableName);

        assertThat(tableFuture, willSucceedFast());

        return (TableImpl) tableFuture.join();
    }

    /**
     * Returns the index ID from the catalog, {@code null} if there is no index.
     *
     * @param node Node.
     * @param indexName Index name.
     */
    static @Nullable Integer indexId(Ignite node, String indexName) {
        CatalogIndexDescriptor indexDescriptor = getIndexDescriptor(node, indexName);

        return indexDescriptor == null ? null : indexDescriptor.id();
    }

    /**
     * Returns table descriptor of the given table at the given node, or {@code null} if no such table exists.
     *
     * @param node Node.
     * @param tableName Table name.
     */
    private static @Nullable CatalogTableDescriptor getTableDescriptor(Ignite node, String tableName) {
        IgniteImpl nodeImpl = (IgniteImpl) node;

        return getTable(nodeImpl.catalogManager(), tableName, nodeImpl.clock().nowLong());
    }
}
