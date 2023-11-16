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

package org.apache.ignite.internal.sql;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.util.InjectQueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerFactory;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for SQL integration tests.
 */
@ExtendWith(QueryCheckerExtension.class)
public class BaseSqlIntegrationTest extends ClusterPerClassIntegrationTest {
    @InjectQueryCheckerFactory
    protected static QueryCheckerFactory queryCheckerFactory;

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
    protected static QueryChecker assertQuery(InternalTransaction tx, String qry) {
        IgniteImpl node = CLUSTER.aliveNode();

        return queryCheckerFactory.create(node.queryEngine(), node.transactions(), tx, qry);
    }

    /**
     * Used for join checks, disables other join rules for executing exact join algo.
     *
     * @param qry Query for check.
     * @param joinType Type of join algo.
     * @param rules Additional rules need to be disabled.
     */
    protected static QueryChecker assertQuery(String qry, JoinType joinType, String... rules) {
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
    protected static QueryChecker assertQuery(String qry, AggregateType aggregateType, String... rules) {
        return assertQuery(qry)
                .disableRules(aggregateType.disabledRules)
                .disableRules(rules);
    }

    /**
     * Join type.
     */
    protected enum JoinType {
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

    /**
     * Aggregate type.
     */
    protected enum AggregateType {
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
     * Returns transaction manager for first cluster node.
     */
    protected IgniteTransactions igniteTx() {
        return CLUSTER.aliveNode().transactions();
    }

    /**
     * Gets the SQL API.
     *
     * @return SQL API.
     */
    protected IgniteSql igniteSql() {
        return CLUSTER.aliveNode().sql();
    }

    /**
     * Returns internal  {@code SqlQueryProcessor} for first cluster node.
     */
    protected SqlQueryProcessor queryProcessor() {
        return (SqlQueryProcessor) CLUSTER.aliveNode().queryEngine();
    }

    /**
     * Returns internal {@code TxManager} for first cluster node.
     */
    protected TxManager txManager() {
        return CLUSTER.aliveNode().txManager();
    }

    protected static Table table(String canonicalName) {
        return CLUSTER.aliveNode().tables().table(canonicalName);
    }

    /**
     * Returns internal {@code SystemViewManager} for first cluster node.
     */
    protected SystemViewManagerImpl systemViewManager() {
        return (SystemViewManagerImpl) CLUSTER.aliveNode().systemViewManager();
    }

    /**
     * Waits for the index to be built on all nodes.
     *
     * @param tableName Table name.
     * @param indexName Index name.
     * @return Nodes on which the partition index was built.
     * @throws Exception If failed.
     */
    protected static Map<Integer, List<Ignite>> waitForIndexBuild(String tableName, String indexName) {
        Map<Integer, List<Ignite>> partitionIdToNodes = new HashMap<>();

        CLUSTER.runningNodes().forEach(clusterNode -> {
            try {
                TableViewInternal table = getTableView(clusterNode, tableName);

                assertNotNull(table, clusterNode.name() + " : " + tableName);

                InternalTable internalTable = table.internalTable();

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
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        return partitionIdToNodes;
    }

    /**
     * Returns table descriptor of the given table at the given node, or {@code null} if no such table exists.
     *
     * @param node Node.
     * @param tableName Table name.
     */
    private static @Nullable CatalogTableDescriptor getTableDescriptor(Ignite node, String tableName) {
        IgniteImpl nodeImpl = (IgniteImpl) node;

        return TableTestUtils.getTable(nodeImpl.catalogManager(), tableName, nodeImpl.clock().nowLong());
    }

    /**
     * Returns the table by name, {@code null} if absent.
     *
     * @param node Node.
     * @param tableName Table name.
     */
    protected static @Nullable TableViewInternal getTableView(Ignite node, String tableName) {
        CompletableFuture<Table> tableFuture = node.tables().tableAsync(tableName);

        assertThat(tableFuture, willSucceedFast());

        return (TableViewInternal) tableFuture.join();
    }

    /**
     * Returns table index descriptor of the given index at the given node, or {@code null} if no such index exists.
     *
     * @param node Node.
     * @param indexName Index name.
     */
    protected static @Nullable CatalogIndexDescriptor getIndexDescriptor(Ignite node, String indexName) {
        IgniteImpl nodeImpl = (IgniteImpl) node;

        return nodeImpl.catalogManager().index(indexName, nodeImpl.clock().nowLong());
    }

    /**
     * Waits for the given indexes to become available in SQL schema.
     *
     * @param indexNames Index names to wait for.
     */
    protected static void waitForIndexToBecomeAvailable(String... indexNames) {
        List<IgniteImpl> nodes = CLUSTER.runningNodes().collect(Collectors.toList());
        Collections.shuffle(nodes);

        try {
            waitForCondition(() -> {
                IgniteImpl nodeImpl = nodes.get(0);
                long ts = nodeImpl.clock().nowLong();
                int availableNum = 0;

                for (String indexName : indexNames) {
                    CatalogIndexDescriptor index = nodeImpl.catalogManager().index(indexName, ts);
                    if (index != null && index.available()) {
                        availableNum ++;
                    }
                }

                return availableNum == indexNames.length;
            }, 10_000);

            // See TxManagerImpl::currentReadTimestamp
            long delay = HybridTimestamp.CLOCK_SKEW + TestIgnitionManager.DEFAULT_PARTITION_IDLE_SYNC_TIME_INTERVAL_MS;
            TimeUnit.MILLISECONDS.sleep(delay);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException(e);
        }
    }
}
