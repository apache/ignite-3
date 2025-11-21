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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.fsm.ExecutionPhase;
import org.apache.ignite.internal.sql.engine.statistic.SqlStatisticManagerImpl;
import org.apache.ignite.internal.sql.engine.util.InjectQueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.IgniteTransactions;
import org.awaitility.Awaitility;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

/**
 * Base class for SQL integration tests.
 */
@ExtendWith(QueryCheckerExtension.class)
public abstract class BaseSqlIntegrationTest extends ClusterPerClassIntegrationTest {
    protected static String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

    @InjectQueryCheckerFactory
    protected static QueryCheckerFactory queryCheckerFactory;

    /**
     * Executes the query and validates any asserts passed to the builder.
     *
     * @param qry Query to execute.
     * @return Instance of QueryChecker.
     */
    protected static QueryChecker assertQuery(String qry) {
        return assertQuery((InternalTransaction) null, qry);
    }

    protected static QueryChecker assertQuery(Ignite node, String qry) {
        return assertQuery(node, null, qry);
    }

    /**
     * Executes the query with the given transaction and validates any asserts passed to the builder.
     *
     * @param tx Transaction.
     * @param qry Query to execute.
     * @return Instance of QueryChecker.
     */
    protected static QueryChecker assertQuery(@Nullable InternalTransaction tx, String qry) {
        return assertQuery(CLUSTER.aliveNode(), tx, qry);
    }

    protected static QueryChecker assertQuery(Ignite node, @Nullable InternalTransaction tx, String qry) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node);
        return queryCheckerFactory.create(igniteImpl.name(), igniteImpl.queryEngine(), igniteImpl.observableTimeTracker(), tx, qry);
    }

    /**
     * Used for join checks, disables other join rules for executing exact join algo.
     *
     * @param qry Query for check.
     * @param joinType Type of join algo.
     * @param rules Additional rules need to be disabled.
     */
    protected static QueryChecker assertQuery(String qry, JoinType joinType, String... rules) {
        return assertQuery(qry, rules).disableRules(joinType.disabledRules);
    }

    /**
     * Query check with disabled rules.
     *
     * @param qry Query for check.
     * @param rules Additional rules need to be disabled.
     */
    protected static QueryChecker assertQuery(String qry, String... rules) {
        return assertQuery(qry).disableRules(rules);
    }

    /**
     * Used for query with aggregates checks, disables other aggregate rules for executing exact aggregate algo.
     *
     * @param qry Query for check.
     * @param aggregateType Type of aggregate algo.
     * @param rules Additional rules need to be disabled.
     */
    protected static QueryChecker assertQuery(String qry, AggregateType aggregateType, String... rules) {
        return assertQuery(qry, rules).disableRules(aggregateType.disabledRules);
    }

    /**
     * Join type.
     */
    protected enum JoinType {
        NESTED_LOOP(
                "CorrelatedNestedLoopJoin",
                "JoinCommuteRule",
                "MergeJoinConverter",
                "HashJoinConverter"
        ),

        MERGE(
                "CorrelatedNestedLoopJoin",
                "JoinCommuteRule",
                "NestedLoopJoinConverter",
                "HashJoinConverter"
        ),

        CORRELATED(
                "MergeJoinConverter",
                "JoinCommuteRule",
                "NestedLoopJoinConverter",
                "HashJoinConverter"
        ),

        HASH(
                "MergeJoinConverter",
                "JoinCommuteRule",
                "NestedLoopJoinConverter",
                "CorrelatedNestedLoopJoin"
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
     * Returns observable time of first cluster node.
     */
    protected HybridTimestampTracker observableTimeTracker() {
        return unwrapIgniteImpl(CLUSTER.aliveNode()).observableTimeTracker();
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
        return (SqlQueryProcessor) unwrapIgniteImpl(CLUSTER.aliveNode()).queryEngine();
    }

    /**
     * Returns internal {@code TxManager} for first cluster node.
     */
    protected TxManager txManager() {
        return unwrapIgniteImpl(CLUSTER.aliveNode()).txManager();
    }

    protected static Table table(String canonicalName) {
        return CLUSTER.aliveNode().tables().table(canonicalName);
    }

    /**
     * Returns internal {@code SystemViewManager} for first cluster node.
     */
    protected SystemViewManagerImpl systemViewManager() {
        return (SystemViewManagerImpl) unwrapIgniteImpl(CLUSTER.aliveNode()).systemViewManager();
    }

    /**
     * Waits until the number of running queries matches the specified matcher.
     *
     * @param matcher Matcher to check the number of running queries.
     * @throws AssertionError If after waiting the number of running queries still does not match the specified matcher.
     */
    protected void waitUntilRunningQueriesCount(Matcher<Integer> matcher) {
        SqlTestUtils.waitUntilRunningQueriesCount(queryProcessor(), matcher);
    }

    /**
     * Waits until the number of queries in {@link ExecutionPhase#CURSOR_PUBLICATION Cursor Publication} phase matches the specified
     * matcher.
     *
     * @param matcher THe matcher to check the number of running queries.
     * @throws AssertionError If after waiting the number of running queries still does not match the specified matcher.
     */
    protected void waitUntilQueriesInCursorPublicationPhaseCount(Matcher<Integer> matcher) {
        SqlTestUtils.waitUntilQueriesInPhaseCount(queryProcessor(), ExecutionPhase.CURSOR_PUBLICATION, matcher);
    }

    /**
     * Waits until the number of active (pending) transactions matches the specified matcher.
     *
     * @param matcher Matcher to check the number of active transactions.
     * @throws AssertionError If after waiting the number of active transactions still does not match the specified matcher.
     */
    protected void waitUntilActiveTransactionsCount(Matcher<Integer> matcher) {
        Awaitility.await().timeout(5, SECONDS).untilAsserted(() -> assertThat(txManager().pending(), matcher));
    }

    protected static void gatherStatistics() {
        SqlStatisticManagerImpl statisticManager = (SqlStatisticManagerImpl) ((SqlQueryProcessor) unwrapIgniteImpl(CLUSTER.aliveNode())
                .queryEngine()).sqlStatisticManager();

        statisticManager.forceUpdateAll();
        try {
            statisticManager.lastUpdateStatisticFuture().get(5_000, SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /** An executable that retrieves the data from the specified cursor. */
    public static class DrainCursor implements Executable {
        private final AsyncSqlCursor<?> cursor;

        public DrainCursor(AsyncSqlCursor<?> cursor) {
            this.cursor = cursor;
        }

        @Override
        public void execute() throws Throwable {
            BatchedResult<?> batch;
            do {
                batch = await(cursor.requestNextAsync(1));
            } while (batch.hasMore());
        }
    }
}
