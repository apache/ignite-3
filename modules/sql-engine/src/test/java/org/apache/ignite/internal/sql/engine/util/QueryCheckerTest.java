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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.Matchers.containsString;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests to verify correctness of {@link QueryChecker} framework.
 */
@SuppressWarnings("ThrowableNotThrown")
@ExtendWith(QueryCheckerExtension.class)
@WithSystemProperty(key = "FAST_QUERY_OPTIMIZATION_ENABLED", value = "false")
public class QueryCheckerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "N1";

    @InjectQueryCheckerFactory
    private static QueryCheckerFactory queryCheckerFactory;

    private static final TestCluster CLUSTER = TestBuilders.cluster()
            .nodes(NODE_NAME)
            .build();

    @BeforeAll
    @AfterAll
    public static void resetFlag() {
        Commons.resetFastQueryOptimizationFlag();
    }

    @BeforeAll
    static void startCluster() {
        CLUSTER.start();

        //noinspection ConcatenationWithEmptyString
        CLUSTER.node("N1").initSchema(""
                + "CREATE ZONE test_zone (partitions 1) storage profiles ['Default'];"
                + "CREATE TABLE t1 (id INT PRIMARY KEY, val INT) ZONE test_zone");

        CLUSTER.setAssignmentsProvider("T1", (partitionCount, b) -> IntStream.range(0, partitionCount)
                .mapToObj(i -> List.of("N1"))
                .collect(Collectors.toList()));
        CLUSTER.setDataProvider("T1", TestBuilders.tableScan(DataProvider.fromCollection(
                List.of(new Object[]{1, 1, 1, 1, 1}, new Object[]{2, 2, 2, 2, 2})
        )));
    }

    @AfterAll
    static void stopCluster() throws Exception {
        CLUSTER.stop();
    }

    @Test
    void testDisabledRulesAndPlanMatcher() {
        {
            String[] disableAllAggregatesButColocatedHash = {
                    "MapReduceHashAggregateConverterRule",
                    "MapReduceSortAggregateConverterRule",
                    "ColocatedSortAggregateConverterRule"
            };

            assertQuery("SELECT COUNT(*) FROM t1")
                    .disableRules(disableAllAggregatesButColocatedHash)
                    .matches(containsString("ColocatedHashAggregate"))
                    .check();

            assertThrowsWithCause(
                    () -> assertQuery("SELECT COUNT(*) FROM t1")
                            .disableRules(disableAllAggregatesButColocatedHash)
                            .matches(containsString("ColocatedSortAggregate"))
                            .check(),
                    AssertionError.class,
                    "Invalid plan"
            );
        }

        {
            String[] disableAllAggregatesButColocatedSort = {
                    "MapReduceHashAggregateConverterRule",
                    "MapReduceSortAggregateConverterRule",
                    "ColocatedHashAggregateConverterRule"
            };

            assertQuery("SELECT COUNT(*) FROM t1")
                    .disableRules(disableAllAggregatesButColocatedSort)
                    .matches(containsString("ColocatedSortAggregate"))
                    .check();

            assertThrowsWithCause(
                    () -> assertQuery("SELECT COUNT(*) FROM t1")
                            .disableRules(disableAllAggregatesButColocatedSort)
                            .matches(containsString("ColocatedHashAggregate"))
                            .check(),
                    AssertionError.class,
                    "Invalid plan"
            );
        }
    }

    @Test
    void testReturns() {
        assertQuery("SELECT * FROM t1")
                .returnSomething()
                .check();

        assertQuery("SELECT * FROM t1")
                .returns(1, 1)
                .returns(2, 2)
                .check();

        // by default comparison is order agnostic
        assertQuery("SELECT * FROM t1")
                .returns(2, 2)
                .returns(1, 1)
                .check();

        // query returns in different order
        assertThrowsWithCause(
                () -> assertQuery("SELECT * FROM t1")
                        .ordered()
                        .returns(2, 2)
                        .returns(1, 1)
                        .check(),
                AssertionError.class,
                "Collections are not equal (position 0)"
        );

        // query returns something
        assertThrowsWithCause(
                () -> assertQuery("SELECT * FROM t1")
                        .returnNothing()
                        .check(),
                AssertionError.class,
                "Expected: an empty collection"
        );

        // query returns more than expected
        assertThrowsWithCause(
                () -> assertQuery("SELECT * FROM t1")
                        .returns(1, 1)
                        .check(),
                AssertionError.class,
                "Collections sizes are not equal"
        );

        // query returns less than expected
        assertThrowsWithCause(
                () -> assertQuery("SELECT * FROM t1")
                        .returns(1, 1)
                        .returns(2, 2)
                        .returns(3, 3)
                        .check(),
                AssertionError.class,
                "Collections sizes are not equal"
        );

        // query returns different values
        assertThrowsWithCause(
                () -> assertQuery("SELECT * FROM t1")
                        .ordered()
                        .returns(1, 1)
                        .returns(3, 3)
                        .check(),
                AssertionError.class,
                "Collections are not equal (position 0)"
        );

        // query returns different types
        assertThrowsWithCause(
                () -> assertQuery("SELECT * FROM t1")
                        .ordered()
                        .returns(1, 1)
                        .returns(2, 2L)
                        .check(),
                AssertionError.class,
                "Collections are not equal (position 1)"
        );
    }

    @Test
    void testResultSetMatcher() {
        assertQuery("SELECT * FROM t1")
                .returnSomething()
                .check();

        // by default returned rows are ordered
        assertQuery("SELECT * FROM t1")
                .results(new ListOfListsMatcher(
                        Matchers.contains(1, 1),
                        Matchers.contains(2, 2)
                ))
                .check();

        // query returns more than expected
        assertThrowsWithCause(
                () -> assertQuery("SELECT * FROM t1")
                        .results(new ListOfListsMatcher(
                                Matchers.contains(1, 1)
                        ))
                        .check(),
                AssertionError.class,
                "Result set does not match"
        );

        // query returns less than expected
        assertThrowsWithCause(
                () -> assertQuery("SELECT * FROM t1")
                        .results(new ListOfListsMatcher(
                                Matchers.contains(1, 1),
                                Matchers.contains(2, 2),
                                Matchers.contains(3, 3)
                        ))
                        .check(),
                AssertionError.class,
                "Result set does not match"
        );

        // query returns different types
        assertThrowsWithCause(
                () -> assertQuery("SELECT * FROM t1")
                        .results(new ListOfListsMatcher(
                                Matchers.contains(1, 1),
                                Matchers.contains(2, 2L)
                        ))
                        .check(),
                AssertionError.class,
                "Result set does not match"
        );
    }

    @Test
    void testMetadata() {
        assertQueryMeta("SELECT * FROM t1")
                .columnNames("ID", "VAL")
                .check();

        assertQueryMeta("SELECT * FROM t1")
                .columnTypes(Integer.class, Integer.class)
                .check();

        assertQueryMeta("SELECT id, val::DECIMAL(19, 2) as val_dec, id::VARCHAR(64) as id_str FROM t1")
                .columnMetadata(
                        new MetadataMatcher()
                                .name("ID")
                                .type(ColumnType.INT32)
                                .precision(10)
                                .scale(0)
                                .nullable(false),

                        new MetadataMatcher()
                                .name("VAL_DEC")
                                .type(ColumnType.DECIMAL)
                                .precision(19)
                                .scale(2)
                                .nullable(true),

                        new MetadataMatcher()
                                .name("ID_STR")
                                .type(ColumnType.STRING)
                                .precision(64)
                                .scale(ColumnMetadata.UNDEFINED_SCALE)
                                .nullable(false)
                )
                .check();

        // Test that validates the results cannot be executed correctly without actually executing the query.
        assertThrows(
                IllegalStateException.class,
                () -> assertQueryMeta("SELECT * FROM t1")
                        .columnTypes(Integer.class, Integer.class)
                        .returns(1, 1)
                        .returns(2, 2)
                        .check(),
                "Expected that the query will only be prepared, but not executed"
        );

        // Test that only checks metadata should not execute the query.
        assertThrows(
                IllegalStateException.class,
                () -> assertQuery("SELECT * FROM t1")
                        .columnTypes(Integer.class, Integer.class)
                        .check(),
                "Expected that the query will be executed"
        );
    }

    private static QueryChecker assertQuery(String qry) {
        TestNode testNode = CLUSTER.node(NODE_NAME);

        return queryCheckerFactory.create(
                NODE_NAME,
                new TestQueryProcessor(testNode, false),
                HybridTimestampTracker.atomicTracker(null),
                null,
                qry
        );
    }

    private static QueryChecker assertQueryMeta(String qry) {
        TestNode testNode = CLUSTER.node(NODE_NAME);

        return queryCheckerFactory.create(
                NODE_NAME,
                new TestQueryProcessor(testNode, true),
                HybridTimestampTracker.atomicTracker(null),
                null,
                qry
        );
    }

    private static class TestQueryProcessor implements QueryProcessor {
        private final TestNode node;
        private final boolean prepareOnly;

        TestQueryProcessor(TestNode node, boolean prepareOnly) {
            this.node = node;
            this.prepareOnly = prepareOnly;
        }

        @Override
        public CompletableFuture<QueryMetadata> prepareSingleAsync(
                SqlProperties properties,
                @Nullable InternalTransaction transaction,
                String qry,
                Object... params
        ) {
            assert params == null || params.length == 0 : "params are not supported";

            if (!prepareOnly) {
                throw new IllegalStateException("Expected that the query will be executed");
            }

            QueryPlan plan = node.prepare(qry);

            return CompletableFuture.completedFuture(new QueryMetadata(plan.metadata(), plan.parameterMetadata()));
        }

        @Override
        public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryAsync(
                SqlProperties properties,
                HybridTimestampTracker observableTimeTracker,
                @Nullable InternalTransaction transaction,
                @Nullable CancellationToken cancellationToken,
                String qry,
                Object... params
        ) {
            assert params == null || params.length == 0 : "params are not supported";

            if (prepareOnly) {
                throw new IllegalStateException("Expected that the query will only be prepared, but not executed");
            }

            AsyncSqlCursor<InternalSqlRow> sqlCursor = node.executeQuery(qry);

            return CompletableFuture.completedFuture(sqlCursor);
        }

        @Override
        public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
            // NO-OP
            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
            // NO-OP

            return nullCompletedFuture();
        }
    }

}
