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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.hamcrest.Matchers.containsString;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursorImpl;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
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
public class QueryCheckerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "N1";

    @InjectQueryCheckerFactory
    private static QueryCheckerFactory queryCheckerFactory;

    // @formatter:off
    private static final TestCluster CLUSTER = TestBuilders.cluster()
            .nodes(NODE_NAME)
            .addTable()
                    .name("T1")
                    .addKeyColumn("ID", NativeTypes.INT32)
                    .addColumn("VAL", NativeTypes.INT32)
                    .end()
            .dataProvider(NODE_NAME, "T1", TestBuilders.tableScan(DataProvider.fromCollection(List.of(
                    new Object[] {1, 1}, new Object[] {2, 2}
            ))))
            .build();
    // @formatter:on

    @BeforeAll
    static void startCluster() {
        CLUSTER.start();
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
    void testMetadata() {
        assertQuery("SELECT * FROM t1")
                .columnNames("ID", "VAL")
                .check();

        assertQuery("SELECT * FROM t1")
                .columnTypes(Integer.class, Integer.class)
                .check();

        assertQuery("SELECT id, val::DECIMAL(19, 2) as val_dec, id::VARCHAR(64) as id_str FROM t1")
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
    }

    private static QueryChecker assertQuery(String qry) {
        TestNode testNode = CLUSTER.node(NODE_NAME);

        return queryCheckerFactory.create(
                new TestQueryProcessor(testNode),
                new TestIgniteTransactions(),
                null,
                qry
        );
    }

    private static class TestQueryProcessor implements QueryProcessor {
        private final TestNode node;

        TestQueryProcessor(TestNode node) {
            this.node = node;
        }

        @Override
        public CompletableFuture<AsyncSqlCursor<List<Object>>> querySingleAsync(
                SqlProperties properties,
                IgniteTransactions transactions,
                @Nullable InternalTransaction transaction,
                String qry,
                Object... params
        ) {
            assert params == null || params.length == 0 : "params are not supported";

            QueryPlan plan = node.prepare(qry);
            AsyncCursor<List<Object>> dataCursor = node.executePlan(plan);

            SqlQueryType type = plan.type();

            assert type != null;

            AsyncSqlCursor<List<Object>> sqlCursor = new AsyncSqlCursorImpl<>(
                    type,
                    plan.metadata(),
                    new QueryTransactionWrapper(new NoOpTransaction("test"), false),
                    dataCursor,
                    () -> {}
            );

            return CompletableFuture.completedFuture(sqlCursor);
        }

        @Override
        public void start() {
            // NO-OP
        }

        @Override
        public void stop() {
            // NO-OP
        }
    }

    private static class TestIgniteTransactions implements IgniteTransactions {
        @Override
        public Transaction begin(@Nullable TransactionOptions options) {
            String name = "test";

            return options != null && options.readOnly()
                    ? NoOpTransaction.readOnly(name)
                    : NoOpTransaction.readWrite(name);
        }

        @Override
        public CompletableFuture<Transaction> beginAsync(@Nullable TransactionOptions options) {
            return CompletableFuture.completedFuture(begin(options));
        }
    }
}
