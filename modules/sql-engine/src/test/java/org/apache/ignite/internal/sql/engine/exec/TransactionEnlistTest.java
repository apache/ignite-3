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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursorImpl;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapperImpl;
import org.apache.ignite.internal.sql.engine.util.InjectQueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/** Transactions enlist count test. */
@ExtendWith(QueryCheckerExtension.class)
public class TransactionEnlistTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME1 = "N1";

    @InjectQueryCheckerFactory
    private static QueryCheckerFactory queryCheckerFactory;

    private static final TestCluster CLUSTER = TestBuilders.cluster()
            .nodes(NODE_NAME1, true)
            .addTable()
            .name("T1")
            .addKeyColumn("ID", NativeTypes.INT32)
            .addColumn("VAL", NativeTypes.INT32)
            .end()
            .dataProvider(NODE_NAME1, "T1", TestBuilders.tableScan(DataProvider.fromCollection(List.of())))
            .build(); // add method use table partitions

    @BeforeAll
     static void startCluster() {
        CLUSTER.start();
    }

    @AfterAll
    static void stopCluster() throws Exception {
        CLUSTER.stop();
    }

    /**
     * Check that tx enlist is called only for exact partitions.
     */
    @Test
    void testEnlistCall() {
        NoOpTransaction tx = NoOpTransaction.readWrite("t1");

        NoOpTransaction spiedTx = Mockito.spy(tx);

        try {
            assertQuery("INSERT INTO t1 VALUES(1, 2), (2, 3)", spiedTx).check();
        } catch (Exception ex) {
            // No op.
        }

        Mockito.verify(spiedTx, times(2)).enlist(any(), any());
    }

    private static QueryChecker assertQuery(String qry, InternalTransaction tx) {
        TestNode testNode = CLUSTER.node(NODE_NAME1);

        return queryCheckerFactory.create(
                testNode.name(),
                new TestQueryProcessor(testNode, false),
                null,
                tx,
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
        public CompletableFuture<QueryMetadata> prepareSingleAsync(SqlProperties properties,
                @Nullable InternalTransaction transaction, String qry, Object... params) {
            assert params == null || params.length == 0 : "params are not supported";
            assert prepareOnly : "Expected that the query will be executed";

            QueryPlan plan = node.prepare(qry);

            return CompletableFuture.completedFuture(new QueryMetadata(plan.metadata(), plan.parameterMetadata()));
        }

        @Override
        public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryAsync(
                SqlProperties properties,
                IgniteTransactions transactions,
                @Nullable InternalTransaction transaction,
                String qry,
                Object... params
        ) {
            assert params == null || params.length == 0 : "params are not supported";
            assert !prepareOnly : "Expected that the query will only be prepared, but not executed";

            QueryPlan plan = node.prepare(qry);
            AsyncCursor<InternalSqlRow> dataCursor = node.executePlan(plan, transaction);

            SqlQueryType type = plan.type();

            assert type != null;

            AsyncSqlCursor<InternalSqlRow> sqlCursor = new AsyncSqlCursorImpl<>(
                    type,
                    plan.metadata(),
                    new QueryTransactionWrapperImpl(transaction != null ? transaction : new NoOpTransaction("test"), false),
                    dataCursor,
                    nullCompletedFuture(),
                    null
            );

            return CompletableFuture.completedFuture(sqlCursor);
        }

        @Override
        public CompletableFuture<Void> startAsync(ExecutorService startupExecutor) {
            // NO-OP
            return nullCompletedFuture();
        }

        @Override
        public CompletableFuture<Void> stopAsync() {
            // NO-OP
            return nullCompletedFuture();
        }
    }
}
