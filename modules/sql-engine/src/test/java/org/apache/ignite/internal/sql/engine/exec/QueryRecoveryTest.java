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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.ExplainablePlan;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for test execution runtime used in benchmarking.
 */
public class QueryRecoveryTest extends BaseIgniteAbstractTest {

//    private final ScannableTable table = new ScannableTable() {
//        @Override
//        public <RowT> Publisher<RowT> scan(
//                ExecutionContext<RowT> ctx,
//                PartitionWithConsistencyToken partWithConsistencyToken,
//                RowFactory<RowT> rowFactory,
//                @Nullable BitSet requiredColumns
//        ) {
//
//            return new TransformingPublisher<>(
//                    SubscriptionUtils.fromIterable(
//                            DataProvider.fromRow(
//                                    new Object[]{42, UUID.randomUUID().toString()}, 3_333
//                            )
//                    ), rowFactory::create
//            );
//        }
//
//        @Override
//        public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
//                RowFactory<RowT> rowFactory, int indexId, List<String> columns, @Nullable RangeCondition<RowT> cond,
//                @Nullable BitSet requiredColumns) {
//
//            return new TransformingPublisher<>(
//                    SubscriptionUtils.fromIterable(
//                            DataProvider.fromRow(
//                                    new Object[]{42, UUID.randomUUID().toString()}, 10
//                            )
//                    ), rowFactory::create
//            );
//        }
//
//        @Override
//        public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
//                RowFactory<RowT> rowFactory, int indexId, List<String> columns, RowT key, @Nullable BitSet requiredColumns) {
//
//            return new TransformingPublisher<>(
//                    SubscriptionUtils.fromIterable(
//                            DataProvider.fromRow(
//                                    new Object[]{42, UUID.randomUUID().toString()}, 1
//                            )
//                    ), rowFactory::create
//            );
//        }
//
//        @Override
//        public <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(ExecutionContext<RowT> ctx, InternalTransaction explicitTx,
//                RowFactory<RowT> rowFactory, RowT key, @Nullable BitSet requiredColumns) {
//            return CompletableFuture.completedFuture(rowFactory.create());
//        }
//
//        @Override
//        public CompletableFuture<Long> estimatedSize() {
//            return CompletableFuture.completedFuture(42L);
//        }
//    };

    // @formatter:off
    private final TestCluster cluster = TestBuilders.cluster()
            .nodes("N1", "N2", "N3")
//            .addTable()
//                .name("T1")
//                .addKeyColumn("ID", NativeTypes.INT32)
//                .addColumn("VAL", NativeTypes.stringOf(64))
//                .end()
//            .dataProvider("N1", "T1", table)
//            .dataProvider("N2", "T1", table)
//            // table T2 will be created later by DDL
//            .dataProvider("N1", "T2", table)
//            .dataProvider("N2", "T2", table)
//            // Register system views
//            .addSystemView(SystemViews.<Object[]>clusterViewBuilder()
//                    .name("NODES")
//                    .addColumn("PID", NativeTypes.INT64, v -> v[0])
//                    .addColumn("OS_NAME", NativeTypes.stringOf(64), v -> v[1])
//                    .dataProvider(SubscriptionUtils.fromIterable(Collections.singleton(new Object[]{42L, "mango"})))
//                    .build())
//            .addSystemView(SystemViews.<Object[]>nodeViewBuilder()
//                    .name("NODE_N2")
//                    .nodeNameColumnAlias("NODE_NAME")
//                    .addColumn("RND", NativeTypes.INT32, v -> v[0])
//                    .dataProvider(SubscriptionUtils.fromIterable(Collections.singleton(new Object[]{42})))
//                    .build())
//            .registerSystemView("N1", "NODES")
//            .registerSystemView("N1", "NODES")
//            .registerSystemView("N2", "NODE_N2")
            .build();
    // @formatter:on

    @AfterEach
    public void stopCluster() throws Exception {
        cluster.stop();
    }

    /**
     * Runs a simple SELECT query.
     */
    @Test
    public void testSimpleQuery() {
        cluster.start();

        TestNode gatewayNode = cluster.node("N1");

        gatewayNode.initSchema("" 
                + "CREATE TABLE t1 (id INT PRIMARY KEY, val INT);"
                + "CREATE TABLE t2 (id INT PRIMARY KEY, val INT);"
        );

        gatewayNode.executeQuery("SELECT * FROM t1, t2 WHERE t1.id = t2.id");

        QueryPlan plan = gatewayNode.prepare("SELECT * FROM t1, t2 WHERE t1.id = t2.id");

        System.out.println(((ExplainablePlan) plan).explain());
    }

    private static IgniteRel lastNode(IgniteRel root) {
        while (!root.getInputs().isEmpty()) {
            root = (IgniteRel) root.getInput(0);
        }

        return root;
    }
}
