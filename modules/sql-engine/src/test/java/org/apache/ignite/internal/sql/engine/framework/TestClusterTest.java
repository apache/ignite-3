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

package org.apache.ignite.internal.sql.engine.framework;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.convertSqlRows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.temporal.ChronoUnit;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableTable;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueGetPlan;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.AsyncCursor;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.internal.util.subscription.TransformingPublisher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for test execution runtime used in benchmarking.
 */
public class TestClusterTest extends BaseIgniteAbstractTest {

    private final ScannableTable table = new ScannableTable() {
        @Override
        public <RowT> Publisher<RowT> scan(
                ExecutionContext<RowT> ctx,
                PartitionWithConsistencyToken partWithConsistencyToken,
                RowFactory<RowT> rowFactory,
                @Nullable BitSet requiredColumns
        ) {

            return new TransformingPublisher<>(
                    SubscriptionUtils.fromIterable(
                            DataProvider.fromRow(
                                    new Object[]{42, UUID.randomUUID().toString()}, 3_333
                            )
                    ), rowFactory::create
            );
        }

        @Override
        public <RowT> Publisher<RowT> indexRangeScan(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                RowFactory<RowT> rowFactory, int indexId, List<String> columns, @Nullable RangeCondition<RowT> cond,
                @Nullable BitSet requiredColumns) {

            return new TransformingPublisher<>(
                    SubscriptionUtils.fromIterable(
                            DataProvider.fromRow(
                                    new Object[]{42, UUID.randomUUID().toString()}, 10
                            )
                    ), rowFactory::create
            );
        }

        @Override
        public <RowT> Publisher<RowT> indexLookup(ExecutionContext<RowT> ctx, PartitionWithConsistencyToken partWithConsistencyToken,
                RowFactory<RowT> rowFactory, int indexId, List<String> columns, RowT key, @Nullable BitSet requiredColumns) {

            return new TransformingPublisher<>(
                    SubscriptionUtils.fromIterable(
                            DataProvider.fromRow(
                                    new Object[]{42, UUID.randomUUID().toString()}, 1
                            )
                    ), rowFactory::create
            );
        }

        @Override
        public <RowT> CompletableFuture<@Nullable RowT> primaryKeyLookup(ExecutionContext<RowT> ctx, InternalTransaction explicitTx,
                RowFactory<RowT> rowFactory, RowT key, @Nullable BitSet requiredColumns) {
            return CompletableFuture.completedFuture(rowFactory.create());
        }
    };

    // @formatter:off
    private final TestCluster cluster = TestBuilders.cluster()
            .nodes("N1", "N2")
            .addTable()
                .name("T1")
                .addKeyColumn("ID", NativeTypes.INT32)
                .addColumn("VAL", NativeTypes.stringOf(64))
                .addSortedIndex()
                    .name("SORTED_IDX")
                    .addColumn("ID", Collation.ASC_NULLS_FIRST)
                    .end()
                .end()
            .dataProvider("N1", "T1", table)
            .dataProvider("N2", "T1", table)
            // table T2 will be created later by DDL
            .dataProvider("N1", "T2", table)
            .dataProvider("N2", "T2", table)
            // Register system views
            .addSystemView(SystemViews.<Object[]>clusterViewBuilder()
                    .name("NODES")
                    .addColumn("PID", NativeTypes.INT64, v -> v[0])
                    .addColumn("OS_NAME", NativeTypes.stringOf(64), v -> v[1])
                    .dataProvider(SubscriptionUtils.fromIterable(Collections.singleton(new Object[]{42L, "mango"})))
                    .build())
            .addSystemView(SystemViews.<Object[]>nodeViewBuilder()
                    .name("NODE_N2")
                    .nodeNameColumnAlias("NODE_NAME")
                    .addColumn("RND", NativeTypes.INT32, v -> v[0])
                    .dataProvider(SubscriptionUtils.fromIterable(Collections.singleton(new Object[]{42})))
                    .build())
            .registerSystemView("N1", "NODES")
            .registerSystemView("N1", "NODES")
            .registerSystemView("N2", "NODE_N2")
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

        var gatewayNode = cluster.node("N1");
        var plan = gatewayNode.prepare("SELECT * FROM t1");

        for (var row : await(gatewayNode.executePlan(plan).requestNextAsync(10_000)).items()) {
            assertNotNull(row);
        }

        // Ensure the plan contains full table scan.
        assertTrue(plan instanceof MultiStepPlan);
        assertTrue(lastNode(((MultiStepPlan) plan).root()) instanceof IgniteTableScan);
    }

    @Test
    public void testSimpleFromCreatedTableByDdl() {
        cluster.start();

        var gatewayNode = cluster.node("N1");

        gatewayNode.initSchema(
                "CREATE TABLE t2 (id INT PRIMARY KEY, val VARCHAR(64))"
        );

        QueryPlan plan = gatewayNode.prepare("SELECT * FROM t2");

        for (var row : await(gatewayNode.executePlan(plan).requestNextAsync(10_000)).items()) {
            assertNotNull(row);
        }

        // Ensure the plan contains full table scan.
        assertTrue(plan instanceof MultiStepPlan);
        assertTrue(lastNode(((MultiStepPlan) plan).root()) instanceof IgniteTableScan);
    }

    @Test
    public void testSelectByKey() {
        cluster.start();

        TestNode gatewayNode = cluster.node("N1");
        QueryPlan plan = gatewayNode.prepare("SELECT * FROM t1 WHERE ID = 1");

        for (InternalSqlRow row : await(gatewayNode.executePlan(plan).requestNextAsync(10_000)).items()) {
            assertNotNull(row);
        }

        // Ensure the plan uses index.
        assertTrue(plan instanceof KeyValueGetPlan);
    }

    @Test
    public void testSelectRange() {
        cluster.start();

        TestNode gatewayNode = cluster.node("N1");
        QueryPlan plan = gatewayNode.prepare("SELECT * FROM t1 WHERE ID > 1");

        for (InternalSqlRow row : await(gatewayNode.executePlan(plan).requestNextAsync(10_000)).items()) {
            assertNotNull(row);
        }

        // Ensure the plan uses index.
        assertTrue(plan instanceof MultiStepPlan);
        assertTrue(lastNode(((MultiStepPlan) plan).root()) instanceof IgniteIndexScan);
        assertEquals("SORTED_IDX", ((IgniteIndexScan) lastNode(((MultiStepPlan) plan).root())).indexName());
    }

    /** Check that already stopped message service correctly process incoming message. */
    @Test
    public void stoppedMessageServiceNotThrowsException() throws Exception {
        cluster.start();

        TestNode gatewayNode = cluster.node("N1");

        TestNode stoppedNode = cluster.node("N2");

        QueryPlan plan = gatewayNode.prepare("SELECT * FROM t1 WHERE ID > 1");

        AsyncCursor<InternalSqlRow> cur = gatewayNode.executePlan(plan);

        await(cur.requestNextAsync(1));

        stoppedNode.holdLock().block();

        try {
            stoppedNode.messageService().stop();

            await(cur.closeAsync());

            gatewayNode.stop();

            assertFalse(stoppedNode.exceptionRaised);
        } finally {
            stoppedNode.holdLock().unblock();
        }
    }

    /** Checks the propagation of hybrid logical time from the initiator to other nodes. */
    @Test
    public void testHybridTimestampPropagationFromInitiator() {
        cluster.start();

        TestNode initiator = cluster.node("N1");

        HybridClock initiatorClock = initiator.clock();
        HybridClock otherNodeClock = cluster.node("N2").clock();

        initiatorClock.update(initiatorClock.now().addPhysicalTime(ChronoUnit.YEARS.getDuration().toMillis()));

        assertTrue(initiator.clockService().after(initiatorClock.now(), otherNodeClock.now()));

        QueryPlan plan = initiator.prepare("SELECT * FROM t1");

        AsyncCursor<InternalSqlRow> cur = initiator.executePlan(plan);

        await(cur.requestNextAsync(1));

        assertEquals(initiator.clock().now().getPhysical(), otherNodeClock.now().getPhysical());

        await(cur.closeAsync());
    }

    /** Checks the propagation of hybrid logical time from other nodes to the initiator. */
    @Test
    public void testHybridTimestampPropagationToInitiator() {
        cluster.start();

        TestNode initiator = cluster.node("N1");
        TestNode otherNode = cluster.node("N2");

        HybridClock initiatorClock = initiator.clock();
        HybridClock otherNodeClock = otherNode.clock();

        otherNodeClock.update(otherNodeClock.now().addPhysicalTime(ChronoUnit.YEARS.getDuration().toMillis()));

        assertTrue(otherNode.clockService().after(otherNodeClock.now(), initiatorClock.now()));

        QueryPlan plan = initiator.prepare("SELECT * FROM t1");

        AsyncCursor<InternalSqlRow> cur = initiator.executePlan(plan);

        await(cur.requestNextAsync(10_000));

        assertEquals(otherNodeClock.now().getPhysical(), initiatorClock.now().getPhysical());

        await(cur.closeAsync());
    }

    private static IgniteRel lastNode(IgniteRel root) {
        while (!root.getInputs().isEmpty()) {
            root = (IgniteRel) root.getInput(0);
        }

        return root;
    }

    @Test
    public void testQuerySystemViews() {
        cluster.start();

        TestNode gatewayNode = cluster.node("N1");
        QueryPlan plan = gatewayNode.prepare("SELECT * FROM SYSTEM.NODES, SYSTEM.NODE_N2");

        BatchedResult<InternalSqlRow> results = await(gatewayNode.executePlan(plan).requestNextAsync(10_000));
        List<List<Object>> rows = convertSqlRows(results.items());

        assertEquals(List.of(List.of(42L, "mango", "N2", 42)), rows);
    }

    @Test
    public void testNodeInitSchema() {
        cluster.start();

        TestNode gatewayNode = cluster.node("N1");

        gatewayNode.initSchema("CREATE INDEX T1_NEW_HASH_VAK_IDX ON T1 USING HASH (VAL)");

        MultiStepPlan plan = (MultiStepPlan) gatewayNode.prepare("SELECT * FROM t1 WHERE val = ?");
        assertThat(plan.explain(), containsString("index=[T1_NEW_HASH_VAK_IDX], type=[HASH]"));
    }
}
