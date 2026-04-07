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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.InjectQueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.CancellationToken;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

/** Transactions enlist count test. */
@ExtendWith(QueryCheckerExtension.class)
public class TransactionEnlistTest extends BaseIgniteAbstractTest {
    private static final int PARTITIONS_COUNT = 3;
    private static final String NODE_NAME1 = "N1";

    @InjectQueryCheckerFactory
    private static QueryCheckerFactory queryCheckerFactory;

    private static final TestCluster CLUSTER = TestBuilders.cluster()
            .nodes(NODE_NAME1)
            .build(); // add method use table partitions

    @BeforeAll
     static void startCluster() {
        CLUSTER.start();

        //noinspection ConcatenationWithEmptyString
        CLUSTER.node("N1").initSchema(""
                + "CREATE ZONE test_zone (partitions " + PARTITIONS_COUNT + ") storage profiles ['Default'];"
                + "CREATE TABLE t1 (id INT PRIMARY KEY, val INT) ZONE test_zone;");

        CLUSTER.setAssignmentsProvider("T1", (partitionCount, b) -> IntStream.range(0, partitionCount)
                .mapToObj(i -> List.of("N1"))
                .collect(Collectors.toList()));
        CLUSTER.setDataProvider("T1", TestBuilders.tableScan(DataProvider.fromCollection(List.of())));
        CLUSTER.setUpdatableTable("T1", new UpdatableTable() {
            @Override
            public TableDescriptor descriptor() {
                return null;
            }

            @Override
            public <RowT> CompletableFuture<?> insertAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
                return nullCompletedFuture();
            }

            @Override
            public <RowT> CompletableFuture<Void> insert(@Nullable InternalTransaction explicitTx, ExecutionContext<RowT> ectx, RowT row) {
                return nullCompletedFuture();
            }

            @Override
            public <RowT> CompletableFuture<?> upsertAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
                return nullCompletedFuture();
            }

            @Override
            public <RowT> CompletableFuture<Boolean> delete(@Nullable InternalTransaction explicitTx, ExecutionContext<RowT> ectx,
                    RowT key) {
                return nullCompletedFuture();
            }

            @Override
            public <RowT> CompletableFuture<?> deleteAll(ExecutionContext<RowT> ectx, List<RowT> rows, ColocationGroup colocationGroup) {
                return nullCompletedFuture();
            }
        });
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
        NoOpTransaction tx = NoOpTransaction.readWrite("t1", false);

        NoOpTransaction spiedTx = Mockito.spy(tx);

        try {
            assertQuery("INSERT INTO t1 VALUES(1, 2), (2, 3)", spiedTx).check();
        } catch (Exception ignored) {
            // No op.
        }

        Mockito.verify(spiedTx, times(2)).enlist(any(), anyInt(), any(), anyLong());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void testCommitPartitionChoice(int id) {
        NoOpTransaction tx = NoOpTransaction.readWrite("t1", false);

        NoOpTransaction spiedTx = Mockito.spy(tx);

        assertQuery("UPDATE t1 /*+ no_index */ SET val = 42 WHERE id = ?", spiedTx)
                .withParam(id)
                .check();

        int expectedPartition = expectedPartition(id);
        {
            ArgumentMatcher<ZonePartitionId> partitionIdMatch = zonePartitionId -> zonePartitionId.partitionId() == expectedPartition;
            // We expect commit partitions to be assigned once for given transaction.
            Mockito.verify(spiedTx, times(1))
                    .assignCommitPartition(argThat(partitionIdMatch));
            // Individual partition on the other hand will be enlisted for every source.
            // In this particular case -- first time for scan and second for Modify node.
            Mockito.verify(spiedTx, times(2))
                    .enlist(argThat(partitionIdMatch), anyInt(), any(), anyLong());
        }

        {
            // Due to partition pruning we don't expect any more enlistment.
            // We should not try to assign other partition as commit partition as well.
            ArgumentMatcher<ZonePartitionId> partitionIdMismatch = zonePartitionId -> zonePartitionId.partitionId() != expectedPartition;
            Mockito.verify(spiedTx, never())
                    .assignCommitPartition(argThat(partitionIdMismatch));
            Mockito.verify(spiedTx, never())
                    .enlist(argThat(partitionIdMismatch), anyInt(), any(), anyLong());
        }
    }

    @Test
    void testNoCommitPartitionAssignment() {
        NoOpTransaction tx = NoOpTransaction.readWrite("t1", false);
        tx.assignCommitPartition(new ZonePartitionId(1, 1));

        NoOpTransaction spiedTx = Mockito.spy(tx);

        assertQuery("UPDATE t1 /*+ no_index */ SET val = 42 WHERE id = ?", spiedTx)
                .withParam(1)
                .check();

        // Transaction already has a commit partition, so no assignment is expected during query processing.
        Mockito.verify(spiedTx, never()).assignCommitPartition(any());
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
        public CompletableFuture<QueryMetadata> prepareSingleAsync(
                SqlProperties properties,
                @Nullable InternalTransaction transaction,
                String qry,
                Object... params
        ) {
            assert params == null || params.length == 0 : "params are not supported";
            assert prepareOnly : "Expected that the query will be executed";

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
            assert !prepareOnly : "Expected that the query will only be prepared, but not executed";

            AsyncSqlCursor<InternalSqlRow> sqlCursor = node.executeQuery(transaction, qry, params);

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

    private static int expectedPartition(int key) {
        var calculator = new PartitionCalculator(PARTITIONS_COUNT, new NativeType[] {NativeTypes.INT32});
        calculator.append(key);
        return calculator.partition();
    }
}
