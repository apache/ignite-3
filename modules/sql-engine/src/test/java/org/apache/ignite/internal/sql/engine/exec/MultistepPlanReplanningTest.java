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

import static org.apache.ignite.internal.catalog.CatalogTestUtils.columnParams;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.exec.QueryRecoveryTest.TxType;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransactionalOperationTracker;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.PrepareServiceWithPrepareCallback;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapperImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests that outdated SQL query execution plans are re-planned.
 */
public class MultistepPlanReplanningTest extends BaseIgniteAbstractTest {
    private static final List<String> DATA_NODES = List.of("DATA_1", "DATA_2");
    private static final String GATEWAY_NODE_NAME = "gateway";

    private TestCluster cluster;

    @BeforeAll
    static void warmUpCluster() throws Exception {
        TestBuilders.warmupTestCluster();
    }

    @BeforeEach
    void startCluster() {
        cluster = TestBuilders.cluster()
                .nodes(GATEWAY_NODE_NAME, DATA_NODES.toArray(new String[0]))
                .build();

        cluster.start();

        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);

        cluster.setAssignmentsProvider("T1", (partitionCount, b) -> {
                    return IntStream.range(0, partitionCount)
                            .mapToObj(i -> DATA_NODES)
                            .collect(Collectors.toList());
                }
        );

        gatewayNode.initSchema("CREATE TABLE t1 (id INT PRIMARY KEY, part_id INT, node VARCHAR(128))");

        cluster.setDataProvider("T1", TestBuilders.tableScan((nodeName, partId) ->
                Collections.singleton(new Object[]{partId, partId, nodeName}))
        );
    }

    @ParameterizedTest
    @EnumSource(TxType.class)
    void schemaChangedDuringPlanning(TxType type) throws Exception {
        AtomicInteger prepareCalls = new AtomicInteger();

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch execLatch = new CountDownLatch(1);
        QueryTransactionContext txContext = createTxContext(type, barrier, execLatch);

        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);

        ((PrepareServiceWithPrepareCallback) gatewayNode.prepareService())
                .setPrepareCallback(prepareCalls::incrementAndGet);

        assertThat(prepareCalls.get(), is(0));

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> fut = gatewayNode.executeQueryAsync(new SqlProperties(), txContext,
                "SELECT * FROM t1"
        );

        barrier.await(10, TimeUnit.SECONDS);

        assertThat(prepareCalls.get(), is(1));

        CatalogCommand command = CreateTableCommand.builder()
                .tableName("test")
                .schemaName("PUBLIC")
                .columns(List.of(columnParams("key1", INT32), columnParams("key2", INT32), columnParams("val", INT32, true)))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key1", "key2")).build())
                .colocationColumns(List.of("key2"))
                .build();

        await(cluster.catalogManager().execute(command));

        execLatch.countDown();

        await(await(fut).closeAsync());

        assertThat(prepareCalls.get(), is(2));
    }

    private static QueryTransactionContext createTxContext(TxType type, CyclicBarrier barrier, CountDownLatch execLatch) {
        return new TestTxContext(type, barrier, execLatch);
    }

    private static class TestTxContext implements QueryTransactionContext {
        private final TxType txType;
        private final CyclicBarrier barrier;
        private final CountDownLatch execLatch;
        private final AtomicBoolean firstCall = new AtomicBoolean();

        private volatile QueryTransactionWrapper txWrapper;

        TestTxContext(TxType txType, CyclicBarrier barrier, CountDownLatch execLatch) {
            this.txType = txType;
            this.barrier = barrier;
            this.execLatch = execLatch;
        }

        @Override
        public QueryTransactionWrapper getOrStartSqlManaged(boolean readOnlyIgnored, boolean implicit) {
            assert firstCall.compareAndSet(false, true) : "should be called only once";

            try {
                barrier.await();

                execLatch.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }

            if (txWrapper == null) {
                InternalTransaction tx = txType.create();
                txWrapper = new QueryTransactionWrapperImpl(tx, true, NoOpTransactionalOperationTracker.INSTANCE);
            }

            return txWrapper;
        }

        @Override
        public void updateObservableTime(HybridTimestamp time) {
            // NO-OP
        }

        @Override
        public @Nullable QueryTransactionWrapper explicitTx() {
            return null;
        }
    }
}
