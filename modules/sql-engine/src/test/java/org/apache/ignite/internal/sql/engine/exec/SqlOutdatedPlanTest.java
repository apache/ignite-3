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
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnCommand;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlPlanToTxSchemaVersionValidator;
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
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests that outdated SQL query execution plan is re-planned using the
 * {@link InternalTransaction#schemaTimestamp() schema time} of the started transaction.
 *
 * <p>Currently, the transaction starts after query planning is completed, if the schema has changed during planning
 * (the catalog version used in the plan does not match the catalog version of the corresponding transaction start time),
 * then this plan is considered outdated and the planning phase should be repeated using the transaction start time.
 *
 * @see SqlPlanOutdatedException
 * @see SqlPlanToTxSchemaVersionValidator
 */
public class SqlOutdatedPlanTest extends BaseIgniteAbstractTest {
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

        cluster.setAssignmentsProvider("T1",
                (partitionCount, b) -> IntStream.range(0, partitionCount)
                        .mapToObj(i -> DATA_NODES)
                        .collect(Collectors.toList())
        );

        gatewayNode.initSchema("CREATE TABLE t1 (id INT PRIMARY KEY)");

        cluster.setDataProvider("T1", TestBuilders.tableScan((nodeName, partId) ->
                Collections.singleton(new Object[]{partId}))
        );
    }

    @AfterEach
    void stopCluster() throws Exception {
        cluster.stop();
    }

    @ParameterizedTest
    @EnumSource(TxType.class)
    void planningIsRepeatedUsingTheSameTransaction(TxType type) {
        TestTxContext txContext = new TestTxContext(type);
        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);
        PrepareServiceSpy prepareServiceSpy = new PrepareServiceSpy(gatewayNode);

        CompletableFuture<Semaphore> semaphoreFut1 = prepareServiceSpy.resetAndBlockNextCall();

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> fut =
                gatewayNode.executeQueryAsync(new SqlProperties(), txContext, "SELECT id FROM t1");

        await(semaphoreFut1);
        assertThat(prepareServiceSpy.callsCounter.get(), is(1));
        assertThat(txContext.startedTxCounter.get(), is(0));

        // Simulate concurrent schema modification.
        await(cluster.catalogManager().execute(
                makeAddColumnCommand("VAL1")));

        CompletableFuture<Semaphore> semaphoreFut2 = prepareServiceSpy.resetAndBlockNextCall();
        Semaphore semaphore2 = await(semaphoreFut2);
        assertThat(prepareServiceSpy.callsCounter.get(), is(2));
        assertThat(txContext.startedTxCounter.get(), is(1));

        // Simulate another one schema modification.
        await(cluster.catalogManager().execute(
                makeAddColumnCommand("VAL2")));

        semaphore2.release();

        AsyncSqlCursor<InternalSqlRow> cursor = await(fut);

        // Planning must be repeated, but only once.
        assertThat(prepareServiceSpy.callsCounter.get(), is(2));

        // Transaction should be started only once.
        assertThat(txContext.startedTxCounter.get(), is(1));

        drainAndCloseCursor(cursor);
    }

    @Test
    void schemaChangedAndNodeDisconnectedDuringPlanning() {
        TestTxContext txContext = new TestTxContext(TxType.RO);
        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);
        PrepareServiceSpy prepareServiceSpy = new PrepareServiceSpy(gatewayNode);

        CompletableFuture<Semaphore> semaphoreFut = prepareServiceSpy.resetAndBlockNextCall();

        CompletableFuture<AsyncSqlCursor<InternalSqlRow>> fut =
                gatewayNode.executeQueryAsync(new SqlProperties(), txContext, "SELECT id FROM t1");

        Semaphore semaphore = await(semaphoreFut);
        assertThat(prepareServiceSpy.callsCounter.get(), is(1));
        assertThat(txContext.startedTxCounter.get(), is(0));

        // Simulate concurrent schema modification.
        await(cluster.catalogManager().execute(
                makeAddColumnCommand("VAL1")));

        // And node disconnection.
        cluster.node(DATA_NODES.get(0)).disconnect();

        semaphore.release();

        AsyncSqlCursor<InternalSqlRow> cursor = await(fut);

        // Planning must be repeated, but only once.
        assertThat(prepareServiceSpy.callsCounter.get(), is(2));

        // The plan execution must be repeated using a new transaction.
        assertThat(txContext.startedTxCounter.get(), is(2));

        drainAndCloseCursor(cursor);
    }

    private static void drainAndCloseCursor(AsyncSqlCursor<InternalSqlRow> cursor) {
        BatchedResult<InternalSqlRow> result = await(cursor.requestNextAsync(1024));
        assertThat(result.items(), is(not(empty())));
        assertThat(result.hasMore(), is(false));
        await(cursor.closeAsync());
    }

    private static CatalogCommand makeAddColumnCommand(String columnName) {
        return AlterTableAddColumnCommand.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .tableName("T1")
                .columns(List.of(columnParams(columnName, INT32, true)))
                .build();
    }

    private static class PrepareServiceSpy {
        private final AtomicInteger callsCounter = new AtomicInteger();
        private final AtomicReference<Semaphore> prepareBlockHolder = new AtomicReference<>();

        PrepareServiceSpy(TestNode gatewayNode) {
            ((PrepareServiceWithPrepareCallback) gatewayNode.prepareService())
                    .setPrepareCallback(() -> {
                        callsCounter.incrementAndGet();

                        Semaphore semaphore = prepareBlockHolder.get();

                        try {
                            semaphore.tryAcquire(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } finally {
                            semaphore.release();
                        }
                    });
        }

        CompletableFuture<Semaphore> resetAndBlockNextCall() {
            Semaphore nextSemaphore = new Semaphore(0);

            Semaphore prevSemaphore = prepareBlockHolder.getAndSet(nextSemaphore);

            if (prevSemaphore != null) {
                prevSemaphore.release();
            }

            return CompletableFuture.supplyAsync(() -> {
                Awaitility.await().until(nextSemaphore::hasQueuedThreads);

                return nextSemaphore;
            });
        }
    }

    private static class TestTxContext implements QueryTransactionContext {
        private final TxType txType;
        private final AtomicInteger startedTxCounter = new AtomicInteger();

        TestTxContext(TxType txType) {
            this.txType = txType;
        }

        @Override
        public QueryTransactionWrapper getOrStartSqlManaged(boolean readOnlyIgnored, boolean implicit) {
            startedTxCounter.incrementAndGet();

            return new QueryTransactionWrapperImpl(txType.create(), true, NoOpTransactionalOperationTracker.INSTANCE);
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
