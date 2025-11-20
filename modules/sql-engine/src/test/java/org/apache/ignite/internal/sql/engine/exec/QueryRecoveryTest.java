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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.sneakyThrow;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransactionalOperationTracker;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryMetadata;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapperImpl;
import org.apache.ignite.internal.sql.engine.util.InjectQueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests for test execution runtime used in benchmarking.
 */
@SuppressWarnings("ThrowableNotThrown")
@ExtendWith(QueryCheckerExtension.class)
public class QueryRecoveryTest extends BaseIgniteAbstractTest {
    private static final List<String> DATA_NODES = List.of("DATA_1", "DATA_2");
    private static final String GATEWAY_NODE_NAME = "gateway";

    @InjectQueryCheckerFactory
    private static QueryCheckerFactory queryCheckerFactory;

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

        gatewayNode.initSchema("CREATE TABLE t1 (id INT PRIMARY KEY, part_id INT, node VARCHAR(128))");

        cluster.setAssignmentsProvider("T1", (partitionCount, b) ->
                IntStream.range(0, partitionCount)
                        .mapToObj(i -> DATA_NODES)
                        .collect(Collectors.toList())
        );

        cluster.setDataProvider("T1", TestBuilders.tableScan((nodeName, partId) ->
                Collections.singleton(new Object[]{partId, partId, nodeName}))
        );
    }

    @AfterEach
    void stopCluster() throws Exception {
        cluster.stop();
    }

    @ParameterizedTest
    @EnumSource
    void queryWithImplicitTxRecoversWhenNodeDisconnectsBeforeFragmentHasBeenSent(TxType txType) {
        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);

        QueryTransactionContext txContext = createTxContext(txType, true);

        // mapping is supposed to be stable, thus if it returns 0th node from DATA_NODES on some environment,
        // it should return the same node on all environments
        String firstExpectedNode = DATA_NODES.get(0);
        assertQuery(gatewayNode, "SELECT node FROM t1 WHERE part_id = 0", txContext)
                .returns(firstExpectedNode)
                .check();

        // Disconnect just removes node from physical topology, but not logical. This implies, that query
        // will be mapped on disconnected node, and after first unsuccessful try query should be remapped
        // on other node, if any.
        cluster.node(firstExpectedNode).disconnect();

        // first expected node is not available, query must be remapped to next available node
        assertQuery(gatewayNode, "SELECT node FROM t1 WHERE part_id = 0", txContext)
                .returns(DATA_NODES.get(1))
                .check();
    }

    @ParameterizedTest
    @EnumSource
    void queryWithImplicitTxNotFailsWhenNodeLeftClusterBeforeFragmentHasBeenSent(TxType txType) throws Exception {
        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);

        QueryTransactionContext txContext = createTxContext(txType, true);

        // mapping is supposed to be stable, thus if it returns 0th node from DATA_NODES on some environment,
        // it should return the same node on all environments
        String firstExpectedNode = DATA_NODES.get(0);
        assertQuery(gatewayNode, "SELECT node FROM t1 WHERE part_id = 0", txContext)
                .returns(firstExpectedNode)
                .check();

        // Node stop removes node both from physical and logic topology. This implies that query won't be
        // mapped on missed node if there are alternatives.
        cluster.node(firstExpectedNode).stop();

        // first expected node is not available, query must be remapped to next available node
        assertQuery(gatewayNode, "SELECT node FROM t1 WHERE part_id = 0", txContext)
                .returns(DATA_NODES.get(1))
                .check();
    }

    @ParameterizedTest
    @EnumSource
    void queryWithExplicitTxCannotRecoverWhenNodeDisconnectsBeforeFragmentHasBeenSent(TxType txType) {
        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);

        QueryTransactionContext txContext = createTxContext(txType, false);

        // mapping is supposed to be stable, thus if it returns 0th node from DATA_NODES on some environment,
        // it should return the same node on all environments
        String firstExpectedNode = DATA_NODES.get(0);
        assertQuery(gatewayNode, "SELECT node FROM t1 WHERE part_id = 0", txContext)
                .returns(firstExpectedNode)
                .check();

        // Disconnect just removes node from physical topology, but not logical. This implies, that query
        // will be mapped on disconnected node, and after first unsuccessful try related transaction will
        // be invalidated, which makes it impossible to recover.
        cluster.node(firstExpectedNode).disconnect();

        assertThrows(
                SqlException.class,
                () -> assertQuery(gatewayNode, "SELECT node FROM t1 WHERE part_id = 0", txContext).check(),
                "Node left the cluster. Node: " + firstExpectedNode
        );
    }

    @ParameterizedTest
    @EnumSource
    void queryWithExplicitTxNotFailsWhenNodeLeftClusterBeforeFragmentHasBeenSent(TxType txType) throws Exception {
        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);

        QueryTransactionContext txContext = createTxContext(txType, false);

        // mapping is supposed to be stable, thus if it returns 0th node from DATA_NODES on some environment,
        // it should return the same node on all environments
        String firstExpectedNode = DATA_NODES.get(0);
        assertQuery(gatewayNode, "SELECT node FROM t1 WHERE part_id = 0", txContext)
                .returns(firstExpectedNode)
                .check();

        // Node stop removes node both from physical and logic topology. This implies that query won't be
        // mapped on missed node if there are alternatives.
        cluster.node(firstExpectedNode).stop();

        // first expected node is not available, query must be mapped to next available node
        assertQuery(gatewayNode, "SELECT node FROM t1 WHERE part_id = 0", txContext)
                .returns(DATA_NODES.get(1))
                .check();
    }

    @Test
    void queryWithImplicitTxRecoversFromReplicaMiss() {
        AtomicBoolean firstTimeThrow = new AtomicBoolean(true);
        AtomicBoolean reassignmentHappened = new AtomicBoolean(false);

        String beforeReassignmentNode = DATA_NODES.get(0);
        String afterReassignmentNode = DATA_NODES.get(1);
        cluster.setAssignmentsProvider("T1", (partitionCount, b) ->
                IntStream.range(0, partitionCount)
                        .mapToObj(i -> List.of(reassignmentHappened.get() ? afterReassignmentNode : beforeReassignmentNode))
                        .collect(Collectors.toList())
        );

        cluster.setDataProvider("T1", TestBuilders.tableScan((nodeName, partId) -> {
            if (firstTimeThrow.compareAndSet(true, false)) {
                reassignmentHappened.set(true);
                return () -> new FailingIterator<>(new PrimaryReplicaMissException(UUID.randomUUID(), 0L, 0L));
            }

            return Collections.singleton(new Object[]{partId, partId, nodeName});
        }));

        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);

        QueryTransactionContext txContext = createTxContext(TxType.RW, true);

        assertQuery(gatewayNode, "INSERT INTO blackhole SELECT 1 FROM t1 WHERE part_id = 0", txContext)
                .returns(1L)
                .check();
    }

    @Test
    void queryWithImplicitTxRecoversFromLockConflict() {
        AtomicBoolean firstTimeThrow = new AtomicBoolean(true);

        String expectedNode = DATA_NODES.get(0);
        cluster.setAssignmentsProvider("T1", (partitionCount, b) ->
                IntStream.range(0, partitionCount)
                        .mapToObj(i -> List.of(expectedNode))
                        .collect(Collectors.toList())
        );

        cluster.setDataProvider("T1", TestBuilders.tableScan((nodeName, partId) -> {
            if (firstTimeThrow.compareAndSet(true, false)) {
                return () -> new FailingIterator<>(new LockException(Transactions.ACQUIRE_LOCK_ERR, "Lock conflict on " + nodeName));
            }

            return Collections.singleton(new Object[]{partId, partId, nodeName});
        }));

        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);

        QueryTransactionContext txContext = createTxContext(TxType.RW, true);

        assertQuery(gatewayNode, "INSERT INTO blackhole SELECT 1 FROM t1 WHERE part_id = 0", txContext)
                .returns(1L)
                .check();
    }

    private static QueryTransactionContext createTxContext(TxType type, boolean implicit) {
        InternalTransaction tx = type.create();
        QueryTransactionWrapper wrapper = new QueryTransactionWrapperImpl(tx, implicit, NoOpTransactionalOperationTracker.INSTANCE);
        return new PredefinedTxContext(wrapper);
    }

    private static QueryChecker assertQuery(TestNode node, String query, QueryTransactionContext txContext) {
        return queryCheckerFactory.create(
                node.name(),
                new QueryProcessor() {
                    @Override
                    public CompletableFuture<QueryMetadata> prepareSingleAsync(SqlProperties properties,
                            @Nullable InternalTransaction transaction, String qry, Object... params) {
                        throw new AssertionError("Should not be called");
                    }

                    @Override
                    public CompletableFuture<AsyncSqlCursor<InternalSqlRow>> queryAsync(
                            SqlProperties properties,
                            HybridTimestampTracker observableTime,
                            @Nullable InternalTransaction transaction,
                            @Nullable CancellationToken token,
                            String qry,
                            Object... params
                    ) {
                        return completedFuture(node.executeQuery(txContext, query));
                    }

                    @Override
                    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
                        return nullCompletedFuture();
                    }

                    @Override
                    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
                        return nullCompletedFuture();
                    }
                },
                null,
                null,
                query
        );
    }

    static class PredefinedTxContext implements QueryTransactionContext {
        private final QueryTransactionWrapper txWrapper;

        PredefinedTxContext(QueryTransactionWrapper txWrapper) {
            this.txWrapper = txWrapper;
        }

        @Override
        public QueryTransactionWrapper getOrStartSqlManaged(boolean readOnly, boolean implicit) {
            return txWrapper;
        }

        @Override
        public void updateObservableTime(HybridTimestamp time) {
            // NO-OP
        }

        @Override
        public @Nullable QueryTransactionWrapper explicitTx() {
            return txWrapper.implicit() ? null : txWrapper;
        }
    }

    enum TxType {
        RW {
            @Override
            InternalTransaction create() {
                return NoOpTransaction.readWrite("LOCALHOST", false);
            }
        },

        RO {
            @Override
            InternalTransaction create() {
                return NoOpTransaction.readOnly("LOCALHOST", false);
            }
        };

        abstract InternalTransaction create();
    }

    @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException")
    private static class FailingIterator<T> implements Iterator<T> {
        private final Exception ex;

        FailingIterator(Exception ex) {
            this.ex = ex;
        }

        @Override
        public boolean hasNext() {
            sneakyThrow(ex);

            return false;
        }

        @SuppressWarnings("ReturnOfNull")
        @Override
        public T next() {
            sneakyThrow(ex);

            return null;
        }
    }
}
