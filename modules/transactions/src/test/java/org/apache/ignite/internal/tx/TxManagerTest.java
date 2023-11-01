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

package org.apache.ignite.internal.tx;


import static java.lang.Math.abs;
import static org.apache.ignite.internal.Kludges.IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS_PROPERTY;
import static org.apache.ignite.internal.hlc.HybridTimestamp.CLOCK_SKEW;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.replicator.ReplicaManager.idleSafeTimePropagationPeriodMs;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_PRIMARY_REPLICA_EXPIRED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.PrimaryReplicaExpiredException;
import org.apache.ignite.internal.tx.impl.SimpleFailHandler;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Basic tests for a transaction manager.
 */
@ExtendWith(MockitoExtension.class)
public class TxManagerTest extends IgniteAbstractTest {
    private static final ClusterNode LOCAL_NODE = new ClusterNodeImpl("local_id", "local", new NetworkAddress("127.0.0.1", 2004), null);

    /** Timestamp tracker. */
    private HybridTimestampTracker hybridTimestampTracker = new HybridTimestampTracker();

    private TxManager txManager;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ReplicaService replicaService;

    private final HybridClock clock = spy(new HybridClockImpl());

    @Mock
    private PlacementDriver placementDriver;

    /** Init test callback. */
    @BeforeEach
    public void setup() {
        clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

        when(clusterService.topologyService().localMember().address()).thenReturn(LOCAL_NODE.address());

        replicaService = mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        when(replicaService.invoke(any(ClusterNode.class), any())).thenReturn(CompletableFuture.completedFuture(null));

        when(replicaService.invoke(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));

        txManager = new TxManagerImpl(
                replicaService,
                new HeapLockManager(),
                clock,
                new TransactionIdGenerator(0xdeadbeef),
                LOCAL_NODE::id,
                placementDriver,
                new SimpleFailHandler()
        );

        txManager.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        txManager.beforeNodeStop();
        txManager.stop();
    }

    @Test
    public void testBegin() {
        InternalTransaction tx0 = txManager.begin(hybridTimestampTracker);
        InternalTransaction tx1 = txManager.begin(hybridTimestampTracker);
        InternalTransaction tx2 = txManager.begin(hybridTimestampTracker, true);

        assertNotNull(tx0.id());
        assertNotNull(tx1.id());
        assertNotNull(tx2.id());

        assertFalse(tx0.isReadOnly());
        assertFalse(tx1.isReadOnly());
        assertTrue(tx2.isReadOnly());
    }

    @Test
    public void testEnlist() {
        NetworkAddress addr = clusterService.topologyService().localMember().address();

        assertEquals(LOCAL_NODE.address(), addr);

        InternalTransaction tx = txManager.begin(hybridTimestampTracker);

        TablePartitionId tablePartitionId = new TablePartitionId(1, 0);

        ClusterNode node = mock(ClusterNode.class);

        tx.enlist(tablePartitionId, new IgniteBiTuple<>(node, 1L));

        assertEquals(new IgniteBiTuple<>(node, 1L), tx.enlistedNodeAndTerm(tablePartitionId));
    }

    @Test
    public void testId() throws Exception {
        UUID txId1 = TestTransactionIds.newTransactionId();
        UUID txId2 = TestTransactionIds.newTransactionId();
        UUID txId3 = TestTransactionIds.newTransactionId();

        Thread.sleep(1);

        UUID txId4 = TestTransactionIds.newTransactionId();

        assertTrue(txId2.compareTo(txId1) > 0);
        assertTrue(txId3.compareTo(txId2) > 0);
        assertTrue(txId4.compareTo(txId3) > 0);
    }

    @Test
    void testCreateNewRoTxAfterUpdateLowerWatermark() {
        when(clock.now()).thenReturn(new HybridTimestamp(10_000, 10));

        assertThat(txManager.updateLowWatermark(new HybridTimestamp(10_000, 11)), willSucceedFast());

        IgniteInternalException exception =
                assertThrows(IgniteInternalException.class, () -> txManager.begin(hybridTimestampTracker, true));

        assertEquals(Transactions.TX_READ_ONLY_TOO_OLD_ERR, exception.code());
    }

    @Test
    void testUpdateLowerWatermark() {
        // Let's check the absence of transactions.
        assertThat(txManager.updateLowWatermark(clock.now()), willSucceedFast());

        InternalTransaction rwTx0 = txManager.begin(hybridTimestampTracker);

        hybridTimestampTracker.update(clock.now());

        InternalTransaction roTx0 = txManager.begin(hybridTimestampTracker, true);
        InternalTransaction roTx1 = txManager.begin(hybridTimestampTracker, true);

        CompletableFuture<Void> readOnlyTxsFuture = txManager.updateLowWatermark(roTx1.readTimestamp());
        assertFalse(readOnlyTxsFuture.isDone());

        assertThat(rwTx0.commitAsync(), willSucceedFast());
        assertFalse(readOnlyTxsFuture.isDone());

        assertThat(roTx0.commitAsync(), willSucceedFast());
        assertFalse(readOnlyTxsFuture.isDone());

        assertThat(roTx1.rollbackAsync(), willSucceedFast());
        assertTrue(readOnlyTxsFuture.isDone());

        // Let's check only RW transactions.
        txManager.begin(hybridTimestampTracker);
        txManager.begin(hybridTimestampTracker);

        assertThat(txManager.updateLowWatermark(clock.now()), willSucceedFast());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTestOnlyPendingCommit(boolean startReadOnlyTransaction) {
        assertEquals(0, txManager.pending());
        assertEquals(0, txManager.finished());

        // Start transaction.
        InternalTransaction tx = txManager.begin(hybridTimestampTracker, true);
        assertEquals(1, txManager.pending());
        assertEquals(0, txManager.finished());

        // Commit transaction.
        tx.commit();
        assertEquals(0, txManager.pending());
        assertEquals(1, txManager.finished());

        // Check that tx.commit() is idempotent within the scope of txManager.pending() and txManager.finished()
        tx.commit();
        assertEquals(0, txManager.pending());
        assertEquals(1, txManager.finished());

        // Check that tx.rollback() after tx.commit() won't effect txManager.pending() and txManager.finished()
        tx.rollback();
        assertEquals(0, txManager.pending());
        assertEquals(1, txManager.finished());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTestOnlyPendingRollback(boolean startReadOnlyTransaction) {
        assertEquals(0, txManager.pending());
        assertEquals(0, txManager.finished());

        // Start transaction.
        InternalTransaction tx =
                startReadOnlyTransaction ? txManager.begin(hybridTimestampTracker, true) : txManager.begin(hybridTimestampTracker);
        assertEquals(1, txManager.pending());
        assertEquals(0, txManager.finished());

        // Rollback transaction.
        tx.rollback();
        assertEquals(0, txManager.pending());
        assertEquals(1, txManager.finished());

        // Check that tx.rollback() is idempotent within the scope of txManager.pending() and txManager.finished()
        tx.rollback();
        assertEquals(0, txManager.pending());
        assertEquals(1, txManager.finished());

        // Check that tx.commit() after tx.rollback() won't effect txManager.pending() and txManager.finished()
        tx.commit();
        assertEquals(0, txManager.pending());
        assertEquals(1, txManager.finished());
    }

    @Test
    @WithSystemProperty(key = IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS_PROPERTY, value = "1000")
    public void testObservableTimestamp() {
        long compareThreshold = 50;
        // Check that idle safe time propagation period is significantly greater than compareThreshold.
        assertTrue(idleSafeTimePropagationPeriodMs() + CLOCK_SKEW > compareThreshold * 5);

        HybridTimestamp now = clock.now();

        InternalTransaction tx = txManager.begin(hybridTimestampTracker, true);

        assertTrue(abs(now.getPhysical() - tx.readTimestamp().getPhysical()) > compareThreshold);
        tx.commit();

        hybridTimestampTracker.update(now);

        tx = txManager.begin(hybridTimestampTracker, true);

        assertTrue(abs(now.getPhysical() - tx.readTimestamp().getPhysical()) < compareThreshold);
        tx.commit();

        HybridTimestamp timestampInPast = new HybridTimestamp(
                now.getPhysical() - idleSafeTimePropagationPeriodMs() * 2,
                now.getLogical()
        );

        hybridTimestampTracker = new HybridTimestampTracker();

        hybridTimestampTracker.update(timestampInPast);

        tx = txManager.begin(hybridTimestampTracker, true);

        long readTime = now.getPhysical() - idleSafeTimePropagationPeriodMs() - CLOCK_SKEW;

        assertThat(abs(readTime - tx.readTimestamp().getPhysical()), Matchers.lessThan(compareThreshold));

        tx.commit();
    }

    @Test
    @WithSystemProperty(key = IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS_PROPERTY, value = "1000")
    public void testObservableTimestampLocally() {
        long compareThreshold = 50;
        // Check that idle safe time propagation period is significantly greater than compareThreshold.
        assertTrue(idleSafeTimePropagationPeriodMs() + CLOCK_SKEW > compareThreshold * 5);

        HybridTimestamp now = clock.now();

        InternalTransaction tx = txManager.begin(hybridTimestampTracker, true);

        HybridTimestamp firstReadTs = tx.readTimestamp();

        assertTrue(firstReadTs.compareTo(now) < 0);

        assertTrue(now.getPhysical() - firstReadTs.getPhysical() < compareThreshold
                + idleSafeTimePropagationPeriodMs() + CLOCK_SKEW);
        tx.commit();

        tx = txManager.begin(hybridTimestampTracker, true);

        assertTrue(firstReadTs.compareTo(tx.readTimestamp()) <= 0);

        assertTrue(abs(now.getPhysical() - tx.readTimestamp().getPhysical()) < compareThreshold
                + idleSafeTimePropagationPeriodMs() + CLOCK_SKEW);
        tx.commit();
    }

    @Test
    public void testFinishSamePrimary() {
        // Same primary that was enlisted is returned during finish phase and commitTimestamp is less that primary.expirationTimestamp.
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(CompletableFuture.completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(CompletableFuture.completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        // Ensure that commit doesn't throw exceptions.
        InternalTransaction committedTransaction = prepareTransaction();
        committedTransaction.commit();
        assertEquals(TxState.COMMITED, txManager.stateMeta(committedTransaction.id()).txState());

        // Ensure that rollback doesn't throw exceptions.
        assertRollbackSucceeds();
    }

    @Test
    public void testFinishExpiredWithNullPrimary() {
        // Null is returned as primaryReplica during finish phase.
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(CompletableFuture.completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), hybridTimestamp(10))));

        assertCommitThrowsTransactionExceptionWithPrimaryReplicaExpiredExceptionAsCause();

        assertRollbackSucceeds();
    }

    @Test
    public void testFinishExpiredWithExpiredPrimary() {
        // Primary with expirationTimestamp less than commitTimestamp is returned.
        // It's impossible from the point of view of getPrimaryReplica to return expired lease,
        // given test checks that an assertion exception will be thrown and wrapped with proper transaction public one.
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(CompletableFuture.completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), hybridTimestamp(10))));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(CompletableFuture.completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), hybridTimestamp(10))));

        InternalTransaction committedTransaction = prepareTransaction();
        Throwable throwable = assertThrowsWithCause(committedTransaction::commit, AssertionError.class);

        assertSame(TransactionException.class, throwable.getClass());
        // short cast is useful for better error code readability
        //noinspection NumericCastThatLosesPrecision
        assertEquals((short) TX_COMMIT_ERR, (short) ((TransactionException) throwable).code());

        assertEquals(TxState.ABORTED, txManager.stateMeta(committedTransaction.id()).txState());

        assertRollbackSucceeds();
    }

    @Test
    public void testFinishExpiredWithDifferentEnlistmentConsistencyToken() {
        // Primary with another enlistment consistency token is returned.
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(CompletableFuture.completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(2), HybridTimestamp.MAX_VALUE)));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(CompletableFuture.completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(2), HybridTimestamp.MAX_VALUE)));

        assertCommitThrowsTransactionExceptionWithPrimaryReplicaExpiredExceptionAsCause();

        assertRollbackSucceeds();
    }

    private InternalTransaction prepareTransaction() {
        InternalTransaction tx = txManager.begin(hybridTimestampTracker);

        ClusterNode node = mock(ClusterNode.class);

        TablePartitionId tablePartitionId1 = new TablePartitionId(1, 0);
        tx.enlist(tablePartitionId1, new IgniteBiTuple<>(node, 1L));
        tx.assignCommitPartition(tablePartitionId1);

        return tx;
    }

    private void assertCommitThrowsTransactionExceptionWithPrimaryReplicaExpiredExceptionAsCause() {
        InternalTransaction committedTransaction = prepareTransaction();
        Throwable throwable = assertThrowsWithCause(committedTransaction::commit, PrimaryReplicaExpiredException.class);

        assertSame(TransactionException.class, throwable.getClass());
        // short cast is useful for better error code readability
        //noinspection NumericCastThatLosesPrecision
        assertEquals((short) TX_PRIMARY_REPLICA_EXPIRED_ERR, (short) ((TransactionException) throwable).code());

        assertEquals(TxState.ABORTED, txManager.stateMeta(committedTransaction.id()).txState());
    }

    private void assertRollbackSucceeds() {
        InternalTransaction abortedTransaction = prepareTransaction();
        abortedTransaction.rollback();
        assertEquals(TxState.ABORTED, txManager.stateMeta(abortedTransaction.id()).txState());
    }
}
