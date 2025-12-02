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
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_PRIMARY_REPLICA_EXPIRED_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.PrimaryReplicaExpiredException;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.MismatchingTransactionOutcomeException;
import org.apache.ignite.tx.TransactionException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.verification.VerificationMode;

/**
 * Basic tests for a transaction manager.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
public class TxManagerTest extends IgniteAbstractTest {
    private static final InternalClusterNode LOCAL_NODE = new ClusterNodeImpl(
            randomUUID(), "local", new NetworkAddress("127.0.0.1", 2004), null
    );

    private static final InternalClusterNode REMOTE_NODE =
            new ClusterNodeImpl(randomUUID(), "remote", new NetworkAddress("127.1.1.1", 2024), null);

    private HybridTimestampTracker hybridTimestampTracker = HybridTimestampTracker.atomicTracker(null);

    private final LongSupplier idleSafeTimePropagationPeriodMsSupplier = () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;

    private TxManager txManager;

    private ClusterService clusterService;

    private ReplicaService replicaService;

    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = spy(new TestClockService(clock));

    private PlacementDriver placementDriver;

    @InjectConfiguration
    private TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration
    private SystemDistributedConfiguration systemDistributedConfiguration;

    @InjectExecutorService
    private ScheduledExecutorService commonScheduler;

    private final LocalRwTxCounter localRwTxCounter = spy(new TestLocalRwTxCounter());

    private final TestLowWatermark lowWatermark = spy(new TestLowWatermark());

    private TransactionInflights transactionInflights;

    @BeforeEach
    public void setup() {
        clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        replicaService = mock(ReplicaService.class, RETURNS_DEEP_STUBS);
        placementDriver = mock(PlacementDriver.class);

        when(clusterService.topologyService().localMember()).thenReturn(LOCAL_NODE);

        when(replicaService.invoke(any(InternalClusterNode.class), any())).thenReturn(nullCompletedFuture());

        when(replicaService.invoke(anyString(), any())).thenReturn(nullCompletedFuture());

        RemotelyTriggeredResourceRegistry resourceRegistry = new RemotelyTriggeredResourceRegistry();

        transactionInflights = new TransactionInflights(placementDriver, clockService);

        txManager = new TxManagerImpl(
                txConfiguration,
                systemDistributedConfiguration,
                clusterService,
                replicaService,
                lockManager(),
                clockService,
                new TransactionIdGenerator(0xdeadbeef),
                placementDriver,
                idleSafeTimePropagationPeriodMsSupplier,
                localRwTxCounter,
                resourceRegistry,
                transactionInflights,
                lowWatermark,
                commonScheduler,
                new TestMetricManager()
        );

        assertThat(txManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    private LockManager lockManager() {
        HeapLockManager lockManager = new HeapLockManager(systemLocalConfiguration);
        lockManager.start(new WaitDieDeadlockPreventionPolicy());
        return lockManager;
    }

    @AfterEach
    public void tearDown() {
        txManager.beforeNodeStop();

        assertThat(txManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        // TODO: IGNITE-23956 Move this line in the base class.
        // It is necessary to do after each test to prevent OOM in the middle of the test class execution.
        framework().clearInlineMocks();
    }

    @Test
    public void testBegin() {
        InternalTransaction tx0 = txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());
        InternalTransaction tx1 = txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults());
        InternalTransaction tx2 = txManager.beginImplicitRw(hybridTimestampTracker);
        InternalTransaction tx3 = txManager.beginImplicitRo(hybridTimestampTracker);

        assertNotNull(tx0.id());
        assertNotNull(tx1.id());
        assertNotNull(tx2.id());
        assertNotNull(tx3.id());

        assertFalse(tx0.isReadOnly());
        assertTrue(tx1.isReadOnly());
        assertFalse(tx2.isReadOnly());
        assertTrue(tx3.isReadOnly());
    }

    @Test
    public void testEnlist() {
        NetworkAddress addr = clusterService.topologyService().localMember().address();

        assertEquals(LOCAL_NODE.address(), addr);

        InternalTransaction tx = txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());

        ZonePartitionId partitionIdForEnlistment = new ZonePartitionId(1, 0);

        tx.assignCommitPartition(partitionIdForEnlistment);
        tx.enlist(partitionIdForEnlistment, 10, REMOTE_NODE.name(), 1L);

        PendingTxPartitionEnlistment actual = tx.enlistedPartition(partitionIdForEnlistment);
        assertEquals(REMOTE_NODE.name(), actual.primaryNodeConsistentId());
        assertEquals(1L, actual.consistencyToken());
        assertEquals(Set.of(10), actual.tableIds());

        // Avoid NPE on finish.
        var meta = mock(ReplicaMeta.class);
        when(meta.getStartTime()).thenReturn(clock.current());
        when(meta.getExpirationTime()).thenReturn(clock.current());
        when(meta.getLeaseholder()).thenReturn("test");
        when(meta.getLeaseholderId()).thenReturn(randomUUID());

        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(meta));

        HybridTimestamp commitTimestamp = clockService.now();
        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(completedFuture(new TransactionResult(TxState.COMMITTED, commitTimestamp)));
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
        when(clockService.now()).thenReturn(new HybridTimestamp(10_000, 10));

        assertThat(lowWatermark.updateAndNotify(new HybridTimestamp(10_000, 11)), willSucceedFast());

        IgniteInternalException exception = assertThrows(
                IgniteInternalException.class,
                () -> txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults())
        );

        assertEquals(Transactions.TX_READ_ONLY_TOO_OLD_ERR, exception.code());
    }

    @Test
    void testUpdateLowerWatermark() {
        // Let's check the absence of transactions.
        assertThat(lowWatermark.updateAndNotify(clockService.now()), willSucceedFast());

        InternalTransaction rwTx0 = txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());

        hybridTimestampTracker.update(clockService.now());

        InternalTransaction roTx0 = txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults());
        InternalTransaction roTx1 = txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults());

        CompletableFuture<Void> readOnlyTxsFuture = lowWatermark.updateAndNotify(roTx1.readTimestamp());
        assertFalse(readOnlyTxsFuture.isDone());

        assertThat(rwTx0.commitAsync(), willSucceedFast());
        assertFalse(readOnlyTxsFuture.isDone());

        assertThat(roTx0.commitAsync(), willSucceedFast());
        assertFalse(readOnlyTxsFuture.isDone());

        assertThat(roTx1.rollbackAsync(), willSucceedFast());
        assertTrue(readOnlyTxsFuture.isDone());

        // Let's check only RW transactions.
        txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());
        txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());

        assertThat(lowWatermark.updateAndNotify(clockService.now()), willSucceedFast());
    }

    @Test
    public void testRepeatedCommitRollbackAfterCommit() throws Exception {
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        HybridTimestamp commitTimestamp = clockService.now();
        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(completedFuture(new TransactionResult(TxState.COMMITTED, commitTimestamp)));

        InternalTransaction tx = txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());

        ZonePartitionId replicationGroupId = new ZonePartitionId(1, 0);

        tx.enlist(replicationGroupId, 10, REMOTE_NODE.name(), 1L);
        tx.assignCommitPartition(replicationGroupId);
        transactionInflights.track(tx.id()); // Enable cleanup path.

        tx.commit();

        tx.commitAsync().get(3, TimeUnit.SECONDS);
        tx.rollbackAsync().get(3, TimeUnit.SECONDS);
    }

    @Test
    public void testRepeatedCommitRollbackAfterRollback() throws Exception {
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(completedFuture(new TransactionResult(TxState.ABORTED, null)));

        InternalTransaction tx = txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());

        ZonePartitionId replicationGroupId = new ZonePartitionId(1, 0);

        tx.enlist(replicationGroupId, 10, REMOTE_NODE.name(), 1L);
        tx.assignCommitPartition(replicationGroupId);
        transactionInflights.track(tx.id()); // Enable cleanup path.

        tx.rollback();

        tx.commitAsync().get(3, TimeUnit.SECONDS);
        tx.rollbackAsync().get(3, TimeUnit.SECONDS);
    }

    @Test
    void testRepeatedCommitRollbackAfterCommitWithException() throws Exception {
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(failedFuture(
                        new MismatchingTransactionOutcomeInternalException(
                                "Test exception",
                                new TransactionResult(TxState.ABORTED, null
                                )
                        )));

        InternalTransaction tx = txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());

        ZonePartitionId replicationGroupId = new ZonePartitionId(1, 0);

        transactionInflights.track(tx.id());
        tx.enlist(replicationGroupId, 10, REMOTE_NODE.name(), 1L);
        tx.assignCommitPartition(replicationGroupId);

        TransactionException transactionException = assertThrows(TransactionException.class, tx::commit);

        assertInstanceOf(MismatchingTransactionOutcomeException.class, transactionException);

        tx.commitAsync().get(3, TimeUnit.SECONDS);
        tx.rollbackAsync().get(3, TimeUnit.SECONDS);
    }

    @Test
    public void testRepeatedCommitRollbackAfterRollbackWithException() throws Exception {
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(failedFuture(
                        new MismatchingTransactionOutcomeInternalException(
                                "Test exception",
                                new TransactionResult(TxState.ABORTED, null
                                )
                        )));

        InternalTransaction tx = txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());

        ZonePartitionId replicationGroupId = new ZonePartitionId(1, 0);

        transactionInflights.track(tx.id());
        tx.enlist(replicationGroupId, 10, REMOTE_NODE.name(), 1L);
        tx.assignCommitPartition(replicationGroupId);

        TransactionException transactionException = assertThrows(TransactionException.class, tx::rollback);

        assertInstanceOf(MismatchingTransactionOutcomeException.class, transactionException);

        tx.commitAsync().get(3, TimeUnit.SECONDS);
        tx.rollbackAsync().get(3, TimeUnit.SECONDS);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOnlyPendingCommit(boolean startReadOnlyTransaction) {
        assertEquals(0, txManager.pending());
        assertEquals(0, txManager.finished());

        // Start transaction.
        InternalTransaction tx = txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults());
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
    public void testOnlyPendingRollback(boolean startReadOnlyTransaction) {
        assertEquals(0, txManager.pending());
        assertEquals(0, txManager.finished());

        // Start transaction.
        InternalTransaction tx =
                startReadOnlyTransaction ? txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults())
                        : txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());
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
    public void testObservableTimestamp() {
        long compareThreshold = 50;
        // Check that idle safe time propagation period is significantly greater than compareThreshold.
        assertTrue(idleSafeTimePropagationPeriodMsSupplier.getAsLong() + clockService.maxClockSkewMillis() > compareThreshold * 5);

        HybridTimestamp now = clockService.now();

        InternalTransaction tx = txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults());

        assertTrue(abs(now.getPhysical() - tx.readTimestamp().getPhysical()) > compareThreshold);
        tx.commit();

        hybridTimestampTracker.update(now);

        tx = txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults());

        assertTrue(abs(now.getPhysical() - tx.readTimestamp().getPhysical()) < compareThreshold);
        tx.commit();

        HybridTimestamp timestampInPast = new HybridTimestamp(
                now.getPhysical() - idleSafeTimePropagationPeriodMsSupplier.getAsLong() * 2,
                now.getLogical()
        );

        hybridTimestampTracker = HybridTimestampTracker.atomicTracker(null);

        hybridTimestampTracker.update(timestampInPast);

        tx = txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults());

        long readTime = now.getPhysical() - idleSafeTimePropagationPeriodMsSupplier.getAsLong() - clockService.maxClockSkewMillis();

        assertThat(abs(readTime - tx.readTimestamp().getPhysical()), Matchers.lessThan(compareThreshold));

        tx.commit();
    }

    @Test
    public void testObservableTimestampLocally() {
        long compareThreshold = 50;
        // Check that idle safe time propagation period is significantly greater than compareThreshold.
        assertTrue(idleSafeTimePropagationPeriodMsSupplier.getAsLong() + clockService.maxClockSkewMillis() > compareThreshold * 5);

        HybridTimestamp now = clockService.now();

        InternalTransaction tx = txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults());

        HybridTimestamp firstReadTs = tx.readTimestamp();

        assertTrue(firstReadTs.compareTo(now) < 0);

        assertTrue(now.getPhysical() - firstReadTs.getPhysical() < compareThreshold
                + idleSafeTimePropagationPeriodMsSupplier.getAsLong() + clockService.maxClockSkewMillis());
        tx.commit();

        tx = txManager.beginExplicitRo(hybridTimestampTracker, InternalTxOptions.defaults());

        assertTrue(firstReadTs.compareTo(tx.readTimestamp()) <= 0);

        assertTrue(abs(now.getPhysical() - tx.readTimestamp().getPhysical()) < compareThreshold
                + idleSafeTimePropagationPeriodMsSupplier.getAsLong() + clockService.maxClockSkewMillis());
        tx.commit();
    }

    @Test
    public void testFinishSamePrimary() {
        // Same primary that was enlisted is returned during finish phase and commitTimestamp is less that primary.expirationTimestamp.
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        HybridTimestamp commitTimestamp = clockService.now();
        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(completedFuture(new TransactionResult(TxState.COMMITTED, commitTimestamp)));
        // Ensure that commit doesn't throw exceptions.
        InternalTransaction committedTransaction = prepareTransaction();
        committedTransaction.commit();
        assertEquals(TxState.COMMITTED, txManager.stateMeta(committedTransaction.id()).txState());

        // Ensure that rollback doesn't throw exceptions.
        assertRollbackSucceeds();
    }

    @Test
    public void testPrimaryMissOnFirstCall() {
        // First call to the commit partition primary fails with PrimaryReplicaMissException,
        // then we retry and the second call succeeds.
        when(placementDriver.getPrimaryReplica(any(), any()))
                .thenReturn(
                        completedFuture(new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), hybridTimestamp(10)))
                );
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any()))
                .thenReturn(
                        completedFuture(new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), hybridTimestamp(10))),
                        completedFuture(new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(12), HybridTimestamp.MAX_VALUE))
                );

        HybridTimestamp commitTimestamp = hybridTimestamp(9);

        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(
                        failedFuture(new PrimaryReplicaMissException(
                                LOCAL_NODE.name(),
                                null,
                                LOCAL_NODE.id(),
                                null,
                                10L,
                                null,
                                null
                        )),
                        completedFuture(new TransactionResult(TxState.COMMITTED, commitTimestamp))
                );

        when(clockService.now()).thenReturn(hybridTimestamp(5));
        // Ensure that commit doesn't throw exceptions.
        InternalTransaction committedTransaction = prepareTransaction();

        when(clockService.now()).thenReturn(commitTimestamp, hybridTimestamp(9));

        committedTransaction.commit();
        assertEquals(TxState.COMMITTED, txManager.stateMeta(committedTransaction.id()).txState());
        assertEquals(hybridTimestamp(9), txManager.stateMeta(committedTransaction.id()).commitTimestamp());

        // Ensure that rollback doesn't throw exceptions.
        assertRollbackSucceeds();
    }

    @Test
    public void testFinishExpiredWithNullPrimary() {
        // Null is returned as primaryReplica during finish phase.
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(nullCompletedFuture());
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), hybridTimestamp(10))));
        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(completedFuture(new TransactionResult(TxState.ABORTED, null)));

        assertCommitThrowsTransactionExceptionWithPrimaryReplicaExpiredExceptionAsCause();

        assertRollbackSucceeds();
    }

    @Test
    public void testExpiredExceptionDoesNotShadeResponseExceptions() {
        // Null is returned as primaryReplica during finish phase.
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(2), HybridTimestamp.MAX_VALUE)));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), hybridTimestamp(10))));
        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(failedFuture(new MismatchingTransactionOutcomeInternalException(
                        "TX already finished.",
                        new TransactionResult(TxState.ABORTED, null)
                )));

        InternalTransaction committedTransaction = prepareTransaction();

        assertThrowsWithCause(committedTransaction::commit, MismatchingTransactionOutcomeException.class);

        assertEquals(TxState.ABORTED, txManager.stateMeta(committedTransaction.id()).txState());

        assertRollbackSucceeds();
    }

    @Test
    public void testOnlyPrimaryExpirationAffectsTransaction() {
        // Prepare transaction.
        InternalTransaction tx = txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());

        String nodeName = "SomeNode";

        ZonePartitionId partitionIdForEnlistment1 = new ZonePartitionId(1, 0);
        tx.enlist(partitionIdForEnlistment1, 10, nodeName, 1L);
        tx.assignCommitPartition(partitionIdForEnlistment1);

        ReplicationGroupId partitionIdForEnlistment2 = new ZonePartitionId(2, 0);
        tx.enlist(partitionIdForEnlistment2, 20, nodeName, 1L);

        when(placementDriver.getPrimaryReplica(eq(partitionIdForEnlistment1), any()))
                .thenReturn(completedFuture(
                        new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));
        when(placementDriver.awaitPrimaryReplica(eq(partitionIdForEnlistment1), any(), anyLong(), any()))
                .thenReturn(completedFuture(
                        new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        lenient().when(placementDriver.getPrimaryReplica(eq(partitionIdForEnlistment2), any()))
                .thenReturn(nullCompletedFuture());
        lenient().when(placementDriver.awaitPrimaryReplica(eq(partitionIdForEnlistment2), any(), anyLong(), any()))
                .thenReturn(completedFuture(
                        new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), hybridTimestamp(10))));

        HybridTimestamp commitTimestamp = clockService.now();
        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(completedFuture(new TransactionResult(TxState.COMMITTED, commitTimestamp)));

        // Ensure that commit doesn't throw exceptions.
        InternalTransaction committedTransaction = prepareTransaction();
        committedTransaction.commit();
        assertEquals(TxState.COMMITTED, txManager.stateMeta(committedTransaction.id()).txState());

        // Ensure that rollback doesn't throw exceptions.
        assertRollbackSucceeds();
    }

    @Test
    public void testFinishExpiredWithExpiredPrimary() {
        // Primary with expirationTimestamp less than commitTimestamp is returned.
        // It's impossible from the point of view of getPrimaryReplica to return expired lease,
        // given test checks that an assertion exception will be thrown and wrapped with proper transaction public one.
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), hybridTimestamp(10))));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), hybridTimestamp(10))));
        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(completedFuture(new TransactionResult(TxState.ABORTED, null)));

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
        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(2), HybridTimestamp.MAX_VALUE)));
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(completedFuture(
                new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(2), HybridTimestamp.MAX_VALUE)));
        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(completedFuture(new TransactionResult(TxState.ABORTED, null)));

        assertCommitThrowsTransactionExceptionWithPrimaryReplicaExpiredExceptionAsCause();

        assertRollbackSucceeds();
    }

    @ParameterizedTest(name = "readOnly = {0}")
    @ValueSource(booleans = {true, false})
    void testIncrementLocalRwTxCounterOnBeginTransaction(boolean readOnly) {
        InternalTransaction tx = txManager.beginExplicit(hybridTimestampTracker, readOnly, InternalTxOptions.defaults());

        VerificationMode verificationMode = readOnly ? never() : times(1);

        verify(localRwTxCounter, verificationMode).inUpdateRwTxCountLock(any());
        verify(localRwTxCounter, verificationMode).incrementRwTxCount(eq(beginTs(tx)));
    }

    @ParameterizedTest(name = "readOnly = {0}, commit = {1}")
    @MethodSource("txTypeAndWayCompleteTx")
    void testDecrementLocalRwTxCounterOnCompleteTransaction(boolean readOnly, boolean commit) {
        InternalTransaction tx = txManager.beginExplicit(hybridTimestampTracker, readOnly, InternalTxOptions.defaults());

        clearInvocations(localRwTxCounter);

        assertThat(commit ? tx.commitAsync() : tx.rollbackAsync(), willSucceedFast());

        VerificationMode verificationMode = readOnly ? never() : times(1);

        verify(localRwTxCounter, verificationMode).inUpdateRwTxCountLock(any());
        verify(localRwTxCounter, verificationMode).decrementRwTxCount(eq(beginTs(tx)));
    }

    @ParameterizedTest(name = "commit = {0}")
    @ValueSource(booleans = {true, false})
    void testDecrementLocalRwTxCounterOnCompleteTransactionWithEnlistedPartitions(boolean commit) {
        InternalTransaction rwTx = prepareTransaction();

        clearInvocations(localRwTxCounter);

        ReplicaMeta replicaMeta = new TestReplicaMetaImpl(LOCAL_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE);
        CompletableFuture<ReplicaMeta> primaryReplicaMetaFuture = completedFuture(replicaMeta);

        when(placementDriver.getPrimaryReplica(any(), any())).thenReturn(primaryReplicaMetaFuture);
        when(placementDriver.awaitPrimaryReplica(any(), any(), anyLong(), any())).thenReturn(primaryReplicaMetaFuture);

        var txResult = new TransactionResult(commit ? TxState.COMMITTED : TxState.ABORTED, clockService.now());

        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class))).thenReturn(completedFuture(txResult));

        assertThat(commit ? rwTx.commitAsync() : rwTx.rollbackAsync(), willSucceedFast());

        verify(localRwTxCounter).inUpdateRwTxCountLock(any());
        verify(localRwTxCounter).decrementRwTxCount(eq(beginTs(rwTx)));
    }

    @Test
    void testCreateBeginTsInsideInUpdateRwTxCount() {
        doAnswer(invocation -> {
            clearInvocations(clockService);

            Object result = invocation.callRealMethod();

            verify(clockService).now();

            return result;
        }).when(localRwTxCounter).inUpdateRwTxCountLock(any());

        txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());

        doReturn(null).when(localRwTxCounter).inUpdateRwTxCountLock(any());
    }

    @Test
    void testReadWithExplicitTimestamp() {
        HybridTimestamp now = clockService.now();

        HybridTimestamp readTimestampInThePast = now.subtractPhysicalTime(100_000);

        InternalTxOptions optionsWithExplicitReadTimestamp = InternalTxOptions.builder()
                .readTimestamp(readTimestampInThePast)
                .build();

        InternalTxOptions defaultOptions = InternalTxOptions.defaults();

        InternalTransaction tx1 = txManager.beginExplicitRo(hybridTimestampTracker, defaultOptions);
        InternalTransaction tx2 = txManager.beginExplicitRo(hybridTimestampTracker, optionsWithExplicitReadTimestamp);

        assertThat(tx1.readTimestamp(), is(greaterThanOrEqualTo(now.subtractPhysicalTime(roReadTimestampSkew()))));
        assertThat(tx2.readTimestamp(), is(readTimestampInThePast));
    }

    private long roReadTimestampSkew() {
        return idleSafeTimePropagationPeriodMsSupplier.getAsLong() + clockService.maxClockSkewMillis();
    }

    private InternalTransaction prepareTransaction() {
        InternalTransaction tx = txManager.beginExplicitRw(hybridTimestampTracker, InternalTxOptions.defaults());

        ZonePartitionId replicationGroupId = new ZonePartitionId(1, 0);

        tx.enlist(replicationGroupId, 10, REMOTE_NODE.name(), 1L);
        tx.assignCommitPartition(replicationGroupId);

        transactionInflights.track(tx.id());

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
        when(replicaService.invoke(anyString(), any(TxFinishReplicaRequest.class)))
                .thenReturn(completedFuture(new TransactionResult(TxState.ABORTED, null)));

        InternalTransaction abortedTransaction = prepareTransaction();
        abortedTransaction.rollback();
        assertEquals(TxState.ABORTED, txManager.stateMeta(abortedTransaction.id()).txState());
    }

    private static Stream<Arguments> txTypeAndWayCompleteTx() {
        return Stream.of(
                Arguments.of(true, true),  // Read-only, commit.
                Arguments.of(true, false), // Read-only, rollback.
                Arguments.of(false, true), // Read-write, commit.
                Arguments.of(false, false) // Read-write, rollback.
        );
    }

    private static HybridTimestamp beginTs(InternalTransaction tx) {
        return TransactionIds.beginTimestamp(tx.id());
    }
}
