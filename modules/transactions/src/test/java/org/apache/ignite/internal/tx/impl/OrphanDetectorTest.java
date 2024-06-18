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

package org.apache.ignite.internal.tx.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestReplicaMetaImpl;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test how OrphanDetector reacts on tx lock conflicts.
 */
@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
public class OrphanDetectorTest extends BaseIgniteAbstractTest {

    private static final ClusterNode LOCAL_NODE =
            new ClusterNodeImpl("local_id", "local", new NetworkAddress("127.0.0.1", 2024), null);

    private static final ClusterNode REMOTE_NODE =
            new ClusterNodeImpl("remote_id", "remote", new NetworkAddress("127.1.1.1", 2024), null);

    @Mock(answer = RETURNS_DEEP_STUBS)
    private TopologyService topologyService;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private ReplicaService replicaService;

    @Mock
    private PlacementDriver placementDriver;

    private final HeapLockManager lockManager = new HeapLockManager();

    private final HybridClock clock = new HybridClockImpl();

    private final ClockService clockService = new TestClockService(clock);

    @InjectConfiguration
    private TransactionConfiguration txConfiguration;

    private VolatileTxStateMetaStorage txStateMetaStorage;

    private TransactionIdGenerator idGenerator;

    @BeforeEach
    public void setup() {
        idGenerator = new TransactionIdGenerator(LOCAL_NODE.name().hashCode());

        PlacementDriverHelper placementDriverHelper = new PlacementDriverHelper(placementDriver, clockService);

        OrphanDetector orphanDetector = new OrphanDetector(topologyService, replicaService, placementDriverHelper, lockManager);

        txStateMetaStorage = new VolatileTxStateMetaStorage();

        txStateMetaStorage.start();

        orphanDetector.start(txStateMetaStorage, txConfiguration.abandonedCheckTs());
    }

    @Test
    void testNoTriggerNoState() {
        UUID orphanTxId = idGenerator.transactionIdFor(clock.now());

        RowId rowId = new RowId(0);

        // Coordinator is dead.
        when(topologyService.getById(eq(LOCAL_NODE.id()))).thenReturn(null);

        lockManager.acquire(orphanTxId, new LockKey(1, rowId), LockMode.X);

        UUID concurrentTxId = idGenerator.transactionIdFor(clock.now());

        // Should trigger lock conflict listener in OrphanDetector.
        lockManager.acquire(concurrentTxId, new LockKey(1, rowId), LockMode.X);

        TxStateMeta orphanState = txStateMetaStorage.state(orphanTxId);

        // OrphanDetector didn't change the state.
        assertNull(orphanState);

        verifyNoInteractions(replicaService);
    }

    @Test
    void testNoTriggerCommittedState() {
        UUID orphanTxId = idGenerator.transactionIdFor(clock.now());

        TablePartitionId tpId = new TablePartitionId(1, 0);

        RowId rowId = new RowId(tpId.partitionId());

        // Coordinator is dead.
        when(topologyService.getById(eq(LOCAL_NODE.id()))).thenReturn(null);

        lockManager.acquire(orphanTxId, new LockKey(tpId.tableId(), rowId), LockMode.X);

        UUID concurrentTxId = idGenerator.transactionIdFor(clock.now());

        TxStateMeta committedState = new TxStateMeta(TxState.COMMITTED, LOCAL_NODE.id(), tpId, clock.now());

        txStateMetaStorage.updateMeta(orphanTxId, stateMeta -> committedState);

        // Should trigger lock conflict listener in OrphanDetector.
        lockManager.acquire(concurrentTxId, new LockKey(1, rowId), LockMode.X);

        TxStateMeta orphanState = txStateMetaStorage.state(orphanTxId);

        // OrphanDetector didn't change the state.
        assertEquals(committedState, orphanState);

        verifyNoInteractions(replicaService);
    }

    @Test
    void testNoTriggerAbortedState() {
        UUID orphanTxId = idGenerator.transactionIdFor(clock.now());

        TablePartitionId tpId = new TablePartitionId(1, 0);

        RowId rowId = new RowId(tpId.partitionId());

        // Coordinator is dead.
        when(topologyService.getById(eq(LOCAL_NODE.id()))).thenReturn(null);

        lockManager.acquire(orphanTxId, new LockKey(tpId.tableId(), rowId), LockMode.X);

        UUID concurrentTxId = idGenerator.transactionIdFor(clock.now());

        TxStateMeta abortedState = new TxStateMeta(TxState.ABORTED, LOCAL_NODE.id(), tpId, null);

        txStateMetaStorage.updateMeta(orphanTxId, stateMeta -> abortedState);

        // Should trigger lock conflict listener in OrphanDetector.
        lockManager.acquire(concurrentTxId, new LockKey(1, rowId), LockMode.X);

        TxStateMeta orphanState = txStateMetaStorage.state(orphanTxId);

        // OrphanDetector didn't change the state.
        assertEquals(abortedState, orphanState);

        verifyNoInteractions(replicaService);
    }

    @Test
    void testNoTriggerFinishingState() {
        UUID orphanTxId = idGenerator.transactionIdFor(clock.now());

        TablePartitionId tpId = new TablePartitionId(1, 0);

        RowId rowId = new RowId(tpId.partitionId());

        lockManager.acquire(orphanTxId, new LockKey(tpId.tableId(), rowId), LockMode.X);

        UUID concurrentTxId = idGenerator.transactionIdFor(clock.now());

        TxStateMeta finishingState = new TxStateMeta(TxState.FINISHING, LOCAL_NODE.id(), tpId, null);

        txStateMetaStorage.updateMeta(orphanTxId, stateMeta -> finishingState);

        // Coordinator is dead.
        when(topologyService.getById(eq(LOCAL_NODE.id()))).thenReturn(null);

        // Should trigger lock conflict listener in OrphanDetector.
        lockManager.acquire(concurrentTxId, new LockKey(1, rowId), LockMode.X);

        TxStateMeta orphanState = txStateMetaStorage.state(orphanTxId);

        // OrphanDetector didn't change the state.
        assertEquals(finishingState, orphanState);

        verifyNoInteractions(replicaService);
    }

    @Test
    void testNoTriggerCoordinatorAlive() {
        UUID orphanTxId = idGenerator.transactionIdFor(clock.now());

        TablePartitionId tpId = new TablePartitionId(1, 0);

        RowId rowId = new RowId(tpId.partitionId());

        lockManager.acquire(orphanTxId, new LockKey(tpId.tableId(), rowId), LockMode.X);

        UUID concurrentTxId = idGenerator.transactionIdFor(clock.now());

        TxStateMeta pendingState = new TxStateMeta(TxState.PENDING, LOCAL_NODE.id(), tpId, null);

        txStateMetaStorage.updateMeta(orphanTxId, stateMeta -> pendingState);

        when(topologyService.getById(eq(LOCAL_NODE.id()))).thenReturn(mock(ClusterNode.class));

        // Should trigger lock conflict listener in OrphanDetector.
        lockManager.acquire(concurrentTxId, new LockKey(1, rowId), LockMode.X);

        TxStateMeta orphanState = txStateMetaStorage.state(orphanTxId);

        // OrphanDetector didn't change the state.
        assertEquals(pendingState, orphanState);

        verifyNoInteractions(replicaService);
    }

    @Test
    void testTriggerOnLockConflictCoordinatorDead() {
        UUID orphanTxId = idGenerator.transactionIdFor(clock.now());

        TablePartitionId tpId = new TablePartitionId(1, 0);

        RowId rowId = new RowId(tpId.partitionId());

        when(placementDriver.awaitPrimaryReplica(eq(tpId), any(), anyLong(), any()))
                .thenReturn(completedFuture(new TestReplicaMetaImpl(REMOTE_NODE, hybridTimestamp(1), HybridTimestamp.MAX_VALUE)));

        lockManager.acquire(orphanTxId, new LockKey(tpId.tableId(), rowId), LockMode.X);

        UUID concurrentTxId = idGenerator.transactionIdFor(clock.now());

        TxStateMeta pendingState = new TxStateMeta(TxState.PENDING, LOCAL_NODE.id(), tpId, null);

        txStateMetaStorage.updateMeta(orphanTxId, stateMeta -> pendingState);

        // Coordinator is dead.
        when(topologyService.getById(eq(LOCAL_NODE.id()))).thenReturn(null);

        // Should trigger lock conflict listener in OrphanDetector.
        CompletableFuture<Lock> acquire = lockManager.acquire(concurrentTxId, new LockKey(1, rowId), LockMode.X);

        TxStateMeta orphanState = txStateMetaStorage.state(orphanTxId);

        // OrphanDetector didn't change the state.
        assertEquals(TxState.ABANDONED, orphanState.txState());

        // Send tx recovery message.
        verify(replicaService).invoke(any(ClusterNode.class), any());

        assertThat(acquire, willThrow(LockException.class, "Failed to acquire an abandoned lock due to a possible deadlock"));
    }
}
