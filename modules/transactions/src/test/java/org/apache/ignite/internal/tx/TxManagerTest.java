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


import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Basic tests for a transaction manager.
 */
@ExtendWith(MockitoExtension.class)
public class TxManagerTest extends IgniteAbstractTest {
    private static final NetworkAddress ADDR = new NetworkAddress("127.0.0.1", 2004);

    private TxManager txManager;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ReplicaService replicaService;

    private final HybridClock clock = spy(new HybridClockImpl());

    /** Init test callback. */
    @BeforeEach
    public void before() {
        clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

        when(clusterService.topologyService().localMember().address()).thenReturn(ADDR);

        replicaService = mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        txManager = new TxManagerImpl(replicaService, new HeapLockManager(), clock, new TransactionIdGenerator(0xdeadbeef));
    }

    @Test
    public void testBegin() {
        InternalTransaction tx0 = txManager.begin();
        InternalTransaction tx1 = txManager.begin(false);
        InternalTransaction tx2 = txManager.begin(true);

        assertNotNull(tx0.id());
        assertNotNull(tx1.id());
        assertNotNull(tx2.id());
    }

    @Test
    public void testEnlist() {
        NetworkAddress addr = clusterService.topologyService().localMember().address();

        assertEquals(ADDR, addr);

        InternalTransaction tx = txManager.begin();

        TablePartitionId tablePartitionId = new TablePartitionId(UUID.randomUUID(), 0);

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
        when(clock.now()).thenReturn(new HybridTimestamp(10, 10));

        assertThat(txManager.updateLowWatermark(new HybridTimestamp(10, 11)), willSucceedFast());

        IgniteInternalException exception = assertThrows(IgniteInternalException.class, () -> txManager.begin(true));

        assertEquals(Transactions.TX_READ_ONLY_TOO_OLD_ERR, exception.code());
    }

    @Test
    void testUpdateLowerWatermark() {
        // Let's check the absence of transactions.
        assertThat(txManager.updateLowWatermark(clock.now()), willSucceedFast());

        InternalTransaction rwTx0 = txManager.begin(false);

        InternalTransaction roTx0 = txManager.begin(true);
        InternalTransaction roTx1 = txManager.begin(true);

        CompletableFuture<Void> readOnlyTxsFuture = txManager.updateLowWatermark(roTx1.readTimestamp());
        assertFalse(readOnlyTxsFuture.isDone());

        assertThat(rwTx0.commitAsync(), willSucceedFast());
        assertFalse(readOnlyTxsFuture.isDone());

        assertThat(roTx0.commitAsync(), willSucceedFast());
        assertFalse(readOnlyTxsFuture.isDone());

        assertThat(roTx1.rollbackAsync(), willSucceedFast());
        assertTrue(readOnlyTxsFuture.isDone());

        // Let's check only RW transactions.
        txManager.begin(false);
        txManager.begin(false);

        assertThat(txManager.updateLowWatermark(clock.now()), willSucceedFast());
    }
}
