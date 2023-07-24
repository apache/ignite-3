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

package org.apache.ignite.internal.table;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests repeated commit/rollback operations.
 */
public class RepeatedFinishReadWriteTransactionTest {
    private final ClusterNode clusterNode = new ClusterNodeImpl("test", "test", new NetworkAddress("test", 1000));

    @Test
    public void testRepeatedCommitRollbackAfterCommit() throws Exception {
        CountDownLatch txFinishStartedLatch = new CountDownLatch(1);
        CountDownLatch secondFinishLatch = new CountDownLatch(1);

        TestTxManager txManager = new TestTxManager(txFinishStartedLatch, secondFinishLatch);

        ReadWriteTransactionImpl tx = new ReadWriteTransactionImpl(txManager, UUID.randomUUID());

        TablePartitionId partId = testTablePartitionId();

        tx.enlist(partId, new IgniteBiTuple<>(clusterNode, 1L));

        tx.assignCommitPartition(partId);

        tx.enlistResultFuture(completedFuture(null));

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstCommitFut = fut.thenComposeAsync((ignored) -> tx.commitAsync());

        fut.complete(null);

        txFinishStartedLatch.await();

        CompletableFuture<Void> secondCommitFut = tx.commitAsync();

        CompletableFuture<Void> rollbackFut = tx.rollbackAsync();

        assertNotSame(firstCommitFut, secondCommitFut);
        assertSame(secondCommitFut, rollbackFut);
        assertSame(secondCommitFut, tx.commitAsync());
        assertSame(rollbackFut, tx.rollbackAsync());

        assertFalse(firstCommitFut.isDone());
        assertFalse(secondCommitFut.isDone());
        assertFalse(rollbackFut.isDone());

        secondFinishLatch.countDown();

        firstCommitFut.get(3, TimeUnit.SECONDS);
        assertTrue(firstCommitFut.isDone());
        assertTrue(secondCommitFut.isDone());
        assertTrue(rollbackFut.isDone());
    }

    private static TablePartitionId testTablePartitionId() {
        return new TablePartitionId(1, 1);
    }

    @Test
    public void testRepeatedCommitRollbackAfterRollback() throws Exception {
        CountDownLatch txFinishStartedLatch = new CountDownLatch(1);
        CountDownLatch secondFinishLatch = new CountDownLatch(1);

        TestTxManager txManager = new TestTxManager(txFinishStartedLatch, secondFinishLatch);

        ReadWriteTransactionImpl tx = new ReadWriteTransactionImpl(txManager, UUID.randomUUID());

        TablePartitionId partId = testTablePartitionId();

        tx.enlist(partId, new IgniteBiTuple<>(clusterNode, 1L));

        tx.assignCommitPartition(partId);

        tx.enlistResultFuture(completedFuture(null));

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstRollbackFut = fut.thenComposeAsync((ignored) -> tx.rollbackAsync());

        fut.complete(null);

        txFinishStartedLatch.await();

        CompletableFuture<Void> commitFut = tx.commitAsync();

        CompletableFuture<Void> secondRollbackFut = tx.rollbackAsync();

        assertNotSame(firstRollbackFut, secondRollbackFut);
        assertSame(secondRollbackFut, commitFut);
        assertSame(commitFut, tx.commitAsync());
        assertSame(secondRollbackFut, tx.rollbackAsync());

        assertFalse(firstRollbackFut.isDone());
        assertFalse(secondRollbackFut.isDone());
        assertFalse(commitFut.isDone());

        secondFinishLatch.countDown();

        firstRollbackFut.get(3, TimeUnit.SECONDS);
        assertTrue(firstRollbackFut.isDone());
        assertTrue(secondRollbackFut.isDone());
        assertTrue(commitFut.isDone());
    }

    @Test
    public void testRepeatedCommitRollbackAfterCommitWithException() throws Exception {
        TestTxManager txManager = mock(TestTxManager.class);

        when(txManager.finish(any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(failedFuture(new Exception("Expected exception.")));

        ReadWriteTransactionImpl tx = new ReadWriteTransactionImpl(txManager, UUID.randomUUID());

        TablePartitionId partId = testTablePartitionId();

        tx.enlist(partId, new IgniteBiTuple<>(null, null));

        tx.assignCommitPartition(partId);

        tx.enlistResultFuture(completedFuture(null));

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstCommitFut = fut.thenComposeAsync((ignored) -> tx.commitAsync());

        fut.complete(null);

        try {
            firstCommitFut.get(3, TimeUnit.SECONDS);

            fail();
        } catch (Exception ignored) {
            // No op.
        }

        tx.commitAsync().get(3, TimeUnit.SECONDS);
        tx.rollbackAsync().get(3, TimeUnit.SECONDS);
    }

    @Test
    public void testRepeatedCommitRollbackAfterRollbackWithException() throws Exception {
        TestTxManager txManager = mock(TestTxManager.class);

        when(txManager.finish(any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(failedFuture(new Exception("Expected exception.")));

        ReadWriteTransactionImpl tx = new ReadWriteTransactionImpl(txManager, UUID.randomUUID());

        TablePartitionId partId = testTablePartitionId();

        tx.enlist(partId, new IgniteBiTuple<>(null, null));

        tx.assignCommitPartition(partId);

        tx.enlistResultFuture(completedFuture(null));

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> rollbackFut = fut.thenComposeAsync((ignored) -> tx.rollbackAsync());

        fut.complete(null);

        try {
            rollbackFut.get(3, TimeUnit.SECONDS);

            fail();
        } catch (Exception ignored) {
            // No op.
        }

        tx.commitAsync().get(3, TimeUnit.SECONDS);
        tx.rollbackAsync().get(3, TimeUnit.SECONDS);
    }

    private static class TestTxManager implements TxManager {
        private final CountDownLatch txFinishStartedLatch;

        private final CountDownLatch secondFinishLatch;

        TestTxManager(CountDownLatch txFinishStartedLatch, CountDownLatch secondFinishLatch) {
            this.txFinishStartedLatch = txFinishStartedLatch;
            this.secondFinishLatch = secondFinishLatch;
        }

        @Override
        public InternalTransaction begin() {
            return null;
        }

        @Override
        public InternalTransaction begin(boolean readOnly, HybridTimestamp observableTimestamp) {
            return null;
        }

        @Override
        public @Nullable TxState state(UUID txId) {
            return null;
        }

        @Override
        public void changeState(UUID txId, @Nullable TxState before, TxState after) {
            // No-op.
        }

        @Override
        public LockManager lockManager() {
            return null;
        }

        @Override
        public CompletableFuture<Void> finish(TablePartitionId commitPartition, ClusterNode recipientNode, Long term, boolean commit,
                Map<ClusterNode, List<IgniteBiTuple<TablePartitionId, Long>>> groups, UUID txId) {
            txFinishStartedLatch.countDown();

            try {
                secondFinishLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> cleanup(ClusterNode recipientNode,
                List<IgniteBiTuple<TablePartitionId, Long>> tablePartitionIds, UUID txId, boolean commit,
                @Nullable HybridTimestamp commitTimestamp) {
            return null;
        }

        @Override
        public int finished() {
            return 0;
        }

        @Override
        public int pending() {
            return 0;
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public CompletableFuture<Void> updateLowWatermark(HybridTimestamp newLowWatermark) {
            return completedFuture(null);
        }
    }
}
