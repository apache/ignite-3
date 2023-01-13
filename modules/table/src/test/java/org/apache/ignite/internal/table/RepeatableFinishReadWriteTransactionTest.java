package org.apache.ignite.internal.table;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests repeatable commit/rollback operations.
 */
public class RepeatableFinishReadWriteTransactionTest {
    @Test
    public void testRepeatableCommitRollbackAfterCommit() throws Exception {
        CountDownLatch txStartedLatch = new CountDownLatch(1);

        CountDownLatch secondFinishLatch = new CountDownLatch(1);

        TestTxManager txManager = new TestTxManager(txStartedLatch, secondFinishLatch);

        ReadWriteTransactionImpl tx = new ReadWriteTransactionImpl(txManager, UUID.randomUUID());

        TablePartitionId partId = new TablePartitionId(UUID.randomUUID(), 1);

        tx.enlist(partId, new IgniteBiTuple<>(null, null));

        tx.assignCommitPartition(partId);

        CompletableFuture<Object> enlistFut = completedFuture(null);

        tx.enlistResultFuture(enlistFut);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstCommitFut = fut.thenComposeAsync((ignored) -> tx.commitAsync());

        fut.complete(null);


//        CompletableFuture<Object> fut = new CompletableFuture<>();
//
//        CompletableFuture<Void> firstCommitFut = fut.thenComposeAsync((ignored) -> tx.commitAsync());
//
//        fut.complete(null);


        txStartedLatch.await();

        CompletableFuture<Void> secondCommitFut = tx.commitAsync();

        CompletableFuture<Void> rollbackFut = tx.rollbackAsync();

        assertFalse(firstCommitFut == secondCommitFut);
        assertFalse(secondCommitFut == rollbackFut);
        assertTrue(secondCommitFut == tx.commitAsync());
        assertFalse(rollbackFut == tx.rollbackAsync());

        assertFalse(firstCommitFut.isDone());
        assertFalse(secondCommitFut.isDone());
        assertTrue(rollbackFut.isDone());

        secondFinishLatch.countDown();

        firstCommitFut.get(3, TimeUnit.SECONDS);
        assertTrue(secondCommitFut.isDone());
    }
    @Test
    public void testRepeatableCommitRollbackAfterRollback() throws Exception {
        CountDownLatch txStartedLatch = new CountDownLatch(1);

        CountDownLatch secondFinishLatch = new CountDownLatch(1);

        TestTxManager txManager = new TestTxManager(txStartedLatch, secondFinishLatch);

        ReadWriteTransactionImpl tx = new ReadWriteTransactionImpl(txManager, UUID.randomUUID());

        TablePartitionId partId = new TablePartitionId(UUID.randomUUID(), 1);

        tx.enlist(partId, new IgniteBiTuple<>(null, null));

        tx.assignCommitPartition(partId);

        CompletableFuture<Object> enlistFut = completedFuture(null);

        tx.enlistResultFuture(enlistFut);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        CompletableFuture<Void> firstRollbackFut = fut.thenComposeAsync((ignored) -> tx.rollbackAsync());

        fut.complete(null);


        txStartedLatch.await();

        CompletableFuture<Void> secondCommitFut = tx.commitAsync();

        CompletableFuture<Void> rollbackFut = tx.rollbackAsync();

        assertFalse(firstRollbackFut == secondCommitFut);
        assertFalse(secondCommitFut == rollbackFut);

        assertFalse(firstRollbackFut.isDone());
        assertTrue(secondCommitFut.isDone());
        assertTrue(rollbackFut.isDone());

        secondFinishLatch.countDown();

        firstRollbackFut.get(3, TimeUnit.SECONDS);
        assertTrue(secondCommitFut.isDone());
        assertTrue(rollbackFut.isDone());
    }

    private static class TestTxManager implements TxManager {
        private final CountDownLatch txStartedLatch;

        private final CountDownLatch secondFinishLatch;

        public TestTxManager(CountDownLatch txStartedLatch, CountDownLatch secondFinishLatch) {
            this.txStartedLatch = txStartedLatch;
            this.secondFinishLatch = secondFinishLatch;
        }

        @Override
        public InternalTransaction begin() {
            return null;
        }

        @Override
        public InternalTransaction begin(boolean readOnly) {
            return null;
        }

        @Override
        public @Nullable TxState state(UUID txId) {
            return null;
        }

        @Override
        public boolean changeState(UUID txId, @Nullable TxState before, TxState after) {
            return false;
        }

        @Override
        public LockManager lockManager() {
            return null;
        }

        @Override
        public CompletableFuture<Void> finish(ReplicationGroupId commitPartition, ClusterNode recipientNode, Long term, boolean commit,
                Map<ClusterNode, List<IgniteBiTuple<ReplicationGroupId, Long>>> groups, UUID txId) {
            txStartedLatch.countDown();

            try {
                secondFinishLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> cleanup(ClusterNode recipientNode,
                List<IgniteBiTuple<ReplicationGroupId, Long>> replicationGroupIds, UUID txId, boolean commit,
                HybridTimestamp commitTimestamp) {
            txStartedLatch.countDown();

            try {
                secondFinishLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return completedFuture(null);
        }

        @Override
        public int finished() {
            return 0;
        }

        @Override
        public void start() {

        }

        @Override
        public void stop() throws Exception {

        }
    }

}
