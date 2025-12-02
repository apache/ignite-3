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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** Concurrent test for {@link TransactionExpirationRegistry}. */
class TransactionExpirationRegistryConcurrentTest extends BaseIgniteAbstractTest {

    private final TransactionExpirationRegistry registry = new TransactionExpirationRegistry();

    @Test
    void registerExpireUnregister() {
        // Given.
        var workers = List.of(
                // Time: 0...100_000
                new Worker(registry, 1, 0, 100_000),
                // Time: 100_000...200_000
                new Worker(registry, 2, 100_000, 100_000),
                // Time: 200_000...300_000
                new Worker(registry, 3, 200_000, 100_000),
                // Time: 300_000...400_000
                new Worker(registry, 4, 300_000, 100_000),
                // Time: 0...400_000
                new Worker(registry, 5, 0, 400_000)
        );

        // When do concurrent work.
        workers.forEach(Worker::run);
        // And all workers finish.
        workers.forEach(w -> w.waitFinished(10_000));

        // Then expired transactions are rolled back.
        workers.forEach(Worker::assertAllTransactionsRolledBack);
    }

    /** Performs the workload in run method. */
    static class Worker {
        /** The registry under test. */
        private final TransactionExpirationRegistry transactionExpirationRegistry;
        /** Transactions pool. */
        private final ArrayList<FakeInternalTransaction> txPool;

        /** How many transactions are in the worker pool. */
        private final long count;
        /** Timestamp offset starting from witch transactions from pool are registered in registry. */
        private final long offset;
        /** Unique worker id. */
        private final int workerId;

        /** Latch that is == 0 when the workload is done. */
        private final CountDownLatch latch;

        Worker(TransactionExpirationRegistry transactionExpirationRegistry, int workerId, int offset, int count) {
            this.transactionExpirationRegistry = transactionExpirationRegistry;
            this.count = count;
            this.offset = offset;
            this.workerId = workerId;
            this.txPool = new ArrayList<>(count);
            this.latch = new CountDownLatch(1);

            generateTxns();
        }

        /**
         * Run the main workload.
         *
         *<p>Logic:
         * <ul>
         *    <li>register all transactions from pool with expirationTime == i + offset, where i is the index of tx in pool</li>
         *    <li>unregister all tx with even i</li>
         *    <li>expire all tx with odd i</li>
         *    <li>count down latch</li>
         * </ul>
         *
         *<p>Invariant: each tx with odd i must be eventually rolled back.
         */
        void run() {
            new Thread(() -> {
                for (int i = 0; i < count; i++) {
                    transactionExpirationRegistry.register(txPool.get(i), offset + i);
                }

                for (int i = 0; i < count; i++) {
                    if (i % 2 == 0) {
                        transactionExpirationRegistry.unregister(txPool.get(i), offset + i);
                        continue;
                    }

                    transactionExpirationRegistry.expireUpTo(offset + i);
                }

                latch.countDown();
            }).start();
        }

        private void generateTxns() {
            for (int i = 0; i < count; i++) {
                txPool.add(new FakeInternalTransaction(workerId * 1_000_000 + i));
            }
        }

        void waitFinished(int timeoutMs) {
            try {
                if (this.latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            throw new RuntimeException("Thread did not finish in time.");
        }

        /** Asserts that all transactions with odd i are eventually rolled back. */
        void assertAllTransactionsRolledBack() {
            for (int i = 0; i < count; i++) {
                if (i % 2 == 0) {
                    continue;
                }

                int spinCnt = 100;
                while (spinCnt-- != 0 && txPool.get(i).getRollbackCount() != 1) {
                    try {
                        // Wait for async rollback.
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                if (spinCnt <= 0) {
                    throw new RuntimeException("Transaction was not rolled back.");
                }
            }
        }
    }

    /** Fake transaction for testing purposes. Only has an id (for  equals/hashCode) and counts rollbackAsync invocations. */
    private static class FakeInternalTransaction implements InternalTransaction {
        private final int id;
        private final AtomicInteger rollbackCount = new AtomicInteger();

        FakeInternalTransaction(int id) {
            this.id = id;
        }

        @Override
        public UUID id() {
            return new UUID(id, 0);
        }

        @Override
        public PendingTxPartitionEnlistment enlistedPartition(ZonePartitionId replicationGroupId) {
            return null;
        }

        @Override
        public TxState state() {
            return null;
        }

        @Override
        public boolean assignCommitPartition(ZonePartitionId commitPartitionId) {
            return false;
        }

        @Override
        public TablePartitionId commitPartition() {
            return null;
        }

        @Override
        public void enlist(ReplicationGroupId replicationGroupId, int tableId, String primaryNodeConsistentId, long consistencyToken) {

        }

        @Override
        public @Nullable HybridTimestamp readTimestamp() {
            return null;
        }

        @Override
        public HybridTimestamp schemaTimestamp() {
            return null;
        }

        @Override
        public UUID coordinatorId() {
            return null;
        }

        @Override
        public boolean implicit() {
            return false;
        }

        @Override
        public CompletableFuture<Void> finish(
                boolean commit, @Nullable HybridTimestamp executionTimestamp, boolean full, boolean timeoutExceeded
        ) {
            return nullCompletedFuture();
        }

        @Override
        public boolean isFinishingOrFinished() {
            return false;
        }

        @Override
        public long getTimeout() {
            return 0;
        }

        @Override
        public CompletableFuture<Void> kill() {
            return null;
        }

        @Override
        public CompletableFuture<Void> rollbackTimeoutExceededAsync() {
            // Count the number of calls.
            rollbackCount.incrementAndGet();

            return nullCompletedFuture();
        }

        @Override
        public boolean isRolledBackWithTimeoutExceeded() {
            return false;
        }

        @Override
        public void commit() throws TransactionException {

        }

        @Override
        public CompletableFuture<Void> commitAsync() {
            return null;
        }

        @Override
        public void rollback() throws TransactionException {
            // Do nothing.
        }

        @Override
        public CompletableFuture<Void> rollbackAsync() {
            this.rollbackCount.incrementAndGet();
            return nullCompletedFuture();
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FakeInternalTransaction that = (FakeInternalTransaction) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        int getRollbackCount() {
            return rollbackCount.get();
        }
    }

}
