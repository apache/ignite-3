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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

/** Benchmark for TransactionExpirationRegistry. */
@State(Scope.Benchmark)
@OutputTimeUnit(SECONDS)
@Timeout(time = 200, timeUnit = MILLISECONDS)
@Warmup(iterations = 2, time = 5, timeUnit = MILLISECONDS)
@Measurement(time = 5, timeUnit = MILLISECONDS, iterations = 5)
public class TransactionExpirationRegistryBenchmark {
    private static final int ITERATIONS_COUNT = 100_000;

    private static final List<InternalTransaction> transactions = new ArrayList<>(ITERATIONS_COUNT);

    /** Register fake transactions. */
    @Setup
    public static void setup() {
        for (int i = 0; i < ITERATIONS_COUNT; i++) {
            transactions.add(new FakeInternalTransaction(i));
        }
    }

    /** Register transactions in the cycle. */
    @Benchmark
    public static void register() {
        TransactionExpirationRegistry registry = new TransactionExpirationRegistry();
        for (int i = 0; i < ITERATIONS_COUNT; i++) {
            registry.register(transactions.get(i), i);
        }
    }

    /** Register transactions in batches of 10, using the same expiration time for each batch. */
    @Benchmark
    public static void register10() {
        TransactionExpirationRegistry registry = new TransactionExpirationRegistry();
        int iterCnt = ITERATIONS_COUNT / 10;
        for (int i = 0; i < iterCnt; i++) {
            for (int j = 0; j < 10; j++) {
                registry.register(transactions.get(i * 10 + j), i);
            }
        }
    }

    /** Register and unregister transactions in the cycle. */
    @Benchmark
    public static void registerUnregister() {
        TransactionExpirationRegistry registry = new TransactionExpirationRegistry();
        for (int i = 0; i < ITERATIONS_COUNT; i++) {
            registry.register(transactions.get(i), i);
        }

        for (int i = 0; i < ITERATIONS_COUNT; i++) {
            registry.unregister(transactions.get(i));
        }
    }

    /** Register and expire transactions in the cycle. */
    @Benchmark
    public static void registerExpire() {
        TransactionExpirationRegistry registry = new TransactionExpirationRegistry();
        for (int i = 0; i < ITERATIONS_COUNT; i++) {
            registry.register(transactions.get(i), i);
        }

        for (int i = ITERATIONS_COUNT; i > 0; i--) {
            registry.expireUpTo(i);
        }
    }

    private static class FakeInternalTransaction implements InternalTransaction {
        private final int id;

        public FakeInternalTransaction(int id) {
            this.id = id;
        }

        @Override
        public UUID id() {
            return UUID.fromString(id + "");
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
        public ZonePartitionId commitPartition() {
            return null;
        }

        @Override
        public void enlist(
                ReplicationGroupId replicationGroupId,
                int tableId,
                String primaryNodeConsistentId,
                long consistencyToken
        ) {
            // No-op.
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
            return null;
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

        }

        @Override
        public CompletableFuture<Void> rollbackAsync() {
            return nullCompletedFuture();
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FakeInternalTransaction that = (FakeInternalTransaction) o;
            return id == that.id;
        }
    }
}
