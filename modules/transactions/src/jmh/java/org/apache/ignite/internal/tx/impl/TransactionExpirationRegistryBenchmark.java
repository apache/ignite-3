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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class TransactionExpirationRegistryBenchmark {
    private static final int ITERATIONS_COUNT = 100_000;

    private TransactionExpirationRegistry registry;

    @Benchmark
    public void register(Blackhole bh) {
        registry = new TransactionExpirationRegistry();
        for (int i = 0; i < ITERATIONS_COUNT; i++) {
            registry.register(new FakeInternalTransaction(i), i);
        }
    }

    @Benchmark
    public void registerUnregister(Blackhole bh) {
        registry = new TransactionExpirationRegistry();
        for (int i = 0; i < ITERATIONS_COUNT; i++) {
            registry.register(new FakeInternalTransaction(i), i);
        }

        for (int i = 0; i < ITERATIONS_COUNT; i++) {
            registry.unregister(new FakeInternalTransaction(i));
        }
    }

    @Benchmark
    public void registerExpire(Blackhole bh) {
        registry = new TransactionExpirationRegistry();
        for (int i = 0; i < ITERATIONS_COUNT; i++) {
            registry.register(new FakeInternalTransaction(i), i);
        }

        for (int i = ITERATIONS_COUNT; i > 0; i--) {
            registry.expireUpTo(i);
        }
    }

    private static class FakeInternalTransaction implements InternalTransaction {
        private final int i;

        public FakeInternalTransaction(int i) {
            this.i = i;
        }

        @Override
        public UUID id() {
            return UUID.fromString(i + "");
        }

        @Override
        public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndConsistencyToken(TablePartitionId tablePartitionId) {
            return null;
        }

        @Override
        public TxState state() {
            return null;
        }

        @Override
        public boolean assignCommitPartition(TablePartitionId tablePartitionId) {
            return false;
        }

        @Override
        public TablePartitionId commitPartition() {
            return null;
        }

        @Override
        public IgniteBiTuple<ClusterNode, Long> enlist(TablePartitionId tablePartitionId,
                IgniteBiTuple<ClusterNode, Long> nodeAndConsistencyToken) {
            return null;
        }

        @Override
        public @Nullable HybridTimestamp readTimestamp() {
            return null;
        }

        @Override
        public HybridTimestamp startTimestamp() {
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
        public CompletableFuture<Void> finish(boolean commit, @Nullable HybridTimestamp executionTimestamp, boolean full) {
            return null;
        }

        @Override
        public boolean isFinishingOrFinished() {
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
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public int hashCode() {
            return i;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FakeInternalTransaction that = (FakeInternalTransaction) o;
            return i == that.i;
        }
    }
}
