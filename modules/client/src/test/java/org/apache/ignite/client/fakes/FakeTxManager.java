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

package org.apache.ignite.client.fakes;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Fake transaction manager.
 */
public class FakeTxManager implements TxManager {
    private final HybridClock clock;

    private HybridTimestamp lastObservableTimestamp = null;

    public FakeTxManager(HybridClock clock) {
        this.clock = clock;
    }

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void stop() throws Exception {
        // No-op.
    }

    @Override
    public InternalTransaction begin() {
        return begin(false, null);
    }

    @Override
    public InternalTransaction begin(boolean readOnly, @Nullable HybridTimestamp observableTimestamp) {
        lastObservableTimestamp = observableTimestamp;

        return new InternalTransaction() {
            private final UUID id = UUID.randomUUID();

            private final HybridTimestamp timestamp = clock.now();

            @Override
            public @NotNull UUID id() {
                return id;
            }

            @Override
            public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndTerm(TablePartitionId tablePartitionId) {
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
            public IgniteBiTuple<ClusterNode, Long> enlist(
                    TablePartitionId tablePartitionId,
                    IgniteBiTuple<ClusterNode, Long> nodeAndTerm) {
                return null;
            }

            @Override
            public void enlistResultFuture(CompletableFuture<?> resultFuture) {
            }

            @Override
            public void commit() throws TransactionException {

            }

            @Override
            public CompletableFuture<Void> commitAsync() {
                return CompletableFuture.completedFuture(null);
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
            public HybridTimestamp readTimestamp() {
                return observableTimestamp;
            }

            @Override
            public HybridTimestamp startTimestamp() {
                return timestamp;
            }
        };
    }

    @Override
    public @Nullable TxState state(UUID txId) {
        return null;
    }

    @Override
    public void changeState(UUID txId, @Nullable TxState before, TxState after) {

    }

    @Override
    public LockManager lockManager() {
        return null;
    }

    @Override
    public CompletableFuture<Void> finish(TablePartitionId commitPartition, ClusterNode recipientNode, Long term, boolean commit,
            Map<ClusterNode, List<IgniteBiTuple<TablePartitionId, Long>>> groups, UUID txId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> cleanup(ClusterNode recipientNode, List<IgniteBiTuple<TablePartitionId, Long>> tablePartitionIds,
            UUID txId, boolean commit, @Nullable HybridTimestamp commitTimestamp) {
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
    public CompletableFuture<Void> updateLowWatermark(HybridTimestamp newLowWatermark) {
        return null;
    }

    public @Nullable HybridTimestamp lastObservableTimestamp() {
        return lastObservableTimestamp;
    }
}
