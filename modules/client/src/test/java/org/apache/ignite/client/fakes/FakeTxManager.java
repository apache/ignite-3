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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.InternalTxOptions;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Fake transaction manager.
 */
public class FakeTxManager implements TxManager {
    private final HybridClock clock;

    public FakeTxManager(HybridClock clock) {
        this.clock = clock;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        // No-op.
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        // No-op.
        return nullCompletedFuture();
    }

    @Override
    public InternalTransaction beginImplicit(HybridTimestampTracker timestampTracker, boolean readOnly) {
        return begin(timestampTracker, true, readOnly, InternalTxOptions.defaults());
    }

    @Override
    public InternalTransaction beginExplicit(HybridTimestampTracker timestampTracker, boolean readOnly, InternalTxOptions txOptions) {
        return begin(timestampTracker, false, readOnly, txOptions);
    }

    private InternalTransaction begin(HybridTimestampTracker tracker, boolean implicit, boolean readOnly, InternalTxOptions options) {
        return new InternalTransaction() {
            private final UUID id = UUID.randomUUID();

            private final HybridTimestamp timestamp = clock.now();

            @Override
            public UUID id() {
                return id;
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
            public IgniteBiTuple<ClusterNode, Long> enlist(
                    TablePartitionId tablePartitionId,
                    IgniteBiTuple<ClusterNode, Long> nodeAndConsistencyToken) {
                return null;
            }

            @Override
            public void commit() throws TransactionException {
                // No-op.
            }

            @Override
            public CompletableFuture<Void> commitAsync() {
                return nullCompletedFuture();
            }

            @Override
            public void rollback() throws TransactionException {
                // No-op.
            }

            @Override
            public CompletableFuture<Void> rollbackAsync() {
                return nullCompletedFuture();
            }

            @Override
            public boolean isReadOnly() {
                return readOnly;
            }

            @Override
            public UUID coordinatorId() {
                return null;
            }

            @Override
            public HybridTimestamp readTimestamp() {
                return tracker.get();
            }

            @Override
            public HybridTimestamp startTimestamp() {
                return timestamp;
            }

            @Override
            public boolean implicit() {
                return false;
            }

            @Override
            public CompletableFuture<Void> finish(boolean commit, HybridTimestamp executionTimestamp, boolean full) {
                return nullCompletedFuture();
            }

            @Override
            public boolean isFinishingOrFinished() {
                return false;
            }
        };
    }

    @Override
    public @Nullable TxStateMeta stateMeta(UUID txId) {
        return null;
    }

    @Override
    public <T extends TxStateMeta> T updateTxMeta(UUID txId, Function<TxStateMeta, TxStateMeta> updater) {
        return null;
    }

    @Override
    public LockManager lockManager() {
        return null;
    }

    @Override
    public CompletableFuture<Void> executeWriteIntentSwitchAsync(Runnable runnable) {
        return CompletableFuture.runAsync(runnable);
    }

    @Override
    public CompletableFuture<Void> finish(
            HybridTimestampTracker timestampTracker,
            TablePartitionId commitPartition,
            boolean commit,
            Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups,
            UUID txId
    ) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> cleanup(
            TablePartitionId commitPartitionId,
            Map<TablePartitionId, String> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> cleanup(
            TablePartitionId commitPartitionId,
            Collection<TablePartitionId> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> cleanup(TablePartitionId commitPartitionId, String node, UUID txId) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> vacuum() {
        return nullCompletedFuture();
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
    public void finishFull(HybridTimestampTracker timestampTracker, UUID txId, HybridTimestamp ts, boolean commit) {
        // No-op.
    }
}
