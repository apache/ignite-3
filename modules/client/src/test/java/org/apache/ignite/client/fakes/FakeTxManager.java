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
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.InternalTxOptions;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.PartitionEnlistment;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.EnlistedPartitionGroup;
import org.apache.ignite.internal.tx.metrics.ResourceVacuumMetrics;
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
            public PendingTxPartitionEnlistment enlistedPartition(ReplicationGroupId replicationGroupId) {
                return null;
            }

            @Override
            public TxState state() {
                return null;
            }

            @Override
            public boolean assignCommitPartition(ReplicationGroupId replicationGroupId) {
                return false;
            }

            @Override
            public TablePartitionId commitPartition() {
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
                return id;
            }

            @Override
            public HybridTimestamp readTimestamp() {
                return tracker.get();
            }

            @Override
            public HybridTimestamp schemaTimestamp() {
                return timestamp;
            }

            @Override
            public boolean implicit() {
                return implicit;
            }

            @Override
            public CompletableFuture<Void> finish(
                    boolean commit, HybridTimestamp executionTimestamp, boolean full, boolean timeoutExceeded
            ) {
                return nullCompletedFuture();
            }

            @Override
            public boolean isFinishingOrFinished() {
                return false;
            }

            @Override
            public long getTimeout() {
                return 10_000;
            }

            @Override
            public CompletableFuture<Void> kill() {
                return nullCompletedFuture();
            }

            @Override
            public CompletableFuture<Void> rollbackTimeoutExceededAsync() {
                return nullCompletedFuture();
            }

            @Override
            public boolean isRolledBackWithTimeoutExceeded() {
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
            ReplicationGroupId commitPartition,
            boolean commitIntent,
            boolean timeoutExceeded,
            boolean recovery,
            Map<ReplicationGroupId, PendingTxPartitionEnlistment> enlistedGroups,
            UUID txId
    ) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> cleanup(
            ReplicationGroupId commitPartitionId,
            Map<ReplicationGroupId, PartitionEnlistment> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> cleanup(
            ReplicationGroupId commitPartitionId,
            Collection<EnlistedPartitionGroup> enlistedPartitions,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp,
            UUID txId
    ) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> cleanup(ReplicationGroupId commitPartitionId, String node, UUID txId) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> vacuum(ResourceVacuumMetrics resourceVacuumMetrics) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Boolean> kill(UUID txId) {
        return nullCompletedFuture();
    }

    @Override
    public int lockRetryCount() {
        return 0;
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
    public void finishFull(
            HybridTimestampTracker timestampTracker, UUID txId, HybridTimestamp ts, boolean commit, boolean timeoutExceeded
    ) {
        // No-op.
    }

    @Override
    public InternalTransaction beginRemote(UUID txId, TablePartitionId commitPartId, UUID coord, long token, long timeout,
            Consumer<Throwable> cb) {
        return null;
    }
}
