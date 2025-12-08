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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * The special lightweight implementation for read-only implicit transaction.
 */
public class ReadOnlyImplicitTransactionImpl implements InternalTransaction {
    private static final UUID FAKE_ID = new UUID(0, 0);

    private final HybridTimestampTracker observableTsTracker;

    private final HybridTimestamp createTs;

    /**
     * The constructor.
     *
     * @param observableTsTracker Observable timestamp tracker.
     * @param createTs Create timestamp.
     */
    ReadOnlyImplicitTransactionImpl(HybridTimestampTracker observableTsTracker, HybridTimestamp createTs) {
        this.observableTsTracker = observableTsTracker;
        this.createTs = createTs;
    }

    @Override
    public UUID id() {
        return FAKE_ID;
    }

    @Override
    public TxState state() {
        return null;
    }

    @Override
    public UUID coordinatorId() {
        return null;
    }

    @Override
    public boolean implicit() {
        return true;
    }

    @Override
    public boolean remote() {
        return false;
    }

    @Override
    public long getTimeout() {
        return 0;
    }

    @Override
    public boolean isRolledBackWithTimeoutExceeded() {
        return false;
    }

    @Override
    public void processDelayedAck(Object val, @Nullable Throwable err) {
        // No-op.
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public HybridTimestamp readTimestamp() {
        return null;
    }

    @Override
    public HybridTimestamp schemaTimestamp() {
        return createTs;
    }

    @Override
    public void enlist(
            ZonePartitionId replicationGroupId,
            int tableId,
            String primaryNodeConsistentId,
            long consistencyToken
    ) {
        // No-op.
    }

    @Override
    public PendingTxPartitionEnlistment enlistedPartition(ZonePartitionId replicationGroupId) {
        return null;
    }

    @Override
    public boolean assignCommitPartition(ZonePartitionId commitPartitionId) {
        return true;
    }

    @Override
    public ZonePartitionId commitPartition() {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> rollbackTimeoutExceededAsync() {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> finish(
            boolean commitIntent,
            HybridTimestamp executionTimestamp,
            boolean full,
            boolean timeoutExceeded
    ) {
        observableTsTracker.update(executionTimestamp);

        return nullCompletedFuture();
    }

    @Override
    public boolean isFinishingOrFinished() {
        return false;
    }

    @Override
    public CompletableFuture<Void> kill() {
        return nullCompletedFuture();
    }

    @Override
    public void commit() throws TransactionException {
        // No-op.
    }

    @Override
    public void rollback() throws TransactionException {
        // No-op.
    }
}
