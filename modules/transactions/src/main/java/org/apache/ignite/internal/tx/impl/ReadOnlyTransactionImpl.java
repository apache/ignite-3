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
import static org.apache.ignite.internal.util.ExceptionUtils.isFinishedDueToTimeout;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ROLLBACK_ERR;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.jetbrains.annotations.Nullable;

/**
 * The read-only implementation of an internal transaction.
 */
public class ReadOnlyTransactionImpl extends IgniteAbstractTransactionImpl {
    /** The read timestamp. */
    private final HybridTimestamp readTimestamp;

    /** Prevents double finish of the transaction. */
    private final AtomicBoolean finishGuard = new AtomicBoolean();

    /** Transaction future. */
    private final CompletableFuture<Void> txFuture;

    /**
     * The constructor.
     *
     * @param txManager The tx manager.
     * @param observableTsTracker Observable timestamp tracker.
     * @param id The id.
     * @param txCoordinatorId Transaction coordinator inconsistent ID.
     * @param timeout The timeout.
     * @param readTimestamp The read timestamp.
     */
    ReadOnlyTransactionImpl(
            TxManagerImpl txManager,
            HybridTimestampTracker observableTsTracker,
            UUID id,
            UUID txCoordinatorId,
            long timeout,
            HybridTimestamp readTimestamp,
            CompletableFuture<Void> txFuture
    ) {
        super(txManager, observableTsTracker, id, txCoordinatorId, false, timeout);

        this.readTimestamp = readTimestamp;
        this.txFuture = txFuture;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public HybridTimestamp readTimestamp() {
        return readTimestamp;
    }

    @Override
    public HybridTimestamp schemaTimestamp() {
        return readTimestamp;
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
        return TransactionsExceptionMapperUtil.convertToPublicFuture(
                finish(true, readTimestamp, false, null),
                TX_COMMIT_ERR
        );
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return TransactionsExceptionMapperUtil.convertToPublicFuture(
                finish(false, readTimestamp, false, null),
                TX_ROLLBACK_ERR
        );
    }

    @Override
    public CompletableFuture<Void> rollbackWithExceptionAsync(Throwable throwable) {
        return TransactionsExceptionMapperUtil.convertToPublicFuture(
                finish(false, readTimestamp, false, throwable),
                TX_ROLLBACK_ERR
        );
    }

    @Override
    public CompletableFuture<Void> finish(
            boolean commitIntent,
            HybridTimestamp executionTimestamp,
            boolean full,
            @Nullable Throwable finishReason
    ) {
        assert !full : "Read-only transactions cannot be full.";
        assert !(commitIntent && finishReason != null) : "Transaction cannot commit with a finish reason.";

        if (!finishGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        observableTsTracker.update(executionTimestamp);

        txFuture.complete(null);

        ((TxManagerImpl) txManager).onCompleteReadOnlyTransaction(
                commitIntent,
                new TxIdAndTimestamp(readTimestamp, id())
        );

        this.timeoutExceeded = isFinishedDueToTimeout(finishReason);

        return txFuture;
    }

    @Override
    public boolean isFinishingOrFinished() {
        return finishGuard.get();
    }

    @Override
    public CompletableFuture<Void> kill() {
        return finish(false, readTimestamp, false, null);
    }
}
