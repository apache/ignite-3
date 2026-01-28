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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;
import static org.apache.ignite.internal.tx.TxStateMeta.recordExceptionInfo;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ROLLBACK_ERR;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * The read-write implementation of an internal transaction.
 */
public class ReadWriteTransactionImpl extends IgniteAbstractTransactionImpl {
    /** Commit partition updater. */
    private static final AtomicReferenceFieldUpdater<ReadWriteTransactionImpl, ZonePartitionId> COMMIT_PART_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ReadWriteTransactionImpl.class, ZonePartitionId.class, "commitPart");

    /** Enlisted partitions: partition id -> partition info. */
    private final Map<ZonePartitionId, PendingTxPartitionEnlistment> enlisted = new ConcurrentHashMap<>();

    /** A partition which stores the transaction state. {@code null} before first enlistment. */
    private volatile @Nullable ZonePartitionId commitPart;

    /** The lock protects the transaction topology from concurrent modification during finishing. */
    private final ReentrantReadWriteLock enlistPartitionLock = new ReentrantReadWriteLock();

    /** The future is initialized when this transaction starts committing or rolling back and is finished together with the transaction. */
    private volatile CompletableFuture<Void> finishFuture;

    /**
     * {@code True} if a transaction is externally killed.
     */
    private boolean killed;

    /**
     * {@code True} if a remote(directly mapped) part of this transaction has no writes.
     */
    private boolean noRemoteWrites = true;

    /**
     * Constructs an explicit read-write transaction.
     *
     * @param txManager The tx manager.
     * @param observableTsTracker Observable timestamp tracker.
     * @param id The id.
     * @param txCoordinatorId Transaction coordinator inconsistent ID.
     * @param implicit True for an implicit transaction, false for an ordinary one.
     * @param timeout The timeout.
     */
    public ReadWriteTransactionImpl(
            TxManager txManager,
            HybridTimestampTracker observableTsTracker,
            UUID id,
            UUID txCoordinatorId,
            boolean implicit,
            long timeout
    ) {
        super(txManager, observableTsTracker, id, txCoordinatorId, implicit, timeout);
    }

    /** {@inheritDoc} */
    @Override
    public boolean assignCommitPartition(ZonePartitionId commitPartitionId) {
        return COMMIT_PART_UPDATER.compareAndSet(this, null, commitPartitionId);
    }

    /** {@inheritDoc} */
    @Override
    public ZonePartitionId commitPartition() {
        return commitPart;
    }

    /** {@inheritDoc} */
    @Override
    public PendingTxPartitionEnlistment enlistedPartition(ZonePartitionId partGroupId) {
        return enlisted.get(partGroupId);
    }

    /** {@inheritDoc} */
    @Override
    public void enlist(
            ZonePartitionId replicationGroupId,
            int tableId,
            String primaryNodeConsistentId,
            long consistencyToken
    ) {
        // No need to wait for lock if commit is in progress.
        if (!enlistPartitionLock.readLock().tryLock()) {
            failEnlist();
            assert false; // Not reachable.
        }

        try {
            checkEnlistPossibility();

            PendingTxPartitionEnlistment enlistment = enlisted.computeIfAbsent(
                    replicationGroupId,
                    k -> new PendingTxPartitionEnlistment(primaryNodeConsistentId, consistencyToken)
            );

            enlistment.addTableId(tableId);
        } finally {
            enlistPartitionLock.readLock().unlock();
        }
    }

    /**
     * Fails the operation.
     */
    private void failEnlist() {
        throw new TransactionException(
                TX_ALREADY_FINISHED_ERR,
                format("Transaction is already finished [{}, txState={}].",
                        formatTxInfo(id(), txManager, false), state()));
    }

    /**
     * Checks that this transaction was not finished and will be able to enlist another partition.
     */
    private void checkEnlistPossibility() {
        if (isFinishingOrFinished()) {
            // This means that the transaction is either in final or FINISHING state.
            failEnlist();
        }
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return TransactionsExceptionMapperUtil.convertToPublicFuture(
                finish(true, null, false, false),
                TX_COMMIT_ERR
        );
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return TransactionsExceptionMapperUtil.convertToPublicFuture(
                finish(false, null, false, false),
                TX_ROLLBACK_ERR
        );
    }

    @Override
    public CompletableFuture<Void> rollbackTimeoutExceededAsync() {
        return TransactionsExceptionMapperUtil.convertToPublicFuture(
                finish(false, null, false, true),
                TX_ROLLBACK_ERR
        );
    }

    @Override
    public CompletableFuture<Void> finish(
            boolean commit,
            @Nullable HybridTimestamp executionTimestamp,
            boolean full,
            boolean timeoutExceeded
    ) {
        assert !(commit && timeoutExceeded) : "Transaction cannot commit with timeout exceeded.";

        if (finishFuture != null) {
            return finishFuture;
        }

        return finishInternal(commit, executionTimestamp, full, true, timeoutExceeded);
    }

    /**
     * Finishes the read-write transaction.
     *
     * @param commit Commit flag.
     * @param executionTimestamp The timestamp is the time when the transaction is applied to the remote node.
     * @param full Full state transaction marker.
     * @param isComplete The flag is true if the transaction is completed through the public API, false for {@link this#kill()} invocation.
     * @param timeoutExceeded {@code True} if rollback reason is the timeout.
     * @return The future.
     */
    private CompletableFuture<Void> finishInternal(
            boolean commit,
            @Nullable HybridTimestamp executionTimestamp,
            boolean full,
            boolean isComplete,
            boolean timeoutExceeded
    ) {
        enlistPartitionLock.writeLock().lock();

        try {
            if (finishFuture == null) {
                if (killed) {
                    if (isComplete) {
                        finishFuture = nullCompletedFuture();

                        return failedFuture(new TransactionException(
                                TX_ALREADY_FINISHED_ERR,
                                format("Transaction is killed [{}, txState={}].",
                                        formatTxInfo(id(), txManager, false), state())
                        ));
                    } else {
                        return nullCompletedFuture();
                    }
                }

                if (full) {
                    txManager.finishFull(observableTsTracker, id(), executionTimestamp, commit, timeoutExceeded);

                    if (isComplete) {
                        finishFuture = nullCompletedFuture();
                        this.timeoutExceeded = timeoutExceeded;
                    } else {
                        killed = true;
                    }
                } else {
                    CompletableFuture<Void> finishFutureInternal = txManager.finish(
                            observableTsTracker,
                            commitPart,
                            commit,
                            timeoutExceeded,
                            false,
                            noRemoteWrites,
                            enlisted,
                            id()
                    );

                    if (isComplete) {
                        finishFuture = finishFutureInternal.handle((unused, throwable) -> null);
                        this.timeoutExceeded = timeoutExceeded;
                    } else {
                        killed = true;
                    }

                    // Return the real future first time.
                    return finishFutureInternal;
                }
            }

            return finishFuture;
        } finally {
            enlistPartitionLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isFinishingOrFinished() {
        return finishFuture != null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isReadOnly() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public HybridTimestamp readTimestamp() {
        return null;
    }

    @Override
    public HybridTimestamp schemaTimestamp() {
        return TransactionIds.beginTimestamp(id());
    }

    @Override
    public CompletableFuture<Void> kill() {
        return finishInternal(false, null, false, false, false);
    }

    @Override
    public boolean isRolledBackWithTimeoutExceeded() {
        // `finishInternal` is called under the write lock, so reading `timeoutExceeded` under the read lock
        // in order to avoid data race.
        enlistPartitionLock.readLock().lock();
        try {
            return timeoutExceeded;
        } finally {
            enlistPartitionLock.readLock().unlock();
        }
    }

    /**
     * Fail the transaction with exception so finishing it is not possible.
     *
     * @param e Fail reason.
     */
    public void fail(TransactionException e) {
        txManager.updateTxMeta(
                id(), old -> recordExceptionInfo(old, e)
        );

        // Thread safety is not needed.
        finishFuture = failedFuture(e);
    }

    /**
     * Set no remote writes flag.
     *
     * @param noRemoteWrites The value.
     */
    public void noRemoteWrites(boolean noRemoteWrites) {
        this.noRemoteWrites = noRemoteWrites;
    }
}
