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
import static org.apache.ignite.internal.tx.TransactionErrors.finishedTransactionErrorCode;
import static org.apache.ignite.internal.tx.TransactionErrors.finishedTransactionErrorMessage;
import static org.apache.ignite.internal.tx.TransactionLogUtils.formatTxInfo;
import static org.apache.ignite.internal.tx.TxState.FINISHING;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.isFinishedDueToTimeout;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ROLLBACK_ERR;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TransactionKilledException;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxStateMeta;
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
     * A closure which is called then a transaction is externally killed (not by direct user call).
     * Not-null only for a client's transaction.
     */
    private final @Nullable Consumer<InternalTransaction> killClosure;

    /**
     * Constructs an explicit read-write transaction.
     *
     * @param txManager The tx manager.
     * @param observableTsTracker Observable timestamp tracker.
     * @param id The id.
     * @param txCoordinatorId Transaction coordinator inconsistent ID.
     * @param implicit True for an implicit transaction, false for an ordinary one.
     * @param timeout The timeout.
     * @param killClosure Kill closure.
     */
    public ReadWriteTransactionImpl(
            TxManager txManager,
            HybridTimestampTracker observableTsTracker,
            UUID id,
            UUID txCoordinatorId,
            boolean implicit,
            long timeout,
            @Nullable Consumer<InternalTransaction> killClosure
    ) {
        super(txManager, observableTsTracker, id, txCoordinatorId, implicit, timeout);
        this.killClosure = killClosure;
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
            throw enlistFailedException();
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
    private RuntimeException enlistFailedException() {
        TxStateMeta meta = txManager.stateMeta(id());
        Throwable cause = meta == null ? null : meta.lastException();
        boolean isFinishedDueToTimeout = meta != null && meta.isFinishedDueToTimeoutOrFalse();
        boolean isFinishedDueToError = meta != null && !isFinishedDueToTimeout && meta.lastExceptionErrorCode() != null;
        Throwable publicCause = isFinishedDueToError ? cause : null;
        Integer causeErrorCode = meta == null ? null : meta.lastExceptionErrorCode();

        return killed ? new TransactionKilledException(id(), txManager) :
                new TransactionException(
                        finishedTransactionErrorCode(isFinishedDueToTimeout, isFinishedDueToError),
                        format("{} [{}, txState={}].",
                                finishedTransactionErrorMessage(
                                        isFinishedDueToTimeout,
                                        isFinishedDueToError,
                                        causeErrorCode,
                                        publicCause != null
                                ),
                                formatTxInfo(id(), txManager, false),
                                state()),
                        publicCause);
    }

    /**
     * Checks that this transaction was not finished and will be able to enlist another partition.
     */
    private void checkEnlistPossibility() {
        if (isFinishingOrFinished() || killed) {
            throw enlistFailedException();
        }

        TxStateMeta meta = txManager.stateMeta(id());

        if (meta != null && (meta.txState() == FINISHING || isFinalState(meta.txState()))) {
            throw enlistFailedException();
        }
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return TransactionsExceptionMapperUtil.convertToPublicFuture(
                finish(true, null, false, null),
                TX_COMMIT_ERR
        );
    }

    @Override
    public CompletableFuture<Void> rollbackAsync() {
        return TransactionsExceptionMapperUtil.convertToPublicFuture(
                finish(false, null, false, null),
                TX_ROLLBACK_ERR
        );
    }

    @Override
    public CompletableFuture<Void> finish(
            boolean commit,
            @Nullable HybridTimestamp executionTimestamp,
            boolean full,
            @Nullable Throwable finishReason
    ) {
        assert !(commit && finishReason != null) : "Transaction cannot be committed with an error.";

        if (finishFuture != null) {
            return finishFuture;
        }

        return finishInternal(commit, executionTimestamp, full, true, finishReason);
    }

    /**
     * Finishes the read-write transaction.
     *
     * @param commit Commit flag.
     * @param executionTimestamp The timestamp is the time when the transaction is applied to the remote node.
     * @param full Full state transaction marker.
     * @param isComplete The flag is true if the transaction is completed through the public API, false for {@link this#kill()} invocation.
     * @param finishReason Optional finish reason (for example, timeout). Must be {@code null} for commit.
     * @return The future.
     */
    private CompletableFuture<Void> finishInternal(
            boolean commit,
            @Nullable HybridTimestamp executionTimestamp,
            boolean full,
            boolean isComplete,
            @Nullable Throwable finishReason
    ) {
        enlistPartitionLock.writeLock().lock();

        try {
            if (finishFuture == null) {
                if (killed) {
                    if (isComplete) {
                        // An attempt to finish a killed transaction.
                        finishFuture = nullCompletedFuture();

                        return failedFuture(new TransactionKilledException(id(), txManager));
                    } else {
                        // Kill is called twice.
                        return nullCompletedFuture();
                    }
                }

                if (full) {
                    CompletableFuture<Void> finishFutureInternal =
                            txManager.finishFull(observableTsTracker, id(), executionTimestamp, commit, finishReason);

                    if (isComplete) {
                        finishFuture = finishFutureInternal.handle((unused, throwable) -> null);
                        this.timeoutExceeded = isFinishedDueToTimeout(finishReason);
                    } else {
                        if (killClosure == null) {
                            throw new AssertionError("Invalid kill state for a full transaction");
                        }
                        killed = true;
                    }

                    return finishFutureInternal;
                } else {
                    CompletableFuture<Void> finishFutureInternal = txManager.finish(
                            observableTsTracker,
                            commitPart,
                            commit,
                            finishReason,
                            false,
                            noRemoteWrites,
                            enlisted,
                            id()
                    );

                    if (isComplete) {
                        finishFuture = finishFutureInternal.handle((unused, throwable) -> null);
                        this.timeoutExceeded = isFinishedDueToTimeout(finishReason);
                    } else {
                        killed = true;

                        return finishFutureInternal.handle((unused, throwable) -> {
                            // TODO https://issues.apache.org/jira/browse/IGNITE-25825 move before finish after async cleanup
                            if (killClosure != null) {
                                // Notify the client about the kill.
                                killClosure.accept(this);
                            }

                            return null;
                        });
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
        return finishInternal(false, null, false, false, new TransactionKilledException(id(), txManager));
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
        txManager.enrichTxMeta(id(), old -> old == null ? null : old.mutate().lastException(e).build());
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
