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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_ERR;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TransactionIds;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;

/**
 * The read-write implementation of an internal transaction.
 */
public class ReadWriteTransactionImpl extends IgniteAbstractTransactionImpl {
    /** Commit partition updater. */
    private static final AtomicReferenceFieldUpdater<ReadWriteTransactionImpl, TablePartitionId> COMMIT_PART_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ReadWriteTransactionImpl.class, TablePartitionId.class, "commitPart");

    /** Enlisted partitions: partition id -> (primary replica node, enlistment consistency token). */
    private final Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlisted = new ConcurrentHashMap<>();

    /** The tracker is used to track an observable timestamp. */
    private final HybridTimestampTracker observableTsTracker;

    /** A partition which stores the transaction state. */
    private volatile TablePartitionId commitPart;

    /** The lock protects the transaction topology from concurrent modification during finishing. */
    private final ReentrantReadWriteLock enlistPartitionLock = new ReentrantReadWriteLock();

    /** The future is initialized when this transaction starts committing or rolling back and is finished together with the transaction. */
    private volatile CompletableFuture<Void> finishFuture;

    /**
     * Constructs an explicit read-write transaction.
     *
     * @param txManager The tx manager.
     * @param observableTsTracker Observable timestamp tracker.
     * @param id The id.
     * @param txCoordinatorId Transaction coordinator inconsistent ID.
     */
    public ReadWriteTransactionImpl(
            TxManager txManager,
            HybridTimestampTracker observableTsTracker,
            UUID id,
            String txCoordinatorId
    ) {
        super(txManager, id, txCoordinatorId);

        this.observableTsTracker = observableTsTracker;
    }

    /** {@inheritDoc} */
    @Override
    public boolean assignCommitPartition(TablePartitionId tablePartitionId) {
        return COMMIT_PART_UPDATER.compareAndSet(this, null, tablePartitionId);
    }

    /** {@inheritDoc} */
    @Override
    public TablePartitionId commitPartition() {
        return commitPart;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBiTuple<ClusterNode, Long> enlistedNodeAndConsistencyToken(TablePartitionId partGroupId) {
        return enlisted.get(partGroupId);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBiTuple<ClusterNode, Long> enlist(
            TablePartitionId tablePartitionId,
            IgniteBiTuple<ClusterNode, Long> nodeAndConsistencyToken
    ) {
        boolean locked = enlistPartitionLock.readLock().tryLock();

        // No need to wait for lock if commit is in progress.
        if (!locked) {
            failEnlist();
            assert false; // Not reachable.
        }

        try {
            checkEnlistPossibility();

            return enlisted.computeIfAbsent(tablePartitionId, k -> nodeAndConsistencyToken);
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
                format("Transaction is already finished [id={}, state={}].", id(), state()));
    }

    /**
     * Checks that this transaction was not finished and will be able to enlist another partition.
     */
    private void checkEnlistPossibility() {
        if (finishFuture != null) {
            // This means that the transaction is either in final or FINISHING state.
            failEnlist();
        }
    }

    /** {@inheritDoc} */
    @Override
    protected CompletableFuture<Void> finish(boolean commit) {
        if (finishFuture != null) {
            return finishFuture;
        }

        enlistPartitionLock.writeLock().lock();

        try {
            if (finishFuture == null) {
                CompletableFuture<Void> finishFutureInternal = finishInternal(commit);

                finishFuture = finishFutureInternal.handle((unused, throwable) -> null);

                // Return the real future first time.
                return finishFutureInternal;
            }

            return finishFuture;
        } finally {
            enlistPartitionLock.writeLock().unlock();
        }
    }

    /**
     * Internal method for finishing this transaction.
     *
     * @param commit {@code true} to commit, false to rollback.
     * @return The future of transaction completion.
     */
    private CompletableFuture<Void> finishInternal(boolean commit) {
        return txManager.finish(observableTsTracker, commitPart, commit, enlisted, id());
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
    public HybridTimestamp startTimestamp() {
        return TransactionIds.beginTimestamp(id());
    }
}
