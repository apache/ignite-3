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
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.util.CompletableFutures.allOfToList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_PRIMARY_REPLICA_EXPIRED_ERR;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.MismatchingTransactionOutcomeInternalException;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.message.FinishedTransactionsBatchMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Contains counters for in-flight requests of the transactions. Read-write transactions can't finish when some requests are in-flight.
 * Read-only transactions can't be included into {@link org.apache.ignite.internal.tx.message.FinishedTransactionsBatchMessage} when
 * some requests are in-flight.
 */
public class TransactionInflights {
    /** Hint for maximum concurrent txns. */
    private static final int MAX_CONCURRENT_TXNS = 1024;

    /** Txn contexts. */
    private final ConcurrentHashMap<UUID, TxContext> txCtxMap = new ConcurrentHashMap<>(MAX_CONCURRENT_TXNS);

    private final PlacementDriver placementDriver;

    private final ClockService clockService;

    public TransactionInflights(PlacementDriver placementDriver, ClockService clockService) {
        this.placementDriver = placementDriver;
        this.clockService = clockService;
    }

    /**
     * Registers the inflight update for a transaction.
     *
     * @param txId The transaction id.
     * @param readOnly Whether the transaction is read-only.
     * @return {@code True} if the inflight was registered. The update must be failed on false.
     */
    public boolean addInflight(UUID txId, boolean readOnly) {
        boolean[] res = {true};

        txCtxMap.compute(txId, (uuid, ctx) -> {
            if (ctx == null) {
                ctx = readOnly ? new ReadOnlyTxContext() : new ReadWriteTxContext(placementDriver, clockService);
            }

            res[0] = ctx.addInflight();

            return ctx;
        });

        return res[0];
    }

    /**
     * Unregisters the inflight for a transaction.
     *
     * @param txId The transaction id.
     */
    public void removeInflight(UUID txId) {
        // Can be null if tx was aborted and inflights were removed from the collection.
        TxContext tuple = txCtxMap.computeIfPresent(txId, (uuid, ctx) -> {
            ctx.removeInflight(txId);

            return ctx;
        });

        // Avoid completion under lock.
        if (tuple != null) {
            tuple.onInflightsRemoved();
        }
    }

    Collection<UUID> finishedReadOnlyTransactions() {
        return txCtxMap.entrySet().stream()
                .filter(e -> e.getValue() instanceof ReadOnlyTxContext && e.getValue().isReadyToFinish())
                .map(Entry::getKey)
                .collect(toSet());
    }

    void removeTxContext(UUID txId) {
        txCtxMap.remove(txId);
    }

    void removeTxContexts(Collection<UUID> txIds) {
        txCtxMap.keySet().removeAll(txIds);
    }

    void cancelWaitingInflights(ReplicationGroupId groupId) {
        for (Map.Entry<UUID, TxContext> ctxEntry : txCtxMap.entrySet()) {
            if (ctxEntry.getValue() instanceof ReadWriteTxContext) {
                ReadWriteTxContext txContext = (ReadWriteTxContext) ctxEntry.getValue();

                if (txContext.isTxFinishing()) {
                    PendingTxPartitionEnlistment enlistment = txContext.enlistedGroups.get(groupId);

                    if (enlistment != null) {
                        txContext.cancelWaitingInflights(groupId, enlistment.consistencyToken());
                    }
                }
            }
        }
    }

    void markReadOnlyTxFinished(UUID txId, boolean timeoutExceeded) {
        txCtxMap.compute(txId, (k, ctx) -> {
            if (ctx == null) {
                ctx = new ReadOnlyTxContext(timeoutExceeded);
            }

            ctx.finishTx(null, timeoutExceeded);

            return ctx;
        });
    }

    ReadWriteTxContext lockTxForNewUpdates(UUID txId, Map<ReplicationGroupId, PendingTxPartitionEnlistment> enlistedGroups) {
        return (ReadWriteTxContext) txCtxMap.compute(txId, (uuid, tuple0) -> {
            if (tuple0 == null) {
                tuple0 = new ReadWriteTxContext(placementDriver, clockService, false); // No writes enlisted.
            }

            assert !tuple0.isTxFinishing() : "Transaction is already finished [id=" + uuid + "].";

            tuple0.finishTx(enlistedGroups, false);

            return tuple0;
        });
    }

    abstract static class TxContext {
        volatile long inflights = 0; // Updated under lock.

        boolean addInflight() {
            if (isTxFinishing()) {
                return false;
            } else {
                //noinspection NonAtomicOperationOnVolatileField
                inflights++;
                return true;
            }
        }

        void removeInflight(UUID txId) {
            assert inflights > 0 : format("No inflights, cannot remove any [txId={}, ctx={}]", txId, this);

            //noinspection NonAtomicOperationOnVolatileField
            inflights--;
        }

        abstract void onInflightsRemoved();

        abstract void finishTx(@Nullable Map<ReplicationGroupId, PendingTxPartitionEnlistment> enlistedGroups, boolean timeoutExceeded);

        abstract boolean isTxFinishing();

        abstract boolean isReadyToFinish();

        abstract boolean isTimeoutExceeded();
    }

    /**
     * Transaction inflights for read-only transactions are needed because of different finishing protocol which doesn't directly close
     * transaction resources (cursors, etc.). The finish of read-only transaction is a local operation, which is followed by the resources
     * vacuum that is made in background, see {@link FinishedReadOnlyTransactionTracker}. Before sending
     * {@link FinishedTransactionsBatchMessage}, the trackers needs to be sure that all operations (i.e. inflights) of the corresponding
     * transaction are finished.
     */
    private static class ReadOnlyTxContext extends TxContext {
        private volatile boolean markedFinished;
        private volatile boolean timeoutExceeded;

        ReadOnlyTxContext() {
            // No-op.
        }

        ReadOnlyTxContext(boolean timeoutExceeded) {
            this.timeoutExceeded = timeoutExceeded;
        }

        @Override
        public void onInflightsRemoved() {
            // No-op.
        }

        @Override
        public void finishTx(@Nullable Map<ReplicationGroupId, PendingTxPartitionEnlistment> enlistedGroups, boolean timeoutExceeded) {
            markedFinished = true;
        }

        @Override
        public boolean isTxFinishing() {
            return markedFinished;
        }

        @Override
        public boolean isReadyToFinish() {
            return markedFinished && inflights == 0;
        }

        @Override
        boolean isTimeoutExceeded() {
            return timeoutExceeded;
        }

        @Override
        public String toString() {
            return "ReadOnlyTxContext [inflights=" + inflights + ']';
        }
    }

    static class ReadWriteTxContext extends TxContext {
        private final CompletableFuture<Void> waitRepFut = new CompletableFuture<>();
        private final PlacementDriver placementDriver;
        private volatile CompletableFuture<Void> finishInProgressFuture = null;
        private volatile Map<ReplicationGroupId, PendingTxPartitionEnlistment> enlistedGroups;
        private final ClockService clockService;
        private volatile boolean timeoutExceeded;

        private ReadWriteTxContext(PlacementDriver placementDriver, ClockService clockService) {
            this(placementDriver, clockService, false);
        }

        private ReadWriteTxContext(PlacementDriver placementDriver, ClockService clockService, boolean timeoutExceeded) {
            this.placementDriver = placementDriver;
            this.clockService = clockService;
            this.timeoutExceeded = timeoutExceeded;
        }

        CompletableFuture<Void> performFinish(boolean commit, Function<Boolean, CompletableFuture<Void>> finishAction) {
            waitReadyToFinish(commit)
                    .whenComplete((ignoredReadyToFinish, readyException) -> finishAction.apply(commit && readyException == null)
                            .whenComplete((ignoredFinishActionResult, finishException) ->
                                    completeFinishInProgressFuture(commit, readyException, finishException))
                    );

            return finishInProgressFuture;
        }

        private void completeFinishInProgressFuture(
                boolean commit,
                @Nullable Throwable readyToFinishException,
                @Nullable Throwable finishException
        ) {
            if (readyToFinishException == null) {
                if (finishException == null) {
                    finishInProgressFuture.complete(null);
                } else {
                    finishInProgressFuture.completeExceptionally(finishException);
                }
            } else {
                Throwable unwrappedReadyToFinishException = unwrapCause(readyToFinishException);

                if (commit && unwrappedReadyToFinishException instanceof PrimaryReplicaExpiredException) {
                    finishInProgressFuture.completeExceptionally(new MismatchingTransactionOutcomeInternalException(
                            TX_PRIMARY_REPLICA_EXPIRED_ERR,
                            "Failed to commit the transaction.",
                            new TransactionResult(ABORTED, null),
                            unwrappedReadyToFinishException
                    ));
                } else {
                    finishInProgressFuture.completeExceptionally(unwrappedReadyToFinishException);
                }
            }
        }

        private CompletableFuture<Void> waitReadyToFinish(boolean commit) {
            if (commit) {
                HybridTimestamp now = clockService.now();

                var futures = new CompletableFuture[enlistedGroups.size()];

                int cntr = 0;

                for (Map.Entry<ReplicationGroupId, PendingTxPartitionEnlistment> e : enlistedGroups.entrySet()) {
                    futures[cntr++] = placementDriver.getPrimaryReplica(e.getKey(), now)
                            .thenApply(replicaMeta -> {
                                long enlistmentConsistencyToken = e.getValue().consistencyToken();

                                if (replicaMeta == null || enlistmentConsistencyToken != replicaMeta.getStartTime().longValue()) {
                                    return failedFuture(new PrimaryReplicaExpiredException(e.getKey(), enlistmentConsistencyToken, null,
                                            replicaMeta));
                                }

                                return nullCompletedFuture();
                            });
                }

                return allOfToList(futures)
                        .thenCompose(unused -> waitNoInflights());
            } else {
                return nullCompletedFuture();
            }
        }

        private CompletableFuture<Void> waitNoInflights() {
            if (inflights == 0) {
                waitRepFut.complete(null);
            }
            return waitRepFut;
        }

        void cancelWaitingInflights(ReplicationGroupId groupId, long enlistmentConsistencyToken) {
            waitRepFut.completeExceptionally(new PrimaryReplicaExpiredException(groupId, enlistmentConsistencyToken, null, null));
        }

        @Override
        public void onInflightsRemoved() {
            if (inflights == 0 && finishInProgressFuture != null) {
                waitRepFut.complete(null);
            }
        }

        @Override
        public void finishTx(Map<ReplicationGroupId, PendingTxPartitionEnlistment> enlistedGroups, boolean timeoutExceeded) {
            this.enlistedGroups = enlistedGroups;
            this.timeoutExceeded = timeoutExceeded;
            finishInProgressFuture = new CompletableFuture<>();
        }

        @Override
        public boolean isTxFinishing() {
            return finishInProgressFuture != null;
        }

        @Override
        public boolean isReadyToFinish() {
            return waitRepFut.isDone();
        }

        @Override
        boolean isTimeoutExceeded() {
            return timeoutExceeded;
        }

        @Override
        public String toString() {
            return "ReadWriteTxContext [inflights=" + inflights + ", waitRepFut=" + waitRepFut
                    + ", finishFut=" + finishInProgressFuture + ']';
        }
    }
}
