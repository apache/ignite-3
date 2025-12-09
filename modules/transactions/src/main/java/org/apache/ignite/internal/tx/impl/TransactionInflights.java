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
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.MismatchingTransactionOutcomeInternalException;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.message.FinishedTransactionsBatchMessage;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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
     * Register the update inflight for RW transaction.
     *
     * @param txId The transaction id.
     * @return {@code True} if the inflight was registered. The update must be failed on false.
     */
    public boolean addInflight(UUID txId) {
        boolean[] res = {true};

        txCtxMap.compute(txId, (uuid, ctx) -> {
            if (ctx == null) {
                ctx = new ReadWriteTxContext(placementDriver, clockService);
            }

            res[0] = ctx.addInflight();

            return ctx;
        });

        return res[0];
    }

    /**
     * Register the scan inflight for RO transaction.
     *
     * @param txId The transaction id.
     * @return {@code True} if the inflight was registered. The scan must be failed on false.
     */
    public boolean addScanInflight(UUID txId) {
        boolean[] res = {true};

        txCtxMap.compute(txId, (uuid, ctx) -> {
            if (ctx == null) {
                ctx = new ReadOnlyTxContext();
            }

            res[0] = ctx.addInflight();

            return ctx;
        });

        return res[0];
    }

    /**
     * Track the given RW transaction until finish.
     * Currently RW tracking is used to enforce cleanup path for SQL RW transactions, which doesn't use RW inflights tracking yet.
     *
     * @param txId The transaction id.
     * @return {@code True} if the was registered and is in active state.
     */
    public boolean track(UUID txId) {
        boolean[] res = {true};

        txCtxMap.compute(txId, (uuid, ctx) -> {
            if (ctx == null) {
                ctx = new ReadWriteTxContext(placementDriver, clockService);
            }

            res[0] = !ctx.isTxFinishing();

            return ctx;
        });

        return res[0];
    }

    /**
     * Track the given RO transaction until finish.
     * Currently RO tracking is used to prevent unclosed cursors.
     *
     * @param txId The transaction id.
     * @return {@code True} if the was registered and is in active state.
     */
    public boolean trackReadOnly(UUID txId) {
        boolean[] res = {true};

        txCtxMap.compute(txId, (uuid, ctx) -> {
            if (ctx == null) {
                ctx = new ReadOnlyTxContext();
            }

            res[0] = !ctx.isTxFinishing();

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
            tuple.onInflightRemoved(tuple.err);
        }
    }

    void removeInflight(UUID txId, Throwable cause) {
        // Can be null if tx was aborted and inflights were removed from the collection.
        TxContext tuple = txCtxMap.computeIfPresent(txId, (uuid, ctx) -> {
            if (cause != null && ctx.err == null) {
                ctx.err = cause; // Retain only first exception.
            }

            // Update inflight counter after assigning error value to avoid issues with visibility.
            ctx.removeInflight(txId);

            return ctx;
        });

        // Avoid completion under lock.
        if (tuple != null) {
            tuple.onInflightRemoved(tuple.err);
        }
    }

    /**
     * Get active inflights.
     *
     * @return {@code True} if has some inflights in progress.
     */
    @TestOnly
    public boolean hasActiveInflights() {
        for (TxContext value : txCtxMap.values()) {
            if (!value.isTxFinishing()) {
                return true;
            }
        }

        return false;
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

    void cancelWaitingInflights(ZonePartitionId groupId) {
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

    void markReadOnlyTxFinished(UUID txId) {
        txCtxMap.compute(txId, (k, ctx) -> {
            if (ctx == null) {
                ctx = new ReadOnlyTxContext();
            } else {
                assert ctx instanceof ReadOnlyTxContext;
            }

            ctx.finishTx(null);

            return ctx;
        });
    }

    ReadWriteTxContext lockTxForNewUpdates(UUID txId, Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroups) {
        return (ReadWriteTxContext) txCtxMap.compute(txId, (uuid, tuple0) -> {
            if (tuple0 == null) {
                tuple0 = new ReadWriteTxContext(placementDriver, clockService, true); // No writes enlisted, can go with unlock only.
            }

            assert !tuple0.isTxFinishing() : "Transaction is already finished [id=" + uuid + "].";

            tuple0.finishTx(enlistedGroups);

            return tuple0;
        });
    }

    abstract static class TxContext {
        volatile long inflights = 0; // Updated under lock.
        Throwable err;

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

        abstract void onInflightRemoved(@Nullable Throwable t);

        abstract void finishTx(@Nullable Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroups);

        abstract boolean isTxFinishing();

        abstract boolean isReadyToFinish();
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

        ReadOnlyTxContext() {
            // No-op.
        }

        @Override
        void onInflightRemoved(Throwable t) {
            // No-op.
        }

        @Override
        public void finishTx(@Nullable Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroups) {
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
        public String toString() {
            return "ReadOnlyTxContext [inflights=" + inflights + ']';
        }
    }

    static class ReadWriteTxContext extends TxContext {
        private final CompletableFuture<Void> waitRepFut = new CompletableFuture<>();
        private final PlacementDriver placementDriver;
        private final boolean noWrites;
        private volatile CompletableFuture<Void> finishInProgressFuture = null;
        private volatile Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroups;
        private final ClockService clockService;

        private ReadWriteTxContext(PlacementDriver placementDriver, ClockService clockService) {
            this(placementDriver, clockService, false);
        }

        private ReadWriteTxContext(PlacementDriver placementDriver, ClockService clockService, boolean noWrites) {
            this.placementDriver = placementDriver;
            this.clockService = clockService;
            this.noWrites = noWrites;
        }

        CompletableFuture<Void> performFinish(boolean commit, Function<Boolean, CompletableFuture<Void>> finishAction) {
            waitReadyToFinish(commit).whenComplete((ignored, readyException) -> {
                try {
                    if (commit) {
                        if (readyException == null) {
                            CompletableFuture<Void> actionFut = finishAction.apply(true);

                            actionFut.whenComplete((ignoredFinishActionResult, finishException) ->
                                    completeFinishInProgressFuture(true, null, finishException));
                        } else {
                            // If we got ready exception, that means some of enlisted partitions could be broken/unavailable.
                            // Respond to caller with the commit failure immediately to reduce potential unavailability window.
                            completeFinishInProgressFuture(true, readyException, null);

                            finishAction.apply(false);
                        }

                        return;
                    }

                    CompletableFuture<Void> actionFut = finishAction.apply(false);

                    actionFut.whenComplete((ignoredFinishActionResult, finishException) ->
                            completeFinishInProgressFuture(false, readyException, finishException));
                } catch (Throwable err) {
                    completeFinishInProgressFuture(commit, readyException, err);
                }
            });

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

                for (Map.Entry<ZonePartitionId, PendingTxPartitionEnlistment> e : enlistedGroups.entrySet()) {
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

                return allOfToList(futures).thenCompose(unused -> waitNoInflights());
            } else {
                return nullCompletedFuture();
            }
        }

        private CompletableFuture<Void> waitNoInflights() {
            // no new inflights are possible due to locked tx for update.
            if (inflights == 0) {
                if (err != null) {
                    waitRepFut.completeExceptionally(err);
                } else {
                    waitRepFut.complete(null);
                }
            }
            return waitRepFut;
        }

        void cancelWaitingInflights(ZonePartitionId groupId, long enlistmentConsistencyToken) {
            waitRepFut.completeExceptionally(new PrimaryReplicaExpiredException(groupId, enlistmentConsistencyToken, null, null));
        }

        @Override
        void onInflightRemoved(@Nullable Throwable t) {
            if (inflights == 0 && finishInProgressFuture != null) {
                if (t == null) {
                    waitRepFut.complete(null);
                } else {
                    waitRepFut.completeExceptionally(t);
                }
            }
        }

        @Override
        public void finishTx(Map<ZonePartitionId, PendingTxPartitionEnlistment> enlistedGroups) {
            this.enlistedGroups = enlistedGroups;
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

        boolean isNoWrites() {
            return noWrites;
        }

        @Override
        public String toString() {
            return "ReadWriteTxContext [inflights=" + inflights + ", waitRepFut=" + waitRepFut
                    + ", noWrites=" + noWrites + ", finishFut=" + finishInProgressFuture + ']';
        }
    }
}
