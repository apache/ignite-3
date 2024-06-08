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
import static org.apache.ignite.internal.util.CompletableFutures.allOf;
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
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.MismatchingTransactionOutcomeException;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.message.FinishedTransactionsBatchMessage;
import org.apache.ignite.network.ClusterNode;
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

    void cancelWaitingInflights(TablePartitionId groupId) {
        for (Map.Entry<UUID, TxContext> ctxEntry : txCtxMap.entrySet()) {
            if (ctxEntry.getValue() instanceof ReadWriteTxContext) {
                ReadWriteTxContext txContext = (ReadWriteTxContext) ctxEntry.getValue();

                if (txContext.isTxFinishing()) {
                    IgniteBiTuple<ClusterNode, Long> nodeAndToken = txContext.enlistedGroups.get(groupId);

                    if (nodeAndToken != null) {
                        txContext.cancelWaitingInflights(groupId, nodeAndToken.get2());
                    }
                }
            }
        }
    }

    void markReadOnlyTxFinished(UUID txId) {
        txCtxMap.compute(txId, (k, ctx) -> {
            if (ctx == null) {
                ctx = new ReadOnlyTxContext();
            }

            ctx.finishTx(null);

            return ctx;
        });
    }

    ReadWriteTxContext lockTxForNewUpdates(UUID txId, Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups) {
        return (ReadWriteTxContext) txCtxMap.compute(txId, (uuid, tuple0) -> {
            if (tuple0 == null) {
                tuple0 = new ReadWriteTxContext(placementDriver, clockService); // No writes enlisted.
            }

            assert !tuple0.isTxFinishing() : "Transaction is already finished [id=" + uuid + "].";

            tuple0.finishTx(enlistedGroups);

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

        abstract void finishTx(@Nullable Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups);

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

        @Override
        public void onInflightsRemoved() {
            // No-op.
        }

        @Override
        public void finishTx(@Nullable Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups) {
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
        private volatile CompletableFuture<Void> finishInProgressFuture = null;
        private volatile Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups;
        private ClockService clockService;

        private ReadWriteTxContext(PlacementDriver placementDriver, ClockService clockService) {
            this.placementDriver = placementDriver;
            this.clockService = clockService;
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
                    finishInProgressFuture.completeExceptionally(new MismatchingTransactionOutcomeException(
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

                for (Map.Entry<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> e : enlistedGroups.entrySet()) {
                    futures[cntr++] = placementDriver.getPrimaryReplica(e.getKey(), now)
                            .thenApply(replicaMeta -> {
                                Long enlistmentConsistencyToken = e.getValue().get2();

                                if (replicaMeta == null || !enlistmentConsistencyToken.equals(replicaMeta.getStartTime().longValue())) {
                                    return failedFuture(new PrimaryReplicaExpiredException(e.getKey(), enlistmentConsistencyToken, null,
                                            replicaMeta));
                                }

                                return nullCompletedFuture();
                            });
                }

                return allOf(futures)
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

        void cancelWaitingInflights(TablePartitionId groupId, Long enlistmentConsistencyToken) {
            waitRepFut.completeExceptionally(new PrimaryReplicaExpiredException(groupId, enlistmentConsistencyToken, null, null));
        }

        @Override
        public void onInflightsRemoved() {
            if (inflights == 0 && finishInProgressFuture != null) {
                waitRepFut.complete(null);
            }
        }

        @Override
        public void finishTx(Map<TablePartitionId, IgniteBiTuple<ClusterNode, Long>> enlistedGroups) {
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

        @Override
        public String toString() {
            return "ReadWriteTxContext [inflights=" + inflights + ", waitRepFut=" + waitRepFut
                    + ", finishFut=" + finishInProgressFuture + ']';
        }
    }
}
