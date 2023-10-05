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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_READ_ONLY_TOO_OLD_ERR;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.TxStateMetaFinishing;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * A transaction manager implementation.
 *
 * <p>Uses 2PC for atomic commitment and 2PL for concurrency control.
 */
public class TxManagerImpl implements TxManager {

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxManagerImpl.class);
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    private final ReplicaService replicaService;

    /** Lock manager. */
    private final LockManager lockManager;

    /** Executor that runs async transaction cleanup actions. */
    private final ExecutorService cleanupExecutor;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    /** Generates transaction IDs. */
    private final TransactionIdGenerator transactionIdGenerator;

    /** The local map for tx states. */
    private final ConcurrentHashMap<UUID, TxStateMeta> txStateMap = new ConcurrentHashMap<>();

    /** Future of a read-only transaction by it {@link TxIdAndTimestamp}. */
    private final ConcurrentNavigableMap<TxIdAndTimestamp, CompletableFuture<Void>> readOnlyTxFutureById = new ConcurrentSkipListMap<>(
            Comparator.comparing(TxIdAndTimestamp::getReadTimestamp).thenComparing(TxIdAndTimestamp::getTxId)
    );

    /**
     * Low watermark, does not allow creating read-only transactions less than or equal to this value, {@code null} means it has never been
     * updated yet.
     */
    private final AtomicReference<HybridTimestamp> lowWatermark = new AtomicReference<>();

    /** Lock to update and read the low watermark. */
    private final ReadWriteLock lowWatermarkReadWriteLock = new ReentrantReadWriteLock();

    private final Lazy<String> localNodeId;

    /**
     * The constructor.
     *
     * @param replicaService Replica service.
     * @param lockManager Lock manager.
     * @param clock A hybrid logical clock.
     * @param transactionIdGenerator Used to generate transaction IDs.
     */
    public TxManagerImpl(
            ReplicaService replicaService,
            LockManager lockManager,
            HybridClock clock,
            TransactionIdGenerator transactionIdGenerator,
            Supplier<String> localNodeIdSupplier
    ) {
        this.replicaService = replicaService;
        this.lockManager = lockManager;
        this.clock = clock;
        this.transactionIdGenerator = transactionIdGenerator;
        this.localNodeId = new Lazy<>(localNodeIdSupplier);

        int cpus = Runtime.getRuntime().availableProcessors();

        cleanupExecutor = new ThreadPoolExecutor(
                cpus,
                cpus,
                100,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("tx-async-cleanup", LOG));
    }

    @Override
    public InternalTransaction begin(HybridTimestampTracker timestampTracker) {
        return begin(timestampTracker, false);
    }

    @Override
    public InternalTransaction begin(HybridTimestampTracker timestampTracker, boolean readOnly) {
        return beginTx(timestampTracker, readOnly, false);
    }

    @Override
    public InternalTransaction beginImplicit(HybridTimestampTracker timestampTracker, boolean readOnly) {
        return beginTx(timestampTracker, readOnly, true);
    }

    private InternalTransaction beginTx(HybridTimestampTracker timestampTracker, boolean readOnly, boolean implicit) {
        HybridTimestamp beginTimestamp = clock.now();
        UUID txId = transactionIdGenerator.transactionIdFor(beginTimestamp);
        updateTxMeta(txId, old -> new TxStateMeta(PENDING, localNodeId.get(), null));

        if (!readOnly) {
            return new ReadWriteTransactionImpl(this, timestampTracker, txId, implicit);
        }

        HybridTimestamp observableTimestamp = timestampTracker.get();

        HybridTimestamp readTimestamp = observableTimestamp != null
                ? HybridTimestamp.max(observableTimestamp, currentReadTimestamp())
                : currentReadTimestamp();

        lowWatermarkReadWriteLock.readLock().lock();

        try {
            HybridTimestamp lowWatermark = this.lowWatermark.get();

            readOnlyTxFutureById.compute(new TxIdAndTimestamp(readTimestamp, txId), (txIdAndTimestamp, readOnlyTxFuture) -> {
                assert readOnlyTxFuture == null : "previous transaction has not completed yet: " + txIdAndTimestamp;

                if (lowWatermark != null && readTimestamp.compareTo(lowWatermark) <= 0) {
                    throw new IgniteInternalException(
                            TX_READ_ONLY_TOO_OLD_ERR,
                            "Timestamp of read-only transaction must be greater than the low watermark: [txTimestamp={}, lowWatermark={}]",
                            readTimestamp, lowWatermark
                    );
                }

                return new CompletableFuture<>();
            });

            return new ReadOnlyTransactionImpl(this, timestampTracker, txId, readTimestamp, implicit);
        } finally {
            lowWatermarkReadWriteLock.readLock().unlock();
        }
    }

    /**
     * Current read timestamp, for calculation of read timestamp of read-only transactions.
     *
     * @return Current read timestamp.
     */
    private HybridTimestamp currentReadTimestamp() {
        return clock.now();

        // TODO: IGNITE-20378 Fix it
        // return new HybridTimestamp(now.getPhysical()
        //         - ReplicaManager.IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS
        //         - HybridTimestamp.CLOCK_SKEW,
        //         0
        // );
    }

    @Override
    public TxStateMeta stateMeta(UUID txId) {
        return txStateMap.get(txId);
    }

    @Override
    public void updateTxMeta(UUID txId, Function<TxStateMeta, TxStateMeta> updater) {
        txStateMap.compute(txId, (k, oldMeta) -> {
            TxStateMeta newMeta = updater.apply(oldMeta);

            if (newMeta == null) {
                return null;
            }

            TxState oldState = oldMeta == null ? null : oldMeta.txState();

            return checkTransitionCorrectness(oldState, newMeta.txState()) ? newMeta : oldMeta;
        });
    }

    @Override
    public void finishFull(HybridTimestampTracker timestampTracker, UUID txId, boolean commit) {
        TxState finalState;

        if (commit) {
            timestampTracker.update(clock.now());

            finalState = COMMITED;
        } else {
            finalState = ABORTED;
        }

        updateTxMeta(txId, old -> new TxStateMeta(finalState, old.txCoordinatorId(), old.commitTimestamp()));
    }

    @Override
    public CompletableFuture<Void> finish(
            HybridTimestampTracker observableTimestampTracker,
            TablePartitionId commitPartition,
            ClusterNode recipientNode,
            Long term,
            boolean commit,
            Map<ClusterNode, List<IgniteBiTuple<TablePartitionId, Long>>> groups,
            UUID txId
    ) {
        assert groups != null;

        // Here we put finishing state meta into the local map, so that all concurrent operations trying to read tx state
        // with using read timestamp could see that this transaction is finishing, see #transactionMetaReadTimestampAware(txId, timestamp).
        // None of them now are able to update node's clock with read timestamp and we can create the commit timestamp that is greater
        // than all the read timestamps processed before.
        // Every concurrent operation will now use a finish future from the finishing state meta and get only final transaction
        // state after the transaction is finished.
        TxStateMetaFinishing finishingStateMeta = new TxStateMetaFinishing(localNodeId.get());
        updateTxMeta(txId, old -> finishingStateMeta);
        HybridTimestamp commitTimestamp = commit ? clock.now() : null;

        // If there are no enlisted groups, just return - we already marked the tx as finished.
        boolean finishRequestNeeded = !groups.isEmpty();

        if (!finishRequestNeeded) {
            updateTxMeta(txId, old -> {
                TxStateMeta finalStateMeta = coordinatorFinalTxStateMeta(commit, commitTimestamp);

                finishingStateMeta.txFinishFuture().complete(finalStateMeta);

                return finalStateMeta;
            });

            return completedFuture(null);
        }

        observableTimestampTracker.update(commitTimestamp);

        TxFinishReplicaRequest req = FACTORY.txFinishReplicaRequest()
                .txId(txId)
                .timestampLong(clock.nowLong())
                .groupId(commitPartition)
                .groups(groups)
                .commit(commit)
                .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                .term(term)
                .build();

        return replicaService.invoke(recipientNode, req)
                .thenRun(() ->
                        updateTxMeta(txId, old -> {
                            if (isFinalState(old.txState())) {
                                finishingStateMeta.txFinishFuture().complete(old);

                                return old;
                            }

                            assert old instanceof TxStateMetaFinishing;

                            TxStateMeta finalTxStateMeta = coordinatorFinalTxStateMeta(commit, commitTimestamp);

                            finishingStateMeta.txFinishFuture().complete(finalTxStateMeta);

                            return finalTxStateMeta;
                        })
                );
    }

    @Override
    public CompletableFuture<Void> cleanup(
            String primaryConsistentId,
            TablePartitionId tablePartitionId,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return replicaService.invoke(
                primaryConsistentId,
                FACTORY.txCleanupReplicaRequest()
                        .groupId(tablePartitionId)
                        .timestampLong(clock.nowLong())
                        .txId(txId)
                        .commit(commit)
                        .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                        .build()
        );
    }

    @Override
    public int finished() {
        return (int) txStateMap.entrySet().stream()
                .filter(e -> isFinalState(e.getValue().txState()))
                .count();
    }

    @Override
    public int pending() {
        return (int) txStateMap.entrySet().stream()
                .filter(e -> e.getValue().txState() == PENDING)
                .count();
    }

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void stop() {
        shutdownAndAwaitTermination(cleanupExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    public LockManager lockManager() {
        return lockManager;
    }

    @Override
    public CompletableFuture<Void> executeCleanupAsync(Runnable runnable) {
        return runAsync(runnable, cleanupExecutor);
    }

    CompletableFuture<Void> completeReadOnlyTransactionFuture(TxIdAndTimestamp txIdAndTimestamp) {
        CompletableFuture<Void> readOnlyTxFuture = readOnlyTxFutureById.remove(txIdAndTimestamp);

        assert readOnlyTxFuture != null : txIdAndTimestamp;

        readOnlyTxFuture.complete(null);

        return readOnlyTxFuture;
    }

    @Override
    public CompletableFuture<Void> updateLowWatermark(HybridTimestamp newLowWatermark) {
        lowWatermarkReadWriteLock.writeLock().lock();

        try {
            lowWatermark.updateAndGet(previousLowWatermark -> {
                if (previousLowWatermark == null) {
                    return newLowWatermark;
                }

                assert newLowWatermark.compareTo(previousLowWatermark) > 0 :
                        "lower watermark should be growing: [previous=" + previousLowWatermark + ", new=" + newLowWatermark + ']';

                return newLowWatermark;
            });

            TxIdAndTimestamp upperBound = new TxIdAndTimestamp(newLowWatermark, new UUID(Long.MAX_VALUE, Long.MAX_VALUE));

            List<CompletableFuture<Void>> readOnlyTxFutures = List.copyOf(readOnlyTxFutureById.headMap(upperBound, true).values());

            return allOf(readOnlyTxFutures.toArray(CompletableFuture[]::new));
        } finally {
            lowWatermarkReadWriteLock.writeLock().unlock();
        }
    }

    /**
     * Creates final {@link TxStateMeta} for coordinator node.
     *
     * @param commit Commit flag.
     * @param commitTimestamp Commit timestamp.
     * @return Transaction meta.
     */
    private TxStateMeta coordinatorFinalTxStateMeta(boolean commit, HybridTimestamp commitTimestamp) {
        return new TxStateMeta(commit ? COMMITED : ABORTED, localNodeId.get(), commitTimestamp);
    }
}
