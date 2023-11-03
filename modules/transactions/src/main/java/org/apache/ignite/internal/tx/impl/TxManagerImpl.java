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
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.replicator.ReplicaManager.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.tracing.TracingManager.asyncSpan;
import static org.apache.ignite.internal.tracing.TracingManager.span;
import static org.apache.ignite.internal.tracing.TracingManager.spanWithResult;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.internal.tx.TxState.checkTransitionCorrectness;
import static org.apache.ignite.internal.tx.TxState.isFinalState;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_READ_ONLY_TOO_OLD_ERR;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
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
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
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
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.jetbrains.annotations.Nullable;

/**
 * A transaction manager implementation.
 *
 * <p>Uses 2PC for atomic commitment and 2PL for concurrency control.
 */
public class TxManagerImpl implements TxManager, NetworkMessageHandler {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxManagerImpl.class);

    /** Hint for maximum concurrent txns. */
    private static final int MAX_CONCURRENT_TXNS = 1024;

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

    /** Txn contexts. */
    private final ConcurrentHashMap<UUID, TxContext> txCtxMap = new ConcurrentHashMap<>(MAX_CONCURRENT_TXNS);

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

    private final PlacementDriver placementDriver;

    private final LongSupplier idleSafeTimePropagationPeriodMsSupplier;

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
            Supplier<String> localNodeIdSupplier,
            PlacementDriver placementDriver
    ) {
        this(
                replicaService,
                lockManager,
                clock,
                transactionIdGenerator,
                localNodeIdSupplier,
                placementDriver,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS
        );
    }

    /**
     * The constructor.
     *
     * @param replicaService Replica service.
     * @param lockManager Lock manager.
     * @param clock A hybrid logical clock.
     * @param transactionIdGenerator Used to generate transaction IDs.
     * @param idleSafeTimePropagationPeriodMsSupplier Used to get idle safe time propagation period in ms.
     */
    public TxManagerImpl(
            ReplicaService replicaService,
            LockManager lockManager,
            HybridClock clock,
            TransactionIdGenerator transactionIdGenerator,
            Supplier<String> localNodeIdSupplier,
            PlacementDriver placementDriver,
            LongSupplier idleSafeTimePropagationPeriodMsSupplier
    ) {
        this.replicaService = replicaService;
        this.lockManager = lockManager;
        this.clock = clock;
        this.transactionIdGenerator = transactionIdGenerator;
        this.localNodeId = new Lazy<>(localNodeIdSupplier);
        this.placementDriver = placementDriver;
        this.idleSafeTimePropagationPeriodMsSupplier = idleSafeTimePropagationPeriodMsSupplier;

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
    public InternalTransaction begin(
            HybridTimestampTracker timestampTracker,
            boolean readOnly
    ) {
        try (var txSpan = asyncSpan("tx operation")) {
            return spanWithResult("TxManagerImpl.begin", (span) -> {
                span.addAttribute("timestampTracker", timestampTracker::toString);
                span.addAttribute("readOnly", () -> String.valueOf(readOnly));

                HybridTimestamp beginTimestamp = clock.now();
                UUID txId = transactionIdGenerator.transactionIdFor(beginTimestamp);
                updateTxMeta(txId, old -> new TxStateMeta(PENDING, localNodeId.get(), null));

                if (!readOnly) {
                    return new ReadWriteTransactionImpl(this, timestampTracker, txId, txSpan);
                }

                HybridTimestamp observableTimestamp = timestampTracker.get();

                HybridTimestamp readTimestamp = observableTimestamp != null
                        ? HybridTimestamp.max(observableTimestamp, currentReadTimestamp())
                        : currentReadTimestamp();

                lowWatermarkReadWriteLock.readLock().lock();

                try {
                    HybridTimestamp lowWatermark1 = this.lowWatermark.get();

                    readOnlyTxFutureById.compute(new TxIdAndTimestamp(readTimestamp, txId), (txIdAndTimestamp, readOnlyTxFuture) -> {
                        assert readOnlyTxFuture == null : "previous transaction has not completed yet: " + txIdAndTimestamp;

                        if (lowWatermark1 != null && readTimestamp.compareTo(lowWatermark1) <= 0) {
                            throw new IgniteInternalException(
                                    TX_READ_ONLY_TOO_OLD_ERR,
                                    "Timestamp of read-only transaction must be greater than the low watermark: [txTimestamp={}, lowWatermark={}]",
                                    readTimestamp, lowWatermark1
                            );
                        }

                        return new CompletableFuture<>();
                    });

                    return new ReadOnlyTransactionImpl(this, timestampTracker, txId, readTimestamp, txSpan);
                } finally {
                    lowWatermarkReadWriteLock.readLock().unlock();
                }
            });
        }
    }

    /**
     * Current read timestamp, for calculation of read timestamp of read-only transactions.
     *
     * @return Current read timestamp.
     */
    private HybridTimestamp currentReadTimestamp() {
        HybridTimestamp now = clock.now();

        return new HybridTimestamp(now.getPhysical()
                - idleSafeTimePropagationPeriodMsSupplier.getAsLong()
                - HybridTimestamp.CLOCK_SKEW,
                0
        );
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
            Map<TablePartitionId, Long> enlistedGroups,
            UUID txId
    ) {
        assert enlistedGroups != null;

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
        boolean finishRequestNeeded = !enlistedGroups.isEmpty();

        if (!finishRequestNeeded) {
            updateTxMeta(txId, old -> {
                TxStateMeta finalStateMeta = coordinatorFinalTxStateMeta(commit, commitTimestamp);

                finishingStateMeta.txFinishFuture().complete(finalStateMeta);

                return finalStateMeta;
            });

            return completedFuture(null);
        }

        Function<Void, CompletableFuture<Void>> clo = ignored -> {
            // In case of commit it's required to check whether current primaries are still the same that were enlisted and whether
            // given primaries are not expired or, in other words, whether commitTimestamp is less or equal to the enlisted primaries
            // expiration timestamps.
            CompletableFuture<Void> verificationFuture =
                    commit ? verifyCommitTimestamp(enlistedGroups, commitTimestamp) : completedFuture(null);

            return verificationFuture.handle(
                    (unused, throwable) -> {
                        Collection<ReplicationGroupId> replicationGroupIds = new HashSet<>(enlistedGroups.keySet());

                        boolean verifiedCommit = throwable == null && commit;

                        TxFinishReplicaRequest req = FACTORY.txFinishReplicaRequest()
                                .txId(txId)
                                .timestampLong(clock.nowLong())
                                .groupId(commitPartition)
                                .groups(replicationGroupIds)
                                // In case of verification future failure transaction will be rolled back.
                                .commit(verifiedCommit)
                                .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                                .term(term)
                                .build();

                        return replicaService.invoke(recipientNode, req).thenRun(
                                () -> {
                                    updateTxMeta(txId, old -> {
                                        if (isFinalState(old.txState())) {
                                            finishingStateMeta.txFinishFuture().complete(old);

                                            return old;
                                        }

                                        assert old instanceof TxStateMetaFinishing;

                                        TxStateMeta finalTxStateMeta = coordinatorFinalTxStateMeta(verifiedCommit, commitTimestamp);

                                        finishingStateMeta.txFinishFuture().complete(finalTxStateMeta);

                                        return finalTxStateMeta;
                                    });

                                    if (verifiedCommit) {
                                        observableTimestampTracker.update(commitTimestamp);
                                    }
                                });
                    })
                    .thenCompose(Function.identity())
                    // verification future is added in order to share proper exception with the client
                    .thenCompose(r -> verificationFuture);
        };

        AtomicReference<CompletableFuture<Void>> ref = new AtomicReference<>();
        TxContext tuple = txCtxMap.compute(txId, (uuid, tuple0) -> {
            if (tuple0 == null) {
                tuple0 = new TxContext(); // No writes enlisted.
            }

            if (tuple0.finishFut == null) {
                tuple0.finishFut = new CompletableFuture<>();
                ref.set(tuple0.finishFut);
            }

            return tuple0;
        });

        if (ref.get() != null) { // This is a finishing thread.
            if (!commit) {
                clo.apply(null).handle((ignored, err) -> {
                    if (err == null) {
                        tuple.finishFut.complete(null);
                    } else {
                        tuple.finishFut.completeExceptionally(err);
                    }
                    return null;
                });
            } else {

                // All inflights have been completed before the finish.
                if (tuple.inflights == 0) {
                    tuple.waitRepFut.complete(null);
                }

                // Wait for commit acks first, then proceed with the finish request.
                tuple.waitRepFut.thenCompose(clo).handle((ignored, err) -> {
                    if (err == null) {
                        tuple.finishFut.complete(null);
                    } else {
                        tuple.finishFut.completeExceptionally(err);
                    }
                    return null;
                });
            }
        }

        return tuple.finishFut;
    }

    @Override
    public CompletableFuture<Void> cleanup(
            String primaryConsistentId,
            TablePartitionId tablePartitionId,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        return spanWithResult("TxManagerImpl.cleanup", (span) -> {
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
        });
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
        replicaService.messagingService().addMessageHandler(ReplicaMessageGroup.class, this);
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

    @Override
    public CompletableFuture<?> executeCleanupAsync(Supplier<CompletableFuture<?>> action) {
        return supplyAsync(action, cleanupExecutor).thenCompose(f -> f);
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

    @Override
    public boolean addInflight(UUID txId) {
        boolean[] res = {true};

        txCtxMap.compute(txId, (uuid, tuple) -> {
            if (tuple == null) {
                tuple = new TxContext();
            }

            if (tuple.finishFut != null) {
                res[0] = false;
                return tuple;
            } else {
                //noinspection NonAtomicOperationOnVolatileField
                tuple.inflights++;
            }

            return tuple;
        });

        return res[0];
    }

    @Override
    public void removeInflight(UUID txId) {
        TxContext tuple = txCtxMap.compute(txId, (uuid, ctx) -> {
            assert ctx != null && ctx.inflights > 0 : ctx;

            //noinspection NonAtomicOperationOnVolatileField
            ctx.inflights--;

            return ctx;
        });

        if (tuple.inflights == 0 && tuple.finishFut != null) {
            tuple.waitRepFut.complete(null); // Avoid completion under lock.
        }
    }

    @Override
    public void onReceived(NetworkMessage message, String senderConsistentId, @Nullable Long correlationId) {
        if (!(message instanceof ReplicaResponse) || correlationId != null) {
            return;
        }

        // Ignore error responses here. A transaction will be rolled back in other place.
        if (message instanceof ErrorReplicaResponse) {
            return;
        }

        // Process directly sent response.
        ReplicaResponse request = (ReplicaResponse) message;

        Object result = request.result();

        if (result instanceof UUID) {
            removeInflight((UUID) result);
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

    /**
     * Check whether previously enlisted primary replicas aren't expired and that commit timestamp is less or equal than primary replicas
     * expiration timestamp. Given method will either complete result future with void or {@link PrimaryReplicaExpiredException}
     *
     * @param enlistedGroups enlisted primary replicas map from groupId to enlistment consistency token.
     * @param commitTimestamp Commit timestamp.
     * @return Verification future.
     */
    private CompletableFuture<Void> verifyCommitTimestamp(Map<TablePartitionId, Long> enlistedGroups, HybridTimestamp commitTimestamp) {
        var verificationFutures = new CompletableFuture[enlistedGroups.size()];
        int cnt = -1;

        for (Map.Entry<TablePartitionId, Long> enlistedGroup : enlistedGroups.entrySet()) {
            TablePartitionId groupId = enlistedGroup.getKey();
            Long expectedEnlistmentConsistencyToken = enlistedGroup.getValue();

            verificationFutures[++cnt] = placementDriver.getPrimaryReplica(groupId, commitTimestamp)
                    .thenAccept(currentPrimaryReplica -> {
                        if (currentPrimaryReplica == null
                                || !expectedEnlistmentConsistencyToken.equals(currentPrimaryReplica.getStartTime().longValue())
                        ) {
                            throw new PrimaryReplicaExpiredException(
                                    groupId,
                                    expectedEnlistmentConsistencyToken,
                                    commitTimestamp,
                                    currentPrimaryReplica
                            );
                        } else {
                            assert commitTimestamp.compareTo(currentPrimaryReplica.getExpirationTime()) <= 0 :
                                    IgniteStringFormatter.format(
                                            "Commit timestamp is greater than primary replica expiration timestamp:"
                                                    + " [groupId = {}, commit timestamp = {}, primary replica expiration timestamp = {}]",
                                            groupId, commitTimestamp, currentPrimaryReplica.getExpirationTime());

                        }
                    });
        }

        return allOf(verificationFutures);
    }

    private static class TxContext {
        volatile long inflights = 0; // Updated under lock.
        final CompletableFuture<Void> waitRepFut = new CompletableFuture<>();
        volatile CompletableFuture<Void> finishFut = null;

        @Override
        public String toString() {
            return "TxContext [inflights=" + inflights + ", waitRepFut=" + waitRepFut + ", finishFut=" + finishFut + ']';
        }
    }
}

