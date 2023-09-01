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
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_READ_ONLY_TOO_OLD_ERR;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager implementation.
 *
 * <p>Uses 2PC for atomic commitment and 2PL for concurrency control.
 */
public class TxManagerImpl implements TxManager {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    private final ReplicaService replicaService;

    /** Lock manager. */
    private final LockManager lockManager;

    /** A hybrid logical clock. */
    private final HybridClock clock;

    /** Generates transaction IDs. */
    private final TransactionIdGenerator transactionIdGenerator;

    // TODO: IGNITE-20033 Consider using Txn state map instead of states.
    /** The storage for tx states. */
    @TestOnly
    private final ConcurrentHashMap<UUID, TxState> states = new ConcurrentHashMap<>();

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
            TransactionIdGenerator transactionIdGenerator
    ) {
        this.replicaService = replicaService;
        this.lockManager = lockManager;
        this.clock = clock;
        this.transactionIdGenerator = transactionIdGenerator;
    }

    @WithSpan
    @Override
    public InternalTransaction begin() {
        return begin(false, null);
    }

    @WithSpan
    @Override
    public InternalTransaction begin(boolean readOnly, @Nullable HybridTimestamp observableTimestamp) {
        assert readOnly || observableTimestamp == null : "Observable timestamp is applicable just for read-only transactions.";

        HybridTimestamp beginTimestamp = clock.now();
        UUID txId = transactionIdGenerator.transactionIdFor(beginTimestamp);
        changeState(txId, null, PENDING);

        if (!readOnly) {
            return new ReadWriteTransactionImpl(this, txId);
        }

        HybridTimestamp readTimestamp = observableTimestamp != null
                ? HybridTimestamp.max(observableTimestamp, currentReadTimestamp())
                : clock.now();

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

            return new ReadOnlyTransactionImpl(this, txId, readTimestamp);
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
        HybridTimestamp now = clock.now();

        return new HybridTimestamp(now.getPhysical()
                - ReplicaManager.IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS
                - HybridTimestamp.CLOCK_SKEW,
                0
        );
    }

    @Override
    public TxState state(UUID txId) {
        return states.get(txId);
    }

    @WithSpan
    @Override
    public void changeState(UUID txId, TxState before, TxState after) {
        TxState computeResult = states.compute(txId, (k, v) -> {
            if (v == before) {
                return after;
            } else {
                return v;
            }
        });

        assert computeResult == after : "Unable to change transaction state, expected = [" + before + "],"
                + " got = [" + computeResult + "], state to set = [" + after + ']';
    }

    @WithSpan
    @Override
    public CompletableFuture<Void> finish(
            TablePartitionId commitPartition,
            ClusterNode recipientNode,
            Long term,
            boolean commit,
            Map<ClusterNode, List<IgniteBiTuple<TablePartitionId, Long>>> groups,
            UUID txId
    ) {
        assert groups != null && !groups.isEmpty();

        HybridTimestamp commitTimestamp = commit ? clock.now() : null;

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
                // TODO: IGNITE-20033 TestOnly code, let's consider using Txn state map instead of states.
                .thenRun(() -> changeState(txId, PENDING, commit ? COMMITED : ABORTED));
    }

    @WithSpan
    @Override
    public CompletableFuture<Void> cleanup(
            ClusterNode recipientNode,
            List<IgniteBiTuple<TablePartitionId, Long>> tablePartitionIds,
            UUID txId,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        var cleanupFutures = new CompletableFuture[tablePartitionIds.size()];

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17582 Grouping replica requests.
        for (int i = 0; i < tablePartitionIds.size(); i++) {
            cleanupFutures[i] = replicaService.invoke(
                    recipientNode,
                    FACTORY.txCleanupReplicaRequest()
                            .groupId(tablePartitionIds.get(i).get1())
                            .timestampLong(clock.nowLong())
                            .txId(txId)
                            .commit(commit)
                            .commitTimestampLong(hybridTimestampToLong(commitTimestamp))
                            .term(tablePartitionIds.get(i).get2())
                            .build()
            );
        }

        return allOf(cleanupFutures);
    }

    @Override
    public int finished() {
        return (int) states.entrySet().stream().filter(e -> e.getValue() == COMMITED || e.getValue() == ABORTED).count();
    }

    @Override
    public int pending() {
        return (int) states.entrySet().stream().filter(e -> e.getValue() == PENDING).count();
    }

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void stop() {
        // No-op.
    }

    @Override
    public LockManager lockManager() {
        return lockManager;
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
}
