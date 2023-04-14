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
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_READ_ONLY_CREATING_ERR;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.Timestamp;
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

    // TODO: IGNITE-17638 Consider using Txn state map instead of states.
    /** The storage for tx states. */
    @TestOnly
    private final ConcurrentHashMap<UUID, TxState> states = new ConcurrentHashMap<>();

    /** Future of a read-only transaction by its read timestamp. */
    private final ConcurrentNavigableMap<HybridTimestamp, CompletableFuture<Void>> readOnlyTxFutureByReadTs = new ConcurrentSkipListMap<>();

    /** Lower bound (exclusive) the timestamps for starting new read-only transactions, {@code null} means there is no lower bound. */
    private final AtomicReference<HybridTimestamp> lowerBoundTsToStartNewReadOnlyTx = new AtomicReference<>();

    /**
     * The constructor.
     *
     * @param replicaService Replica service.
     * @param lockManager Lock manager.
     * @param clock A hybrid logical clock.
     */
    public TxManagerImpl(ReplicaService replicaService, LockManager lockManager, HybridClock clock) {
        this.replicaService = replicaService;
        this.lockManager = lockManager;
        this.clock = clock;
    }

    @Override
    public InternalTransaction begin() {
        return begin(false);
    }

    @Override
    public InternalTransaction begin(boolean readOnly) {
        UUID txId = Timestamp.nextVersion().toUuid();

        if (!readOnly) {
            return new ReadWriteTransactionImpl(this, txId);
        }

        HybridTimestamp readTimestamp = clock.now();

        readOnlyTxFutureByReadTs.compute(readTimestamp, (timestamp, readOnlyTxFuture) -> {
            assert readOnlyTxFuture == null : "previous transaction has not completed yet: " + readTimestamp;

            HybridTimestamp lowerBound = lowerBoundTsToStartNewReadOnlyTx.get();

            if (lowerBound != null && readTimestamp.compareTo(lowerBound) <= 0) {
                throw new IgniteInternalException(
                        TX_READ_ONLY_CREATING_ERR,
                        "Timestamp read-only transaction must be greater than the lower bound: [txTimestamp={}, lowerBound={}]",
                        readTimestamp, lowerBound
                );
            }

            return new CompletableFuture<>();
        });

        return new ReadOnlyTransactionImpl(this, txId, readTimestamp);
    }

    @Override
    public TxState state(UUID txId) {
        return states.get(txId);
    }

    @Override
    public boolean changeState(UUID txId, TxState before, TxState after) {
        return states.compute(txId, (k, v) -> {
            if (v == before) {
                return after;
            } else {
                return v;
            }
        }) == after;
    }

    @Override
    public CompletableFuture<Void> finish(
            ReplicationGroupId commitPartition,
            ClusterNode recipientNode,
            Long term,
            boolean commit,
            Map<ClusterNode, List<IgniteBiTuple<ReplicationGroupId, Long>>> groups,
            UUID txId
    ) {
        assert groups != null && !groups.isEmpty();

        HybridTimestamp commitTimestamp = commit ? clock.now() : null;

        TxFinishReplicaRequest req = FACTORY.txFinishReplicaRequest()
                .txId(txId)
                .groupId(commitPartition)
                .groups(groups)
                .commit(commit)
                .commitTimestamp(commitTimestamp)
                .term(term)
                .build();

        return replicaService.invoke(recipientNode, req)
                // TODO: IGNITE-17638 TestOnly code, let's consider using Txn state map instead of states.
                .thenRun(() -> changeState(txId, null, commit ? TxState.COMMITED : TxState.ABORTED));
    }

    @Override
    public CompletableFuture<Void> cleanup(
            ClusterNode recipientNode,
            List<IgniteBiTuple<ReplicationGroupId, Long>> replicationGroupIds,
            UUID txId,
            boolean commit,
            HybridTimestamp commitTimestamp
    ) {
        var cleanupFutures = new CompletableFuture[replicationGroupIds.size()];

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17582 Grouping replica requests.
        for (int i = 0; i < replicationGroupIds.size(); i++) {
            cleanupFutures[i] = replicaService.invoke(
                    recipientNode,
                    FACTORY.txCleanupReplicaRequest()
                            .groupId(replicationGroupIds.get(i).get1())
                            .txId(txId)
                            .commit(commit)
                            .commitTimestamp(commitTimestamp)
                            .term(replicationGroupIds.get(i).get2())
                            .build()
            );
        }

        return allOf(cleanupFutures);
    }

    @Override
    public int finished() {
        return (int) states.entrySet().stream().filter(e -> e.getValue() == TxState.COMMITED || e.getValue() == TxState.ABORTED).count();
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

    void completeReadOnlyTransactionFuture(HybridTimestamp txTimestamp) {
        CompletableFuture<Void> readOnlyTxFuture = readOnlyTxFutureByReadTs.remove(txTimestamp);

        assert readOnlyTxFuture != null : txTimestamp;

        readOnlyTxFuture.complete(null);
    }

    @Override
    public void updateLowerBoundToStartNewReadOnlyTransaction(@Nullable HybridTimestamp lowerBound) {
        lowerBoundTsToStartNewReadOnlyTx.set(lowerBound);
    }

    @Override
    public CompletableFuture<Void> getFutureAllReadOnlyTransactions(HybridTimestamp timestamp) {
        List<CompletableFuture<Void>> readOnlyTxFutures = List.copyOf(readOnlyTxFutureByReadTs.headMap(timestamp, true).values());

        return allOf(readOnlyTxFutures.toArray(CompletableFuture[]::new));
    }
}
