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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager implementation.
 *
 * <p>Uses 2PC for atomic commitment and 2PL for concurrency control.
 */
public class TxManagerImpl implements TxManager {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    private ReplicaService replicaService;

    /** Lock manager. */
    private final LockManager lockManager;

    // TODO: IGNITE-17638 Consider using Txn state map instead of states.
    /** The storage for tx states. */
    @TestOnly
    private final ConcurrentHashMap<UUID, TxState> states = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param lockManager Lock manager.
     */
    public TxManagerImpl(ReplicaService replicaService,  LockManager lockManager) {
        this.replicaService = replicaService;
        this.lockManager = lockManager;
    }

    /** {@inheritDoc} */
    @Override
    public InternalTransaction begin() {
        UUID txId = Timestamp.nextVersion().toUuid();

        states.put(txId, TxState.PENDING);

        return new TransactionImpl(this, txId);
    }

    /** {@inheritDoc} */
    @Override
    public TxState state(UUID txId) {
        return states.get(txId);
    }

    /** {@inheritDoc} */
    @Override
    public void forget(UUID txId) {
        states.remove(txId);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> commitAsync(UUID txId) {
        if (changeState(txId, TxState.PENDING, TxState.COMMITED) || state(txId) == TxState.COMMITED) {
            unlockAll(txId);

            return completedFuture(null);
        }

        return failedFuture(new TransactionException(format("Failed to commit a transaction [txId={}, state={}]", txId, state(txId))));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> rollbackAsync(UUID txId) {
        if (changeState(txId, TxState.PENDING, TxState.ABORTED) || state(txId) == TxState.ABORTED) {
            unlockAll(txId);

            return completedFuture(null);
        }

        return failedFuture(new TransactionException(format("Failed to rollback a transaction [txId={}, state={}]", txId, state(txId))));
    }

    /**
     * Unlocks all locks for the timestamp.
     *
     * @param txId Transaction id.
     */
    private void unlockAll(UUID txId) {
        lockManager.locks(txId).forEachRemaining(lockManager::release);
    }

    /** {@inheritDoc} */
    @Override
    // TODO: IGNITE-17638 TestOnly code, let's consider using Txn state map instead of states.
    public boolean changeState(UUID txId, TxState before, TxState after) {
        return states.compute(txId, (k, v) -> {
            if (v == null || v == before) {
                return after;
            } else {
                return v;
            }
        }) == after;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Lock> writeLock(UUID lockId, ByteBuffer keyData, UUID txId) {
        // TODO IGNITE-15933 process tx messages in striped fasion to avoid races. But locks can be acquired from any thread !
        TxState state = state(txId);

        if (state != null && state != TxState.PENDING) {
            return failedFuture(new TransactionException(
                    "The operation is attempted for completed transaction"));
        }

        // Should rollback tx on lock error.
        LockKey lockKey = new LockKey(lockId, keyData);

        return lockManager.acquire(txId, lockKey, LockMode.X);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Lock> readLock(UUID lockId, ByteBuffer keyData, UUID txId) {
        TxState state = state(txId);

        if (state != null && state != TxState.PENDING) {
            return failedFuture(new TransactionException(
                    "The operation is attempted for completed transaction"));
        }

        LockKey lockKey = new LockKey(lockId, keyData);

        return lockManager.acquire(txId, lockKey, LockMode.S);
    }

    /** {@inheritDoc} */
    @Override
    public TxState getOrCreateTransaction(UUID txId) {
        return states.putIfAbsent(txId, TxState.PENDING);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> finish(
            ClusterNode recipientNode,
            Long term,
            boolean commit,
            Map<ClusterNode, List<IgniteBiTuple<String, Long>>> groups,
            UUID txId
    ) {
        assert groups != null && !groups.isEmpty();

        TxFinishReplicaRequest req = FACTORY.txFinishReplicaRequest()
                .txId(txId)
                .groupId(groups.values().iterator().next().get(0).get1())
                .groups(groups)
                .commit(commit)
                .term(term)
                .build();

        return replicaService.invoke(recipientNode, req)
                // TODO: IGNITE-17638 TestOnly code, let's consider using Txn state map instead of states.
                .thenRun(() -> changeState(txId, TxState.PENDING, commit ? TxState.COMMITED : TxState.ABORTED));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> cleanup(
            ClusterNode recipientNode,
            List<IgniteBiTuple<String, Long>> replicationGroupIds,
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

    /** {@inheritDoc} */
    @Override
    public int finished() {
        return (int) states.entrySet().stream().filter(e -> e.getValue() == TxState.COMMITED || e.getValue() == TxState.ABORTED).count();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public LockManager lockManager() {
        return lockManager;
    }
}
