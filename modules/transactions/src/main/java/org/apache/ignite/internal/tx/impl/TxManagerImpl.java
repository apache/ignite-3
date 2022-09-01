/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.LockMode;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.TxFinishResponse;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.tx.TransactionException;

/**
 * A transaction manager implementation.
 *
 * <p>Uses 2PC for atomic commitment and 2PL for concurrency control.
 */
public class TxManagerImpl implements TxManager {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Cluster service. */
    protected final ClusterService clusterService;

    private ReplicaService replicaService;

    /** Lock manager. */
    private final LockManager lockManager;

    // TODO <MUTED> IGNITE-15931 use Storage for states, implement max size, implement replication.
    /** The storage for tx states. */
    private final ConcurrentHashMap<UUID, TxState> states = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     * @param lockManager Lock manager.
     */
    public TxManagerImpl(ClusterService clusterService, ReplicaService replicaService,  LockManager lockManager) {
        this.clusterService = clusterService;
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
        lockManager.locks(txId).forEachRemaining(lock -> {
            try {
                lockManager.release(lock);
            } catch (LockException e) {
                assert false; // This shouldn't happen during tx finish.
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean changeState(UUID txId, TxState before, TxState after) {
        return states.replace(txId, before, after);
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

        return lockManager.acquire(txId, lockKey, LockMode.EXCLUSIVE);
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

        return lockManager.acquire(txId, lockKey, LockMode.SHARED);
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
            boolean commit,
            TreeMap<ClusterNode, List<String>> groups,
            UUID txId
    ) {
        assert groups != null && !groups.isEmpty();

        TxFinishReplicaRequest req = FACTORY.txFinishReplicaRequest()
                .groupId(groups.firstEntry().getValue().get(0))
                .groups(groups)
                .commit(commit)
                .build();

        CompletableFuture<NetworkMessage> fut;
        try {
            fut = replicaService.invoke(recipientNode, req);
        } catch (NodeStoppingException e) {
            throw new TransactionException("Failed to finish transaction. Node is stopping.");
        } catch (Throwable t) {
            throw new TransactionException("Failed to finish transaction.");
        }

        // Submit response to a dedicated pool to avoid deadlocks. TODO: IGNITE-15389
        return fut.thenApplyAsync(resp -> ((TxFinishResponse) resp).errorMessage()).thenCompose(msg ->
                msg == null ? completedFuture(null) : failedFuture(new TransactionException(msg)));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> cleanup(
            ClusterNode recipientNode,
            List<String> replicationGroupIds,
            UUID txId,
            boolean commit,
            HybridTimestamp commitTimestamp
    ) {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-17582 Grouping replica requests.
        replicationGroupIds.forEach(groupId -> {
            try {
                replicaService.invoke(
                        recipientNode,
                        FACTORY.txCleanupReplicaRequest()
                                .groupId(groupId)
                                .txId(txId)
                                .commit(commit)
                                .commitTimestamp(commitTimestamp)
                                .build()
                );
            } catch (NodeStoppingException e) {
                throw new TransactionException("Failed to perform tx cleanup, node is stopping.");
            }
        });

        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isLocal(NetworkAddress node) {
        return clusterService.topologyService().localMember().address().equals(node);
    }

    /** {@inheritDoc} */
    @Override
    public int finished() {
        return states.size();
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
