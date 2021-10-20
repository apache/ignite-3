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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.TxFinishRequest;
import org.apache.ignite.internal.tx.message.TxFinishResponse;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

/**
 * TODO asch do we need the interface ?
 */
public class TxManagerImpl implements TxManager {
    /** Factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** */
    private static final int TIMEOUT = 5_000;

    /** */
    private static final CompletableFuture<Void> DONE_FUT = completedFuture(null);

    /** */
    private final ClusterService clusterService;

    /** */
    private final LockManager lockManager;

    /** The storage for tx states. TODO asch use Storage for states, implement max size */
    private final ConcurrentHashMap<Timestamp, TxState> states = new ConcurrentHashMap<>();

    /** The storage for tx locks. Each key is mapped to lock type where true is for read. TODO asch use Storage for locks */
    private final ConcurrentHashMap<Timestamp, Map<TableLockKey, Boolean>> locks = new ConcurrentHashMap<>();

    /** */
    private ThreadLocal<InternalTransaction> threadCtx = new ThreadLocal<>();

    /**
     * @param clusterService Cluster service.
     * @param lockManager Lock manager.
     */
    public TxManagerImpl(ClusterService clusterService, LockManager lockManager) {
        this.clusterService = clusterService;
        this.lockManager = lockManager;
    }

    /** {@inheritDoc} */
    @Override public InternalTransaction begin() {
        Timestamp ts = Timestamp.nextVersion();

        states.put(ts, TxState.PENDING);

        return new TransactionImpl(this, ts, clusterService.topologyService().localMember().address());
    }

    /** {@inheritDoc} */
    @Override public TxState state(Timestamp ts) {
        return states.get(ts);
    }

    /** {@inheritDoc} */
    @Override public void forget(Timestamp ts) {
        states.remove(ts);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> commitAsync(Timestamp ts) {
        // TODO asch remove async
        if (changeState(ts, TxState.PENDING, TxState.COMMITED) || state(ts) == TxState.COMMITED) {
            unlockAll(ts);

            return completedFuture(null);
        }

        return failedFuture(new TransactionException(LoggerMessageHelper.format("Failed to commit a transaction [ts={}, " +
            "state={}]", ts, state(ts))));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> rollbackAsync(Timestamp ts) {
        // TODO asch remove async
        // TODO asch split to tx coordinator and tx manager ?
        if (changeState(ts, TxState.PENDING, TxState.ABORTED) || state(ts) == TxState.ABORTED) {
            unlockAll(ts);

            return completedFuture(null);
        }

        return failedFuture(new TransactionException(LoggerMessageHelper.format("Failed to rollback a transaction [ts={}, " +
            "state={}]", ts, state(ts))));
    }

    /**
     * @param tx The transaction.
     */
    private void unlockAll(Timestamp ts) {
        Map<TableLockKey, Boolean> locks = this.locks.remove(ts);

        if (locks == null)
            return;

        for (Map.Entry<TableLockKey, Boolean> lock : locks.entrySet()) {
            try {
                if (lock.getValue()) {
                    lockManager.tryReleaseShared(lock.getKey(), ts);
                }
                else {
                    lockManager.tryRelease(lock.getKey(), ts);
                }
            }
            catch (LockException e) {
                assert false; // This shouldn't happen during tx finish.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean changeState(Timestamp ts, TxState before, TxState after) {
        return states.replace(ts, before, after);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> writeLock(IgniteUuid tableId, ByteBuffer keyData, Timestamp ts) {
        // TODO asch process tx messages in striped fasion to avoid races. But locks can be acquired from any thread !
        if (state(ts) != TxState.PENDING)
            return failedFuture(new TransactionException("The operation is attempted for completed transaction"));

        // Should rollback tx on lock error.
        TableLockKey key = new TableLockKey(tableId, keyData);

        return lockManager.tryAcquire(key, ts).thenAccept(ignored -> recordLock(key, ts, Boolean.FALSE));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> readLock(IgniteUuid tableId, ByteBuffer keyData, Timestamp ts) {
        if (state(ts) != TxState.PENDING)
            return failedFuture(new TransactionException("The operation is attempted for completed transaction"));

        TableLockKey key = new TableLockKey(tableId, keyData);

        return lockManager.tryAcquireShared(key, ts).thenAccept(ignored -> recordLock(key, ts, Boolean.TRUE));
    }

    /**
     * Records the acquired lock for further unlocking.
     *
     * @param key The key.
     * @param timestamp The tx timestamp.
     * @param read Read lock.
     */
    private void recordLock(TableLockKey key, Timestamp timestamp, Boolean read) {
        locks.compute(timestamp, new BiFunction<Timestamp, Map<TableLockKey, Boolean>, Map<TableLockKey, Boolean>>() {
            @Override public Map<TableLockKey, Boolean> apply(Timestamp timestamp, Map<TableLockKey, Boolean> map) {
                if (map == null)
                    map = new HashMap<>();

                Boolean mode = map.get(key);

                if (mode == null)
                    map.put(key, read);
                else if (read == Boolean.FALSE && mode == Boolean.TRUE) // Override read lock.
                    map.put(key, Boolean.FALSE);

                return map;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean getOrCreateTransaction(Timestamp ts) {
        // TODO asch track originator or use broadcast recovery ?
        return states.putIfAbsent(ts, TxState.PENDING) == null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> finishRemote(
        NetworkAddress addr,
        Timestamp ts,
        boolean commit,
        Set<String> groupIds
    ) {
        assert groupIds != null && !groupIds.isEmpty();

        TxFinishRequest req = FACTORY.txFinishRequest().timestamp(ts).partitions(groupIds).commit(commit).build();

        CompletableFuture<NetworkMessage> fut = clusterService.messagingService().invoke(addr, req, TIMEOUT);

        return fut.thenApply(resp -> ((TxFinishResponse) resp).errorMessage()).thenCompose(msg ->
            msg == null ? completedFuture(null) : failedFuture(new TransactionException(msg)));
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal(NetworkAddress node) {
        return clusterService.topologyService().localMember().address().equals(node);
    }

    @Override public void setTx(InternalTransaction tx) {
        threadCtx.set(tx);
    }

    @Override @Nullable public InternalTransaction tx() throws TransactionException {
        InternalTransaction tx = threadCtx.get();

        if (tx != null && tx.thread() != Thread.currentThread())
            throw new TransactionException("Transactional operation is attempted from another thread");

        return tx;
    }

    /** {@inheritDoc} */
    @Override public void clearTx() {
        threadCtx.remove();
    }

    /** {@inheritDoc} */
    @Override public int finished() {
        return states.size();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        // No-op.
    }

    /** */
    private static class TableLockKey {
        /** */
        private final IgniteUuid tableId;

        /** */
        private final ByteBuffer key;

        /**
         * @param tableId Table ID.
         * @param key The key.
         */
        TableLockKey(IgniteUuid tableId, ByteBuffer key) {
            this.tableId = tableId;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TableLockKey key1 = (TableLockKey) o;
            return tableId.equals(key1.tableId) &&
                key.equals(key1.key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(tableId, key);
        }
    }

    /**
     * @return The lock manager.
     */
    @TestOnly
    public LockManager getLockManager() {
        return lockManager;
    }
}
