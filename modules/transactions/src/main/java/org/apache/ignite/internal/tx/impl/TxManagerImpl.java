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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.tx.TransactionException;

/**
 * TODO asch do we need the interface ?
 */
public class TxManagerImpl implements TxManager {
    /** */
    private static final CompletableFuture<Void> DONE_FUT = CompletableFuture.completedFuture(null);

    /** */
    private final ClusterService clusterService;

    /** */
    private final LockManager lockManager;

    /** The storage for tx states. TODO asch use Storage for states */
    private final ConcurrentHashMap<Timestamp, TxState> states = new ConcurrentHashMap<>();

    /** The storage for tx locks. Each key is mapped to lock type: true for read. TODO asch use Storage for locks */
    private final ConcurrentHashMap<Timestamp, Map<ByteArray, Boolean>> locks = new ConcurrentHashMap<>();

    /**
     * @param clusterService Cluster service.
     * @param lockManager Lock manager.
     */
    public TxManagerImpl(ClusterService clusterService, LockManager lockManager) {
        this.clusterService = clusterService;
        this.lockManager = lockManager;
    }

    @Override public InternalTransaction begin() {
        Timestamp ts = Timestamp.nextVersion();

        states.put(ts, TxState.PENDING);

        return new TransactionImpl(this, ts);
    }

    @Override public TxState state(Timestamp ts) {
        return states.get(ts);
    }

    @Override public void forget(Timestamp ts) {
        states.remove(ts);
    }

    @Override public CompletableFuture<Void> commitAsync(InternalTransaction tx) {
        if (changeState(tx.timestamp(), TxState.PENDING, TxState.COMMITED))
            unlockAll(tx);

        return CompletableFuture.completedFuture(null);
    }

    @Override public CompletableFuture<Void> rollbackAsync(InternalTransaction tx) {
        if (changeState(tx.timestamp(), TxState.PENDING, TxState.ABORTED))
            unlockAll(tx);

        return CompletableFuture.completedFuture(null);
    }

    /**
     * @param tx The transaction.
     */
    private void unlockAll(InternalTransaction tx) {
        Map<ByteArray, Boolean> locks = this.locks.remove(tx.timestamp());

        for (Map.Entry<ByteArray, Boolean> lock : locks.entrySet()) {
            try {
                if (lock.getValue()) {
                    lockManager.tryReleaseShared(lock.getKey(), tx.timestamp());
                }
                else {
                    lockManager.tryRelease(lock.getKey(), tx.timestamp());
                }
            }
            catch (LockException e) {
                assert false; // This shouldn't happen during tx finish.
            }
        }
    }

    /**
     * @param ts The timestamp.
     * @param before Before state.
     * @param after After state.
     * @return {@code} True if the state was changed.
     */
    @Override public boolean changeState(Timestamp ts, TxState before, TxState after) {
        return states.replace(ts, before, after);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> writeLock(ByteArray key, InternalTransaction tx) {
        Timestamp timestamp = tx.timestamp();

        if (state(timestamp) != TxState.PENDING)
            throw new TransactionException("The operation is attempted for completed transaction");

        // Should rollback tx on lock error.
        return lockManager.tryAcquire(key, timestamp)
            .handle((r, e) -> e != null ?
                rollbackAsync(tx).thenCompose(ignored -> CompletableFuture.failedFuture(e)) // Preserve failed state.
                : DONE_FUT)
            .thenCompose(res -> res)
            .thenAccept(ignored -> recordLock(key, timestamp, Boolean.FALSE));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> readLock(ByteArray key, InternalTransaction tx) {
        Timestamp timestamp = tx.timestamp();

        if (state(timestamp) != TxState.PENDING)
            throw new TransactionException("The operation is attempted for completed transaction");

        return lockManager.tryAcquireShared(key, timestamp)
            .handle((r, e) -> e != null ?
                rollbackAsync(tx).thenCompose(ignored -> CompletableFuture.failedFuture(e)) // Preserve failed state.
                : DONE_FUT)
            .thenCompose(res -> res)
            .thenAccept(ignore -> recordLock(key, timestamp, Boolean.TRUE));
    }

    /**
     * Records the acquired lock for further unlocking.
     *
     * @param key The key.
     * @param timestamp The tx timestamp.
     * @param read Read lock.
     */
    private void recordLock(ByteArray key, Timestamp timestamp, Boolean read) {
        locks.compute(timestamp, new BiFunction<Timestamp, Map<ByteArray, Boolean>, Map<ByteArray, Boolean>>() {
            @Override public Map<ByteArray, Boolean> apply(Timestamp timestamp, Map<ByteArray, Boolean> map) {
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

    @Override public void start() {
        // No-op.
    }

    @Override public void stop() throws Exception {
        // No-op.
    }
}
