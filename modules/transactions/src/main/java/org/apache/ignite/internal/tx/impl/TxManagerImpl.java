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
import org.apache.ignite.internal.tx.message.TxFinishRequest;
import org.apache.ignite.internal.tx.message.TxFinishResponse;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.tx.TransactionException;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

/**
 * TODO asch do we need the interface ?
 */
public class TxManagerImpl implements TxManager, NetworkMessageHandler {
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

        clusterService.messagingService().addMessageHandler(TxMessageGroup.class, this);
    }

    /** {@inheritDoc} */
    @Override public InternalTransaction begin() {
        Timestamp ts = Timestamp.nextVersion();

        states.put(ts, TxState.PENDING);

        TransactionImpl transaction = new TransactionImpl(this, ts);

        transaction.enlist(clusterService.topologyService().localMember().address());

        return transaction;
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
        if (changeState(ts, TxState.PENDING, TxState.COMMITED)) {
            unlockAll(ts);

            return completedFuture(null);
        }

        return failedFuture(new TransactionException("Failed to commit a transaction"));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> rollbackAsync(Timestamp ts) {
        if (changeState(ts, TxState.PENDING, TxState.ABORTED)) {
            unlockAll(ts);

            return completedFuture(null);
        }

        return failedFuture(new TransactionException("Failed to rollback a transaction"));
    }

    /**
     * @param tx The transaction.
     */
    private void unlockAll(Timestamp ts) {
        Map<ByteArray, Boolean> locks = this.locks.remove(ts);

        if (locks == null)
            return;

        for (Map.Entry<ByteArray, Boolean> lock : locks.entrySet()) {
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
    @Override public CompletableFuture<Void> writeLock(ByteArray key, Timestamp ts) {
        if (state(ts) != TxState.PENDING)
            return failedFuture(new TransactionException("The operation is attempted for completed transaction"));

        // Should rollback tx on lock error.
        return lockManager.tryAcquire(key, ts)
            .handle(new BiFunction<Void, Throwable, CompletableFuture<Void>>() {
                @Override public CompletableFuture<Void> apply(Void r, Throwable e) {
                    if (e != null) // TODO asch add suppressed exception to rollback exception.
                        return rollbackAsync(ts).thenCompose(ignored -> failedFuture(e)); // Preserve failed state or report rollback exception.
                    else
                        return completedFuture(null).thenAccept(ignored -> recordLock(key, ts, Boolean.FALSE));
                }
            }).thenCompose(x -> x);
    }


    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> readLock(ByteArray key, Timestamp ts) {
        if (state(ts) != TxState.PENDING)
            return failedFuture(new TransactionException("The operation is attempted for completed transaction"));

        return lockManager.tryAcquireShared(key, ts)
            .handle(new BiFunction<Void, Throwable, CompletableFuture<Void>>() {
                @Override public CompletableFuture<Void> apply(Void r, Throwable e) {
                    if (e != null)
                        return rollbackAsync(ts).thenCompose(ignored -> failedFuture(e)); // Preserve failed state.
                    else
                        return completedFuture(null).thenAccept(ignored -> recordLock(key, ts, Boolean.TRUE));
                }
            }).thenCompose(x -> x);
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

    /** {@inheritDoc} */
    @Override public boolean getOrCreateTransaction(Timestamp ts) {
        return states.putIfAbsent(ts, TxState.PENDING) == null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<TxState> sendFinishMessage(NetworkAddress addr, Timestamp ts, boolean commit) {
        TxFinishRequest req = FACTORY.txFinishRequest().timestamp(ts).build();

        CompletableFuture<NetworkMessage> fut = clusterService.messagingService().invoke(addr, req, TIMEOUT);

        return fut.thenApply(resp -> ((TxFinishResponse) resp).state());
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal(NetworkAddress node) {
        return clusterService.topologyService().localMember().address().equals(node);
    }

    /** {@inheritDoc} */
    @Override public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onReceived(NetworkMessage message, NetworkAddress senderAddr, String correlationId) {
        // No-op.
        if (message instanceof TxFinishRequest) {
            TxFinishRequest req = (TxFinishRequest) message;

            if (req.commit()) {
                commitAsync(req.timestamp()).handle(new BiFunction<Void, Throwable, Void>() {
                    @Override public Void apply(Void ignored, Throwable throwable) {
                        clusterService.messagingService().send(senderAddr, FACTORY.txFinishResponse().build(), correlationId);

                        return null;
                    }
                });
            }
            else {
                rollbackAsync(req.timestamp()).handle(new BiFunction<Void, Throwable, Void>() {
                    @Override public Void apply(Void ignored, Throwable throwable) {
                        clusterService.messagingService().send(senderAddr, FACTORY.txFinishResponse().build(), correlationId);

                        return null;
                    }
                });
            }
        }
    }
}
