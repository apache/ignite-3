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

package org.apache.ignite.internal.tx.storage.state.test;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_REBALANCE_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_STOPPED_ERR;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.UnsignedUuidComparator;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of {@link TxStateStorage} based on {@link ConcurrentSkipListMap}.
 */
public class TestTxStateStorage implements TxStateStorage {
    @IgniteToStringInclude
    private final ConcurrentSkipListMap<UUID, TxMeta> storage = new ConcurrentSkipListMap<>(new UnsignedUuidComparator());

    private volatile long lastAppliedIndex;

    private volatile long lastAppliedTerm;

    private final AtomicReference<CompletableFuture<Void>> rebalanceFutureReference = new AtomicReference<>();

    private volatile boolean closed;

    @Override
    @Nullable
    public TxMeta get(UUID txId) {
        checkStorageClosedOrInProgressOfRebalance();

        return storage.get(txId);
    }

    @Override
    public void putForRebalance(UUID txId, TxMeta txMeta) {
        checkStorageClosed();

        storage.put(txId, txMeta);
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
        checkStorageClosed();

        TxMeta old = storage.get(txId);

        boolean result;

        if (old == null && txStateExpected == null) {
            TxMeta oldMeta = storage.putIfAbsent(txId, txMeta);

            result = oldMeta == null;
        } else {
            if (old != null) {
                if (old.txState() == txStateExpected) {
                    result = storage.replace(txId, old, txMeta);
                } else {
                    return old.txState() == txMeta.txState() && Objects.equals(old.commitTimestamp(), txMeta.commitTimestamp());
                }
            } else {
                result = false;
            }
        }

        if (rebalanceFutureReference.get() == null) {
            lastAppliedIndex = commandIndex;
            lastAppliedTerm = commandTerm;
        }

        return result;
    }

    @Override
    public void remove(UUID txId, long commandIndex, long commandTerm) {
        checkStorageClosedOrInProgressOfRebalance();

        storage.remove(txId);

        if (rebalanceFutureReference.get() == null) {
            lastAppliedIndex = commandIndex;
            lastAppliedTerm = commandTerm;
        }
    }

    @Override
    public void removeAll(Collection<UUID> txIds, long commandIndex, long commandTerm) {
        requireNonNull(txIds, "Collection of the transaction IDs intended for removal cannot be null.");

        checkStorageClosedOrInProgressOfRebalance();

        for (UUID txId : txIds) {
            storage.remove(txId);
        }

        if (rebalanceFutureReference.get() == null) {
            lastAppliedIndex = commandIndex;
            lastAppliedTerm = commandTerm;
        }
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        checkStorageClosedOrInProgressOfRebalance();

        Iterator<IgniteBiTuple<UUID, TxMeta>> iterator = storage.entrySet().stream()
                .map(e -> new IgniteBiTuple<>(e.getKey(), e.getValue()))
                .collect(toList())
                .iterator();

        return new Cursor<>() {
            @Override
            public void close() {
                // No-op.
            }

            @Override
            public boolean hasNext() {
                checkStorageClosedOrInProgressOfRebalance();

                return iterator.hasNext();
            }

            @Override
            public IgniteBiTuple<UUID, TxMeta> next() {
                checkStorageClosedOrInProgressOfRebalance();

                return iterator.next();
            }
        };
    }

    @Override
    public void destroy() {
        close();
    }

    @Override
    public CompletableFuture<Void> flush() {
        return nullCompletedFuture();
    }

    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    @Override
    public long lastAppliedTerm() {
        return lastAppliedTerm;
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        checkStorageClosedOrInProgressOfRebalance();

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }

    @Override
    public void close() {
        assert rebalanceFutureReference.get() == null;

        closed = true;

        storage.clear();
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        checkStorageClosed();

        CompletableFuture<Void> rebalanceFuture = new CompletableFuture<>();

        if (!rebalanceFutureReference.compareAndSet(null, rebalanceFuture)) {
            throwRebalanceInProgressException();
        }

        storage.clear();

        lastAppliedIndex = REBALANCE_IN_PROGRESS;
        lastAppliedTerm = REBALANCE_IN_PROGRESS;

        rebalanceFuture.complete(null);

        return rebalanceFuture;
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        checkStorageClosed();

        CompletableFuture<Void> rebalanceFuture = rebalanceFutureReference.getAndSet(null);

        if (rebalanceFuture == null) {
            return nullCompletedFuture();
        }

        return rebalanceFuture
                .whenComplete((unused, throwable) -> {
                    storage.clear();

                    lastAppliedIndex = 0;
                    lastAppliedTerm = 0;
                });
    }

    @Override
    public CompletableFuture<Void> finishRebalance(long lastAppliedIndex, long lastAppliedTerm) {
        checkStorageClosed();

        CompletableFuture<Void> rebalanceFuture = rebalanceFutureReference.getAndSet(null);

        if (rebalanceFuture == null) {
            throw new IgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, "Rebalancing has not started");
        }

        return rebalanceFuture
                .whenComplete((unused, throwable) -> lastApplied(lastAppliedIndex, lastAppliedTerm));
    }

    @Override
    public CompletableFuture<Void> clear() {
        checkStorageClosed();

        if (rebalanceFutureReference.get() != null) {
            throw new IgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, "In the process of rebalancing");
        }

        storage.clear();

        lastAppliedIndex = 0;
        lastAppliedTerm = 0;

        return nullCompletedFuture();
    }

    @Override
    public String toString() {
        return S.toString(TestTxStateStorage.class, this);
    }

    private void checkStorageInProgreesOfRebalance() {
        if (rebalanceFutureReference.get() != null) {
            throwRebalanceInProgressException();
        }
    }

    private void checkStorageClosed() {
        if (closed) {
            throwStorageClosedException();
        }
    }

    private void checkStorageClosedOrInProgressOfRebalance() {
        checkStorageClosed();

        checkStorageInProgreesOfRebalance();
    }

    private static void throwStorageClosedException() {
        throw new IgniteInternalException(TX_STATE_STORAGE_STOPPED_ERR, "Storage is closed");
    }

    private static void throwRebalanceInProgressException() {
        throw new IgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, "Rebalance is already in progress");
    }
}
