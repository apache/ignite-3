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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_REBALANCE_ERR;

import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.UnsignedUuidComparator;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of {@link TxStateStorage} based on {@link ConcurrentSkipListMap}.
 */
public class TestTxStateStorage implements TxStateStorage {
    private final ConcurrentSkipListMap<UUID, TxMeta> storage = new ConcurrentSkipListMap<>(new UnsignedUuidComparator());

    private volatile long lastAppliedIndex;

    private volatile long lastAppliedTerm;

    private final AtomicReference<CompletableFuture<Void>> rebalanceFutureReference = new AtomicReference<>();

    @Override
    @Nullable
    public TxMeta get(UUID txId) {
        checkRebalanceInProgress();

        return storage.get(txId);
    }

    @Override
    public void put(UUID txId, TxMeta txMeta) {
        storage.put(txId, txMeta);
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
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
    public void remove(UUID txId) {
        storage.remove(txId);
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        checkRebalanceInProgress();

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
                assert rebalanceFutureReference.get() == null;

                return iterator.hasNext();
            }

            @Override
            public IgniteBiTuple<UUID, TxMeta> next() {
                assert rebalanceFutureReference.get() == null;

                return iterator.next();
            }
        };
    }

    @Override
    public void destroy() {
        close();

        storage.clear();
    }

    @Override
    public CompletableFuture<Void> flush() {
        return completedFuture(null);
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
        checkRebalanceInProgress();

        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm = lastAppliedTerm;
    }

    @Override
    public long persistedIndex() {
        return lastAppliedIndex;
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
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
        CompletableFuture<Void> rebalanceFuture = rebalanceFutureReference.getAndSet(null);

        if (rebalanceFuture == null) {
            return completedFuture(null);
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
        CompletableFuture<Void> rebalanceFuture = rebalanceFutureReference.getAndSet(null);

        if (rebalanceFuture == null) {
            throw new IgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, "Rebalancing has not started");
        }

        return rebalanceFuture
                .whenComplete((unused, throwable) -> lastApplied(lastAppliedIndex, lastAppliedTerm));
    }

    private void checkRebalanceInProgress() {
        if (rebalanceFutureReference.get() != null) {
            throwRebalanceInProgressException();
        }
    }

    private static void throwRebalanceInProgressException() {
        throw new IgniteInternalException(TX_STATE_STORAGE_REBALANCE_ERR, "Rebalance is already in progress");
    }
}
