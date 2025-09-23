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
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_STATE_STORAGE_ERR;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageClosedException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageDestroyedException;
import org.apache.ignite.internal.tx.storage.state.TxStateStorageRebalanceException;
import org.apache.ignite.internal.tx.storage.state.UnsignedUuidComparator;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of {@link TxStatePartitionStorage} based on {@link ConcurrentSkipListMap}.
 */
public class TestTxStatePartitionStorage implements TxStatePartitionStorage {
    @IgniteToStringInclude
    private final ConcurrentSkipListMap<UUID, TxMeta> storage = new ConcurrentSkipListMap<>(new UnsignedUuidComparator());

    private volatile long lastAppliedIndex;

    private volatile long lastAppliedTerm;

    private volatile byte @Nullable [] config;

    @Nullable
    private volatile LeaseInfo leaseInfo;

    private volatile byte @Nullable [] snapshotInfo;

    private final AtomicReference<CompletableFuture<Void>> rebalanceFutureReference = new AtomicReference<>();

    private volatile boolean closed;

    private volatile boolean destroyed;

    @Override
    @Nullable
    public TxMeta get(UUID txId) {
        checkStorageClosedOrInProgressOfRebalance();

        return storage.get(txId);
    }

    @Override
    public void putForRebalance(UUID txId, TxMeta txMeta) {
        checkStorageClosedOrDestroyed();

        storage.put(txId, txMeta);
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
        checkStorageClosedOrDestroyed();

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
        destroyed = true;

        doClear();
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
        checkStorageInProgressOfRebalance();

        closed = true;

        doClear();
    }

    @Override
    public CompletableFuture<Void> startRebalance() {
        checkStorageClosedOrDestroyed();

        CompletableFuture<Void> rebalanceFuture = new CompletableFuture<>();

        if (!rebalanceFutureReference.compareAndSet(null, rebalanceFuture)) {
            throwRebalanceInProgressException();
        }

        doClear();

        lastAppliedIndex = REBALANCE_IN_PROGRESS;
        lastAppliedTerm = REBALANCE_IN_PROGRESS;

        rebalanceFuture.complete(null);

        return rebalanceFuture;
    }

    @Override
    public CompletableFuture<Void> abortRebalance() {
        checkStorageClosedOrDestroyed();

        CompletableFuture<Void> rebalanceFuture = rebalanceFutureReference.getAndSet(null);

        if (rebalanceFuture == null) {
            return nullCompletedFuture();
        }

        return rebalanceFuture
                .whenComplete((unused, throwable) -> doClear());
    }

    @Override
    public CompletableFuture<Void> finishRebalance(MvPartitionMeta partitionMeta) {
        checkStorageClosedOrDestroyed();

        CompletableFuture<Void> rebalanceFuture = rebalanceFutureReference.getAndSet(null);

        if (rebalanceFuture == null) {
            throw new TxStateStorageRebalanceException("Rebalancing has not started");
        }

        return rebalanceFuture
                .whenComplete((unused, throwable) -> {
                    lastAppliedIndex = partitionMeta.lastAppliedIndex();
                    lastAppliedTerm = partitionMeta.lastAppliedTerm();
                    config = partitionMeta.groupConfig();
                    leaseInfo = partitionMeta.leaseInfo();
                    snapshotInfo = partitionMeta.snapshotInfo();
                });
    }

    @Override
    public CompletableFuture<Void> clear() {
        checkStorageClosedOrInProgressOfRebalance();

        doClear();

        return nullCompletedFuture();
    }

    private void doClear() {
        storage.clear();

        lastAppliedIndex = 0;
        lastAppliedTerm = 0;
        config = null;
        leaseInfo = null;
        snapshotInfo = null;
    }

    @Override
    public void committedGroupConfiguration(byte[] config, long index, long term) {
        checkStorageClosedOrInProgressOfRebalance();

        this.config = config;

        lastAppliedIndex = index;
        lastAppliedTerm = term;
    }

    @Override
    public byte @Nullable [] committedGroupConfiguration() {
        return config;
    }

    @Override
    public void leaseInfo(LeaseInfo leaseInfo, long index, long term) {
        checkStorageClosedOrInProgressOfRebalance();

        this.leaseInfo = leaseInfo;

        lastAppliedIndex = index;
        lastAppliedTerm = term;
    }

    @Override
    public @Nullable LeaseInfo leaseInfo() {
        return leaseInfo;
    }

    @Override
    public void snapshotInfo(byte[] snapshotInfo) {
        checkStorageClosedOrInProgressOfRebalance();

        this.snapshotInfo = snapshotInfo;
    }

    @Override
    public byte @Nullable [] snapshotInfo() {
        return snapshotInfo;
    }

    @Override
    public String toString() {
        return S.toString(TestTxStatePartitionStorage.class, this);
    }

    private void checkStorageInProgressOfRebalance() {
        if (rebalanceFutureReference.get() != null) {
            throwRebalanceInProgressException();
        }
    }

    private void checkStorageClosedOrDestroyed() {
        if (closed) {
            throwStorageClosedException();
        }

        if (destroyed) {
            throw new TxStateStorageDestroyedException(TX_STATE_STORAGE_ERR, "Storage is destroyed");
        }
    }

    private void checkStorageClosedOrInProgressOfRebalance() {
        checkStorageClosedOrDestroyed();

        checkStorageInProgressOfRebalance();
    }

    private static void throwStorageClosedException() {
        throw new TxStateStorageClosedException("Storage is closed");
    }

    private static void throwRebalanceInProgressException() {
        throw new TxStateStorageRebalanceException("Rebalance is already in progress");
    }
}
