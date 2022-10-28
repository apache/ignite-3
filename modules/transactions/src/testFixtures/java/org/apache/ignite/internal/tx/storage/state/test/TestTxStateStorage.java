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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.configuration.storage.StorageException;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.UnsignedUuidComparator;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/**
 * Test implementation of {@link TxStateStorage} based on {@link ConcurrentSkipListMap}.
 */
public class TestTxStateStorage implements TxStateStorage {
    /** Storage. */
    private final ConcurrentSkipListMap<UUID, TxMeta> storage = new ConcurrentSkipListMap<>(new UnsignedUuidComparator());

    private volatile long lastAppliedIndex;

    @Override
    public TxMeta get(UUID txId) {
        return storage.get(txId);
    }

    @Override
    public void put(UUID txId, TxMeta txMeta) {
        storage.put(txId, txMeta);
    }

    @Override
    public boolean compareAndSet(UUID txId, TxState txStateExpected, @NotNull TxMeta txMeta, long commandIndex) {
        while (true) {
            TxMeta old = storage.get(txId);

            if (old == null && txStateExpected == null) {
                TxMeta oldMeta = storage.putIfAbsent(txId, txMeta);
                if (oldMeta == null) {
                    return true;
                } else {
                    return false;
                }
            } else if (old != null) {
                if (old.txState() == txStateExpected) {
                    if (storage.replace(txId, old, txMeta)) {
                        return true;
                    }
                } else {
                    return old.txState() == txMeta.txState() && (
                            (old.commitTimestamp() == null && txMeta.commitTimestamp() == null)
                                    || old.commitTimestamp().equals(txMeta.commitTimestamp()));
                }
            }
        }
    }

    @Override
    public void remove(UUID txId) {
        storage.remove(txId);
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        List<IgniteBiTuple<UUID, TxMeta>> copy = storage.entrySet().stream()
                .map(e -> new IgniteBiTuple<>(e.getKey(), e.getValue()))
                .collect(toList());

        return Cursor.fromIterable(copy);
    }

    @Override
    public void destroy() {
        try {
            close();

            storage.clear();
        } catch (Exception e) {
            throw new StorageException("Failed to destroy the transaction state storage", e);
        }
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
    public void lastAppliedIndex(long lastAppliedIndex) {
        this.lastAppliedIndex = lastAppliedIndex;
    }

    @Override
    public long persistedIndex() {
        return lastAppliedIndex;
    }

    @Override
    public void close() throws Exception {
        // No-op.
    }
}
