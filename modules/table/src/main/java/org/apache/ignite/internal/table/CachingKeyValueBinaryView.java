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

package org.apache.ignite.internal.table;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.cache.CacheStore;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

public class CachingKeyValueBinaryView implements KeyValueView<Tuple, Tuple> {
    private final KeyValueView<Tuple, Tuple> view;
    private final CacheStore store;
    private final InternalTable tbl;

    public CachingKeyValueBinaryView(InternalTable tbl, KeyValueView<Tuple, Tuple> view, CacheStore store) {
        this.tbl = tbl;
        this.view = view;
        this.store = store;
    }
    @Override
    public @Nullable Tuple get(@Nullable Transaction tx, Tuple key) {
        return getAsync(tx, key).join();
    }

    @Override
    public CompletableFuture<Tuple> getAsync(@Nullable Transaction tx, Tuple key) {
        InternalTransaction tx0 = (InternalTransaction) tx;

        boolean implicit = tx0 == null;

        if (implicit) {
            tx0 = tbl.beginExternal();
        }

        Optional<Tuple> val = tx0.enlistStore(store).get(key);

        if (val != null) {
            return val.isEmpty() ? CompletableFutures.nullCompletedFuture() : CompletableFuture.completedFuture(val.get());
        }

        InternalTransaction finalTx = tx0;

        return view.getAsync(tx0, key).thenCompose(valTup -> {
            if (valTup == null) {
                return store.load(key).thenCompose(val0 -> {
                    if (val0 == null) {
                        return CompletableFutures.nullCompletedFuture();
                    }

                    // Notice: lost update is possible on automatic lock retry, need to retry whole transaction.
                    // TODO putIfAbsent
                    return view.putAsync(finalTx, key, val0).thenApply(ignored -> val0);
                });
            }

            return CompletableFuture.completedFuture(valTup);
        }).thenApply(ret -> {
            if (implicit) {
                finalTx.commitAsync(); // Out of async chain op, will do cleanup only.
            }

            return ret;
        });
    }

    @Override
    public void put(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        putAsync(tx, key, val).join(); // Do not unwrap txn, it's done in a delegate.
    }

    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        InternalTransaction tx0 = (InternalTransaction) tx;

        boolean implicit = tx0 == null;

        if (implicit) {
            tx0 = tbl.beginExternal();
        }

        InternalTransaction finalTx = tx0;

        return view.putAsync(tx0, key, val).thenApply(ignored -> {
            finalTx.enlistStore(store).put(key, Optional.ofNullable(val));

            return null;
        }).thenApply(ret -> {
            if (implicit) {
                finalTx.commitAsync(); // Out of async chain op, will do cleanup only.
            }

            return null;
        });
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, Tuple key) {
        InternalTransaction tx0 = (InternalTransaction) tx;

        boolean implicit = tx0 == null;

        if (implicit) {
            tx0 = tbl.beginExternal();
        }

        Optional<Tuple> val = tx0.enlistStore(store).get(key);

        if (val != null && val.isEmpty()) {
            return CompletableFutures.falseCompletedFuture(); // Already removed.
        }

        InternalTransaction finalTx = tx0;

        return view.removeAsync(tx0, key).thenApply(removed -> {
            if (removed) {
                finalTx.enlistStore(store).put(key, Optional.empty());
            }

            return removed;
        }).thenApply(ret -> {
            if (implicit) {
                finalTx.commitAsync(); // Out of async chain op, will do cleanup only.
            }

            return ret;
        });
    }

    @Override
    public CompletableFuture<Void> streamData(Publisher<Entry<Tuple, Tuple>> publisher, @Nullable DataStreamerOptions options) {
        return null;
    }

    @Override
    public NullableValue<Tuple> getNullable(@Nullable Transaction tx, Tuple key) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAsync(@Nullable Transaction tx, Tuple key) {
        return null;
    }

    @Override
    public @Nullable Tuple getOrDefault(@Nullable Transaction tx, Tuple key, @Nullable Tuple defaultValue) {
        return null;
    }

    @Override
    public CompletableFuture<Tuple> getOrDefaultAsync(@Nullable Transaction tx, Tuple key, @Nullable Tuple defaultValue) {
        return null;
    }

    @Override
    public Map<Tuple, Tuple> getAll(@Nullable Transaction tx, Collection<Tuple> keys) {
        return null;
    }

    @Override
    public CompletableFuture<Map<Tuple, Tuple>> getAllAsync(@Nullable Transaction tx, Collection<Tuple> keys) {
        return null;
    }

    @Override
    public boolean contains(@Nullable Transaction tx, Tuple key) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, Tuple key) {
        return null;
    }

    @Override
    public void putAll(@Nullable Transaction tx, Map<Tuple, Tuple> pairs) {

    }

    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<Tuple, Tuple> pairs) {
        return null;
    }

    @Override
    public @Nullable Tuple getAndPut(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return null;
    }

    @Override
    public CompletableFuture<Tuple> getAndPutAsync(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return null;
    }

    @Override
    public NullableValue<Tuple> getNullableAndPut(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAndPutAsync(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return null;
    }

    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return null;
    }

    @Override
    public boolean remove(@Nullable Transaction tx, Tuple key) {
        return false;
    }

    @Override
    public boolean remove(@Nullable Transaction tx, Tuple key, Tuple val) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, Tuple key, Tuple val) {
        return null;
    }

    @Override
    public Collection<Tuple> removeAll(@Nullable Transaction tx, Collection<Tuple> keys) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<Tuple>> removeAllAsync(@Nullable Transaction tx, Collection<Tuple> keys) {
        return null;
    }

    @Override
    public @Nullable Tuple getAndRemove(@Nullable Transaction tx, Tuple key) {
        return null;
    }

    @Override
    public CompletableFuture<Tuple> getAndRemoveAsync(@Nullable Transaction tx, Tuple key) {
        return null;
    }

    @Override
    public NullableValue<Tuple> getNullableAndRemove(@Nullable Transaction tx, Tuple key) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAndRemoveAsync(@Nullable Transaction tx, Tuple key) {
        return null;
    }

    @Override
    public boolean replace(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return false;
    }

    @Override
    public boolean replace(@Nullable Transaction tx, Tuple key, Tuple oldValue, @Nullable Tuple newValue) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, Tuple key, @Nullable Tuple oldVal, @Nullable Tuple newVal) {
        return null;
    }

    @Override
    public @Nullable Tuple getAndReplace(@Nullable Transaction tx, @Nullable Tuple key, @Nullable Tuple val) {
        return null;
    }

    @Override
    public CompletableFuture<Tuple> getAndReplaceAsync(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return null;
    }

    @Override
    public NullableValue<Tuple> getNullableAndReplace(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<Tuple>> getNullableAndReplaceAsync(@Nullable Transaction tx, Tuple key, @Nullable Tuple val) {
        return null;
    }
}
