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
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

public class CachingKeyValueBinaryView<K, V> implements KeyValueView<K, V> {
    private final KeyValueView<K, V> view;
    private final CacheStore<K, V> store;
    private final InternalTable tbl;

    public CachingKeyValueBinaryView(InternalTable tbl, KeyValueView<K, V> view, CacheStore<K, V> store) {
        this.tbl = tbl;
        this.view = view;
        this.store = store;
    }

    @Override
    public @Nullable V get(@Nullable Transaction tx, K key) {
        return getAsync(tx, key).join();
    }

    private InternalTransaction getOrEnlist(@Nullable Transaction tx) {
        return tx != null ? (InternalTransaction) tx : tbl.enlistExternal();
    }

    @Override
    public CompletableFuture<V> getAsync(@Nullable Transaction tx, K key) {
        InternalTransaction tx0 = (InternalTransaction) tx;

        boolean implicit = tx0 == null;

        if (implicit) {
            tx0 = tbl.enlistExternal();
        }

        assert tx0.external() : "Illegal tx mode";

        Optional<V> val = (Optional<V>) tx0.enlistStore(store).get(key);

        if (val != null) {
            return val.isEmpty() ? CompletableFutures.nullCompletedFuture() : CompletableFuture.completedFuture(val.get());
        }

        InternalTransaction finalTx = tx0;

        return view.getAsync(tx0, key).thenCompose(valTup -> {
            if (valTup == null) {
                return store.loadAsync(key).thenCompose(val0 -> {
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
    public CompletableFuture<Void> streamData(Publisher<Entry<K, V>> publisher, @Nullable DataStreamerOptions options) {
        return null;
    }

    @Override
    public NullableValue<V> getNullable(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public @Nullable V getOrDefault(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return null;
    }

    @Override
    public CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return null;
    }

    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, Collection<K> keys) {
        return null;
    }

    @Override
    public CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return null;
    }

    @Override
    public boolean contains(@Nullable Transaction tx, K key) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public void put(@Nullable Transaction tx, K key, @Nullable V val) {
        putAsync(tx, key, val).join(); // Do not unwrap txn, it's done in a delegate.
    }

    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        InternalTransaction tx0 = (InternalTransaction) tx;

        boolean implicit = tx0 == null;

        if (implicit) {
            tx0 = tbl.enlistExternal();
        }

        assert tx0.external() : "Illegal tx mode";

        InternalTransaction finalTx = tx0;

        return view.putAsync(tx0, key, val).thenApply(ignored -> {
            finalTx.enlistStore(store).put(key, Optional.of(val));

            return null;
        }).thenApply(ret -> {
            if (implicit) {
                finalTx.commitAsync(); // Out of async chain op, will do cleanup only.
            }

            return null;
        });
    }

    @Override
    public void putAll(@Nullable Transaction tx, Map<K, V> pairs) {
    }

    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<K, V> pairs) {
        return null;
    }

    @Override
    public @Nullable V getAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, K key, @Nullable V val) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key) {
        return removeAsync(tx, key).join();
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key, V val) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key) {
        InternalTransaction tx0 = (InternalTransaction) tx;

        boolean implicit = tx0 == null;

        if (implicit) {
            tx0 = tbl.enlistExternal();
        }

        assert tx0.external() : "Illegal tx mode";

        Optional<V> val = (Optional<V>) tx0.enlistStore(store).get(key);

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
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key, V val) {
        return null;
    }

    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, Collection<K> keys) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return null;
    }

    @Override
    public @Nullable V getAndRemove(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, K key) {
        return null;
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, @Nullable V val) {
        return false;
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, V oldValue, @Nullable V newValue) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V oldVal, @Nullable V newVal) {
        return null;
    }

    @Override
    public @Nullable V getAndReplace(@Nullable Transaction tx, @Nullable K key, @Nullable V val) {
        return null;
    }

    @Override
    public CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return null;
    }
}
