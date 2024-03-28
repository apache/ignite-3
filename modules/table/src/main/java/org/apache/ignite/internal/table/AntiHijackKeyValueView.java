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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link KeyValueView} that adds anti-thread-hijacking protection to methods returning {@link CompletableFuture}s.
 *
 * @see PublicApiThreading#preventThreadHijack(CompletableFuture, Executor)
 */
public class AntiHijackKeyValueView<K, V> extends AntiHijackViewBase<Entry<K, V>> implements KeyValueView<K, V> {
    private final KeyValueView<K, V> view;

    /**
     * Constructor.
     *
     * @param view View to wrap.
     * @param asyncContinuationExecutor Executor to which execution will be resubmitted when leaving asynchronous public API endpoints
     *     (to prevent the user from stealing Ignite threads).
     */
    public AntiHijackKeyValueView(KeyValueView<K, V> view, Executor asyncContinuationExecutor) {
        super(view, view, asyncContinuationExecutor);

        assert !(view instanceof Wrapper) : "Wrapping other wrappers is not supported";

        this.view = view;
    }

    @Override
    public @Nullable V get(@Nullable Transaction tx, K key) {
        return view.get(tx, key);
    }

    @Override
    public CompletableFuture<V> getAsync(@Nullable Transaction tx, K key) {
        return preventThreadHijack(view.getAsync(tx, key));
    }

    @Override
    public NullableValue<V> getNullable(@Nullable Transaction tx, K key) {
        return view.getNullable(tx, key);
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, K key) {
        return preventThreadHijack(view.getNullableAsync(tx, key));
    }

    @Override
    public @Nullable V getOrDefault(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return view.getOrDefault(tx, key, defaultValue);
    }

    @Override
    public CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return preventThreadHijack(view.getOrDefaultAsync(tx, key, defaultValue));
    }

    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, Collection<K> keys) {
        return view.getAll(tx, keys);
    }

    @Override
    public CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return preventThreadHijack(view.getAllAsync(tx, keys));
    }

    @Override
    public boolean contains(@Nullable Transaction tx, K key) {
        return view.contains(tx, key);
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, K key) {
        return preventThreadHijack(view.containsAsync(tx, key));
    }

    @Override
    public void put(@Nullable Transaction tx, K key, @Nullable V val) {
        view.put(tx, key, val);
    }

    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return preventThreadHijack(view.putAsync(tx, key, val));
    }

    @Override
    public void putAll(@Nullable Transaction tx, Map<K, V> pairs) {
        view.putAll(tx, pairs);
    }

    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<K, V> pairs) {
        return preventThreadHijack(view.putAllAsync(tx, pairs));
    }

    @Override
    public @Nullable V getAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return view.getAndPut(tx, key, val);
    }

    @Override
    public CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return preventThreadHijack(view.getAndPutAsync(tx, key, val));
    }

    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return view.getNullableAndPut(tx, key, val);
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return preventThreadHijack(view.getNullableAndPutAsync(tx, key, val));
    }

    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, K key, @Nullable V val) {
        return view.putIfAbsent(tx, key, val);
    }

    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return preventThreadHijack(view.putIfAbsentAsync(tx, key, val));
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key) {
        return view.remove(tx, key);
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key, V val) {
        return view.remove(tx, key, val);
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key) {
        return preventThreadHijack(view.removeAsync(tx, key));
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key, V val) {
        return preventThreadHijack(view.removeAsync(tx, key, val));
    }

    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, Collection<K> keys) {
        return view.removeAll(tx, keys);
    }

    @Override
    public CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return preventThreadHijack(view.removeAllAsync(tx, keys));
    }

    @Override
    public @Nullable V getAndRemove(@Nullable Transaction tx, K key) {
        return view.getAndRemove(tx, key);
    }

    @Override
    public CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, K key) {
        return preventThreadHijack(view.getAndRemoveAsync(tx, key));
    }

    @Override
    public NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, K key) {
        return view.getNullableAndRemove(tx, key);
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, K key) {
        return preventThreadHijack(view.getNullableAndRemoveAsync(tx, key));
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, @Nullable V val) {
        return view.replace(tx, key, val);
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, V oldValue, @Nullable V newValue) {
        return view.replace(tx, key, oldValue, newValue);
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return preventThreadHijack(view.replaceAsync(tx, key, val));
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V oldVal, @Nullable V newVal) {
        return preventThreadHijack(view.replaceAsync(tx, key, oldVal, newVal));
    }

    @Override
    public @Nullable V getAndReplace(@Nullable Transaction tx, @Nullable K key, @Nullable V val) {
        return view.getAndReplace(tx, key, val);
    }

    @Override
    public CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return preventThreadHijack(view.getAndReplaceAsync(tx, key, val));
    }

    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, K key, @Nullable V val) {
        return view.getNullableAndReplace(tx, key, val);
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return preventThreadHijack(view.getNullableAndReplaceAsync(tx, key, val));
    }
}
