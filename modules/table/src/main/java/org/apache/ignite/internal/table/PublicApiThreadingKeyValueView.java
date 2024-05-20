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
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around {@link KeyValueView} that maintains public API invariants relating to threading.
 *
 * <p>That is, it adds protection against thread hijacking by users and also marks threads as 'executing a sync user operation' or
 * 'executing an async user operation'.
 *
 * @see PublicApiThreading#preventThreadHijack(CompletableFuture, Executor)
 */
public class PublicApiThreadingKeyValueView<K, V> extends PublicApiThreadingViewBase<Entry<K, V>> implements KeyValueView<K, V> {
    private final KeyValueView<K, V> view;

    /**
     * Constructor.
     *
     * @param view View to wrap.
     * @param asyncContinuationExecutor Executor to which execution will be resubmitted when leaving asynchronous public API endpoints
     *     (to prevent the user from stealing Ignite threads).
     */
    public PublicApiThreadingKeyValueView(KeyValueView<K, V> view, Executor asyncContinuationExecutor) {
        super(view, view, asyncContinuationExecutor);

        this.view = view;
    }

    @Override
    public @Nullable V get(@Nullable Transaction tx, K key) {
        return executeSyncOp(() -> view.get(tx, key));
    }

    @Override
    public CompletableFuture<V> getAsync(@Nullable Transaction tx, K key) {
        return executeAsyncOp(() -> view.getAsync(tx, key));
    }

    @Override
    public NullableValue<V> getNullable(@Nullable Transaction tx, K key) {
        return executeSyncOp(() -> view.getNullable(tx, key));
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, K key) {
        return executeAsyncOp(() -> view.getNullableAsync(tx, key));
    }

    @Override
    public @Nullable V getOrDefault(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return executeSyncOp(() -> view.getOrDefault(tx, key, defaultValue));
    }

    @Override
    public CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return executeAsyncOp(() -> view.getOrDefaultAsync(tx, key, defaultValue));
    }

    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, Collection<K> keys) {
        return executeSyncOp(() -> view.getAll(tx, keys));
    }

    @Override
    public CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return executeAsyncOp(() -> view.getAllAsync(tx, keys));
    }

    @Override
    public boolean contains(@Nullable Transaction tx, K key) {
        return executeSyncOp(() -> view.contains(tx, key));
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, K key) {
        return executeAsyncOp(() -> view.containsAsync(tx, key));
    }

    @Override
    public void put(@Nullable Transaction tx, K key, @Nullable V val) {
        executeSyncOp(() -> view.put(tx, key, val));
    }

    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeAsyncOp(() -> view.putAsync(tx, key, val));
    }

    @Override
    public void putAll(@Nullable Transaction tx, Map<K, V> pairs) {
        executeSyncOp(() -> view.putAll(tx, pairs));
    }

    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<K, V> pairs) {
        return executeAsyncOp(() -> view.putAllAsync(tx, pairs));
    }

    @Override
    public @Nullable V getAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeSyncOp(() -> view.getAndPut(tx, key, val));
    }

    @Override
    public CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeAsyncOp(() -> view.getAndPutAsync(tx, key, val));
    }

    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeSyncOp(() -> view.getNullableAndPut(tx, key, val));
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeAsyncOp(() -> view.getNullableAndPutAsync(tx, key, val));
    }

    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeSyncOp(() -> view.putIfAbsent(tx, key, val));
    }

    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeAsyncOp(() -> view.putIfAbsentAsync(tx, key, val));
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key) {
        return executeSyncOp(() -> view.remove(tx, key));
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key, V val) {
        return executeSyncOp(() -> view.remove(tx, key, val));
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key) {
        return executeAsyncOp(() -> view.removeAsync(tx, key));
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key, V val) {
        return executeAsyncOp(() -> view.removeAsync(tx, key, val));
    }

    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, Collection<K> keys) {
        return executeSyncOp(() -> view.removeAll(tx, keys));
    }

    @Override
    public CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return executeAsyncOp(() -> view.removeAllAsync(tx, keys));
    }

    @Override
    public @Nullable V getAndRemove(@Nullable Transaction tx, K key) {
        return executeSyncOp(() -> view.getAndRemove(tx, key));
    }

    @Override
    public CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, K key) {
        return executeAsyncOp(() -> view.getAndRemoveAsync(tx, key));
    }

    @Override
    public NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, K key) {
        return executeSyncOp(() -> view.getNullableAndRemove(tx, key));
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, K key) {
        return executeAsyncOp(() -> view.getNullableAndRemoveAsync(tx, key));
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeSyncOp(() -> view.replace(tx, key, val));
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, V oldValue, @Nullable V newValue) {
        return executeSyncOp(() -> view.replace(tx, key, oldValue, newValue));
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeAsyncOp(() -> view.replaceAsync(tx, key, val));
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V oldVal, @Nullable V newVal) {
        return executeAsyncOp(() -> view.replaceAsync(tx, key, oldVal, newVal));
    }

    @Override
    public @Nullable V getAndReplace(@Nullable Transaction tx, @Nullable K key, @Nullable V val) {
        return executeSyncOp(() -> view.getAndReplace(tx, key, val));
    }

    @Override
    public CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeAsyncOp(() -> view.getAndReplaceAsync(tx, key, val));
    }

    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeSyncOp(() -> view.getNullableAndReplace(tx, key, val));
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return executeAsyncOp(() -> view.getNullableAndReplaceAsync(tx, key, val));
    }
}
