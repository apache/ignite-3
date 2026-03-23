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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.DataStreamerTarget;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Adapter for {@link KeyValueView} to run async methods using sync methods.
 *
 * <p>NOTE: Class does not support {@link CriteriaQuerySource} and {@link DataStreamerTarget} methods.
 */
public class AsyncApiKeyValueViewAdapter<K, V> implements KeyValueView<K, V> {
    private final KeyValueView<K, V> delegate;

    AsyncApiKeyValueViewAdapter(KeyValueView<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    @Nullable public V get(@Nullable Transaction tx, K key) {
        return await(delegate.getAsync(tx, key));
    }

    @Override
    public NullableValue<V> getNullable(@Nullable Transaction tx, K key) {
        return await(delegate.getNullableAsync(tx, key));
    }

    @Override
    @Nullable public V getOrDefault(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return await(delegate.getOrDefaultAsync(tx, key, defaultValue));
    }

    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, Collection<K> keys) {
        return await(delegate.getAllAsync(tx, keys));
    }

    @Override
    public void put(@Nullable Transaction tx, K key, @Nullable V val) {
        delegate.put(tx, key, val);
    }

    @Override
    public boolean contains(@Nullable Transaction tx, K key) {
        return await(delegate.containsAsync(tx, key));
    }

    @Override
    public boolean containsAll(@Nullable Transaction tx, Collection<K> keys) {
        return await(delegate.containsAllAsync(tx, keys));
    }

    @Override
    public void putAll(@Nullable Transaction tx, Map<K, V> pairs) {
        await(delegate.putAllAsync(tx, pairs));
    }

    @Override
    @Nullable public V getAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return await(delegate.getAndPutAsync(tx, key, val));
    }

    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return await(delegate.getNullableAndPutAsync(tx, key, val));
    }

    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, K key, @Nullable V val) {
        return await(delegate.putIfAbsentAsync(tx, key, val));
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key) {
        return await(delegate.removeAsync(tx, key));
    }

    @Override
    public boolean removeExact(@Nullable Transaction tx, K key, V val) {
        return await(delegate.removeExactAsync(tx, key, val));
    }

    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, Collection<K> keys) {
        return await(delegate.removeAllAsync(tx, keys));
    }

    @Override
    public void removeAll(@Nullable Transaction tx) {
        removeAll(tx);
    }

    @Override
    @Nullable public V getAndRemove(@Nullable Transaction tx, K key) {
        return await(delegate.getAndRemoveAsync(tx, key));
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, @Nullable V val) {
        return await(delegate.replaceAsync(tx, key, val));
    }

    @Override
    public boolean replaceExact(@Nullable Transaction tx, K key, @Nullable V oldValue, @Nullable V newValue) {
        return await(delegate.replaceExactAsync(tx, key, oldValue, newValue));
    }

    @Override
    @Nullable public V getAndReplace(@Nullable Transaction tx, K key, @Nullable V val) {
        return await(delegate.getAndReplaceAsync(tx, key, val));
    }

    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, K key, @Nullable V val) {
        return await(delegate.getNullableAndReplaceAsync(tx, key, val));
    }

    @Override
    public CompletableFuture<V> getAsync(@Nullable Transaction tx, K key) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, K key) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, K key) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> containsAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<K, V> pairs) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> removeExactAsync(@Nullable Transaction tx, K key, V val) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Void> removeAllAsync(@Nullable Transaction tx) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, K key) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, K key) {
        return await(delegate.getNullableAndRemoveAsync(tx, key));
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, K key) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Boolean> replaceExactAsync(@Nullable Transaction tx, K key, @Nullable V oldVal, @Nullable V newVal) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public Cursor<Entry<K, V>> query(@Nullable Transaction tx, @Nullable Criteria criteria) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor<Entry<K, V>> query(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor<Entry<K, V>> query(@Nullable Transaction tx, @Nullable Criteria criteria, @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<AsyncCursor<Entry<K, V>>> queryAsync(@Nullable Transaction tx, @Nullable Criteria criteria) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<AsyncCursor<Entry<K, V>>> queryAsync(@Nullable Transaction tx, @Nullable Criteria criteria,
            @Nullable String indexName) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<AsyncCursor<Entry<K, V>>> queryAsync(@Nullable Transaction tx, @Nullable Criteria criteria,
            @Nullable String indexName, @Nullable CriteriaQueryOptions opts) {
        throw new UnsupportedOperationException("Must not be called");
    }

    @Override
    public CompletableFuture<Void> streamData(Publisher<DataStreamerItem<Entry<K, V>>> publisher, @Nullable DataStreamerOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E, S, A, R> CompletableFuture<Void> streamData(Publisher<E> publisher, DataStreamerReceiverDescriptor<S, A, R> receiver,
            Function<E, Entry<K, V>> keyFunc, Function<E, S> payloadFunc, @Nullable A receiverArg,
            @Nullable Flow.Subscriber<R> resultSubscriber,
            @Nullable DataStreamerOptions options) {
        throw new UnsupportedOperationException();
    }
}
