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

package org.apache.ignite.internal.restart;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Reference to {@link KeyValueView} under a swappable {@link Ignite} instance. When a restart happens, this switches to the new Ignite
 * instance.
 *
 * <p>API operations on this are linearized with respect to node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
class RestartProofKeyValueView<K, V> extends RestartProofApiObject<KeyValueView<K, V>> implements KeyValueView<K, V>, Wrapper {
    RestartProofKeyValueView(
            IgniteAttachmentLock attachmentLock,
            Ignite initialIgnite,
            Function<Ignite, KeyValueView<K, V>> viewFactory
    ) {
        super(attachmentLock, initialIgnite, viewFactory);
    }

    @Override
    public @Nullable V get(@Nullable Transaction tx, K key) {
        return attached(view -> view.get(tx, key));
    }

    @Override
    public CompletableFuture<V> getAsync(@Nullable Transaction tx, K key) {
        return attachedAsync(view -> view.getAsync(tx, key));
    }

    @Override
    public NullableValue<V> getNullable(@Nullable Transaction tx, K key) {
        return attached(view -> view.getNullable(tx, key));
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, K key) {
        return attachedAsync(view -> view.getNullableAsync(tx, key));
    }

    @Override
    public @Nullable V getOrDefault(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return attached(view -> view.getOrDefault(tx, key, defaultValue));
    }

    @Override
    public CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, K key, @Nullable V defaultValue) {
        return attachedAsync(view -> view.getOrDefaultAsync(tx, key, defaultValue));
    }

    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, Collection<K> keys) {
        return attached(view -> view.getAll(tx, keys));
    }

    @Override
    public CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return attachedAsync(view -> view.getAllAsync(tx, keys));
    }

    @Override
    public boolean contains(@Nullable Transaction tx, K key) {
        return attached(view -> view.contains(tx, key));
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, K key) {
        return attachedAsync(view -> view.containsAsync(tx, key));
    }

    @Override
    public boolean containsAll(@Nullable Transaction tx, Collection<K> keys) {
        return attached(view -> view.containsAll(tx, keys));
    }

    @Override
    public CompletableFuture<Boolean> containsAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return attachedAsync(view -> view.containsAllAsync(tx, keys));
    }

    @Override
    public void put(@Nullable Transaction tx, K key, @Nullable V val) {
        consumeAttached(view -> view.put(tx, key, val));
    }

    @Override
    public CompletableFuture<Void> putAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return attachedAsync(view -> view.putAsync(tx, key, val));
    }

    @Override
    public void putAll(@Nullable Transaction tx, Map<K, V> pairs) {
        consumeAttached(view -> view.putAll(tx, pairs));
    }

    @Override
    public CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<K, V> pairs) {
        return attachedAsync(view -> view.putAllAsync(tx, pairs));
    }

    @Override
    @Nullable
    public V getAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return attached(view -> view.getAndPut(tx, key, val));
    }

    @Override
    public CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return attachedAsync(view -> view.getAndPutAsync(tx, key, val));
    }

    @Override
    public NullableValue<V> getNullableAndPut(@Nullable Transaction tx, K key, @Nullable V val) {
        return attached(view -> view.getNullableAndPut(tx, key, val));
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return attachedAsync(view -> view.getNullableAndPutAsync(tx, key, val));
    }

    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, K key, @Nullable V val) {
        return attached(view -> view.putIfAbsent(tx, key, val));
    }

    @Override
    public CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return attachedAsync(view -> view.putIfAbsentAsync(tx, key, val));
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key) {
        return attached(view -> view.remove(tx, key));
    }

    @Override
    public boolean remove(@Nullable Transaction tx, K key, V val) {
        return attached(view -> view.remove(tx, key, val));
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key) {
        return attachedAsync(view -> view.removeAsync(tx, key));
    }

    @Override
    public CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key, V val) {
        return attachedAsync(view -> view.removeAsync(tx, key, val));
    }

    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, Collection<K> keys) {
        return attached(view -> view.removeAll(tx, keys));
    }

    @Override
    public void removeAll(@Nullable Transaction tx) {
        consumeAttached(view -> view.removeAll(tx));
    }

    @Override
    public CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, Collection<K> keys) {
        return attachedAsync(view -> view.removeAllAsync(tx, keys));
    }

    @Override
    public CompletableFuture<Void> removeAllAsync(@Nullable Transaction tx) {
        return attachedAsync(view -> view.removeAllAsync(tx));
    }

    @Override
    @Nullable
    public V getAndRemove(@Nullable Transaction tx, K key) {
        return attached(view -> view.getAndRemove(tx, key));
    }

    @Override
    public CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, K key) {
        return attachedAsync(view -> view.getAndRemoveAsync(tx, key));
    }

    @Override
    public NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, K key) {
        return attached(view -> view.getNullableAndRemove(tx, key));
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, K key) {
        return attachedAsync(view -> view.getNullableAndRemoveAsync(tx, key));
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, @Nullable V val) {
        return attached(view -> view.replace(tx, key, val));
    }

    @Override
    public boolean replace(@Nullable Transaction tx, K key, @Nullable V oldValue, @Nullable V newValue) {
        return attached(view -> view.replace(tx, key, oldValue, newValue));
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return attachedAsync(view -> view.replaceAsync(tx, key, val));
    }

    @Override
    public CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V oldVal, @Nullable V newVal) {
        return attachedAsync(view -> view.replaceAsync(tx, key, oldVal, newVal));
    }

    @Override
    @Nullable
    public V getAndReplace(@Nullable Transaction tx, @Nullable K key, @Nullable V val) {
        return attached(view -> view.getAndReplace(tx, key, val));
    }

    @Override
    public CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return attachedAsync(view -> view.getAndReplaceAsync(tx, key, val));
    }

    @Override
    public NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, K key, @Nullable V val) {
        return attached(view -> view.getNullableAndReplace(tx, key, val));
    }

    @Override
    public CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val) {
        return attachedAsync(view -> view.getNullableAndReplaceAsync(tx, key, val));
    }

    @Override
    public CompletableFuture<Void> streamData(Publisher<DataStreamerItem<Entry<K, V>>> publisher, @Nullable DataStreamerOptions options) {
        return attachedAsync(view -> view.streamData(publisher, options));
    }

    @Override
    public <E, V1, A, R> CompletableFuture<Void> streamData(
            Publisher<E> publisher,
            DataStreamerReceiverDescriptor<V1, A, R> receiver,
            Function<E, Entry<K, V>> keyFunc,
            Function<E, V1> payloadFunc,
            @Nullable A receiverArg,
            @Nullable Subscriber<R> resultSubscriber,
            @Nullable DataStreamerOptions options) {
        return attachedAsync(view -> view.streamData(publisher, receiver, keyFunc, payloadFunc, receiverArg, resultSubscriber, options));
    }

    // TODO: IGNITE-23011 - support cursor transparency?

    @Override
    public Cursor<Entry<K, V>> query(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts
    ) {
        return attached(view -> view.query(tx, criteria, indexName, opts));
    }

    @Override
    public CompletableFuture<AsyncCursor<Entry<K, V>>> queryAsync(
            @Nullable Transaction tx,
            @Nullable Criteria criteria,
            @Nullable String indexName,
            @Nullable CriteriaQueryOptions opts
    ) {
        return attachedAsync(view -> view.queryAsync(tx, criteria, indexName, opts));
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attached(view -> Wrappers.unwrap(view, classToUnwrap));
    }
}
