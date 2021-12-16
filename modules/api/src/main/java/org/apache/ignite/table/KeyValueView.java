/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Key-Value view of table provides methods to access table records.
 *
 * @param <K> Mapped key type.
 * @param <V> Mapped value type.
 * @apiNote 'Key/value class field' &gt;-&lt; 'table column' mapping laid down in implementation.
 * @see org.apache.ignite.table.mapper.Mapper
 */
public interface KeyValueView<K, V> {
    /**
     * Gets a value associated with the given key.
     *
     * <p>Note: If the value mapper implies a value can be {@code null}, then a suitable method
     * {@link #getNullable(Object, Transaction)} must be used instead.
     *
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Value or {@code null}, if it does not exist.
     * @throws IllegalStateException If value for the key exists, and it is {@code null}.
     * @see #getNullable(Object, Transaction)
     */
    V get(@NotNull K key, @Nullable Transaction tx);

    /**
     * Asynchronously gets a value associated with the given key.
     *
     * <p>Note: If the value mapper implies a value can be {@code null}, then a suitable method
     * {@link #getNullableAsync(Object, Transaction)} must be used instead.
     *
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     * @see #getNullableAsync(Object, Transaction)
     * @see #getNullableAsync(Object, Transaction)
     */
    @NotNull CompletableFuture<V> getAsync(@NotNull K key, @Nullable Transaction tx);

    /**
     * Gets a nullable value associated with the given key.
     *
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Wrapped nullable value or {@code null}, if it does not exist.
     */
    default NullableValue<V> getNullable(K key, @Nullable Transaction tx) {
        //TODO: to be implemented https://issues.apache.org/jira/browse/IGNITE-16115
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Gets a nullable value associated with the given key.
     *
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     * @see #getNullable(Object, Transaction)
     */
    default @NotNull CompletableFuture<NullableValue<V>> getNullableAsync(K key, @Nullable Transaction tx) {
        //TODO: to be implemented https://issues.apache.org/jira/browse/IGNITE-16115
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Gets a value associated with the given key if exists, returns {@code defaultValue} otherwise.
     *
     * <p>Note: method has same semantic as {@link #get(Object, Transaction)} with regard to {@code null} values.
     *
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Value or {@code defaultValue}, if does not exist.
     * @throws IllegalStateException If value for the key exists, and it is {@code null}.
     */
    default V getOrDefault(K key, V defaultValue, @Nullable Transaction tx) {
        //TODO: to be implemented https://issues.apache.org/jira/browse/IGNITE-16115
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Gets a nullable value associated with the given key.
     *
     * <p>Note: method has same semantic as {@link #get(Object, Transaction)} with regard to {@code null} values.
     *
     * @param key A key which associated the value is to be returned. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     * @see #getOrDefaultAsync(Object, Object, Transaction)
     */
    default @NotNull CompletableFuture<V> getOrDefaultAsync(K key, V defaultValue, @Nullable Transaction tx) {
        //TODO: to be implemented https://issues.apache.org/jira/browse/IGNITE-16115
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Get values associated with given keys.
     *
     * @param keys Keys which associated values are to be returned. The keys cannot be {@code null}.
     * @param tx   The transaction or {@code null} to auto commit.
     * @return Values associated with given keys.
     */
    Map<K, V> getAll(@NotNull Collection<K> keys, @Nullable Transaction tx);

    /**
     * Get values associated with given keys.
     *
     * @param keys Keys whose associated values are to be returned. The keys cannot be {@code null}.
     * @param tx   The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Map<K, V>> getAllAsync(@NotNull Collection<K> keys, @Nullable Transaction tx);

    /**
     * Determines if the table contains an entry for the specified key.
     *
     * @param key A key which presence is to be tested. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return {@code True} if a value exists for the specified key, {@code false} otherwise.
     */
    boolean contains(@NotNull K key, @Nullable Transaction tx);

    /**
     * Determines if the table contains an entry for the specified key.
     *
     * @param key A key which presence is to be tested. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> containsAsync(@NotNull K key, @Nullable Transaction tx);

    /**
     * Puts value associated with given key into the table.
     *
     * @param key A key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @param tx  The transaction or {@code null} to auto commit.
     */
    void put(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Asynchronously puts value associated with given key into the table.
     *
     * @param key A key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> putAsync(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Put associated key-value pairs.
     *
     * @param pairs Key-value pairs. The pairs cannot be {@code null}.
     * @param tx    The transaction or {@code null} to auto commit.
     */
    void putAll(@NotNull Map<K, V> pairs, @Nullable Transaction tx);

    /**
     * Asynchronously put associated key-value pairs.
     *
     * @param pairs Key-value pairs. The pairs cannot be {@code null}.
     * @param tx    The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<K, V> pairs, @Nullable Transaction tx);

    /**
     * Puts new or replaces existed value associated with given key into the table.
     *
     * @param key A key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Replaced value or {@code null}, if not existed.
     */
    V getAndPut(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Asynchronously puts new or replaces existed value associated with given key into the table.
     *
     * @param key A key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndPutAsync(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Puts value associated with given key into the table if not exists.
     *
     * @param key A key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return {@code True} if successful, {@code false} otherwise.
     */
    boolean putIfAbsent(@NotNull K key, @NotNull V val, @Nullable Transaction tx);

    /**
     * Asynchronously puts value associated with given key into the table if not exists.
     *
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Removes value associated with given key from the table.
     *
     * @param key A key which mapping is to be removed from the table. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return {@code True} if a value associated with the specified key was successfully removed, {@code false} otherwise.
     */
    boolean remove(@NotNull K key, @Nullable Transaction tx);

    /**
     * Removes an expected value associated with the given key from the table.
     *
     * @param key A key which associated value is to be removed from the table. The key cannot be {@code null}.
     * @param val Expected value.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return {@code True} if the expected value for the specified key was successfully removed, {@code false} otherwise.
     */
    boolean remove(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Asynchronously removes value associated with given key from the table.
     *
     * @param key A key which mapping is to be removed from the table. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key, @Nullable Transaction tx);

    /**
     * Asynchronously removes expected value associated with given key from the table.
     *
     * @param key A key which associated the value is to be removed from the table. The key cannot be {@code null}.
     * @param val Expected value.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Remove values associated with given keys from the table.
     *
     * @param keys Keys which mapping is to be removed from the table. The keys cannot be {@code null}.
     * @param tx   The transaction or {@code null} to auto commit.
     * @return Keys which did not exist.
     */
    Collection<K> removeAll(@NotNull Collection<K> keys, @Nullable Transaction tx);

    /**
     * Asynchronously remove values associated with given keys from the table.
     *
     * @param keys Keys which mapping is to be removed from the table. The keys cannot be {@code null}.
     * @param tx   The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<K>> removeAllAsync(@NotNull Collection<K> keys, @Nullable Transaction tx);

    /**
     * Gets then removes value associated with given key from the table.
     *
     * @param key A key which associated value is to be removed from the table. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Removed value or {@code null}, if not existed.
     */
    V getAndRemove(@NotNull K key, @Nullable Transaction tx);

    /**
     * Asynchronously gets then removes value associated with given key from the table.
     *
     * @param key A Key which mapping is to be removed from the table. The key cannot be {@code null}.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndRemoveAsync(@NotNull K key, @Nullable Transaction tx);

    /**
     * Replaces the value for a key only if exists. This is equivalent to
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param key A key with which the specified value is associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     */
    boolean replace(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Replaces the expected value for a key. This is equivalent to
     * <pre><code>
     * if (cache.get(key) == oldVal) {
     *   cache.put(key, newVal);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param key    A key with which the specified value is associated. The key cannot be {@code null}.
     * @param oldVal Expected value associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @param tx     The transaction or {@code null} to auto commit.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     */
    boolean replace(@NotNull K key, V oldVal, V newVal, @Nullable Transaction tx);

    /**
     * Asynchronously replaces the value for a key only if exists. See {@link #replace(Object, Object, Transaction)}.
     *
     * @param key A key with which the specified value is associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Asynchronously replaces the expected value for a key. See {@link #replace(Object, Object, Object, Transaction)}
     *
     * @param key    A key with which the specified value is associated. The key cannot be {@code null}.
     * @param oldVal Expected value associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @param tx     The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V oldVal, V newVal, @Nullable Transaction tx);

    /**
     * Replaces the value for a given key only if exists. This is equivalent to
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   V oldValue = cache.get(key);
     *   cache.put(key, value);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param key A key with which the specified value is associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Replaced value, or {@code null} if not existed.
     */
    V getAndReplace(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Asynchronously replaces the value for a given key only if exists. See {@link #getAndReplace(Object, Object, Transaction)}
     *
     * @param key A key with which the specified value is associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @param tx  The transaction or {@code null} to auto commit.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndReplaceAsync(@NotNull K key, V val, @Nullable Transaction tx);

    /**
     * Executes invoke processor code against the value associated with the provided key.
     *
     * @param key  A key associated with the value that invoke processor will be applied to. The key cannot be {@code null}.
     * @param proc Invoke processor.
     * @param tx   The transaction or {@code null} to auto commit.
     * @param args Optional invoke processor arguments.
     * @param <R>  Invoke processor result type.
     * @return Result of the processing.
     * @see InvokeProcessor
     */
    <R extends Serializable> R invoke(@NotNull K key, InvokeProcessor<K, V, R> proc, @Nullable Transaction tx, Serializable... args);

    /**
     * Asynchronously executes invoke processor code against the value associated with the provided key.
     *
     * @param key  A key associated with the value that invoke processor will be applied to. The key cannot be {@code null}.
     * @param proc Invoke processor.
     * @param tx   The transaction or {@code null} to auto commit.
     * @param args Optional invoke processor arguments.
     * @param <R>  Invoke processor result type.
     * @return Future representing pending completion of the operation.
     * @see InvokeProcessor
     */
    @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
            @NotNull K key,
            InvokeProcessor<K, V, R> proc,
            @Nullable Transaction tx,
            Serializable... args);

    /**
     * Executes invoke processor code against values associated with the provided keys.
     *
     * @param <R>  Invoke processor result type.
     * @param keys Ordered collection of keys which values associated with should be processed. The keys cannot be {@code null}.
     * @param proc Invoke processor.
     * @param tx   The transaction or {@code null} to auto commit.
     * @param args Optional invoke processor arguments.
     * @return Results of the processing.
     * @see InvokeProcessor
     */
    <R extends Serializable> Map<K, R> invokeAll(
            @NotNull Collection<K> keys,
            InvokeProcessor<K, V, R> proc,
            @Nullable Transaction tx,
            Serializable... args);

    /**
     * Asynchronously executes invoke processor code against values associated with the provided keys.
     *
     * @param <R>  Invoke processor result type.
     * @param keys Ordered collection of keys which values associated with should be processed. The keys cannot be {@code null}.
     * @param proc Invoke processor.
     * @param tx   The transaction or {@code null} to auto commit.
     * @param args Optional invoke processor arguments.
     * @return Future representing pending completion of the operation.
     * @see InvokeProcessor
     */
    @NotNull <R extends Serializable> CompletableFuture<Map<K, R>> invokeAllAsync(
            @NotNull Collection<K> keys,
            InvokeProcessor<K, V, R> proc,
            @Nullable Transaction tx,
            Serializable... args);
}
