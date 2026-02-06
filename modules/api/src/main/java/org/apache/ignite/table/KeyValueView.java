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

package org.apache.ignite.table;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.lang.UnexpectedNullValueException;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Key-Value view of a table provides methods to access table records.
 *
 * @param <K> Mapped key type.
 * @param <V> Mapped value type.
 * @apiNote 'Key/value class field' &gt;-&lt; 'table column' mapping laid down in implementation.
 * @see org.apache.ignite.table.mapper.Mapper
 */
public interface KeyValueView<K, V> extends DataStreamerTarget<Entry<K, V>>, CriteriaQuerySource<Entry<K, V>> {
    /**
     * Gets a value associated with a given key.
     *
     * <p>Note: If the value mapper implies a value can be {@code null}, a suitable method
     * {@link #getNullable(Transaction, Object)} must be used.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @return Value or {@code null}, if it does not exist.
     * @throws MarshallerException if the key doesn't match the schema.
     * @throws UnexpectedNullValueException If value for the key exists, and it is {@code null}.
     * @see #getNullable(Transaction, Object)
     */
    @Nullable V get(@Nullable Transaction tx, K key);

    /**
     * Gets a value associated with a given key.
     *
     * <p>Note: If the value mapper implies a value can be {@code null}, a suitable method
     * {@link #getNullable(Object)} must be used.
     *
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @return Value or {@code null}, if it does not exist.
     * @throws MarshallerException if the key doesn't match the schema.
     * @throws UnexpectedNullValueException If value for the key exists, and it is {@code null}.
     * @see #getNullable(Object)
     */
    default @Nullable V get(K key) {
        return get(null, key);
    }

    /**
     * Asynchronously gets a value associated with a given key.
     *
     * <p>Note: If the value mapper implies a value can be {@code null}, a suitable method
     * {@link #getNullableAsync(Transaction, Object)} must be used.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     * @see #getNullableAsync(Transaction, Object)
     * @see #get(Transaction, Object)
     */
    CompletableFuture<V> getAsync(@Nullable Transaction tx, K key);

    /**
     * Asynchronously gets a value associated with a given key.
     *
     * <p>Note: If the value mapper implies a value can be {@code null}, a suitable method
     * {@link #getNullableAsync(Object)} must be used.
     *
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     * @see #getNullableAsync(Object)
     * @see #get(Object)
     */
    default CompletableFuture<V> getAsync(K key) {
        return getAsync(null, key);
    }

    /**
     * Gets a nullable value associated with a given key.
     *
     * <p>Examples:
     *     {@code getNullable(tx, key)} returns {@code null} after {@code remove(tx, key)}.
     *     {@code getNullable(tx, key)} returns {@code Nullable.of(null)} after {@code put(tx, key, null)}.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @return Wrapped nullable value or {@code null} if it does not exist.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    NullableValue<V> getNullable(@Nullable Transaction tx, K key);

    /**
     * Gets a nullable value associated with a given key.
     *
     * <p>Examples:
     *     {@code getNullable(key)} returns {@code null} after {@code remove(key)}.
     *     {@code getNullable(key)} returns {@code Nullable.of(null)} after {@code put(key, null)}.
     *
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @return Wrapped nullable value or {@code null} if it does not exist.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default NullableValue<V> getNullable(K key) {
        return getNullable(null, key);
    }

    /**
     * Gets a nullable value associated with a given key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     *     The future returns wrapped nullable value or {@code null} if the row with the given key does not exist.
     * @throws MarshallerException if the key doesn't match the schema.
     * @see #getNullable(Transaction, Object)
     */
    CompletableFuture<NullableValue<V>> getNullableAsync(@Nullable Transaction tx, K key);

    /**
     * Gets a nullable value associated with a given key.
     *
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     *     The future returns wrapped nullable value or {@code null} if the row with the given key does not exist.
     * @throws MarshallerException if the key doesn't match the schema.
     * @see #getNullable(Object)
     */
    default CompletableFuture<NullableValue<V>> getNullableAsync(K key) {
        return getNullableAsync(null, key);
    }

    /**
     * Gets a value associated with a given key, if it exists and is not null, otherwise returns {@code defaultValue}.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @param defaultValue Default value.
     * @return Value or {@code defaultValue} if does not exist.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    @Nullable V getOrDefault(@Nullable Transaction tx, K key, @Nullable V defaultValue);

    /**
     * Gets a value associated with a given key, if it exists and is not null, otherwise returns {@code defaultValue}.
     *
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @param defaultValue Default value.
     * @return Value or {@code defaultValue} if does not exist.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default @Nullable V getOrDefault(K key, @Nullable V defaultValue) {
        return getOrDefault(null, key, defaultValue);
    }

    /**
     * Gets a value associated with a given key, if it exists and is not null, otherwise returns {@code defaultValue}.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @param defaultValue Default value.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     * @see #getOrDefault(Transaction, Object, Object)
     */
    CompletableFuture<V> getOrDefaultAsync(@Nullable Transaction tx, K key, @Nullable V defaultValue);

    /**
     * Gets a value associated with a given key, if it exists and is not null, otherwise returns {@code defaultValue}.
     *
     * @param key Key whose value is to be returned. The key cannot be {@code null}.
     * @param defaultValue Default value.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     * @see #getOrDefault(Object, Object)
     */
    default CompletableFuture<V> getOrDefaultAsync(K key, @Nullable V defaultValue) {
        return getOrDefaultAsync(null, key, defaultValue);
    }

    /**
     * Get values associated with given keys.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keys Keys whose values are to be returned. The keys cannot be {@code null}.
     * @return Values associated with given keys.
     *      If a requested key does not exist, it will have no corresponding entry in the returned map.
     * @throws MarshallerException if the keys don't match the schema.
     */
    Map<K, V> getAll(@Nullable Transaction tx, Collection<K> keys);

    /**
     * Get values associated with given keys.
     *
     * @param keys Keys whose values are to be returned. The keys cannot be {@code null}.
     * @return Values associated with given keys.
     *      If a requested key does not exist, it will have no corresponding entry in the returned map.
     * @throws MarshallerException if the keys don't match the schema.
     */
    default Map<K, V> getAll(Collection<K> keys) {
        return getAll(null, keys);
    }

    /**
     * Get values associated with given keys.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keys Keys whose values are to be returned. The keys cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, Collection<K> keys);

    /**
     * Get values associated with given keys.
     *
     * @param keys Keys whose values are to be returned. The keys cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default CompletableFuture<Map<K, V>> getAllAsync(Collection<K> keys) {
        return getAllAsync(null, keys);
    }

    /**
     * Determines whether a table contains an entry for the specified key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose presence is to be verified. The key cannot be {@code null}.
     * @return {@code True} if a value exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    boolean contains(@Nullable Transaction tx, K key);

    /**
     * Determines whether a table contains an entry for the specified key.
     *
     * @param key Key whose presence is to be verified. The key cannot be {@code null}.
     * @return {@code True} if a value exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default boolean contains(K key) {
        return contains(null, key);
    }

    /**
     * Determines whether a table contains an entry for the specified key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose presence is to be verified. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, K key);

    /**
     * Determines whether a table contains an entry for the specified key.
     *
     * @param key Key whose presence is to be verified. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default CompletableFuture<Boolean> containsAsync(K key) {
        return containsAsync(null, key);
    }

    /**
     * Determines whether a table contains entries for all given keys.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keys Keys whose presence is to be verified. The collection and it's values cannot be {@code null}.
     * @return {@code True} if a value exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    boolean containsAll(@Nullable Transaction tx, Collection<K> keys);

    /**
     * Determines whether a table contains entries for all given keys.
     *
     * @param keys Keys whose presence is to be verified. The collection and it's values cannot be {@code null}.
     * @return {@code True} if a value exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default boolean containsAll(Collection<K> keys) {
        return containsAll(null, keys);
    }

    /**
     * Determines whether a table contains entries for all given keys.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keys Keys whose presence is to be verified. The collection and it's values cannot be {@code null}.
     * @return Future that represents the pending completion of the operation. The result of the future will be {@code true} if a value
     *      exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    CompletableFuture<Boolean> containsAllAsync(@Nullable Transaction tx, Collection<K> keys);

    /**
     * Determines whether a table contains entries for all given keys.
     *
     * @param keys Keys whose presence is to be verified. The collection and it's values cannot be {@code null}.
     * @return Future that represents the pending completion of the operation. The result of the future will be {@code true} if a value
     *      exists for every specified key, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default CompletableFuture<Boolean> containsAllAsync(Collection<K> keys) {
        return containsAllAsync(null, keys);
    }

    /**
     * Puts into a table a value associated with the given key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be null when mapped to a single column with a simple type.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    void put(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Puts into a table a value associated with the given key.
     *
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be null when mapped to a single column with a simple type.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    default void put(K key, @Nullable V val) {
        put(null, key, val);
    }

    /**
     * Asynchronously puts into a table a value associated with the given key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be null when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    CompletableFuture<Void> putAsync(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Asynchronously puts into a table a value associated with the given key.
     *
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be null when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    default CompletableFuture<Void> putAsync(K key, @Nullable V val) {
        return putAsync(null, key, val);
    }

    /**
     * Puts associated key-value pairs.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param pairs Key-value pairs. The pairs cannot be {@code null}.
     * @throws MarshallerException if one of key, or values doesn't match the schema.
     */
    void putAll(@Nullable Transaction tx, Map<K, V> pairs);

    /**
     * Puts associated key-value pairs.
     *
     * @param pairs Key-value pairs. The pairs cannot be {@code null}.
     * @throws MarshallerException if one of key, or values doesn't match the schema.
     */
    default void putAll(Map<K, V> pairs) {
        putAll(null, pairs);
    }

    /**
     * Asynchronously puts associated key-value pairs.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param pairs Key-value pairs. The pairs cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if one of key, or values doesn't match the schema.
     */
    CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, Map<K, V> pairs);

    /**
     * Asynchronously puts associated key-value pairs.
     *
     * @param pairs Key-value pairs. The pairs cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if one of key, or values doesn't match the schema.
     */
    default CompletableFuture<Void> putAllAsync(Map<K, V> pairs) {
        return putAllAsync(null, pairs);
    }

    /**
     * Puts into a table a new, or replaces an existing, value associated with the given key.
     *
     * <p>NB: The method doesn't support {@code null} column value, use {@link #getNullableAndPut(Transaction, Object, Object)} instead.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Replaced value or {@code null} if it did not exist.
     * @throws MarshallerException if one of the keys or values doesn't match the schema.
     * @throws UnexpectedNullValueException If value for the key exists, and it is {@code null}.
     */
    @Nullable V getAndPut(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Puts into a table a new, or replaces an existing, value associated with the given key.
     *
     * <p>NB: The method doesn't support {@code null} column value, use {@link #getNullableAndPut(Object, Object)} instead.
     *
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Replaced value or {@code null} if it did not exist.
     * @throws MarshallerException if one of the keys or values doesn't match the schema.
     * @throws UnexpectedNullValueException If value for the key exists, and it is {@code null}.
     */
    default @Nullable V getAndPut(K key, @Nullable V val) {
        return getAndPut(null, key, val);
    }

    /**
     * Asynchronously puts into a table a new, or replaces an existing, value associated with given key.
     *
     * <p>NB: The method doesn't support {@code null} column value, use {@link #getNullableAndPutAsync(Transaction, Object, Object)}
     *     instead.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Asynchronously puts into a table a new, or replaces an existing, value associated with given key.
     *
     * <p>NB: The method doesn't support {@code null} column value, use {@link #getNullableAndPutAsync(Object, Object)}
     *     instead.
     *
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    default CompletableFuture<V> getAndPutAsync(K key, @Nullable V val) {
        return getAndPutAsync(null, key, val);
    }

    /**
     * Puts into a table a new, or replaces an existing, value associated with given key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Wrapped nullable value that was replaced or {@code null} if it did no exist.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    NullableValue<V> getNullableAndPut(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Puts into a table a new, or replaces an existing, value associated with given key.
     *
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Wrapped nullable value that was replaced or {@code null} if it did no exist.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    default NullableValue<V> getNullableAndPut(K key, @Nullable V val) {
        return getNullableAndPut(null, key, val);
    }

    /**
     * Asynchronously puts into a table a new, or replaces an existing, value associated with given key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    CompletableFuture<NullableValue<V>> getNullableAndPutAsync(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Asynchronously puts into a table a new, or replaces an existing, value associated with given key.
     *
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    default CompletableFuture<NullableValue<V>> getNullableAndPutAsync(K key, @Nullable V val) {
        return getNullableAndPutAsync(null, key, val);
    }

    /**
     * Puts into a table a value associated with the given key if this value does not exists.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return {@code True} if successful, {@code false} otherwise.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    boolean putIfAbsent(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Puts into a table a value associated with the given key if this value does not exists.
     *
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return {@code True} if successful, {@code false} otherwise.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    default boolean putIfAbsent(K key, @Nullable V val) {
        return putIfAbsent(null, key, val);
    }

    /**
     * Asynchronously puts into a table a value associated with the given key if this value does not exist.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Asynchronously puts into a table a value associated with the given key if this value does not exist.
     *
     * @param key Key with which the specified value is to be associated. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    default CompletableFuture<Boolean> putIfAbsentAsync(K key, @Nullable V val) {
        return putIfAbsentAsync(null, key, val);
    }

    /**
     * Removes from a table a value associated with the given key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return {@code True} if a value associated with the specified key was successfully removed, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    boolean remove(@Nullable Transaction tx, K key);

    /**
     * Removes from a table a value associated with the given key.
     *
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return {@code True} if a value associated with the specified key was successfully removed, {@code false} otherwise.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default boolean remove(K key) {
        return remove(null, key);
    }

    /**
     * Removes from a table an expected value associated with the given key.
     * Deprecated: use {@link #removeExact(Transaction, Object, Object)} instead.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @param val Expected value.
     * @return {@code True} if the expected value for the specified key was successfully removed, {@code false} otherwise.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    @Deprecated(forRemoval = true)
    default boolean remove(@Nullable Transaction tx, K key, V val) {
        return removeExact(tx, key, val);
    }

    /**
     * Removes from a table an expected value associated with the given key.
     *
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @param val Expected value.
     * @return {@code True} if the expected value for the specified key was successfully removed, {@code false} otherwise.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    boolean removeExact(@Nullable Transaction tx, K key, V val);

    /**
     * Removes from a table an expected value associated with the given key.
     *
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @param val Expected value.
     * @return {@code True} if the expected value for the specified key was successfully removed, {@code false} otherwise.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    default boolean removeExact(K key, V val) {
        return removeExact(null, key, val);
    }

    /**
     * Asynchronously removes from a table a value associated with the given key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key A key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key);

    /**
     * Asynchronously removes from a table a value associated with the given key.
     *
     * @param key A key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default CompletableFuture<Boolean> removeAsync(K key) {
        return removeAsync(null, key);
    }

    /**
     * Asynchronously removes from a table an expected value associated with the given key.
     * Deprecated: use {@link #removeExactAsync(Transaction, Object, Object)} instead.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @param val Expected value.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    @Deprecated(forRemoval = true)
    default CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, K key, V val) {
        return removeExactAsync(tx, key, val);
    }

    /**
     * Asynchronously removes from a table an expected value associated with the given key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @param val Expected value.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    CompletableFuture<Boolean> removeExactAsync(@Nullable Transaction tx, K key, V val);

    /**
     * Asynchronously removes from a table an expected value associated with the given key.
     *
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @param val Expected value.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    default CompletableFuture<Boolean> removeExactAsync(K key, V val) {
        return removeExactAsync(null, key, val);
    }

    /**
     * Removes all entries from a table.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     */
    void removeAll(@Nullable Transaction tx);

    /**
     * Removes all entries from a table.
     */
    default void removeAll() {
        removeAll(null);
    }

    /**
     * Removes from a table values associated with the given keys.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keys Keys whose values are to be removed from the table. The keys cannot be {@code null}.
     * @return Keys that did not exist.
     * @throws MarshallerException if one of keys doesn't match the schema.
     */
    Collection<K> removeAll(@Nullable Transaction tx, Collection<K> keys);

    /**
     * Asynchronously remove from a table values associated with the given keys.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param keys Keys whose values are to be removed from the table. The keys cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if one of the keys doesn't match the schema.
     */
    CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, Collection<K> keys);

    /**
     * Asynchronously remove all entries from a table.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     */
    CompletableFuture<Void> removeAllAsync(@Nullable Transaction tx);

    /**
     * Asynchronously remove all entries from a table.
     */
    default CompletableFuture<Void> removeAllAsync() {
        return removeAllAsync(null);
    }

    /**
     * Gets and removes from a table a value associated with the given key.
     *
     * <p>NB: Method doesn't support {@code null} column value, use {@link #getNullableAndRemove(Transaction, Object)} instead.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return Removed value or {@code null} if the value did not exist.
     * @throws UnexpectedNullValueException If the key value is {@code null}.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    @Nullable V getAndRemove(@Nullable Transaction tx, K key);

    /**
     * Gets and removes from a table a value associated with the given key.
     *
     * <p>NB: Method doesn't support {@code null} column value, use {@link #getNullableAndRemove(Object)} instead.
     *
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return Removed value or {@code null} if the value did not exist.
     * @throws UnexpectedNullValueException If the key value is {@code null}.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default @Nullable V getAndRemove(K key) {
        return getAndRemove(null, key);
    }

    /**
     * Asynchronously gets and removes from a table a value associated with the given key.
     *
     * <p>NB: Method doesn't support {@code null} column value, use {@link #getNullableAndRemoveAsync(Transaction, Object)} instead.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, K key);

    /**
     * Asynchronously gets and removes from a table a value associated with the given key.
     *
     * <p>NB: Method doesn't support {@code null} column value, use {@link #getNullableAndRemoveAsync(Object)} instead.
     *
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default CompletableFuture<V> getAndRemoveAsync(K key) {
        return getAndRemoveAsync(null, key);
    }

    /**
     * Gets and removes from a table a value associated with the given key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return Wrapped nullable value that was removed or {@code null} if it did not exist.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    NullableValue<V> getNullableAndRemove(@Nullable Transaction tx, K key);

    /**
     * Gets and removes from a table a value associated with the given key.
     *
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return Wrapped nullable value that was removed or {@code null} if it did not exist.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default NullableValue<V> getNullableAndRemove(K key) {
        return getNullableAndRemove(null, key);
    }

    /**
     * Asynchronously gets and removes from a table a value associated with the given key.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(@Nullable Transaction tx, K key);

    /**
     * Asynchronously gets and removes from a table a value associated with the given key.
     *
     * @param key Key whose value is to be removed from the table. The key cannot be {@code null}.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key doesn't match the schema.
     */
    default CompletableFuture<NullableValue<V>> getNullableAndRemoveAsync(K key) {
        return getNullableAndRemoveAsync(null, key);
    }

    /**
     * Replaces a value for a key if it exists. This is equivalent to
     * <pre><code>
     * if (cache.containsKey(tx, key)) {
     *   cache.put(tx, key, value);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except the action is performed atomically.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    boolean replace(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Replaces a value for a key if it exists. This is equivalent to
     * <pre>{@code
     * if (cache.containsKey(tx, key)) {
     *   cache.put(tx, key, value);
     *   return true;
     * } else {
     *   return false;
     * }}</pre>
     * except the action is performed atomically.
     *
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     * @throws MarshallerException if the key and/or the value doesn't match the schema.
     */
    default boolean replace(K key, @Nullable V val) {
        return replace(null, key, val);
    }

    /**
     * Replaces an expected value for a key. This is equivalent to
     * Deprecated: use {@link #replaceExact(Transaction, Object, Object, Object)} instead.
     * <pre><code>
     * if (cache.get(tx, key) == oldValue) {
     *   cache.put(tx, key, newValue);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except the action is performed atomically.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param oldValue Expected value associated with the specified key. Can be {@code null} when mapped to a single column
     *     with a simple type.
     * @param newValue Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     * @throws MarshallerException if the key, the oldValue, or the newValue doesn't match the schema.
     */
    @Deprecated(forRemoval = true)
    default boolean replace(@Nullable Transaction tx, K key, @Nullable V oldValue, @Nullable V newValue) {
        return replaceExact(tx, key, oldValue, newValue);
    }

    /**
     * Replaces an expected value for a key. This is equivalent to
     * <pre><code>
     * if (cache.get(tx, key) == oldValue) {
     *   cache.put(tx, key, newValue);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except the action is performed atomically.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param oldValue Expected value associated with the specified key. Can be {@code null} when mapped to a single column
     *     with a simple type.
     * @param newValue Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     * @throws MarshallerException if the key, the oldValue, or the newValue doesn't match the schema.
     */
    boolean replaceExact(@Nullable Transaction tx, K key, @Nullable V oldValue, @Nullable V newValue);

    /**
     * Replaces an expected value for a key. This is equivalent to
     * <pre>{@code
     * if (cache.get(tx, key) == oldValue) {
     *   cache.put(tx, key, newValue);
     *   return true;
     * } else {
     *   return false;
     * }}</pre>
     * except the action is performed atomically.
     *
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param oldValue Expected value associated with the specified key. Can be {@code null} when mapped to a single column
     *     with a simple type.
     * @param newValue Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     * @throws MarshallerException if the key, the oldValue, or the newValue doesn't match the schema.
     */
    default boolean replaceExact(K key, @Nullable V oldValue, @Nullable V newValue) {
        return replaceExact(null, key, oldValue, newValue);
    }

    /**
     * Asynchronously replaces a value for a key if it exists. See {@link #replace(Transaction, Object, Object)}.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key or the oldValue doesn't match the schema.
     */
    CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Asynchronously replaces a value for a key if it exists. See {@link #replace(Object, Object)}.
     *
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key or the oldValue doesn't match the schema.
     */
    default CompletableFuture<Boolean> replaceAsync(K key, @Nullable V val) {
        return replaceAsync(null, key, val);
    }

    /**
     * Asynchronously replaces an expected value for a key. See {@link #replace(Transaction, Object, Object, Object)}
     * Deprecated: use {@link #replaceExactAsync(Transaction, Object, Object, Object)} instead.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param oldVal Expected value associated with the specified key. Can be {@code null} when mapped to a single column
     *     with a simple type.
     * @param newVal Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key, the oldValue, or the newValue doesn't match the schema.
     */
    @Deprecated(forRemoval = true)
    default CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, K key, @Nullable V oldVal, @Nullable V newVal) {
        return replaceExactAsync(tx, key, oldVal, newVal);
    }

    /**
     * Asynchronously replaces an expected value for a key. See {@link #replaceExact(Transaction, Object, Object, Object)}
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param oldVal Expected value associated with the specified key. Can be {@code null} when mapped to a single column
     *     with a simple type.
     * @param newVal Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key, the oldValue, or the newValue doesn't match the schema.
     */
    CompletableFuture<Boolean> replaceExactAsync(@Nullable Transaction tx, K key, @Nullable V oldVal, @Nullable V newVal);

    /**
     * Asynchronously replaces an expected value for a key. See {@link #replaceExact(Object, Object, Object)}
     *
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param oldVal Expected value associated with the specified key. Can be {@code null} when mapped to a single column
     *     with a simple type.
     * @param newVal Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key, the oldValue, or the newValue doesn't match the schema.
     */
    default CompletableFuture<Boolean> replaceExactAsync(K key, @Nullable V oldVal, @Nullable V newVal) {
        return replaceExactAsync(null, key, oldVal, newVal);
    }

    /**
     * Replaces a value for a given key if it exists. This is equivalent to
     * <pre><code>
     * if (cache.containsKey(tx, key)) {
     *   V oldValue = cache.get(tx, key);
     *   cache.put(tx, key, value);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     * except the action is performed atomically.
     *
     * <p>NB: Method doesn't support {@code null} column value, use {@link #getNullableAndReplace(Transaction, Object, Object)} instead.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Replaced value, or {@code null} if it did not exist.
     * @throws UnexpectedNullValueException If the value for the key is {@code null}.
     * @throws MarshallerException if the key, or the value doesn't match the schema.
     */
    @Nullable V getAndReplace(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Replaces a value for a given key if it exists. This is equivalent to
     * <pre>{@code
     * if (cache.containsKey(tx, key)) {
     *   V oldValue = cache.get(tx, key);
     *   cache.put(tx, key, value);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * }</pre>
     * except the action is performed atomically.
     *
     * <p>NB: Method doesn't support {@code null} column value, use {@link #getNullableAndReplace(Object, Object)} instead.
     *
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Replaced value, or {@code null} if it did not exist.
     * @throws UnexpectedNullValueException If the value for the key is {@code null}.
     * @throws MarshallerException if the key, or the value doesn't match the schema.
     */
    default @Nullable V getAndReplace(K key, @Nullable V val) {
        return getAndReplace(null, key, val);
    }

    /**
     * Asynchronously replaces a value for a given key if it exists.
     *
     * <p>NB: Method doesn't support {@code null} column value, use {@link #getNullableAndReplaceAsync(Transaction, Object, Object)}
     *     instead.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key or the value doesn't match the schema.
     * @see #getAndReplace(Transaction, Object, Object)
     */
    CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Asynchronously replaces a value for a given key if it exists.
     *
     * <p>NB: Method doesn't support {@code null} column value, use {@link #getNullableAndReplaceAsync(Object, Object)}
     *     instead.
     *
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key or the value doesn't match the schema.
     * @see #getAndReplace(Object, Object)
     */
    default CompletableFuture<V> getAndReplaceAsync(K key, @Nullable V val) {
        return getAndReplaceAsync(null, key, val);
    }

    /**
     * Replaces a value for a given key if it exists.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Wrapped nullable value that was replaced or {@code null} if it did not exist.
     * @throws MarshallerException if the key or the value doesn't match the schema.
     * @see #getAndReplace(Transaction, Object, Object)
     */
    NullableValue<V> getNullableAndReplace(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Replaces a value for a given key if it exists.
     *
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Wrapped nullable value that was replaced or {@code null} if it did not exist.
     * @throws MarshallerException if the key or the value doesn't match the schema.
     * @see #getAndReplace(Object, Object)
     */
    default NullableValue<V> getNullableAndReplace(K key, @Nullable V val) {
        return getNullableAndReplace(null, key, val);
    }

    /**
     * Asynchronously replaces a value for a given key if it exists.
     *
     * @param tx Transaction or {@code null} for implicit transaction.
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key or the value doesn't match the schema.
     * @see #getAndReplace(Transaction, Object, Object)
     */
    CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(@Nullable Transaction tx, K key, @Nullable V val);

    /**
     * Asynchronously replaces a value for a given key if it exists.
     *
     * @param key Key the specified value is associated with. The key cannot be {@code null}.
     * @param val Value to be associated with the specified key. Can be {@code null} when mapped to a single column with a simple type.
     * @return Future that represents the pending completion of the operation.
     * @throws MarshallerException if the key or the value doesn't match the schema.
     * @see #getAndReplace(Object, Object)
     */
    default CompletableFuture<NullableValue<V>> getNullableAndReplaceAsync(K key, @Nullable V val) {
        return getNullableAndReplaceAsync(null, key, val);
    }
}
