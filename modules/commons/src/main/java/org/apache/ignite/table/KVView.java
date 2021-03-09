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

import java.util.List;
import java.util.SortedMap;

/**
 * Key-Value adapter for Table.
 */
public interface KVView<K, V> {
    /**
     * Gets value associated with given key.
     *
     * @param key The key whose associated value is to be returned.
     * @return Value or {@code null}, if it does not exist.
     */
    public V get(K key);

    /**
     * Gets values associated with given keys.
     *
     * @param keys Sorted collection of keys whose associated values are to be returned.
     * @return Values associated with given keys.
     */
    public List<V> getAll(List<K> keys);

    /**
     * Determines if the table contains an entry for the specified key.
     *
     * @param key key whose presence in this cache is to be tested.
     * @return {@code true} if this map contains a mapping for the specified key, {@code false} otherwise.
     */
    boolean containsKey(K key);

    /**
     * Puts value associated with given key into the table.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     */
    public void put(K key, V val);

    /**
     * Put associated key-value pairs.
     *
     * @param pairs Sorted collection of key-value pairs.
     */
    public void putAll(SortedMap<K, V> pairs);

    /**
     * Puts and return value associated with given key into the table.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return Previous value or {@code null}, if it does not exist.
     */
    public V getAndPut(K key, V val);

    /**
     * Puts value associated with given key into the table if it is not exists.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return {@code True} if put was successful, {@code false} otherwise.
     */
    public boolean putIfAbsent(K key, V val);

    /**
     * Removes value associated with given key from the table.
     *
     * @param key Key whose mapping is to be removed from the table.
     * @return {@code True} if a value associated with the specified key was successfully removed, {@code false} otherwise.
     */
    public boolean remove(K key);

    /**
     * Removes exact value associated with given key from the table.
     *
     * @param key key whose mapping is to be removed from the table.
     * @param val Value expected to be associated with the specified key.
     * @return {@code True} if the value associated with the specified key was successfully removed, {@code false} otherwise.
     */
    public boolean remove(K key, V val);

    /**
     * Removes values associated with given keys from the table.
     *
     * @param keys Sorted collection of keys whose mapping is to be removed from the table.
     */
    public void removeAll(List<K> keys);

    /**
     * Removes and returns value associated with given key from the table.
     *
     * @param key Key whose mapping is to be removed from the table.
     * @return The value if one existed or {@code null} if no mapping existed for this key
     */
    public V getAndRemove(K key);

    /**
     * Atomically replaces the entry for a key only if currently mapped to some value. This is equivalent to
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param key Key with which the specified value is associated.
     * @param val Value to be associated with the specified key.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     */
    public boolean replace(K key, V val);

    /**
     * Atomically replaces the entry for a key only if currently mapped to some value. This is equivalent to
     * <pre><code>
     * if (cache.get(key) == oldVal) {
     *   cache.put(key, newVal);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param key Key with which the specified value is associated.
     * @param oldVal Expected value associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     */
    public boolean replace(K key, V oldVal, V newVal);

    /**
     * Atomically replaces the value for a given key if and only if there is a value currently mapped by the key.
     * This is equivalent to
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
     * @param key Key with which the specified value is associated.
     * @param val Value to be associated with the specified key.
     * @return Previous value associated with the specified key, or {@code null} if there was no mapping for the key.
     */
    public V getAndReplace(K key, V val);

    /**
     * Invokes an InvokeProcessor against the row associated with the provided key.
     *
     * @param key Key that associated with the row that invoke processor will be applied to.
     * @param proc Processor to invoke.
     * @param <R> Result type.
     * @return Result of the processing.
     */
    public <R> R invoke(K key, InvokeProcessor<KVViewEntry<K, V>, R> proc);

    /**
     * Invokes an InvokeProcessor against the rows associated with the provided keys.
     *
     * @param keys Sorted collection of key associated with table row.
     * @param proc Processor to invoke.
     * @param <R> Result type.
     * @return Results of the processing.
     */
    public <R> List<R> invokeAll(List<K> keys, InvokeProcessor<KVViewEntry<K, V>, R> proc);

    /**
     * Key-value view entry.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     */
    public static interface KVViewEntry<K, V> {
        /**
         * @return Entry key.
         */
        K key();

        /**
         * @return Entry value.
         */
        V value();

        /**
         * Sets new value to entry.
         */
        void setValue(V value);
    }
}
