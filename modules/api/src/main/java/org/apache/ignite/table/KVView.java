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

/**
 * Key-Value interface.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public interface KVView<K, V> {
    /**
     * Gets value associated with given key.
     *
     * @param key The key whose associated value is to be returned.
     * @return Value or {@code null}, if it does not exist.
     */
    V get(K key);

    /**
     * Gets values associated with given keys.
     *
     * @param keys Ordered collection of keys whose associated values are to be returned.
     * @return Values associated with given keys.
     */
    Collection<V> getAll(Collection<K> keys);

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
    void put(K key, V val);

    /**
     * Put associated key-value pairs.
     *
     * @param pairs Ordered collection of key-value pairs.
     */
    void putAll(Map<K, V> pairs);

    /**
     * Puts and return value associated with given key into the table.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return Previous value or {@code null}, if it does not exist.
     */
    V getAndPut(K key, V val);

    /**
     * Puts value associated with given key into the table if it is not exists.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return {@code True} if put was successful, {@code false} otherwise.
     */
    boolean putIfAbsent(K key, V val);

    /**
     * Removes value associated with given key from the table.
     *
     * @param key Key whose mapping is to be removed from the table.
     * @return {@code True} if a value associated with the specified key was successfully removed, {@code false} otherwise.
     */
    boolean remove(K key);

    /**
     * Removes exact value associated with given key from the table.
     *
     * @param key key whose mapping is to be removed from the table.
     * @param val Value expected to be associated with the specified key.
     * @return {@code True} if the value associated with the specified key was successfully removed, {@code false} otherwise.
     */
    boolean remove(K key, V val);

    /**
     * Removes values associated with given keys from the table.
     *
     * @param keys Ordered collection of keys whose mapping is to be removed from the table.
     * @return Keys that had not been exists and were not removed.
     */
    Collection<K> removeAll(Collection<K> keys);

    /**
     * Removes and returns value associated with given key from the table.
     *
     * @param key Key whose mapping is to be removed from the table.
     * @return The value if one existed or {@code null} if no mapping existed for this key
     */
    V getAndRemove(K key);

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
    boolean replace(K key, V val);

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
    boolean replace(K key, V oldVal, V newVal);

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
    V getAndReplace(K key, V val);

    /**
     * Invokes an invoke processor code against the value associated with the provided key.
     *
     * @param key Key that associated with the value that invoke processor will be applied to.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @param <R> Invoke processor result type.
     * @return Result of the processing.
     * @see InvokeProcessor
     */
    <R extends Serializable> R invoke(K key, InvokeProcessor<K, V, R> proc, Serializable... args);

    /**
     * Invokes an invoke processor code against values associated with the provided keys.
     *
     * @param <R> Invoke processor result type.
     * @param keys Ordered collection of keys which values associated with should be processed.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @return Results of the processing.
     * @see InvokeProcessor
     */
    <R extends Serializable> Map<K, R> invokeAll(Collection<K> keys, InvokeProcessor<K, V, R> proc,
        Serializable... args);

}
