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

package org.apache.ignite.internal.sql.engine.util.cache;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

/**
 * A mapping from keys to values.
 *
 * <p>Implementations of this interface are expected to be thread-safe, and can be safely accessed by
 * multiple concurrent threads.
 *
 * @param <K> Type of the key object.
 * @param <V> Type of the value object.
 */
public interface Cache<K, V> {
    /**
     * Returns value associated with given key, or null if there no mapping exists.
     *
     * @param key A key to look up value for.
     * @return A value.
     */
    @Nullable
    V get(K key);

    /**
     * Returns the value associated with the {@code key} in this cache, obtaining that value from the {@code mappingFunction} if necessary.
     * This method provides a simple substitute for the conventional "if cached, return; otherwise create, cache and return" pattern.
     *
     * @param key A key to look up value for.
     * @param mappingFunction the function to compute a value.
     * @return the current (existing or computed) value associated with the specified key.
     */
    V get(K key, Function<? super K, ? extends V> mappingFunction);

    /**
     * Associates the {@code value} with the {@code key} in this cache. If the cache previously contained a value associated with the
     * {@code key}, the old value is replaced by the new {@code value}.
     *
     * @param key A key with which the specified value is to be associated.
     * @param value A value to be associated with the specified key.
     */
    void put(K key, V value);

    /** Clears the given cache. That is, remove all keys and associated values. */
    void clear();

    /**
     * Attempts to compute a mapping for the specified key and its current mapped value (or {@code null} if there is no current mapping).
     *
     * @param key Key with which the specified value is to be associated.
     * @param remappingFunction The remapping function to compute a value.
     * @return The new value associated with the specified key, or null if none.
     */
    V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

    /**
     * Removes all cache entries whose values match the specified predicate.
     *
     * @param valueFilter A predicate which returns {@code true} for the values of entries to be removed.
     */
    void removeIfValue(Predicate<? super V> valueFilter);

    /**
     * Invalidates a cache entry for the given key.
     *
     * @param key Key.
     */
    void invalidate(K key);

    /**
     * Returns the number for entries this cache.
     *
     * @return The number for entries this cache.
     */
    int size();
}
