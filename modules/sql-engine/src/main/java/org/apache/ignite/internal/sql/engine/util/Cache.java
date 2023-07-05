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

package org.apache.ignite.internal.sql.engine.util;

import org.jetbrains.annotations.Nullable;

/**
 *  A mapping from keys to values.
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
    @Nullable V get(K key);

    /**
     * Associates the {@code value} with the {@code key} in this cache. If the cache previously
     * contained a value associated with the {@code key}, the old value is replaced by the new
     * {@code value}.
     *
     * @param key A key with which the specified value is to be associated.
     * @param value A value to be associated with the specified key.
     */
    void put(K key, V value);

    /** Clears the given cache. That is, remove all keys and associated values. */
    void clear();
}
