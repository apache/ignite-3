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

package org.apache.ignite.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

public interface CacheStore<K, V> {
    CacheSession beginSession();

    V load(K key) throws IgniteException;

    CompletableFuture<V> loadAsync(K key) throws IgniteException;

    Map<K, V> loadAll(Iterable<? extends K> keys) throws IgniteException;

    void write(@Nullable CacheSession session, Entry<? extends K, ? extends V> entry) throws IgniteException;

    void writeAll(@Nullable CacheSession session, Collection<Entry<? extends K, ? extends V>> entries) throws IgniteException;

    void delete(@Nullable CacheSession session, Object key) throws IgniteException;

    void deleteAll(@Nullable CacheSession session, Collection<?> keys) throws IgniteException; // TODO cache exception.
}
