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

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.StatsCounter;
import org.jetbrains.annotations.Nullable;

/**
 * A factory creating cache that keeps no objects.
 *
 * <p>That is, any instance of cache returned by this factory always returns {@code null}
 * from {@link Cache#get(Object)} method, and do nothing when {@link Cache#put(Object, Object)}
 * is invoked.
 */
public class EmptyCacheFactory implements CacheFactory {
    public static CacheFactory INSTANCE = new EmptyCacheFactory();

    private EmptyCacheFactory() {
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> Cache<K, V> create(int size) {
        return new EmptyCache<>();
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> Cache<K, V> create(int size, StatsCounter statCounter) {
        return create(size);
    }

    private static class EmptyCache<K, V> implements Cache<K, V> {
        @Override
        public @Nullable V get(K key) {
            return null;
        }

        @Override
        public V get(K key, Function<? super K, ? extends V> mappingFunction) {
            return mappingFunction.apply(key);
        }

        @Override
        public void put(K key, V value) {
            // NO-OP
        }

        @Override
        public void clear() {
            // NO-OP
        }

        @Override
        public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
            return remappingFunction.apply(key, null);
        }

        @Override
        public void removeIfValue(Predicate<? super V> valueFilter) {
            // NO-OP.
        }

        @Override
        public void invalidate(K key) {
            // NO-OP.
        }

        @Override
        public int size() {
            return 0;
        }
    }
}
