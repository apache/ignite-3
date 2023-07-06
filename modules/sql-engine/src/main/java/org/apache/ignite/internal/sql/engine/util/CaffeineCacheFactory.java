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

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.ConcurrentMap;
import org.jetbrains.annotations.Nullable;

/**
 * Factory that creates caches backed by {@link Caffeine} cache.
 */
public class CaffeineCacheFactory implements CacheFactory {
    public static CacheFactory INSTANCE = new CaffeineCacheFactory();

    private CaffeineCacheFactory() {
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> Cache<K, V> create(int size) {
        return new ConcurrentMapToCacheAdapter<>(
                Caffeine.newBuilder()
                        .maximumSize(size)
                        .<K, V>build()
                        .asMap()
        );
    }

    private static class ConcurrentMapToCacheAdapter<K, V> implements Cache<K, V> {
        private final ConcurrentMap<K, V> map;

        private ConcurrentMapToCacheAdapter(ConcurrentMap<K, V> map) {
            this.map = map;
        }

        @Override
        public @Nullable V get(K key) {
            return map.get(key);
        }

        @Override
        public void put(K key, V value) {
            map.put(key, value);
        }

        @Override
        public void clear() {
            map.clear();
        }
    }
}
