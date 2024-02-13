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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Factory that creates caches backed by {@link Caffeine} cache.
 */
public class CaffeineCacheFactory implements CacheFactory {
    public static final CacheFactory INSTANCE = new CaffeineCacheFactory();

    @Nullable
    private final Executor executor;

    private CaffeineCacheFactory() {
        this(null);
    }

    private CaffeineCacheFactory(@Nullable Executor executor) {
        this.executor = executor;
    }

    /** Creates a cache factory with the given executor for running auxiliary tasks. */
    @TestOnly
    public static CacheFactory create(Executor executor) {
        return new CaffeineCacheFactory(executor);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> Cache<K, V> create(int size, StatsCounter statCounter) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(size);

        if (executor != null) {
            builder.executor(executor);
        }

        if (statCounter != null) {
            builder.recordStats(() -> new CaffeineStatsCounterAdapter(statCounter));
        }

        return new CaffeineCacheToCacheAdapter<>(builder.build());
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> Cache<K, V> create(int size) {
        return create(size, null);
    }

    private static class CaffeineStatsCounterAdapter implements com.github.benmanes.caffeine.cache.stats.StatsCounter {
        private final StatsCounter statsCounter;

        private CaffeineStatsCounterAdapter(StatsCounter statsCounter) {
            this.statsCounter = statsCounter;
        }

        @Override
        public void recordHits(@NonNegative int count) {
            statsCounter.recordHits(count);
        }

        @Override
        public void recordMisses(@NonNegative int count) {
            statsCounter.recordMisses(count);
        }

        @Override
        public void recordLoadSuccess(@NonNegative long loadTime) {
            // No op
        }

        @Override
        public void recordLoadFailure(@NonNegative long loadTime) {
            // No op
        }

        @Override
        public void recordEviction(@NonNegative int weight, RemovalCause cause) {
            // No op
        }

        @Override
        public CacheStats snapshot() {
            return null;
        }
    }

    private static class CaffeineCacheToCacheAdapter<K, V> implements Cache<K, V> {
        private final com.github.benmanes.caffeine.cache.Cache<K, V> cache;

        private CaffeineCacheToCacheAdapter(com.github.benmanes.caffeine.cache.Cache<K, V> cache) {
            this.cache = cache;
        }

        @Override
        public @Nullable V get(K key) {
            return cache.getIfPresent(key);
        }

        @Override
        public V get(K key, Function<? super K, ? extends V> mappingFunction) {
            return cache.get(key, mappingFunction);
        }

        @Override
        public void put(K key, V value) {
            cache.put(key, value);
        }

        @Override
        public void clear() {
            cache.invalidateAll();
        }

        @Override
        public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
            return cache.asMap().compute(key, remappingFunction);
        }

        @Override
        public void removeIfValue(Predicate<? super V> valueFilter) {
            cache.asMap().values().removeIf(valueFilter);
        }
    }
}
