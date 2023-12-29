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
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Represents bounded cache, with the ability to mark cache entries as outdated through external identifiers (foreign keys).
 *
 * <p>Each entry in this cache must have a primary key, but may also have one or more foreign keys.
 * If at least one of the foreign keys has been invalidated after cache value calculated, then the next cache access will result in a
 * recalculation of the cache value.
 *
 * @param <K1> Key type.
 * @param <K2> Foreign key type.
 * @param <V> Value type.
 */
public class ReferencesCache<K1, K2, V> {
    private final HybridClockImpl clock = new HybridClockImpl();
    private final ConcurrentMap<K1, CacheValue<K2, V>> data;
    private final ConcurrentMap<K2, ForeignKeyState> foreignKeys = new ConcurrentHashMap<>();

    /** Constructs cache. */
    public ReferencesCache(int size, Executor executor) {
        data = Caffeine.newBuilder()
                .executor(executor)
                .maximumSize(size)
                .removalListener(this::onRemoval)
                .build()
                .asMap();
    }

    /** Computes new value if the key is absent in the cache. */
    public V putOrUpdate(
            K1 key,
            Supplier<Collection<K2>> foreignKeys,
            Function<K1, V> mappingFunction
    ) {
        CacheValue<K2, V> cacheValue = data.compute(key, (k, value) -> {
            if (value == null) {
                HybridTimestamp ts = clock.now();
                Collection<K2> refs = foreignKeys.get();

                for (K2 id : refs) {
                    incrementRefCounter(id);
                }

                V newVal = mappingFunction.apply(k);

                return new CacheValue<>(ts, refs, newVal);
            }

            // Check that the value is still valid.
            for (K2 id : value.foreignKeys) {
                if (value.ts.compareTo(this.foreignKeys.get(id).ts) < 0) {
                    // The value may be outdated.
                    return value.copy(mappingFunction.apply(key));
                }
            }

            return value;
        });

        return cacheValue.value;
    }

    /** Invalidates cache entry by cache value id. */
    public void invalidate(K2 id) {
        foreignKeys.computeIfPresent(id, (k, v) -> new ForeignKeyState(v.counter, clock.now()));
    }

    /** Invalidates cache entries. */
    public void clear() {
        data.clear();
    }

    @TestOnly
    ConcurrentMap<K1, CacheValue<K2, V>> data() {
        return data;
    }

    @TestOnly
    ConcurrentMap<K2, ForeignKeyState> foreignKeys() {
        return foreignKeys;
    }

    private void onRemoval(@Nullable K1 planId, @Nullable CacheValue<K2, V> value, RemovalCause cause) {
        if (!cause.wasEvicted() && cause != RemovalCause.EXPLICIT) {
            return;
        }

        assert value != null;

        for (K2 id : value.foreignKeys) {
            decrementRefCounter(id);
        }
    }

    private void incrementRefCounter(K2 id) {
        ForeignKeyState initial = new ForeignKeyState(1, HybridTimestamp.MIN_VALUE);

        for (;;) {
            ForeignKeyState prevVal = foreignKeys.putIfAbsent(id, initial);

            if (prevVal == null) {
                return;
            }

            ForeignKeyState newVal = new ForeignKeyState(prevVal.counter + 1, prevVal.ts);

            if (foreignKeys.replace(id, prevVal, newVal)) {
                return;
            }
        }
    }

    private void decrementRefCounter(K2 id) {
        for (;;) {
            ForeignKeyState prevVal = foreignKeys.get(id);

            if (prevVal.counter == 1) {
                if (foreignKeys.remove(id, prevVal)) {
                    return;
                }

                continue;
            }

            if (foreignKeys.replace(id, prevVal, new ForeignKeyState(prevVal.counter - 1, prevVal.ts))) {
                return;
            }
        }
    }

    static class CacheValue<K, V> {
        private final Collection<K> foreignKeys;
        private final HybridTimestamp ts;
        private final V value;

        CacheValue(HybridTimestamp ts, Collection<K> foreignKeys, V value) {
            this.ts = ts;
            this.foreignKeys = foreignKeys;
            this.value = value;
        }

        CacheValue<K, V> copy(V value) {
            return new CacheValue<>(ts, foreignKeys, value);
        }

        @TestOnly
        Collection<K> foreignKeys() {
            return foreignKeys;
        }
    }

    static class ForeignKeyState {
        final int counter;
        final HybridTimestamp ts;

        ForeignKeyState(int counter, @Nullable HybridTimestamp ts) {
            this.counter = counter;
            this.ts = ts;
        }
    }
}
