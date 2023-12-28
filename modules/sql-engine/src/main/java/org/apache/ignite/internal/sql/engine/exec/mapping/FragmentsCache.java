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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Cache for mapped fragments.
 */
public class FragmentsCache<K, V> {
    private static final HybridClockImpl clock = new HybridClockImpl();

    private final Cache<K, CacheValue<V>> values;
    private final ConcurrentMap<Integer, RefValue> states = new ConcurrentHashMap<>();

    /** Constructs cache. */
    public FragmentsCache(int size) {
        values = Caffeine.newBuilder()
                .maximumSize(size)
                .removalListener(this::onRemoval)
                .build();
    }

    /** Computes new value if the key is absent in the cache. */
    public CacheValue<V> computeIfAbsent(K planId, Function<K, CacheValue<V>> mappingFunction) {
        return values.asMap().compute(planId, (k, v) -> {
            if (v == null) {
                CacheValue<V> newVal = mappingFunction.apply(k);

                increment(newVal.ids);

                return newVal;
            }

            if (isValidTs(v.ts, v.ids)) {
                return v;
            }

            return mappingFunction.apply(planId);
        });
    }

    /** Invalidates cache entry by cache value id. */
    public void invalidateById(int id) {
        states.computeIfPresent(id, (k, v) -> new RefValue(v.counter, clock.now()));
    }

    /** Invalidates cache entries. */
    public void clear() {
        values.invalidateAll();
    }

    @TestOnly
    ConcurrentMap<K, CacheValue<V>> values() {
        return values.asMap();
    }

    @TestOnly
    ConcurrentMap<Integer, RefValue> states() {
        return states;
    }

    private void onRemoval(@Nullable K planId, @Nullable CacheValue<V> value, RemovalCause cause) {
        if (value != null) {
            decrement(value.ids);
        }
    }

    private boolean isValidTs(HybridTimestamp cachedTs, Set<Integer> ids) {
        for (Integer id : ids) {
            if (cachedTs.compareTo(states.get(id).ts) < 0) {
                return false;
            }
        }

        return true;
    }

    private void increment(Set<Integer> ids) {
        for (Integer id : ids) {
            increment(id);
        }
    }

    private void increment(Integer id) {
        RefValue initial = new RefValue(1, HybridTimestamp.MIN_VALUE);

        for (;;) {
            RefValue prevVal = states.putIfAbsent(id, initial);

            if (prevVal == null) {
                return;
            }

            RefValue newVal = new RefValue(prevVal.counter + 1, prevVal.ts);

            if (states.replace(id, prevVal, newVal)) {
                return;
            }
        }
    }

    private void decrement(Set<Integer> ids) {
        for (int id : ids) {
            decrement(id);
        }
    }

    private void decrement(int id) {
        for (;;) {
            RefValue prevVal = states.get(id);

            if (prevVal.counter == 1) {
                if (states.remove(id, prevVal)) {
                    return;
                }

                continue;
            }

            if (states.replace(id, prevVal, new RefValue(prevVal.counter - 1, prevVal.ts))) {
                return;
            }
        }
    }

    static class CacheValue<V> {
        private final Set<Integer> ids;
        private final V mapping;
        private final HybridTimestamp ts;

        CacheValue(Set<Integer> ids, V mapping) {
            this.ids = ids;
            this.mapping = mapping;
            this.ts = clock.now();
        }

        public V mapping() {
            return mapping;
        }

        @TestOnly
        Set<Integer> ids() {
            return ids;
        }
    }

    class RefValue {
        final int counter;
        final HybridTimestamp ts;

        RefValue(int counter, @Nullable HybridTimestamp ts) {
            this.counter = counter;
            this.ts = ts;
        }
    }
}
