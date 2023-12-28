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
 * Represents bounded cache, with the ability to mark cache entries as outdated through external identifiers.
 *
 * <p>Each entry in this cache must have a primary key, but may also have one or more foreign keys.
 * If at least one of the foreign keys has been invalidated after cache value calculated, then the next cache access will result in a
 * recalculation of the cache value.
 */
public class FragmentsCache<K, V> {
    private final HybridClockImpl clock = new HybridClockImpl();
    private final ConcurrentMap<K, CacheValue<V>> values;
    private final ConcurrentMap<Integer, StateValue> states = new ConcurrentHashMap<>();

    /** Constructs cache. */
    FragmentsCache(int size, Executor executor) {
        values = Caffeine.newBuilder()
                .executor(executor)
                .maximumSize(size)
                .removalListener(this::onRemoval)
                .build()
                .asMap();
    }

    /** Computes new value if the key is absent in the cache. */
    public V putIfAbsentUpdateIfNeeded(K key, Supplier<Collection<Integer>> refSupplier, Function<K, V> mappingFunction) {
        return values.compute(key, (k, value) -> {
            if (value == null) {
                HybridTimestamp ts = clock.now();
                Collection<Integer> refs = refSupplier.get();

                for (Integer id : refs) {
                    incrementRefCounter(id);
                }

                V newVal = mappingFunction.apply(k);

                return new CacheValue<>(ts, refs, newVal);
            }

            // Check that the value is still valid.
            for (Integer id : value.refs) {
                if (value.ts.compareTo(states.get(id).ts) < 0) {
                    // The value may be outdated.
                    return value.copy(mappingFunction.apply(key));
                }
            }

            return value;
        }).value;
    }

    /** Invalidates cache entry by cache value id. */
    public void invalidate(int id) {
        states.computeIfPresent(id, (k, v) -> new StateValue(v.counter, clock.now()));
    }

    /** Invalidates cache entries. */
    public void clear() {
        values.clear();
    }

    @TestOnly
    ConcurrentMap<K, CacheValue<V>> values() {
        return values;
    }

    @TestOnly
    ConcurrentMap<Integer, StateValue> states() {
        return states;
    }

    private void onRemoval(@Nullable K planId, @Nullable CacheValue<V> value, RemovalCause cause) {
        if (!cause.wasEvicted() && cause != RemovalCause.EXPLICIT) {
            return;
        }

        assert value != null;

        for (int id : value.refs) {
            decrementRefCounter(id);
        }
    }

    private void incrementRefCounter(int id) {
        StateValue initial = new StateValue(1, HybridTimestamp.MIN_VALUE);

        for (;;) {
            StateValue prevVal = states.putIfAbsent(id, initial);

            if (prevVal == null) {
                return;
            }

            StateValue newVal = new StateValue(prevVal.counter + 1, prevVal.ts);

            if (states.replace(id, prevVal, newVal)) {
                return;
            }
        }
    }

    private void decrementRefCounter(int id) {
        for (;;) {
            StateValue prevVal = states.get(id);

            if (prevVal.counter == 1) {
                if (states.remove(id, prevVal)) {
                    return;
                }

                continue;
            }

            if (states.replace(id, prevVal, new StateValue(prevVal.counter - 1, prevVal.ts))) {
                return;
            }
        }
    }

    static class CacheValue<V> {
        private final Collection<Integer> refs;
        private final V value;
        private final HybridTimestamp ts;

        CacheValue(HybridTimestamp ts, Collection<Integer> refs, V value) {
            this.ts = ts;
            this.refs = refs;
            this.value = value;
        }

        CacheValue<V> copy(V value) {
            return new CacheValue<>(ts, refs, value);
        }

        @TestOnly
        Collection<Integer> refs() {
            return refs;
        }

        @TestOnly
        V value() {
            return value;
        }
    }

    static class StateValue {
        final int counter;
        final HybridTimestamp ts;

        StateValue(int counter, @Nullable HybridTimestamp ts) {
            this.counter = counter;
            this.ts = ts;
        }
    }
}
