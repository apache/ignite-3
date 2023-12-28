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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentsCache.CacheValue;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentsCache.StateValue;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Test class to verify {@link FragmentsCache}.
 */
public class FragmentsCacheTest {
    private static final long EVICTION_TIMEOUT_MS = 5_000;

    @Test
    public void referencesRemovedOnCacheClear() throws InterruptedException {
        TestCacheDecorator<Object> tester = new TestCacheDecorator<>(10);

        tester.put(1, new Object());
        tester.put(2, List.of(2), new Object());
        tester.put(3, List.of(3, 1), new Object());
        tester.put(4, List.of(1, 3), new Object());
        tester.put(5, List.of(1, 2, 3), new Object());

        assertThat(tester.states().entrySet(), hasSize(3));

        tester.cache.clear();

        IgniteTestUtils.waitForCondition(() -> tester.states().isEmpty(), EVICTION_TIMEOUT_MS);
    }

    @Test
    public void entryMustBeRefreshedAfterInvalidationUsingForeignKey() {
        TestCacheDecorator<Object> tester = new TestCacheDecorator<>(1024);

        Object initialValue = new Object();
        List<Integer> foreignKeys = List.of(12, -5, 6, 17);

        tester.put(1, foreignKeys, initialValue);
        tester.put(2, initialValue);

        assertSame(initialValue, tester.put(1, new Object()));
        assertSame(initialValue, tester.put(2, new Object()));

        // invalid foreign key.
        tester.cache.invalidate(1);

        assertSame(initialValue, tester.put(1, new Object()));
        assertSame(initialValue, tester.put(2, new Object()));

        Object newValue = null;

        for (int key : foreignKeys) {
            tester.cache.invalidate(key);

            newValue = new Object();

            assertSame(newValue, tester.put(1, newValue));
            assertSame(initialValue, tester.put(2, newValue));
        }

        assertSame(newValue, tester.put(1, newValue));
        assertSame(initialValue, tester.put(2, newValue));
    }

    @Test
    public void checkForeignKeysEntriesCleanupAfterEviction() throws InterruptedException {
        int cacheSize = 10;
        int valuesCount = 100;
        int foreignKeysCount = 20;
        int foreignKeysPerValue = 3;

        TestCacheDecorator<Object> tester = new TestCacheDecorator<>(cacheSize);

        for (int i = 0; i < valuesCount; i++) {
            tester.put(i, randomSet(foreignKeysPerValue, foreignKeysCount), new Object());
        }

        tester.verifyConsistency();
    }

    @Test
    public void checkAllOperationsConsistencyMultiThreaded() throws Exception {
        TestCacheDecorator<Object> tester = new TestCacheDecorator<>(100);

        int totalRefs = 400;
        int refsPerKey = 5;
        int keysPerThread = 5_000;
        int threadsCount = 8;

        CyclicBarrier barrier = new CyclicBarrier(threadsCount);
        AtomicInteger threadCounter = new AtomicInteger();

        IgniteTestUtils.runMultiThreaded(() -> {
            int idx = threadCounter.getAndIncrement();

            List<Integer> keys = new ArrayList<>(keysPerThread);
            List<Set<Integer>> sets = new ArrayList<>(keysPerThread);

            int rangeStart = idx * keysPerThread;
            int rangeEnd = rangeStart + keysPerThread;

            for (int i = rangeStart; i < rangeEnd; i++) {
                keys.add(i);
                sets.add(randomSet(refsPerKey, totalRefs));
            }

            barrier.await();

            for (int attempts = 0; attempts < 20; attempts++) {
                for (int i = 0; i < ThreadLocalRandom.current().nextInt(10); i++) {
                    tester.cache.invalidate(ThreadLocalRandom.current().nextInt(totalRefs));
                }

                for (int i = 0; i < keys.size(); i++) {
                    tester.put(keys.get(i), sets.get(i), new Object());
                }

                if (idx % 2 != 0 && attempts < attempts / 2) {
                    tester.cache.clear();
                }
            }

            return null;
        }, threadsCount, "worker");

        tester.verifyConsistency();
    }

    static class TestCacheDecorator<V> {
        private final FragmentsCache<Integer, V> cache;
        private final ThreadPoolExecutor pool =
                new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

        TestCacheDecorator(int size) {
            this.cache = new FragmentsCache<>(size, pool);
        }

        V put(Integer key, V value) {
            return put(key, Collections.emptyList(), value);
        }

        V put(Integer key, Collection<Integer> refs, V value) {
            return cache.putIfAbsentUpdateIfNeeded(key, () -> refs, (k) -> value);
        }

        Map<Integer, StateValue> states() {
            return cache.states();
        }

        void verifyConsistency() throws InterruptedException {
            // Ensure that all cache background tasks completed.
            IgniteTestUtils.waitForCondition(() -> pool.getQueue().isEmpty() && pool.getActiveCount() == 0, EVICTION_TIMEOUT_MS);

            Map<Integer, Integer> expectedForeignKeys = new HashMap<>();

            for (CacheValue<?> v : cache.values().values()) {
                for (Integer id : v.refs()) {
                    expectedForeignKeys.merge(id, 1, Integer::sum);
                }
            }

            assertThat(cache.states().keySet(), equalTo(expectedForeignKeys.keySet()));

            for (Entry<Integer, Integer> e : expectedForeignKeys.entrySet()) {
                StateValue actual = cache.states().get(e.getKey());

                assertNotNull(actual, "Foreign key is missing in states cache: " + e.getKey());
                assertEquals(e.getValue(), actual.counter, "Invalid references count for key: " + e.getKey());
            }
        }
    }

    private static Set<Integer> randomSet(int size, int totalRefs) {
        Set<Integer> res = new HashSet<>(size);

        for (int i = 0; i < size; i++) {
            res.add(ThreadLocalRandom.current().nextInt(totalRefs));
        }

        return res;
    }
}
