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
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentsCache.CacheValue;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentsCache.RefValue;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Test class to verify {@link FragmentsCache}.
 */
public class FragmentsCacheTest {
    @Test
    public void testClear() {
        FragmentsCache<Integer, String> cache = new FragmentsCache<>(3);

        cache.computeIfAbsent(1, ignore -> new CacheValue<>(Set.of(1, 2), "1"));
        cache.computeIfAbsent(2, ignore -> new CacheValue<>(Set.of(2, 3), "2"));
        cache.computeIfAbsent(3, ignore -> new CacheValue<>(Set.of(3, 2), "3"));

        assertThat(cache.states().entrySet(), hasSize(3));

        cache.clear();

        assertThat(cache.states().entrySet(), Matchers.empty());
    }

    @Test
    public void testStateChange() {
        FragmentsCache<Integer, Object> cache = new FragmentsCache<>(1024);

        Object exp = new Object();
        CacheValue<Object> v = new CacheValue<>(Set.of(1, 2), exp);
        cache.computeIfAbsent(1, ignore -> v);
        // TODO TBD
    }

    @Test
    public void basicTimestampCacheRotation() throws InterruptedException {
        FragmentsCache<Integer, String> cache = new FragmentsCache<>(5);

        cache.computeIfAbsent(1, ignore -> new CacheValue<>(Set.of(1, 2), "1"));
        cache.computeIfAbsent(2, ignore -> new CacheValue<>(Set.of(2, 1), "2"));

        assertThat(cache.states().entrySet(), hasSize(2));
        assertThat(cache.values().entrySet(), hasSize(2));

        List<Integer> keys = IntStream.range(3, 20).boxed().collect(Collectors.toList());
        putRange(cache, 3, 17);

        touch(cache, keys);

        boolean success = IgniteTestUtils.waitForCondition(() -> {
            try {
                verifyConsistency(cache);
                return true;
            } catch (Throwable t) {
                return false;
            }
        }, 10_000);

        assertTrue(success);
    }

    Set<Integer> putRange(FragmentsCache<Integer, String> cache, int off, int cnt) {
        Set<Integer> res = new HashSet<>(cnt);

        for (int i = off; i < off + cnt; i++) {
            String str = String.valueOf(i);
            Set<Integer> set = randomSet(off, off + cnt, 3);
            cache.computeIfAbsent(i, ignore -> new CacheValue<>(set, str));

            res.addAll(set);
        }

        return res;
    }

    private Set<Integer> randomSet(int min, int max, int cnt) {
        Set<Integer> res = new HashSet<>(cnt);

        for (int i = 0; i < cnt; i++) {
            res.add(min + ThreadLocalRandom.current().nextInt(max - min));
        }

        return res;
    }

    void verifyConsistency(FragmentsCache<Integer, String> cache) {
        Map<Integer, Integer> expected = new HashMap<>();

        System.out.println(">xxx> values ");
        for (Entry<Integer, CacheValue<String>> e : cache.values().entrySet()) {
            System.out.println(" " + e.getKey() + " " + e.getValue().ids());
        }

        System.out.println(">xxx> refs ");

        for (Entry<Integer, FragmentsCache<Integer, String>.RefValue> e : cache.states().entrySet()) {
            System.out.println(" " + e.getKey() + " " + e.getValue().counter);
        }

        for (CacheValue<String> v : cache.values().values()) {
            for (Integer id : v.ids()) {
                expected.merge(id, 1, Integer::sum);
            }
        }

        assertThat(cache.states().entrySet(), hasSize(expected.size()));

        for (Entry<Integer, Integer> e : expected.entrySet()) {
            RefValue actual = cache.states().get(e.getKey());

            assertNotNull(actual, "key=" + e.getKey());

            assertEquals(e.getValue(), actual.counter, "key=" + e.getKey());
        }
    }

    private <K> void touch(FragmentsCache<K, ?> cache, Collection<K> keys) {
        keys.forEach(k -> cache.values().get(k));
    }
}
