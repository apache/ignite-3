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

package org.apache.ignite.internal.util;

import static java.util.Collections.synchronizedCollection;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runMultiThreaded;
import static org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue.EMPTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * For {@link IgniteConcurrentMultiPairQueue} testing.
 */
public class IgniteConcurrentMultiPairQueueTest {
    private IgniteConcurrentMultiPairQueue<Integer, Integer> queue;

    private IgniteConcurrentMultiPairQueue<Integer, Integer> queue2;

    private Map<Integer, Collection<Integer>> mapForCheck;

    private Map<Integer, Collection<Integer>> mapForCheck2;

    private Integer[] arr1 = {1, 3, 5, 7, 9, 11, 13, 15, 17, 19};

    private Integer[] arr2 = {2, 4};

    private Integer[] arr3 = {100, 200, 300, 400, 500, 600, 600, 700};

    private Integer[] arr4 = {};

    private Integer[] arr5 = {};

    private Integer[] arr6 = {};

    @BeforeEach
    void setUp() {
        Collection<IgniteBiTuple<Integer, Integer[]>> keyWithArr = new HashSet<>();

        mapForCheck = new ConcurrentHashMap<>();

        mapForCheck2 = new ConcurrentHashMap<>();

        keyWithArr.add(new IgniteBiTuple<>(10, arr2));
        keyWithArr.add(new IgniteBiTuple<>(20, arr1));
        keyWithArr.add(new IgniteBiTuple<>(30, arr4));
        keyWithArr.add(new IgniteBiTuple<>(40, arr5));
        keyWithArr.add(new IgniteBiTuple<>(50, arr3));
        keyWithArr.add(new IgniteBiTuple<>(60, arr6));

        mapForCheck.put(10, synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        mapForCheck.put(20, synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        mapForCheck.put(50, synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));

        mapForCheck2.put(10, synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        mapForCheck2.put(20, synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        mapForCheck2.put(50, synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));

        queue = new IgniteConcurrentMultiPairQueue<>(keyWithArr);

        Map<Integer, Collection<Integer>> keyWithColl = new HashMap<>();

        keyWithColl.put(10, synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        keyWithColl.put(20, synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        keyWithColl.put(30, synchronizedCollection(new ArrayList<>(Arrays.asList(arr4))));
        keyWithColl.put(40, synchronizedCollection(new ArrayList<>(Arrays.asList(arr5))));
        keyWithColl.put(50, synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));
        keyWithColl.put(60, synchronizedCollection(new ArrayList<>(Arrays.asList(arr6))));

        queue2 = new IgniteConcurrentMultiPairQueue<>(keyWithColl);
    }

    @Test
    public void testGridConcurrentMultiPairQueueCorrectness() throws Exception {
        runMultiThreaded(() -> {
            Result<Integer, Integer> res = new Result<>();

            while (queue.next(res)) {
                assertTrue(mapForCheck.containsKey(res.getKey()));

                assertTrue(mapForCheck.get(res.getKey()).remove(res.getValue()));

                Collection<Integer> coll = mapForCheck.get(res.getKey());

                if (coll != null && coll.isEmpty()) {
                    mapForCheck.remove(res.getKey(), coll);
                }
            }

            return null;
        }, current().nextInt(1, 20), "GridConcurrentMultiPairQueue arr test");

        assertTrue(mapForCheck.isEmpty());

        assertTrue(queue.isEmpty());

        assertEquals(queue.initialSize(), arr1.length + arr2.length + arr3.length + arr4.length);

        runMultiThreaded(() -> {
            Result<Integer, Integer> res = new Result<>();

            while (queue2.next(res)) {
                assertTrue(mapForCheck2.containsKey(res.getKey()));

                assertTrue(mapForCheck2.get(res.getKey()).remove(res.getValue()));

                Collection<Integer> coll = mapForCheck2.get(res.getKey());

                if (coll != null && coll.isEmpty()) {
                    mapForCheck2.remove(res.getKey(), coll);
                }
            }

            return null;
        }, current().nextInt(1, 20), "GridConcurrentMultiPairQueue coll test");

        assertTrue(mapForCheck2.isEmpty());

        assertTrue(queue2.isEmpty());

        assertEquals(queue2.initialSize(), arr1.length + arr2.length + arr3.length + arr4.length);
    }

    @Test
    void testSize() {
        assertEquals(0, EMPTY.size());

        assertEquals(2, new IgniteConcurrentMultiPairQueue<>(Map.of(0, List.of(1, 2))).size());
        assertEquals(5, new IgniteConcurrentMultiPairQueue<>(Map.of(0, List.of(1, 2, 3, 4, 5))).size());

        assertEquals(7, new IgniteConcurrentMultiPairQueue<>(Map.of(0, List.of(1, 2), 1, List.of(3, 4, 5, 6, 7))).size());

        IgniteConcurrentMultiPairQueue pairQueue = new IgniteConcurrentMultiPairQueue<>(Map.of(0, List.of(1, 2, 3)));

        assertEquals(3, pairQueue.size());

        assertTrue(pairQueue.next(new Result()));
        assertEquals(2, pairQueue.size());

        assertTrue(pairQueue.next(new Result()));
        assertEquals(1, pairQueue.size());

        assertTrue(pairQueue.next(new Result()));
        assertEquals(0, pairQueue.size());

        assertFalse(pairQueue.next(new Result()));
        assertEquals(0, pairQueue.size());
    }
}
