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

import static java.util.function.Predicate.not;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.tostring.S;

/**
 * Concurrent queue that wraps collection of {@code Pair<K, V[]>}.
 *
 * <p>The guarantee {@link #next} provided is sequentially emptify values per key array. i.e. input like:
 *
 * <br> p1 = new Pair<1, [1, 3, 5, 7]>
 * <br> p2 = new Pair<2, [2, 3]>
 * <br> p3 = new Pair<3, [200, 100]>
 * <br> and further sequence of {@code poll} or {@code forEach} calls may produce output like:
 * <br> [1, 1], [1, 3], [1, 5], [1, 7], [2, 2], [2, 3], [3, 200], [3, 100]
 *
 * @param <K> The type of key in input pair collection.
 * @param <V> The type of value array.
 */
public class IgniteConcurrentMultiPairQueue<K, V> {
    /** Empty pair queue. */
    public static final IgniteConcurrentMultiPairQueue EMPTY = new IgniteConcurrentMultiPairQueue<>(Map.of());

    /** Inner holder. */
    private final V[][] vals;

    /** Storage for every array length. */
    private final int[] lenSeq;

    /** Current absolute position. */
    private final AtomicInteger pos = new AtomicInteger();

    /** Precalculated max position. */
    private final int maxPos;

    /** Keys array. */
    private final K[] keysArr;

    /**
     * Constructor.
     *
     * @param items Items.
     */
    public IgniteConcurrentMultiPairQueue(Map<K, ? extends Collection<V>> items) {
        int pairCnt = (int) items.entrySet().stream().map(Map.Entry::getValue).filter(not(Collection::isEmpty)).count();

        vals = (V[][]) new Object[pairCnt][];

        keysArr = (K[]) new Object[pairCnt];

        lenSeq = new int[pairCnt];

        int keyPos = 0;

        int size = -1;

        for (Map.Entry<K, ? extends Collection<V>> p : items.entrySet()) {
            if (p.getValue().isEmpty()) {
                continue;
            }

            keysArr[keyPos] = p.getKey();

            lenSeq[keyPos] = size += p.getValue().size();

            vals[keyPos++] = (V[]) p.getValue().toArray();
        }

        maxPos = size + 1;
    }

    /**
     * Constructor.
     *
     * @param items Items.
     */
    public IgniteConcurrentMultiPairQueue(Collection<IgniteBiTuple<K, V[]>> items) {
        int pairCnt = (int) items.stream().map(Map.Entry::getValue).filter(k -> k.length > 0).count();

        vals = (V[][]) new Object[pairCnt][];

        keysArr = (K[]) new Object[pairCnt];

        lenSeq = new int[pairCnt];

        int keyPos = 0;

        int size = -1;

        for (Map.Entry<K, V[]> p : items) {
            if (p.getValue().length == 0) {
                continue;
            }

            keysArr[keyPos] = p.getKey();

            lenSeq[keyPos] = size += p.getValue().length;

            vals[keyPos++] = p.getValue();
        }

        maxPos = size + 1;
    }

    /**
     * Retrieves and removes the head of this queue, or returns {@code false} if this queue is empty.
     *
     * @param res State holder.
     * @return {@code true} if not empty result, or {@code false} if this queue is empty.
     */
    public boolean next(Result<K, V> res) {
        int absPos = pos.getAndIncrement();

        if (absPos >= maxPos) {
            res.set(null, null, 0);

            return false;
        }

        int segment = res.getSegment();

        if (absPos > lenSeq[segment]) {
            segment = Arrays.binarySearch(lenSeq, segment, lenSeq.length - 1, absPos);

            segment = segment < 0 ? -segment - 1 : segment;
        }

        int relPos = segment == 0 ? absPos : (absPos - lenSeq[segment - 1] - 1);

        K key = keysArr[segment];

        res.set(key, vals[segment][relPos], segment);

        return true;
    }

    /**
     * Returns {@code true} if empty.
     */
    public boolean isEmpty() {
        return pos.get() >= maxPos;
    }

    /**
     * Returns constant initialisation size.
     */
    public int initialSize() {
        return maxPos;
    }

    /**
     * Returns the number of elements in this queue.
     */
    public int size() {
        int currentPosition = pos.get();

        return currentPosition >= maxPos ? 0 : maxPos - currentPosition;
    }

    /**
     * State holder.
     */
    public static class Result<K, V> {
        /** Current segment. */
        private int segment;

        /** Key holder. */
        private K key;

        /** Value holder. */
        private V val;

        /**
         * Current state setter.
         *
         * @param k Key.
         * @param v Value.
         * @param seg Segment.
         */
        public void set(K k, V v, int seg) {
            key = k;
            val = v;
            segment = seg;
        }

        /**
         * Returns current segment.
         */
        private int getSegment() {
            return segment;
        }

        /**
         * Returns current key.
         */
        public K getKey() {
            return key;
        }

        /**
         * Returns current value.
         */
        public V getValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(Result.class, this);
        }
    }
}
