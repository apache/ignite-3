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

import static java.lang.Math.ceil;
import static java.lang.Math.min;
import static java.lang.Math.sqrt;

import java.util.concurrent.atomic.AtomicIntegerArray;

public class SlidingHistogram {
    private static final int BUCKET_COUNT = 500;
    private final AtomicIntegerArray bucketCounters = new AtomicIntegerArray(BUCKET_COUNT);
    private final int[] circularBuffer;
    private final int windowSize;
    private final long estimationDefault;
    private volatile int index = 0;
    private volatile int currentSize = 0;

    public SlidingHistogram(int windowSize, long estimationDefault) {
        this.windowSize = windowSize;
        this.estimationDefault = estimationDefault;
        this.circularBuffer = new int[windowSize];
    }

    public synchronized void record(long value) {
        int bucket = mapToBucket(value);

        if (currentSize == windowSize) {
            int oldBucket = circularBuffer[index];
            bucketCounters.decrementAndGet(oldBucket);
        } else {
            currentSize++;
        }

        circularBuffer[index] = bucket;
        bucketCounters.incrementAndGet(bucket);

        index = (index + 1) % windowSize;
    }

    public synchronized long estimatePercentile(double percentile) {
        if (currentSize < windowSize) {
            // Not enough data to estimate, return default value.
            return estimationDefault;
        }

        int target = (int) ceil(currentSize * percentile);
        int cumulative = 0;

        for (int i = 0; i < BUCKET_COUNT; i++) {
            cumulative += bucketCounters.get(i);
            if (cumulative >= target) {
                return estimateValueFromBucket(i);
            }
        }

        return estimateValueFromBucket(BUCKET_COUNT - 1);
    }

    private static int mapToBucket(long value) {
        if (value <= 0) return 0;
        long b = mappingFunc(value);

        // Last bucket is for overflow.
        return (int) min(b, BUCKET_COUNT - 2);
    }

    private static long estimateValueFromBucket(int bucket) {
        if (bucket == BUCKET_COUNT - 1) return Long.MAX_VALUE; // overflown bucket

        return mappingFuncReverse(bucket);
    }

    private static long mappingFunc(long value) {
        return (long) sqrt(value);
    }

    private static long mappingFuncReverse(long value) {
        return value * value;
    }
}
