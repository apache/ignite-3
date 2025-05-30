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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A utility class to track the sliding average of recorded values, working as a circular buffer.
 * It has non-blocking implementation and considers errors in computing average due to race conditions as
 * not important, providing approximate value.
 */
public class SlidingAverageValueTracker {
    private final AtomicLongArray buffer;
    private final int capacity;
    private final int minValuesCount;
    private final double defaultValue;

    private final AtomicInteger limitedCount = new AtomicInteger(0);
    private final AtomicInteger index = new AtomicInteger(0);
    private final AtomicLong sum = new AtomicLong(0);

    /**
     * Constructor.
     *
     * @param capacity The window size.
     * @param minValuesCount Minimum number of values to compute the average, otherwise default value is provided.
     * @param defaultValue Default value to return if not enough values are recorded.
     */
    public SlidingAverageValueTracker(int capacity, int minValuesCount, double defaultValue) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be > 0.");
        }

        this.capacity = capacity;
        this.minValuesCount = minValuesCount;
        this.defaultValue = defaultValue;
        this.buffer = new AtomicLongArray(capacity);
    }

    /**
     * Records a new value, updating the sliding average.
     *
     * @param value The value to record.
     */
    public void record(long value) {
        int i = index.getAndUpdate(n -> (n + 1) % capacity);
        long old = buffer.getAndSet(i, value);

        int cnt = limitedCount.updateAndGet(n -> n < capacity ? n + 1 : n);
        if (cnt == capacity) {
            sum.addAndGet(value - old);
        } else {
            sum.addAndGet(value);
        }
    }

    /**
     * Returns the current average of the recorded values.
     *
     * @return The average value, or the default value if not enough values are recorded.
     */
    public double avg() {
        if (limitedCount.get() < minValuesCount) {
            return defaultValue;
        }

        return sum.get() / (double) limitedCount.get();
    }
}
