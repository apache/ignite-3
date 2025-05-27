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

public class SlidingAverageValueTracker {
    private final long[] buffer;
    private final int capacity;
    private final int minValuesCount;
    private final double defaultValue;

    private final AtomicInteger size = new AtomicInteger(0);
    private final AtomicInteger index = new AtomicInteger(0);
    private final AtomicLong sum = new AtomicLong(0);

    public SlidingAverageValueTracker(int capacity, int minValuesCount, double defaultValue) {
        if (capacity <= 0)
            throw new IllegalArgumentException("capacity must be > 0");

        this.capacity = capacity;
        this.minValuesCount = minValuesCount;
        this.defaultValue = defaultValue;
        this.buffer = new long[capacity];
    }

    public void record(long value) {
        int i = index.getAndUpdate(n -> (n + 1) % capacity);
        long old = buffer[i];
        buffer[i] = value;

        int currentSize = size.get();
        if (currentSize < capacity) {
            size.incrementAndGet();
            sum.addAndGet(value);
        } else {
            sum.addAndGet(value - old);
        }
    }

    public double avg() {
        if (size.get() < minValuesCount)
            return defaultValue;

        return sum.get() / (double) size.get();
    }
}
