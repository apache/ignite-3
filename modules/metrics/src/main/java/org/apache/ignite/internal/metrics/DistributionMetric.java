/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;

/**
 * Distribution metric calculates counts of measurements that gets into each bounds interval.
 * Note, that {@link #value()} will return array length of {@code bounds.length + 1}.
 * Last element contains count of measurements bigger than most right value of bounds.
 */
public class DistributionMetric extends AbstractMetric implements CompositeMetric {
    /** Count of measurement for each bound. */
    private final AtomicLongArray measurements;

    /** Bounds of measurements. */
    private final long[] bounds;

    /**
     * The constructor.
     *
     * @param name Name.
     * @param desc Description.
     * @param bounds Bounds of the buckets.
     */
    public DistributionMetric(String name, @Nullable String desc, long[] bounds) {
        super(name, desc);

        assert bounds != null && bounds.length > 0;
        assert isSorted(bounds);

        this.bounds = bounds;
        this.measurements = new AtomicLongArray(bounds.length + 1);
    }

    /**
     * Check whether given array is sorted.
     *
     * @param arr Array to check.
     * @return {@code True} if array sorted, {@code false} otherwise.
     */
    public static boolean isSorted(long[] arr) {
        if (arr == null || arr.length < 2)
            return true;

        for (int i = 1; i < arr.length; i++) {
            if (arr[i - 1] > arr[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Sets value.
     *
     * @param x Value.
     */
    public void value(long x) {
        assert x >= 0;

        //Expect arrays of few elements.
        for (int i = 0; i < bounds.length; i++) {
            if (x <= bounds[i]) {
                measurements.incrementAndGet(i);

                return;
            }
        }

        measurements.incrementAndGet(bounds.length);
    }

    /** {@inheritDoc} */
    public long[] value() {
        long[] res = new long[measurements.length()];

        for (int i = 0; i < measurements.length(); i++) {
            res[i] = measurements.get(i);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public @Nullable String getAsString() {
        return asScalarMetrics().stream()
            .map(m -> m.name() + ":" + ((LongMetric) m).value())
            .collect(Collectors.joining(", ", "[", "]"));
    }

    /**
     * Bounds of the buckets of distribution.
     *
     * @return Bounds of the buckets of distribution.
     */
    public long[] bounds() {
        return bounds;
    }

    /** {@inheritDoc} */
    @Override public List<Metric> asScalarMetrics() {
        List<Metric> res = new ArrayList<>();

        for (int i = 0; i < measurements.length(); i++) {
            String from = i == 0 ? "0" : String.valueOf(bounds[i - 1]);
            String to = i == measurements.length() - 1 ? "" : String.valueOf(bounds[i]);

            String name = new StringBuilder(from).append('_').append(to).toString();

            final int index = i;
            LongGauge gauge = new LongGauge(name, "Single distribution bucket", () -> measurements.get(index));

            res.add(gauge);
        }

        return res;
    }
}
