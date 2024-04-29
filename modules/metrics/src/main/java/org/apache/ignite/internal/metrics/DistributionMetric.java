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

package org.apache.ignite.internal.metrics;

import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Distribution metric calculates counts of measurements that gets into each bounds interval.
 * Note, that {@link #value()} will return array length of {@code bounds.length + 1}.
 * Last element contains count of measurements bigger than most right value of bounds.
 */
public class DistributionMetric extends AbstractMetric implements CompositeMetric {
    /** Updater that atomically updates {@link #scalarMetrics} field. */
    private static final AtomicReferenceFieldUpdater<DistributionMetric, List> scalarMetricsUpdater =
            AtomicReferenceFieldUpdater.newUpdater(DistributionMetric.class, List.class, "scalarMetrics");

    /** Distribution metric first interval low bound. */
    public static final long FIRST_LOW_BOUND = 0;

    /** Distribution metric first interval low bound, string representation. */
    public static final String FIRST_LOW_BOUND_STRING = "0";

    /** Distribution metric last interval high bound. */
    public static final String INF = "inf";

    /** Distribution range divider. */
    public static final char RANGE_DIVIDER = '_';

    /** Distribution bucket divider. */
    public static final String BUCKET_DIVIDER = ", ";

    /** Distribution metric name and value divider. */
    public static final char METRIC_DIVIDER = ':';

    /** Count of measurement for each bound. */
    private final AtomicLongArray measurements;

    /** Bounds of measurements. */
    private final long[] bounds;

    /** Value string formatter or {@code null} to use default formatter. */
    private final Function<long[], String> stringFormatter;

    /** List of scalar metrics. */
    private volatile List<Metric> scalarMetrics = null;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param desc Description.
     * @param bounds Bounds of the buckets. The array must be sorted and its elements must be unique. The 0-th bound must be
     *               greater or equal to {@code 0}.
     */
    public DistributionMetric(String name, @Nullable String desc, long[] bounds) {
        this(name, desc, bounds, null);
    }

    /**
     * The constructor.
     *
     * @param name Name.
     * @param desc Description.
     * @param bounds Bounds of the buckets. The array must be sorted and its elements must be unique. The 0-th bound must be
     *               greater or equal to {@code 0}.
     * @param stringFormatter Formatter to represent the metric as a string or {@code null} to use the default formatter.
     */
    public DistributionMetric(String name, @Nullable String desc, long[] bounds, @Nullable Function<long[], String> stringFormatter) {
        super(name, desc);

        assert bounds != null && bounds.length > 0;
        assert bounds[0] >= FIRST_LOW_BOUND;
        assert isSortedAndUnique(bounds);

        this.bounds = bounds;
        this.measurements = new AtomicLongArray(bounds.length + 1);
        this.stringFormatter = stringFormatter;
    }

    /**
     * Check whether given array is sorted and its elements are unique.
     *
     * @param arr Array to check.
     * @return {@code True} if array sorted and its elements are unique, {@code false} otherwise.
     */
    private static boolean isSortedAndUnique(long[] arr) {
        if (arr.length < 2) {
            return true;
        }

        for (int i = 1; i < arr.length; i++) {
            if (arr[i - 1] >= arr[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Adds a value to the interval which the value belongs to.
     *
     * @param x Value.
     */
    public void add(long x) {
        assert x >= 0;

        // Expect arrays of few elements.
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
    @Override
    public String getValueAsString() {
        if (stringFormatter != null) {
            return stringFormatter.apply(asScalarMetrics().stream().mapToLong(m -> ((LongMetric) m).value()).toArray());
        }

        StringBuilder sb = new StringBuilder("[");

        List<Metric> scalarMetrics = asScalarMetrics();

        for (int i = 0; i < scalarMetrics.size(); i++) {
            LongMetric m = (LongMetric) scalarMetrics.get(i);

            sb.append(m.name())
                    .append(METRIC_DIVIDER)
                    .append(m.value());

            if (i < scalarMetrics.size() - 1) {
                sb.append(BUCKET_DIVIDER);
            }
        }

        sb.append(']');

        return sb.toString();
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
    @Override
    public List<Metric> asScalarMetrics() {
        if (scalarMetrics == null) {
            List<Metric> metrics = new ArrayList<>();

            String from = FIRST_LOW_BOUND_STRING;

            for (int i = 0; i < measurements.length(); i++) {
                String to = i == measurements.length() - 1 ? INF : String.valueOf(bounds[i]);

                String name = name() + RANGE_DIVIDER + from + RANGE_DIVIDER + to;

                final int index = i;
                LongGauge gauge = new LongGauge(name, "Single distribution bucket", () -> measurements.get(index));

                metrics.add(gauge);

                from = to;
            }

            scalarMetricsUpdater.compareAndSet(this, null, unmodifiableList(metrics));
        }

        return scalarMetrics;
    }
}
