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

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link MetricSource} with factory methods for creating and registering metrics.
 *
 * <p>Metrics are created via factory methods and stored in a {@link ConcurrentHashMap}.
 * Enable/disable controls registry visibility only — metrics themselves are always alive
 * and values persist across enable/disable cycles.
 *
 * <p>Usage:
 * <pre>{@code
 * public class MyMetrics {
 *     private final LongAdderMetric requests;
 *     private final DistributionMetric duration;
 *
 *     public MyMetrics(SimpleMetricSource source) {
 *         requests = source.longAdder("Requests", "Total requests.");
 *         duration = source.distribution("Duration", "Request duration in ms.",
 *                 new long[]{1, 5, 10, 50, 100, 500});
 *     }
 *
 *     public void onRequest(long durationMs) {
 *         requests.increment();
 *         duration.add(durationMs);
 *     }
 * }
 * }</pre>
 */
public final class SimpleMetricSource implements MetricSource {
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<SimpleMetricSource, MetricSet> METRIC_SET_UPD =
            AtomicReferenceFieldUpdater.newUpdater(SimpleMetricSource.class, MetricSet.class, "metricSet");

    private final String name;

    private final String description;

    private final @Nullable String group;

    private final ConcurrentMap<String, Metric> metrics = new ConcurrentHashMap<>();

    /** Non-null when enabled. */
    private volatile @Nullable MetricSet metricSet;

    /**
     * Constructor.
     *
     * @param name Metric source name.
     * @param description Metric source description.
     */
    public SimpleMetricSource(String name, String description) {
        this(name, description, null);
    }

    /**
     * Constructor.
     *
     * @param name Metric source name.
     * @param description Metric source description.
     * @param group Optional group name for additional grouping in external systems (e.g. JMX).
     */
    public SimpleMetricSource(String name, String description, @Nullable String group) {
        this.name = Objects.requireNonNull(name, "name");
        this.description = Objects.requireNonNull(description, "description");
        this.group = group;
    }

    @Override
    public final String name() {
        return name;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public @Nullable String group() {
        return group;
    }

    /** Creates an {@link AtomicIntMetric}. */
    public AtomicIntMetric atomicInt(String name, @Nullable String description) {
        return register(new AtomicIntMetric(name, description));
    }

    /** Creates an {@link IntGauge}. */
    public IntGauge intGauge(String name, @Nullable String description, IntSupplier supplier) {
        return register(new IntGauge(name, description, supplier));
    }

    /** Creates an {@link AtomicLongMetric}. */
    public AtomicLongMetric atomicLong(String name, @Nullable String description) {
        return register(new AtomicLongMetric(name, description));
    }

    /** Creates a {@link LongAdderMetric}. */
    public LongAdderMetric longAdder(String name, @Nullable String description) {
        return register(new LongAdderMetric(name, description));
    }

    /** Creates a {@link LongGauge}. */
    public LongGauge longGauge(String name, @Nullable String description, LongSupplier supplier) {
        return register(new LongGauge(name, description, supplier));
    }

    /** Creates an {@link AtomicDoubleMetric}. */
    public AtomicDoubleMetric atomicDouble(String name, @Nullable String description) {
        return register(new AtomicDoubleMetric(name, description));
    }

    /** Creates a {@link DoubleAdderMetric}. */
    public DoubleAdderMetric doubleAdder(String name, @Nullable String description) {
        return register(new DoubleAdderMetric(name, description));
    }

    /** Creates a {@link DoubleGauge}. */
    public DoubleGauge doubleGauge(String name, @Nullable String description, DoubleSupplier supplier) {
        return register(new DoubleGauge(name, description, supplier));
    }

    /** Creates a {@link HitRateMetric} with default counters array size. */
    public HitRateMetric hitRate(String name, @Nullable String description, long rateTimeInterval) {
        return register(new HitRateMetric(name, description, rateTimeInterval));
    }

    /** Creates a {@link HitRateMetric} with custom counters array size. */
    public HitRateMetric hitRate(String name, @Nullable String description, long rateTimeInterval, int size) {
        return register(new HitRateMetric(name, description, rateTimeInterval, size));
    }

    /** Creates a {@link DistributionMetric}. Bounds must be sorted, unique, and start from {@code >= 0}. */
    public DistributionMetric distribution(String name, @Nullable String description, long[] bounds) {
        return register(new DistributionMetric(name, description, bounds));
    }

    /**
     * Registers a pre-created metric. Use this for types without a dedicated factory method
     * ({@link StringGauge}, {@link UuidGauge}, etc.).
     *
     * @return The same metric instance.
     * @throws IllegalStateException If a metric with the given name is already registered.
     */
    public <T extends Metric> T register(T metric) {
        Objects.requireNonNull(metric, "metric");

        Metric existing = metrics.putIfAbsent(metric.name(), metric);

        if (existing != null) {
            throw new IllegalStateException("Metric with given name is already registered [sourceName="
                    + this.name + ", metricName=" + metric.name() + ']');
        }

        return metric;
    }

    @Override
    public @Nullable MetricSet enable() {
        if (metricSet != null) {
            return null;
        }

        // TODO: IGNITE-28412 consider making MetricSet observable instead of a single snapshot to support post-enable metric addition.
        MetricSet newMetricSet = new MetricSet(name, description, group, new HashMap<>(metrics));

        if (METRIC_SET_UPD.compareAndSet(this, null, newMetricSet)) {
            return newMetricSet;
        }

        return null;
    }

    @Override
    public void disable() {
        METRIC_SET_UPD.set(this, null);
    }

    @Override
    public boolean enabled() {
        return metricSet != null;
    }

}
