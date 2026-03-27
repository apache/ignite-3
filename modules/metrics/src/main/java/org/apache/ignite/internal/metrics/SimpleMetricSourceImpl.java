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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.jetbrains.annotations.Nullable;

/**
 * Default {@link SimpleMetricSource} implementation.
 *
 * <p>Metrics are created via factory methods and stored in a {@link ConcurrentHashMap}.
 * Enable/disable controls registry visibility only — metrics themselves are always alive
 * and values persist across cycles (unlike {@link AbstractMetricSource} which recreates them).
 *
 * <p>Expensive metrics ({@link HitRateMetric}, {@link DistributionMetric}) are guarded —
 * mutations become no-ops when disabled. Cheap metrics ({@link LongAdderMetric}, etc.)
 * always record to avoid volatile read overhead on hot paths.
 *
 * <p>Usage (composition, not inheritance):
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
 *
 * @see SimpleMetricSource
 */
public final class SimpleMetricSourceImpl implements SimpleMetricSource {
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<SimpleMetricSourceImpl, MetricSet> METRIC_SET_UPD =
            AtomicReferenceFieldUpdater.newUpdater(SimpleMetricSourceImpl.class, MetricSet.class, "metricSet");

    private final String name;

    private final @Nullable String description;

    private final @Nullable String group;

    private final ConcurrentHashMap<String, Metric> metrics = new ConcurrentHashMap<>();

    /** Non-null when enabled. */
    private volatile @Nullable MetricSet metricSet;

    /** Constructor. */
    public SimpleMetricSourceImpl(String name) {
        this(name, null, null);
    }

    /** Constructor. */
    public SimpleMetricSourceImpl(String name, @Nullable String description) {
        this(name, description, null);
    }

    /** Constructor. */
    public SimpleMetricSourceImpl(String name, @Nullable String description, @Nullable String group) {
        this.name = Objects.requireNonNull(name, "Metric source name must not be null");
        this.description = description;
        this.group = group;
    }

    @Override
    public final String name() {
        return name;
    }

    @Override
    public @Nullable String description() {
        return description;
    }

    @Override
    public @Nullable String group() {
        return group;
    }

    @Override
    public AtomicIntMetric atomicInt(String name, @Nullable String description) {
        return register(new AtomicIntMetric(name, description));
    }

    @Override
    public IntGauge intGauge(String name, @Nullable String description, IntSupplier supplier) {
        return register(new IntGauge(name, description, supplier));
    }

    @Override
    public AtomicLongMetric atomicLong(String name, @Nullable String description) {
        return register(new AtomicLongMetric(name, description));
    }

    @Override
    public LongAdderMetric longAdder(String name, @Nullable String description) {
        return register(new LongAdderMetric(name, description));
    }

    @Override
    public LongGauge longGauge(String name, @Nullable String description, LongSupplier supplier) {
        return register(new LongGauge(name, description, supplier));
    }

    @Override
    public AtomicDoubleMetric atomicDouble(String name, @Nullable String description) {
        return register(new AtomicDoubleMetric(name, description));
    }

    @Override
    public DoubleAdderMetric doubleAdder(String name, @Nullable String description) {
        return register(new DoubleAdderMetric(name, description));
    }

    @Override
    public DoubleGauge doubleGauge(String name, @Nullable String description, DoubleSupplier supplier) {
        return register(new DoubleGauge(name, description, supplier));
    }

    @Override
    public HitRateMetric hitRate(String name, @Nullable String description, long rateTimeInterval) {
        return register(new GuardedHitRateMetric(name, description, rateTimeInterval, this::enabled));
    }

    @Override
    public HitRateMetric hitRate(String name, @Nullable String description, long rateTimeInterval, int size) {
        return register(new GuardedHitRateMetric(name, description, rateTimeInterval, size, this::enabled));
    }

    @Override
    public DistributionMetric distribution(String name, @Nullable String description, long[] bounds) {
        return register(new GuardedDistributionMetric(name, description, bounds, this::enabled));
    }

    @Override
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

    // -- Guarded wrappers for expensive metrics (HitRate, Distribution). --
    //
    // These check enabled() before each mutation to avoid expensive work when disabled.
    // Cheap metrics (LongAdder, AtomicLong, etc.) are NOT wrapped — the volatile read
    // overhead on the hot path isn't worth it since metrics are typically enabled.

    @FunctionalInterface
    private interface MetricRecordGuard {
        boolean enabled();
    }

    /** HitRateMetric that becomes a no-op when the source is disabled. */
    private static class GuardedHitRateMetric extends HitRateMetric {
        private final MetricRecordGuard guard;

        GuardedHitRateMetric(String name, @Nullable String desc, long rateTimeInterval, MetricRecordGuard guard) {
            super(name, desc, rateTimeInterval);
            this.guard = guard;
        }

        GuardedHitRateMetric(String name, @Nullable String desc, long rateTimeInterval, int size,
                MetricRecordGuard guard) {
            super(name, desc, rateTimeInterval, size);
            this.guard = guard;
        }

        @Override
        public void add(long hits) {
            if (guard.enabled()) {
                super.add(hits);
            }
        }

        @Override
        public void increment() {
            if (guard.enabled()) {
                super.increment();
            }
        }
    }

    /** DistributionMetric that becomes a no-op when the source is disabled. */
    private static class GuardedDistributionMetric extends DistributionMetric {
        private final MetricRecordGuard guard;

        GuardedDistributionMetric(String name, @Nullable String desc, long[] bounds, MetricRecordGuard guard) {
            super(name, desc, bounds);
            this.guard = guard;
        }

        @Override
        public void add(long x) {
            if (guard.enabled()) {
                super.add(x);
            }
        }
    }
}
