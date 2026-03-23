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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.jetbrains.annotations.Nullable;

/**
 * Metric set builder.
 */
public class MetricSetBuilder {
    /** Metrics set name. */
    private final String name;

    /** Metrics set description. */
    private final String description;

    /** Metrics set group name. */
    private final String group;

    /** Registered metrics. */
    private @Nullable Map<String, Metric> metrics = new LinkedHashMap<>();

    /**
     *  Creates a new instance of metrics set builder with given name.
     *
     * @param name Name of metrics set. Can't be null.
     */
    public MetricSetBuilder(String name) {
        this(name, null, null);
    }

    /**
     *  Creates a new instance of metrics set builder with given name.
     *
     * @param name Name of metrics set. Can't be null.
     */
    public MetricSetBuilder(String name, @Nullable String description, @Nullable String group) {
        Objects.requireNonNull(name, "Metrics set name can't be null");

        this.name = name;
        this.description = description;
        this.group = group;
    }

    /**
     * Build a metric set.
     *
     * @return Metric set.
     */
    public MetricSet build() {
        if (metrics == null) {
            throw new IllegalStateException("Builder can't be used twice.");
        }

        MetricSet reg = new MetricSet(name, description, group, metrics);

        metrics = null;

        return reg;
    }

    /**
     * Returns metrics set name.
     *
     * @return Metrics set name.
     */
    public String name() {
        return name;
    }

    /**
     * Adds existing metric with the specified name.
     *
     * @param metric Metric.
     * @throws IllegalStateException If metric with given name is already added.
     */
    @SuppressWarnings("unchecked")
    public  <T extends Metric> T register(T metric) {
        T old = (T) metrics.putIfAbsent(metric.name(), metric);

        if (old != null) {
            throw new IllegalStateException("Metric with given name is already registered [name=" + name
                    + ", metric=" + metric + ']');
        }

        return metric;
    }

    /**
     * Add an atomic integer metric.
     *
     * @param name Name.
     * @param description Description.
     * @return Atomic integer metric.
     */
    public AtomicIntMetric atomicInt(String name, @Nullable String description) {
        return register(new AtomicIntMetric(name, description));
    }

    /**
     * Add an integer gauge.
     *
     * @param name Name.
     * @param description Description.
     * @param supplier Supplier of the value.
     * @return Integer gauge.
     */
    public IntGauge intGauge(String name, @Nullable String description, IntSupplier supplier) {
        return register(new IntGauge(name, description, supplier));
    }

    /**
     * Add an atomic long metric.
     *
     * @param name Name.
     * @param description Description.
     * @return Atomic long metric.
     */
    public AtomicLongMetric atomicLong(String name, @Nullable String description) {
        return register(new AtomicLongMetric(name, description));
    }

    /**
     * Add a long adder metric.
     *
     * @param name Name.
     * @param description Description.
     * @return Long adder metric.
     */
    public LongAdderMetric longAdder(String name, @Nullable String description) {
        return register(new LongAdderMetric(name, description));
    }

    /**
     * Add a long gauge.
     *
     * @param name Name.
     * @param description Description.
     * @param supplier Supplier of the value.
     * @return Long gauge.
     */
    public LongGauge longGauge(String name, @Nullable String description, LongSupplier supplier) {
        return register(new LongGauge(name, description, supplier));
    }

    /**
     * Add an atomic double metric.
     *
     * @param name Name.
     * @param description Description.
     * @return Atomic double metric.
     */
    public AtomicDoubleMetric atomicDouble(String name, @Nullable String description) {
        return register(new AtomicDoubleMetric(name, description));
    }

    /**
     * Add a double adder metric.
     *
     * @param name Name.
     * @param description Description.
     * @return Double adder metric.
     */
    public DoubleAdderMetric doubleAdder(String name, @Nullable String description) {
        return register(new DoubleAdderMetric(name, description));
    }

    /**
     * Add a double gauge.
     *
     * @param name Name.
     * @param description Description.
     * @param supplier Supplier of the value.
     * @return Double gauge.
     */
    public DoubleGauge doubleGauge(String name, @Nullable String description, DoubleSupplier supplier) {
        return register(new DoubleGauge(name, description, supplier));
    }

    /**
     * Add a hit rate metric.
     *
     * @param name Name.
     * @param description Description.
     * @param rateTimeInterval Rate time interval in milliseconds.
     * @return Hit rate metric.
     */
    public HitRateMetric hitRate(String name, @Nullable String description, long rateTimeInterval) {
        return register(new HitRateMetric(name, description, rateTimeInterval));
    }

    /**
     * Add a hit rate metric.
     *
     * @param name Name.
     * @param description Description.
     * @param rateTimeInterval Rate time interval in milliseconds.
     * @param size Counters array size.
     * @return Hit rate metric.
     */
    public HitRateMetric hitRate(String name, @Nullable String description, long rateTimeInterval, int size) {
        return register(new HitRateMetric(name, description, rateTimeInterval, size));
    }

    /**
     * Add a distribution metric.
     *
     * @param name Name.
     * @param description Description.
     * @param bounds Bounds of the buckets
     * @return Distribution metrics.
     */
    public DistributionMetric distribution(String name, @Nullable String description, long[] bounds) {
        return register(new DistributionMetric(name, description, bounds));
    }
}
