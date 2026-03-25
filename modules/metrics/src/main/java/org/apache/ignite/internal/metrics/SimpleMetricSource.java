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

import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.jetbrains.annotations.Nullable;

/**
 * Metric source with factory methods for creating metrics. Unlike {@link AbstractMetricSource},
 * metrics exist immediately after creation — no Holder classes, no null-checks.
 *
 * <p>Enable/disable controls visibility in the {@link MetricRegistry}, not the metrics themselves.
 *
 * @see SimpleMetricSourceImpl
 */
public interface SimpleMetricSource extends MetricSource {
    /** Creates an {@link AtomicIntMetric}. */
    AtomicIntMetric atomicInt(String name, @Nullable String description);

    /** Creates an {@link IntGauge}. */
    IntGauge intGauge(String name, @Nullable String description, IntSupplier supplier);

    /** Creates an {@link AtomicLongMetric}. */
    AtomicLongMetric atomicLong(String name, @Nullable String description);

    /** Creates a {@link LongAdderMetric}. */
    LongAdderMetric longAdder(String name, @Nullable String description);

    /** Creates a {@link LongGauge}. */
    LongGauge longGauge(String name, @Nullable String description, LongSupplier supplier);

    /** Creates an {@link AtomicDoubleMetric}. */
    AtomicDoubleMetric atomicDouble(String name, @Nullable String description);

    /** Creates a {@link DoubleAdderMetric}. */
    DoubleAdderMetric doubleAdder(String name, @Nullable String description);

    /** Creates a {@link DoubleGauge}. */
    DoubleGauge doubleGauge(String name, @Nullable String description, DoubleSupplier supplier);

    /** Creates a {@link HitRateMetric} with default counters array size. */
    HitRateMetric hitRate(String name, @Nullable String description, long rateTimeInterval);

    /** Creates a {@link HitRateMetric} with custom counters array size. */
    HitRateMetric hitRate(String name, @Nullable String description, long rateTimeInterval, int size);

    /** Creates a {@link DistributionMetric}. Bounds must be sorted, unique, and start from {@code >= 0}. */
    DistributionMetric distribution(String name, @Nullable String description, long[] bounds);

    /**
     * Registers a pre-created metric. Use this for types without a dedicated factory method
     * ({@link StringGauge}, {@link UuidGauge}, etc.). Factory methods delegate here internally.
     *
     * @return The same metric instance.
     * @throws IllegalStateException If a metric with the given name is already registered.
     */
    <T extends Metric> T register(T metric);
}
