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

package org.apache.ignite.internal.storage.pagememory.mv;

import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.pagememory.metrics.CollectionMetricSource;

/**
 * Storage consistency operation metrics.
 *
 * <p>Tracks runConsistently closure execution performance including duration
 * and active call count.
 */
public class StorageConsistencyMetrics {
    /**
     * Histogram buckets for runConsistently duration in nanoseconds.
     *
     * <p>Covers operations from fast single-row writes to slow bulk operations with checkpoint contention:
     * <ul>
     *   <li>10µs: Fast single-row operations without lock contention</li>
     *   <li>100µs: Multi-row operations, small index operations</li>
     *   <li>1ms: Medium-sized operations</li>
     *   <li>10ms: Larger operations, small vacuum operations</li>
     *   <li>100ms: Large operations, significant vacuum work</li>
     *   <li>1s: Very slow operations, likely checkpoint write lock contention</li>
     *   <li>10s: Pathological cases requiring investigation</li>
     * </ul>
     */
    private static final long[] RUN_CONSISTENTLY_NANOS = {
            10_000,          // 10µs
            100_000,         // 100µs
            1_000_000,       // 1ms
            10_000_000,      // 10ms
            100_000_000,     // 100ms
            1_000_000_000,   // 1s
            10_000_000_000L, // 10s
    };

    private final DistributionMetric runConsistentlyDuration;
    private final AtomicIntMetric runConsistentlyActiveCount;

    /**
     * Constructor.
     *
     * @param metricSource Metric source to register metrics with.
     */
    public StorageConsistencyMetrics(CollectionMetricSource metricSource) {
        runConsistentlyDuration = metricSource.addMetric(new DistributionMetric(
                "RunConsistentlyDuration",
                "Time spent in runConsistently closures in nanoseconds.",
                RUN_CONSISTENTLY_NANOS
        ));

        runConsistentlyActiveCount = metricSource.addMetric(new AtomicIntMetric(
                "RunConsistentlyActiveCount",
                "Current number of active runConsistently calls."
        ));
    }

    /**
     * Records the duration of a runConsistently closure execution in nanoseconds.
     */
    public void recordRunConsistentlyDuration(long durationNanos) {
        runConsistentlyDuration.add(durationNanos);
    }

    /** Returns the runConsistently duration metric. */
    DistributionMetric runConsistentlyDuration() {
        return runConsistentlyDuration;
    }

    /** Returns the runConsistently active count metric. */
    AtomicIntMetric runConsistentlyActiveCount() {
        return runConsistentlyActiveCount;
    }
}
