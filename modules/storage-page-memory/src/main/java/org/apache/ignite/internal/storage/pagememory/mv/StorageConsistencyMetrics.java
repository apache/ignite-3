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

import java.util.function.IntSupplier;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.IntMetric;

/**
 * Storage consistency operation metrics.
 *
 * <p>Tracks runConsistently closure execution performance including duration
 * and active call count.
 */
public class StorageConsistencyMetrics {
    private static final long[] RUN_CONSISTENTLY_NANOS = {
            1_000,           // 1µs   - very fast, likely no I/O
            10_000,          // 10µs  - fast
            1_000_000,       // 1ms   - medium operation
            100_000_000,     // 100ms - large
            1_000_000_000,   // 1s    - abnormally large
    };

    private final DistributionMetric runConsistentlyDuration;
    private final AtomicIntMetric runConsistentlyActiveCount;

    /**
     * Constructor.
     *
     * @param metricSource Metric source to register metrics with.
     * @param activeCountSupplier Supplier for the number of active runConsistently calls.
     */
    public StorageConsistencyMetrics(StorageConsistencyMetricSource metricSource) {
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
