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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.pagememory.metrics.CollectionMetricSource;
import org.jetbrains.annotations.TestOnly;

/**
 * Metrics for runConsistently operation.
 *
 * <p>Tracks runConsistently closure execution performance including duration
 * and active call count.
 */
public class RunConsistentlyMetrics {
    /** Histogram bucket bounds for runConsistently duration in nanoseconds. */
    private static final long[] RUN_CONSISTENTLY_DURATION_BOUNDS = {
            TimeUnit.MICROSECONDS.toNanos(10),
            TimeUnit.MICROSECONDS.toNanos(100),
            TimeUnit.MILLISECONDS.toNanos(1),
            TimeUnit.MILLISECONDS.toNanos(10),
            TimeUnit.MILLISECONDS.toNanos(100),
            TimeUnit.SECONDS.toNanos(1),
            TimeUnit.SECONDS.toNanos(10),
    };

    private final DistributionMetric runConsistentlyDuration;
    private final AtomicIntMetric runConsistentlyActiveCount;

    /**
     * Constructor.
     *
     * @param metricSource Metric source to register metrics with.
     */
    public RunConsistentlyMetrics(CollectionMetricSource metricSource) {
        runConsistentlyDuration = metricSource.addMetric(new DistributionMetric(
                "RunConsistentlyDuration",
                "Time spent in runConsistently closures in nanoseconds.",
                RUN_CONSISTENTLY_DURATION_BOUNDS
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

    /**
     * Increments the active count of runConsistently calls.
     */
    public void incrementActiveCount() {
        runConsistentlyActiveCount.increment();
    }

    /**
     * Decrements the active count of runConsistently calls.
     */
    public void decrementActiveCount() {
        runConsistentlyActiveCount.decrement();
    }

    /** Returns the runConsistently duration metric for testing. */
    @TestOnly
    DistributionMetric runConsistentlyDuration() {
        return runConsistentlyDuration;
    }

    /** Returns the runConsistently active count metric for testing. */
    @TestOnly
    AtomicIntMetric runConsistentlyActiveCount() {
        return runConsistentlyActiveCount;
    }
}
