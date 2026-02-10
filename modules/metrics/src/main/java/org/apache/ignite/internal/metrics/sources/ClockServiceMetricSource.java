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

package org.apache.ignite.internal.metrics.sources;

import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.sources.ClockServiceMetricSource.Holder;

/**
 * Metric source, which provides clock service metrics.
 */
public class ClockServiceMetricSource extends AbstractMetricSource<Holder> {
    /** Source name. */
    public static final String SOURCE_NAME = "clock.service";

    /** Histogram buckets for clock skew in milliseconds. */
    private static final long[] HISTOGRAM_BUCKETS =
            {1, 2, 4, 8, 16, 25, 50, 75, 100, 250, 500, 750, 1000, 3000, 5000, 10000, 25000, 60000};

    /**
     * Creates a new instance of {@link ClockServiceMetricSource}.
     */
    public ClockServiceMetricSource() {
        super(SOURCE_NAME, "Clock service metrics.");
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /**
     * Closure to be called when clock skew exceeded max clock skew.
     *
     * @param observedClockSkew Observed max clock skew.
     */
    public void onMaxClockSkewExceeded(long observedClockSkew) {
        Holder holder = holder();

        if (holder != null) {
            holder.clockSkewExceedingMaxClockSkew.add(observedClockSkew);
        }
    }

    /** Holder class. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final DistributionMetric clockSkewExceedingMaxClockSkew = new DistributionMetric(
                "ClockSkewExceedingMaxClockSkew",
                "Observed clock skew that exceeded max clock skew.",
                HISTOGRAM_BUCKETS);

        @Override
        public Iterable<Metric> metrics() {
            return List.of(clockSkewExceedingMaxClockSkew);
        }
    }
}
