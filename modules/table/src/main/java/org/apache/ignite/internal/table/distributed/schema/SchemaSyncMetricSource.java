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

package org.apache.ignite.internal.table.distributed.schema;

import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.Metric;

/**
 * Metric source for schema synchronization metrics.
 */
public class SchemaSyncMetricSource extends AbstractMetricSource<SchemaSyncMetricSource.Holder> {
    private static final String SOURCE_NAME = "schema.sync";

    /**
     * Constructor.
     */
    public SchemaSyncMetricSource() {
        super(SOURCE_NAME);
    }

    /**
     * Histogram bounds (in milliseconds) for schema sync wait time distribution.
     * Buckets: [0..1], [1..5], [5..10], [10..50], [50..100], [100..500], [500..1000], [1000..5000], [5000..inf].
     */
    private static final long[] WAIT_BOUNDS_MS = {1, 5, 10, 50, 100, 500, 1000, 5000};

    /**
     * Records a completed schema sync wait with the given duration.
     *
     * @param durationMs Duration of the wait in milliseconds.
     */
    public void recordWait(long durationMs) {
        Holder holder = holder();

        if (holder != null) {
            holder.waits.add(durationMs);
        }
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /** Holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final DistributionMetric waits = new DistributionMetric(
                "Waits",
                "Histogram of schema synchronization wait times in milliseconds."
                        + " High values may indicate MetaStorage unavailability or slowness.",
                WAIT_BOUNDS_MS
        );

        private final List<Metric> metrics = List.of(waits);

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
