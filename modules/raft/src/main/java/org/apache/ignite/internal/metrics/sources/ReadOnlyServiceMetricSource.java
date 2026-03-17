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
import org.apache.ignite.internal.metrics.AtomicLongMetric;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.Metric;

/**
 * Metrics of read only service.
 */
public class ReadOnlyServiceMetricSource extends AbstractMetricSource<ReadOnlyServiceMetricSource.Holder> {
    public static final String SOURCE_NAME = "raft.readonlyservice";

    /**
     * Constructor.
     */
    public ReadOnlyServiceMetricSource(String groupId) {
        super(sourceName(groupId), "Read only service metrics.", "readOnlyServices");
    }

    private static String sourceName(String groupId) {
        return SOURCE_NAME + '.' + groupId;
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /**
     * Called when read index queue is overloaded.
     */
    public void onReadIndexOverload() {
        Holder holder = holder();

        if (holder != null) {
            holder.overloadTimes.increment();
        }
    }

    /**
     * Called when a read index operation completes.
     *
     * @param duration Duration of the read index operation.
     */
    public void onReadIndex(long duration) {
        Holder holder = holder();

        if (holder != null) {
            holder.indexReadDuration.add(duration);
        }
    }

    /** Metric holder for read only service metrics. */
    static class Holder implements AbstractMetricSource.Holder<Holder> {
        private static final long[] HISTOGRAM_BUCKETS =
                {10, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000};

        AtomicLongMetric overloadTimes = new AtomicLongMetric(
                "OverloadTimes",
                "The times of read only service overload."
        );

        DistributionMetric indexReadDuration = new DistributionMetric(
                "IndexReadDuration",
                "Duration of read index in read only service in milliseconds",
                HISTOGRAM_BUCKETS
        );

        private final List<Metric> metrics = List.of(overloadTimes, indexReadDuration);

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
