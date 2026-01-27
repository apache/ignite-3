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

package org.apache.ignite.internal.table.distributed;

import java.util.List;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.Metric;

/**
 * Metrics related to table partition statistics.
 *
 * <p>This source includes {@link PartitionModificationCounter partition modification counter} metrics.
 */
public class PartitionTableStatsMetricSource extends AbstractMetricSource<PartitionTableStatsMetricSource.Holder> {
    public static final String METRIC_COUNTER = "modificationCount";
    public static final String METRIC_NEXT_MILESTONE = "nextMilestone";
    public static final String METRIC_LAST_MILESTONE_TIMESTAMP = "lastMilestoneTimestamp";

    private final PartitionModificationCounter counter;

    public PartitionTableStatsMetricSource(int tableId, int partitionId, PartitionModificationCounter counter) {
        super(formatSourceName(tableId, partitionId), "Metrics related to table partition statistics.");

        this.counter = counter;
    }

    public static String formatSourceName(int tableId, int partitionId) {
        return IgniteStringFormatter.format("partition.statistics.table.{}.partition.{}", tableId, partitionId);
    }

    @Override
    protected Holder createHolder() {
        return new Holder(counter);
    }

    /** Holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final LongGauge counterValue;
        private final LongGauge nextMilestone;
        private final LongGauge lastMilestoneTimestamp;

        private final List<Metric> metrics;

        private Holder(PartitionModificationCounter counter) {
            counterValue = new LongGauge(
                    METRIC_COUNTER,
                    "The value of the volatile counter of partition modifications. "
                            + "This value is used to determine staleness of the related SQL statistics.",
                    counter::value
            );

            nextMilestone = new LongGauge(
                    METRIC_NEXT_MILESTONE,
                    "The value of the next milestone for the number of partition modifications. "
                            + "This value is used to determine staleness of the related SQL statistics.",
                    counter::nextMilestone
            );

            lastMilestoneTimestamp = new LongGauge(
                    METRIC_LAST_MILESTONE_TIMESTAMP,
                    "The timestamp value representing the commit time of the last modification operation that "
                            + "reached the milestone. This value is used to determine staleness of the related SQL statistics.",
                    () -> counter.lastMilestoneTimestamp().longValue()
            );

            metrics = List.of(counterValue, nextMilestone, lastMilestoneTimestamp);
        }

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
