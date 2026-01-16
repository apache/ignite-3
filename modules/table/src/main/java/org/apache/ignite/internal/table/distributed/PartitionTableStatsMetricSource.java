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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Metrics related to table partition statistics.
 *
 * <p>This source includes {@link PartitionModificationCounter partition modification counter} metrics and
 * the {@link #METRIC_PENDING_WRITE_INTENTS pending write intents} metric.
 */
public class PartitionTableStatsMetricSource implements MetricSource {
    public static final String METRIC_COUNTER = "modificationCount";
    public static final String METRIC_NEXT_MILESTONE = "nextMilestone";
    public static final String METRIC_LAST_MILESTONE_TIMESTAMP = "lastMilestoneTimestamp";
    public static final String METRIC_PENDING_WRITE_INTENTS = "pendingWriteIntents";

    private final Map<String, Metric> metrics = new HashMap<>();
    private final String metricSourceName;

    private boolean enabled;

    public PartitionTableStatsMetricSource(int tableId, int partitionId) {
        this.metricSourceName = formatSourceName(tableId, partitionId);
    }

    @Override
    public String name() {
        return metricSourceName;
    }

    @Override
    public @Nullable MetricSet enable() {
        if (enabled) {
            return null;
        }

        enabled = true;

        return new MetricSet(metricSourceName, Map.copyOf(metrics));
    }

    @Override
    public void disable() {
        enabled = false;
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    /** Adds a metric to the source. */
    public void addMetric(Metric metric) {
        assert !enabled : "Metrics can be added only before enabling the metric source";

        metrics.put(metric.name(), metric);
    }

    public static String formatSourceName(int tableId, int partitionId) {
        return IgniteStringFormatter.format("partition.statistics.table.{}.partition.{}", tableId, partitionId);
    }
}
