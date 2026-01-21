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

import static org.apache.ignite.internal.table.distributed.PartitionTableStatsMetricSource.METRIC_PENDING_WRITE_INTENTS;

import java.util.function.LongSupplier;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSetBuilder;
import org.apache.ignite.internal.metrics.MetricSource;
import org.jetbrains.annotations.Nullable;

/**
 * Global metrics related to table partition statistics.
 *
 * <p>This source provides node-level aggregated statistics across all local table partitions.
 */
public class GlobalPartitionTableStatsMetricSource implements MetricSource {
    /** Metric source name for node-level aggregated partition statistics. */
    public static final String SOURCE_NAME = "global.partition.statistics";

    private final LongSupplier pendingWriteIntentsSupplier;

    /** Enablement status. Accessed from different threads under synchronization on this object. */
    private boolean enabled;

    public GlobalPartitionTableStatsMetricSource(LongSupplier pendingWriteIntentsSupplier) {
        this.pendingWriteIntentsSupplier = pendingWriteIntentsSupplier;
    }

    @Override
    public String name() {
        return SOURCE_NAME;
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        if (enabled) {
            return null;
        }

        enabled = true;

        var builder = new MetricSetBuilder(SOURCE_NAME);

        builder.longGauge(
                METRIC_PENDING_WRITE_INTENTS,
                "Total number of unresolved (uncommitted) write intents across all local partitions on this node.",
                pendingWriteIntentsSupplier
        );

        return builder.build();
    }

    @Override
    public synchronized void disable() {
        enabled = false;
    }

    @Override
    public synchronized boolean enabled() {
        return enabled;
    }
}
