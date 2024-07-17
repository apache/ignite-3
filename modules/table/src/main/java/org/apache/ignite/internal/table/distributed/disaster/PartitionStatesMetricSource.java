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

package org.apache.ignite.internal.table.distributed.disaster;

import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.BROKEN;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.HEALTHY;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.INITIALIZING;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.INSTALLING_SNAPSHOT;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.UNAVAILABLE;

import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.MetricSetBuilder;
import org.apache.ignite.internal.metrics.MetricSource;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.jetbrains.annotations.Nullable;

/** Source of metrics for table partition statuses. */
class PartitionStatesMetricSource implements MetricSource {
    private final String metricSourceName;

    private final int tableId;

    private final DisasterRecoveryManager disasterRecoveryManager;

    /** Enablement status. Accessed from different threads under synchronization on this object. */
    private boolean enabled;

    PartitionStatesMetricSource(
            CatalogTableDescriptor tableDescriptor,
            DisasterRecoveryManager disasterRecoveryManager
    ) {
        this.tableId = tableDescriptor.id();
        this.disasterRecoveryManager = disasterRecoveryManager;

        metricSourceName = String.format("partition.states.zone.%s.table.%s", tableDescriptor.zoneId(), tableDescriptor.id());
    }

    @Override
    public String name() {
        return metricSourceName;
    }

    @Override
    public synchronized @Nullable MetricSet enable() {
        if (enabled) {
            return null;
        }

        var builder = new MetricSetBuilder(metricSourceName);

        builder.longGauge(
                "UnavailablePartitionCount",
                "Count of partitions not yet started.",
                () -> calculatePartitionCountByLocalState(UNAVAILABLE)
        );

        builder.longGauge(
                "HealthyPartitionCount",
                "Count of living partitions with a healthy state machine.",
                () -> calculatePartitionCountByLocalState(HEALTHY)
        );

        builder.longGauge(
                "InitializingPartitionCount",
                "Count of partitions that are starting right now.",
                () -> calculatePartitionCountByLocalState(INITIALIZING)
        );

        builder.longGauge(
                "InstallingSnapshotPartitionCount",
                "Count of partitions that installing Raft snapshots from the leader.",
                () -> calculatePartitionCountByLocalState(INSTALLING_SNAPSHOT)
        );

        builder.longGauge(
                "BrokenPartitionCount",
                "Count of broken partitions.",
                () -> calculatePartitionCountByLocalState(BROKEN)
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

    private long calculatePartitionCountByLocalState(LocalPartitionStateEnum state) {
        long[] count = {0};

        // When receiving/loading metrics for all tables, the total complexity will be O(N*N) where N is the number of tables (for
        // simplicity). While this is done intentionally, in the future it will be necessary to optimize this or wait for the collocation
        // of table partitions within one distribution zone.
        disasterRecoveryManager.raftManager.forEach((raftNodeId, raftGroupService) -> {
            if (raftNodeId.groupId() instanceof TablePartitionId) {
                var tablePartitionId = (TablePartitionId) raftNodeId.groupId();

                if (tablePartitionId.tableId() == tableId) {
                    LocalPartitionStateEnumWithLogIndex localPartitionStateWithLogIndex =
                            LocalPartitionStateEnumWithLogIndex.of(raftGroupService.getRaftNode());

                    if (localPartitionStateWithLogIndex.state == state) {
                        count[0]++;
                    }
                }
            }
        });

        return count[0];
    }
}
