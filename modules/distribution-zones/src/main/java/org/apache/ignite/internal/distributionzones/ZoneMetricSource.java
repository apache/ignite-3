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

package org.apache.ignite.internal.distributionzones;

import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartAssignmentsKey;

import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.replicator.ZonePartitionId;

/**
 * TODO.
 */
public class ZoneMetricSource extends AbstractMetricSource<ZoneMetricSource.Holder> {
    private static final String SOURCE_NAME = "zones";

    private final String consistentId;

    private final CatalogZoneDescriptor zoneDescriptor;

    /** Meta Storage manager. */
    private final MetaStorageManager metaStorageManager;

    protected ZoneMetricSource(String zoneName) {
        super(SOURCE_NAME + '.' + zoneName, "Distribution zone metrics.", "zones");

        this.consistentId = null;
        this.zoneDescriptor = null;
        metaStorageManager = null;
    }

    protected ZoneMetricSource(MetaStorageManager metaStorageManager, String consistentId, CatalogZoneDescriptor zoneDescriptor) {
        super(SOURCE_NAME + '.' + zoneDescriptor.name(), "Distribution zone metrics.", "zones");

        this.consistentId = consistentId;
        this.zoneDescriptor = zoneDescriptor;
        this.metaStorageManager = metaStorageManager;
    }

    @Override
    protected Holder createHolder() {
        return new Holder(this);
    }

    /**
     * TODO.
     */
    public void onUpdateUnrebalancedPartitionsCount() {
        Holder h = holder();

        if (h != null) {
            //h.localUnrebalancedPartitionsCount.increment();
        }
    }

    /**
     * TODO.
     */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final ZoneMetricSource source;

        private final List<Metric> metrics;

        Holder(ZoneMetricSource source) {
            this.source = source;

            var localUnrebalancedPartitionsCount =
                    new AtomicIntMetric("LocalUnrebalancedPartitionsCount", "Number");

            var localUnrebalancedPartitionsCount2 =
                    new IntGauge("LocalUnrebalancedPartitionsCount2", "Number", () -> {
                        int val = 0;

                        for (int i = 0; i < source.zoneDescriptor.partitions(); ++i) {
                            ByteArray pendingPartAssignmentsKey = pendingPartAssignmentsQueueKey(new ZonePartitionId(source.zoneDescriptor.id(), i));
                            Entry pendingEntry = source.metaStorageManager.getLocally(pendingPartAssignmentsKey);
                            AssignmentsQueue pendingAssignmentsQueue = AssignmentsQueue.fromBytes(pendingEntry.value());
                            if (pendingAssignmentsQueue != null) {
                                ByteArray stablePartAssignmentsKey = stablePartAssignmentsKey(new ZonePartitionId(source.zoneDescriptor.id(), i));
                                Entry stableEntry = source.metaStorageManager.getLocally(stablePartAssignmentsKey);

                                Assignments stableAssignments = stableEntry.value() == null ? Assignments.EMPTY : Assignments.fromBytes(stableEntry.value());
                                Assignments targetAssignments = pendingAssignmentsQueue.peekLast();

                                boolean stable = stableAssignments.nodes().stream().anyMatch(assignment -> assignment.consistentId().equals(source.consistentId));
                                boolean pending = targetAssignments.nodes().stream().anyMatch(assignment -> assignment.consistentId().equals(source.consistentId));
                                if (!stable && pending) {
                                    val += 1;
                                }
                            }
                        }

                        return val;
                    });

            metrics = List.of(localUnrebalancedPartitionsCount, localUnrebalancedPartitionsCount2);
        }

        /** Returns the holder metrics. */
        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
