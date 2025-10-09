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
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.jetbrains.annotations.Nullable;

/**
 * Distribution metric source for a specific zone.
 */
public class ZoneMetricSource extends AbstractMetricSource<ZoneMetricSource.Holder> {
    /** Source name. */
    public static final String SOURCE_NAME = "zones";

    /** Metric names. */
    public static final String LOCAL_UNREBALANCED_PARTITIONS_COUNT = "LocalUnrebalancedPartitionsCount";
    public static final String TOTAL_UNREBALANCED_PARTITIONS_COUNT = "TotalUnrebalancedPartitionsCount";

    /** Meta Storage manager. */
    private final MetaStorageManager metaStorageManager;

    /** Node name, aka consistent identifier. */
    private final String nodeName;

    /** Zone identifier. */
    private final int zoneId;

    /** Number of partitions in the zone. */
    private final int partitions;

    /** Zone name. */
    private final String zoneName;

    /**
     * Creates a new zone metric source for a specific zone.
     *
     * @param metaStorageManager Meta Storage manager.
     * @param consistentId Name of the node.
     * @param zoneDescriptor Zone descriptor.
     */
    public ZoneMetricSource(MetaStorageManager metaStorageManager, String consistentId, CatalogZoneDescriptor zoneDescriptor) {
        super(sourceName(zoneDescriptor.name()), "Distribution zone metrics.", "zones");

        this.nodeName = consistentId;
        this.metaStorageManager = metaStorageManager;
        this.zoneId = zoneDescriptor.id();
        this.partitions = zoneDescriptor.partitions();
        this.zoneName = zoneDescriptor.name();
    }

    /**
     * Creates a new zone metric source for a specific zone.
     *
     * @param metaStorageManager Meta Storage manager.
     * @param consistentId Name of the node.
     * @param zoneDescriptor Zone descriptor.
     * @param source Source to copy metrics from.
     */
    public ZoneMetricSource(
            MetaStorageManager metaStorageManager,
            String consistentId,
            CatalogZoneDescriptor zoneDescriptor,
            ZoneMetricSource source
    ) {
        super(sourceName(zoneDescriptor.name()), "Distribution zone metrics.", "zones", Holder.copyFrom(source));

        assert zoneDescriptor.id() == source.zoneId :
                "Zone ID mismatch [expected=" + zoneDescriptor.id() + ", actual=" + source.zoneId + ']';
        assert zoneDescriptor.partitions() == source.partitions :
                "Partitions count mismatch [expected=" + zoneDescriptor.partitions() + ", actual=" + source.partitions + ']';

        this.nodeName = consistentId;
        this.metaStorageManager = metaStorageManager;
        this.zoneId = zoneDescriptor.id();
        this.partitions = zoneDescriptor.partitions();
        this.zoneName = zoneDescriptor.name();
    }

    /**
     * Returns a metric source name for the given distribution zone.
     *
     * @param zoneName Zone name.
     * @return Source name.
     */
    public static String sourceName(String zoneName) {
        return SOURCE_NAME + '.' + zoneName;
    }

    @Override
    protected Holder createHolder() {
        return new Holder(zoneId, partitions, metaStorageManager, nodeName);
    }

    /**
     * Returns the name of the zone.
     *
     * @return Zone name.
     */
    public String zoneName() {
        return zoneName;
    }

    /** Holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** List of actual metrics. */
        private final List<Metric> metrics;

        static @Nullable Holder copyFrom(ZoneMetricSource source) {
            // All metrics are gauge and must relate to the same zone.
            // So, we can safely reuse the existing holder.
            return source.holder();
        }

        Holder(int zoneId, int partitions, MetaStorageManager metaStorageManager, String nodeName) {
            var localUnrebalancedPartitionsCount = new IntGauge(
                    LOCAL_UNREBALANCED_PARTITIONS_COUNT,
                    "The number of partitions that should be moved to this node.",
                    () -> {
                        int unrebalancedParts = 0;

                        for (int i = 0; i < partitions; ++i) {
                            ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, i);

                            Entry pendingEntry = metaStorageManager.getLocally(pendingPartAssignmentsQueueKey(zonePartitionId));
                            AssignmentsQueue pendingAssignmentsQueue = AssignmentsQueue.fromBytes(pendingEntry.value());

                            if (pendingAssignmentsQueue != null) {
                                Entry stableEntry = metaStorageManager.getLocally(stablePartAssignmentsKey(zonePartitionId));

                                Assignments stableAssignments = stableEntry.value() == null
                                        ? Assignments.EMPTY
                                        : Assignments.fromBytes(stableEntry.value());
                                Assignments targetAssignments = pendingAssignmentsQueue.peekLast();

                                boolean stable = presentInAssignments(stableAssignments, nodeName);
                                boolean pending = presentInAssignments(targetAssignments, nodeName);

                                if (!stable && pending) {
                                    unrebalancedParts += 1;
                                }
                            }
                        }

                        return unrebalancedParts;
                    }
            );

            var totalUnrebalancedPartitionsCount = new IntGauge(
                    TOTAL_UNREBALANCED_PARTITIONS_COUNT,
                    "The total number of partitions that should be moved to a new owner.",
                    () -> {
                        int unrebalancedParts = 0;

                        for (int i = 0; i < partitions; ++i) {
                            ZonePartitionId zonePartitionId = new ZonePartitionId(zoneId, i);

                            Entry pendingEntry = metaStorageManager.getLocally(pendingPartAssignmentsQueueKey(zonePartitionId));
                            AssignmentsQueue pendingAssignmentsQueue = AssignmentsQueue.fromBytes(pendingEntry.value());

                            if (pendingAssignmentsQueue != null) {
                                Entry stableEntry = metaStorageManager.getLocally(stablePartAssignmentsKey(zonePartitionId));

                                Assignments stableAssignments = stableEntry.value() == null
                                        ? Assignments.EMPTY
                                        : Assignments.fromBytes(stableEntry.value());
                                Assignments targetAssignments = pendingAssignmentsQueue.peekLast();

                                for (Assignment pendingAssignment : targetAssignments.nodes()) {
                                    if (!presentInAssignments(stableAssignments, pendingAssignment.consistentId())) {
                                        unrebalancedParts += 1;
                                    }
                                }
                            }
                        }

                        return unrebalancedParts;
                    }
            );

            metrics = List.of(localUnrebalancedPartitionsCount, totalUnrebalancedPartitionsCount);
        }

        /** Returns the holder metrics. */
        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }

        /**
         * Checks if the node is present in the assignments.
         *
         * @param assignments Assignments to check.
         * @param nodeName Node name to check.
         * @return {@code true} if the node is present in the assignments, {@code false} otherwise.
         */
        private static boolean presentInAssignments(Assignments assignments, String nodeName) {
            return assignments
                    .nodes()
                    .stream()
                    .anyMatch(assignment -> assignment.consistentId().equals(nodeName));
        }
    }
}
