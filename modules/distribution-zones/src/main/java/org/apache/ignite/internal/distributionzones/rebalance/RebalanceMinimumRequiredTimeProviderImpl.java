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

package org.apache.ignite.internal.distributionzones.rebalance;

import static java.util.Collections.emptyMap;
import static org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor.updateRequiresAssignmentsRecalculation;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_CHANGE_TRIGGER_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;

/**
 * {@link RebalanceMinimumRequiredTimeProvider} implementation for the current implementation of assignments. Assumes that each table has
 * its own assignments, but assignments within a zone are still somehow coordinated.
 */
public class RebalanceMinimumRequiredTimeProviderImpl implements RebalanceMinimumRequiredTimeProvider {
    private final MetaStorageManager metaStorageManager;
    private final CatalogService catalogService;

    /**
     * Constructor.
     */
    public RebalanceMinimumRequiredTimeProviderImpl(MetaStorageManager metaStorageManager, CatalogService catalogService) {
        this.metaStorageManager = metaStorageManager;
        this.catalogService = catalogService;
    }

    @Override
    public long minimumRequiredTime() {
        // Use the same revision to read all the data, in order to guarantee consistency of data.
        long appliedRevision = metaStorageManager.appliedRevision();

        // Ignore the real safe time, having a time associated with revision is enough. Also, acquiring real safe time would be
        // unnecessarily more complicated due to handling of possible data races.
        HybridTimestamp metaStorageSafeTime = metaStorageManager.timestampByRevisionLocally(appliedRevision);

        HybridTimestamp minTimestamp = metaStorageSafeTime;

        Map<Integer, Map<Integer, Assignments>> stableAssignments = readAssignments(STABLE_ASSIGNMENTS_PREFIX_BYTES, appliedRevision);
        Map<Integer, Map<Integer, Assignments>> pendingAssignments = readPendingAssignments(appliedRevision);

        Map<Integer, HybridTimestamp> pendingChangeTriggerTimestamps = readPendingChangeTriggerTimestamps(
                PENDING_CHANGE_TRIGGER_PREFIX_BYTES,
                appliedRevision
        );

        int earliestCatalogVersion = catalogService.earliestCatalogVersion();
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        Map<Integer, Integer> tableIdToZoneIdMap = tableIdToZoneIdMap(earliestCatalogVersion, latestCatalogVersion);

        Map<HybridTimestamp, HybridTimestamp> updateTimestampsToActivationTimeMap = new HashMap<>();
        Map<Integer, NavigableMap<HybridTimestamp, CatalogZoneDescriptor>> allZonesByTimestamp = allZonesByTimestamp(
                earliestCatalogVersion,
                latestCatalogVersion,
                updateTimestampsToActivationTimeMap
        );
        Map<Integer, HybridTimestamp> zoneDeletionTimestamps = zoneDeletionTimestamps(earliestCatalogVersion, latestCatalogVersion);

        for (Map.Entry<Integer, Integer> entry : tableIdToZoneIdMap.entrySet()) {
            Integer tableId = entry.getKey();
            Integer zoneId = entry.getValue();

            NavigableMap<HybridTimestamp, CatalogZoneDescriptor> zoneDescriptors = allZonesByTimestamp.get(zoneId);
            int zonePartitions = zoneDescriptors.lastEntry().getValue().partitions();

            HybridTimestamp pendingChangeTriggerTimestamp = pendingChangeTriggerTimestamps.get(tableId);

            // +-1 here ir required for 2 reasons:
            // - we need timestamp right before deletion, if zone is deleted, thus we must subtract 1;
            // - we need a "metaStorageSafeTime" if zone is not deleted, without any subtractions.
            HybridTimestamp latestTimestamp =
                    hybridTimestamp(zoneDeletionTimestamps.getOrDefault(zoneId, metaStorageSafeTime.tick()).longValue() - 1);

            HybridTimestamp zoneTimestamp = pendingChangeTriggerTimestamp == null
                    ? zoneDescriptors.firstEntry().getValue().updateTimestamp()
                    : pendingChangeTriggerTimestamp;

            NavigableMap<HybridTimestamp, CatalogZoneDescriptor> map = allZonesByTimestamp.get(zoneId);
            Map.Entry<HybridTimestamp, CatalogZoneDescriptor> zone = map.floorEntry(zoneTimestamp);
            HybridTimestamp timestamp = updateTimestampsToActivationTimeMap.get(zone.getValue().updateTimestamp());

            timestamp = ceilTime(zoneDescriptors, timestamp, latestTimestamp);

            // Having empty map instead of null simplifies the code that follows.
            Map<Integer, Assignments> pendingTableAssignments = pendingAssignments.getOrDefault(tableId, emptyMap());

            // If zone wasn't deleted or there is actual stable/pending assignments, then we must take it into account.
            // Otherwise, we can ignore it.
            if (!zoneDeletionTimestamps.containsKey(zoneId)
                    || stableAssignments.get(tableId) != null
                    || !pendingTableAssignments.isEmpty()
            ) {
                minTimestamp = min(minTimestamp, timestamp);
            }

            if (!pendingTableAssignments.isEmpty()) {
                HybridTimestamp pendingTimestamp = findProperTimestampForAssignments(
                        pendingTableAssignments.size() == zonePartitions
                                ? pendingTableAssignments
                                : stableAssignments.getOrDefault(tableId, emptyMap()),
                        zoneDescriptors,
                        latestTimestamp
                );

                minTimestamp = min(minTimestamp, pendingTimestamp);
            }
        }

        return minTimestamp.longValue();
    }

    /**
     * Detects all changes in zones configurations and arranges them in a convenient map. It maps {@code zoneId} into a sorted map, that
     * contains a {@code alterTime -> zoneDescriptor} mapping.
     */
    Map<Integer, NavigableMap<HybridTimestamp, CatalogZoneDescriptor>> allZonesByTimestamp(
            int earliestCatalogVersion,
            int latestCatalogVersion,
            Map<HybridTimestamp, HybridTimestamp> updateTimestampsToActivationTime
    ) {
        Map<Integer, NavigableMap<HybridTimestamp, CatalogZoneDescriptor>> allZones = new HashMap<>();

        for (int catalogVersion = earliestCatalogVersion; catalogVersion <= latestCatalogVersion; catalogVersion++) {
            Catalog catalog = catalogService.catalog(catalogVersion);

            for (CatalogZoneDescriptor zone : catalog.zones()) {
                NavigableMap<HybridTimestamp, CatalogZoneDescriptor> map = allZones.computeIfAbsent(zone.id(), id -> new TreeMap<>());

                if (map.isEmpty() || updateRequiresAssignmentsRecalculation(map.lastEntry().getValue(), zone)) {
                    map.put(zone.updateTimestamp(), zone);

                    updateTimestampsToActivationTime.put(zone.updateTimestamp(), hybridTimestamp(catalog.time()));
                }
            }
        }

        return allZones;
    }

    Map<Integer, HybridTimestamp> zoneDeletionTimestamps(int earliestCatalogVersion, int latestCatalogVersion) {
        Set<Integer> existingZoneIds = new HashSet<>();
        Map<Integer, HybridTimestamp> zoneDeletionTimestamps = new HashMap<>();

        for (int catalogVersion = earliestCatalogVersion; catalogVersion <= latestCatalogVersion; catalogVersion++) {
            Catalog catalog = catalogService.catalog(catalogVersion);

            for (Iterator<Integer> iterator = existingZoneIds.iterator(); iterator.hasNext(); ) {
                Integer zoneId = iterator.next();

                if (catalog.zone(zoneId) == null) {
                    zoneDeletionTimestamps.put(zoneId, hybridTimestamp(catalog.time()));

                    iterator.remove();
                }
            }

            for (CatalogZoneDescriptor zone : catalog.zones()) {
                existingZoneIds.add(zone.id());
            }
        }

        return zoneDeletionTimestamps;
    }

    static HybridTimestamp ceilTime(
            NavigableMap<HybridTimestamp, CatalogZoneDescriptor> zoneDescriptors,
            HybridTimestamp timestamp,
            HybridTimestamp latestTimestamp
    ) {
        // We determine the "next" zone version, the one that comes after the version that corresponds to "timestamp".
        // "ceilingKey" accepts an inclusive boundary, while we have an exclusive one. "+ 1" converts ">=" into ">".
        HybridTimestamp ceilingKey = zoneDescriptors.ceilingKey(timestamp.tick());

        // While having it, we either decrement it to get "previous moment in time", or if there's no "next" version then we use "safeTime".
        return ceilingKey == null ? latestTimestamp : hybridTimestamp(ceilingKey.longValue() - 1);
    }

    /**
     * Detects the specific zone version, associated with the most recent entry of the assignments set, and returns the highest possible
     * timestamp that can be used to read that zone from the catalog. Returns {@code latestTimestamp} if that timestamp corresponds to
     * {@code now}.
     */
    static HybridTimestamp findProperTimestampForAssignments(
            Map<Integer, Assignments> assignments,
            NavigableMap<HybridTimestamp, CatalogZoneDescriptor> zoneDescriptors,
            HybridTimestamp latestTimestamp
    ) {
        long timestamp = assignments.values().stream()
                .mapToLong(Assignments::timestamp)
                // Use "max", assuming that older assignments are already processed in the past, they just had not been changed later.
                .max()
                // If assignments are empty, we shall use the earliest known timestamp for the zone.
                .orElse(zoneDescriptors.firstEntry().getKey().longValue());

        return ceilTime(zoneDescriptors, hybridTimestamp(timestamp), latestTimestamp);
    }

    /**
     * Returns a {@code tableId -> zoneId} mapping for all tables that ever existed between two passed catalog versions, including the
     * boundaries.
     */
    private Map<Integer, Integer> tableIdToZoneIdMap(int earliestCatalogVersion, int latestCatalogVersion) {
        Map<Integer, Integer> tableIdToZoneIdMap = new HashMap<>();
        for (int catalogVersion = earliestCatalogVersion; catalogVersion <= latestCatalogVersion; catalogVersion++) {
            Catalog catalog = catalogService.catalog(catalogVersion);

            for (CatalogTableDescriptor table : catalog.tables()) {
                tableIdToZoneIdMap.putIfAbsent(table.id(), table.zoneId());
            }
        }
        return tableIdToZoneIdMap;
    }

    private Map<Integer, Map<Integer, Assignments>> readPendingAssignments(long appliedRevision) {
        return readAssignments(PENDING_ASSIGNMENTS_QUEUE_PREFIX_BYTES, appliedRevision, bytes -> AssignmentsQueue.fromBytes(bytes).poll());
    }

    private Map<Integer, Map<Integer, Assignments>> readAssignments(byte[] prefix, long appliedRevision) {
        return readAssignments(prefix, appliedRevision, Assignments::fromBytes);
    }

    /**
     * Reads assignments from the metastorage locally. The resulting map is a {@code tableId -> {partitionId -> assignments}} mapping.
     */
    Map<Integer, Map<Integer, Assignments>> readAssignments(
            byte[] prefix,
            long appliedRevision,
            Function<byte[], Assignments> deserializer
    ) {
        Map<Integer, Map<Integer, Assignments>> assignments = new HashMap<>();

        try (Cursor<Entry> entries = readLocallyByPrefix(prefix, appliedRevision)) {
            for (Entry entry : entries) {
                if (entry.empty() || entry.tombstone()) {
                    continue;
                }

                TablePartitionId tablePartitionId = RebalanceUtil.extractTablePartitionId(entry.key(), prefix);
                int tableId = tablePartitionId.tableId();
                int partitionId = tablePartitionId.partitionId();

                assignments.computeIfAbsent(tableId, id -> new HashMap<>())
                        .put(partitionId, deserializer.apply(entry.value()));
            }
        }

        return assignments;
    }

    Map<Integer, HybridTimestamp> readPendingChangeTriggerTimestamps(byte[] prefix, long appliedRevision) {
        Map<Integer, HybridTimestamp> timestamps = new HashMap<>();

        try (Cursor<Entry> entries = readLocallyByPrefix(prefix, appliedRevision)) {
            for (Entry entry : entries) {
                if (entry.empty() || entry.tombstone()) {
                    continue;
                }

                int tableId = RebalanceUtil.extractTablePartitionId(entry.key(), prefix).tableId();

                byte[] valueBytes = entry.value();
                long timestampLong = ByteUtils.bytesToLongKeepingOrder(valueBytes);
                HybridTimestamp timestamp = hybridTimestamp(timestampLong);

                timestamps.compute(tableId, (k, prev) -> prev == null ? timestamp : min(prev, timestamp));
            }
        }

        return timestamps;
    }

    private static HybridTimestamp min(HybridTimestamp a, HybridTimestamp b) {
        return a.compareTo(b) < 0 ? a : b;
    }

    private Cursor<Entry> readLocallyByPrefix(byte[] prefix, long revision) {
        return metaStorageManager.prefixLocally(new ByteArray(prefix), revision);
    }
}
