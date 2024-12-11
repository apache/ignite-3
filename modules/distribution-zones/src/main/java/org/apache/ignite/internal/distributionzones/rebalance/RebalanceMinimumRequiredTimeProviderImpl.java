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

import static java.lang.Math.min;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor.updateRequiresAssignmentsRecalculation;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_ASSIGNMENTS_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_CHANGE_TRIGGER_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX_BYTES;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignments;
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
        long metaStorageSafeTime = metaStorageManager.timestampByRevisionLocally(appliedRevision).longValue();

        long minTimestamp = metaStorageSafeTime;

        Map<Integer, Map<Integer, Assignments>> stableAssignments = readAssignments(STABLE_ASSIGNMENTS_PREFIX_BYTES, appliedRevision);
        Map<Integer, Map<Integer, Assignments>> pendingAssignments = readAssignments(PENDING_ASSIGNMENTS_PREFIX_BYTES, appliedRevision);

        Map<Integer, Long> pendingChangeTriggerRevisions = readPendingChangeTriggerRevisions(
                PENDING_CHANGE_TRIGGER_PREFIX_BYTES,
                appliedRevision
        );

        int earliestCatalogVersion = catalogService.earliestCatalogVersion();
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        Map<Integer, Integer> tableIdToZoneIdMap = tableIdToZoneIdMap(earliestCatalogVersion, latestCatalogVersion);
        Map<Integer, NavigableMap<Long, CatalogZoneDescriptor>> allZonesByTimestamp = allZonesByTimestamp(
                earliestCatalogVersion,
                latestCatalogVersion
        );
        Map<Integer, NavigableMap<Long, CatalogZoneDescriptor>> allZonesByRevision = allZonesByRevision(allZonesByTimestamp);
        Map<Integer, Long> zoneDeletionTimestamps = zoneDeletionTimestamps(earliestCatalogVersion, latestCatalogVersion);

        for (Map.Entry<Integer, Integer> entry : tableIdToZoneIdMap.entrySet()) {
            Integer tableId = entry.getKey();
            Integer zoneId = entry.getValue();

            NavigableMap<Long, CatalogZoneDescriptor> zoneDescriptors = allZonesByTimestamp.get(zoneId);
            int zonePartitions = zoneDescriptors.lastEntry().getValue().partitions();

            Long pendingChangeTriggerRevision = pendingChangeTriggerRevisions.get(tableId);

            // +-1 here ir required for 2 reasons:
            // - we need timestamp right before deletion, if zone is deleted, thus we must subtract 1;
            // - we need a "metaStorageSafeTime" if zone is not deleted, without any subtractions.
            long latestTimestamp = zoneDeletionTimestamps.getOrDefault(zoneId, metaStorageSafeTime + 1) - 1;

            long zoneRevision = pendingChangeTriggerRevision == null
                    ? zoneDescriptors.firstEntry().getValue().updateToken()
                    : pendingChangeTriggerRevision;

            NavigableMap<Long, CatalogZoneDescriptor> map = allZonesByRevision.get(zoneId);
            Map.Entry<Long, CatalogZoneDescriptor> zone = map.floorEntry(zoneRevision);
            long timestamp = metaStorageManager.timestampByRevisionLocally(zone.getValue().updateToken()).longValue();

            timestamp = ceilTime(zoneDescriptors, timestamp, latestTimestamp);

            minTimestamp = min(minTimestamp, timestamp);

            // Having empty map instead of null simplifies the code that follows.
            Map<Integer, Assignments> pendingTableAssignments = pendingAssignments.getOrDefault(tableId, emptyMap());

            if (!pendingTableAssignments.isEmpty()) {
                long pendingTimestamp = findProperTimestampForAssignments(
                        pendingTableAssignments.size() == zonePartitions
                                ? pendingTableAssignments
                                : stableAssignments.getOrDefault(tableId, emptyMap()),
                        zoneDescriptors,
                        latestTimestamp
                );

                minTimestamp = min(minTimestamp, pendingTimestamp);
            }
        }

        return minTimestamp;
    }

    static Map<Integer, NavigableMap<Long, CatalogZoneDescriptor>> allZonesByRevision(
            Map<Integer, NavigableMap<Long, CatalogZoneDescriptor>> allZones
    ) {
        return allZones.entrySet().stream().collect(toMap(Map.Entry::getKey, entry -> {
            NavigableMap<Long, CatalogZoneDescriptor> mapByRevision = new TreeMap<>();

            for (CatalogZoneDescriptor zone : entry.getValue().values()) {
                mapByRevision.put(zone.updateToken(), zone);
            }

            return mapByRevision;
        }));
    }

    /**
     * Detects all changes in zones configurations and arranges them in a convenient map. It maps {@code zoneId} into a sorted map, that
     * contains a {@code alterTime -> zoneDescriptor} mapping.
     */
    Map<Integer, NavigableMap<Long, CatalogZoneDescriptor>> allZonesByTimestamp(
            int earliestCatalogVersion,
            int latestCatalogVersion
    ) {
        Map<Integer, NavigableMap<Long, CatalogZoneDescriptor>> allZones = new HashMap<>();

        for (int catalogVersion = earliestCatalogVersion; catalogVersion <= latestCatalogVersion; catalogVersion++) {
            Catalog catalog = catalogService.catalog(catalogVersion);

            for (CatalogZoneDescriptor zone : catalog.zones()) {
                NavigableMap<Long, CatalogZoneDescriptor> map = allZones.computeIfAbsent(zone.id(), id -> new TreeMap<>());

                if (map.isEmpty() || updateRequiresAssignmentsRecalculation(map.lastEntry().getValue(), zone)) {
                    map.put(catalog.time(), zone);
                }
            }
        }

        return allZones;
    }

    Map<Integer, Long> zoneDeletionTimestamps(int earliestCatalogVersion, int latestCatalogVersion) {
        Set<Integer> existingZoneIds = new HashSet<>();
        Map<Integer, Long> zoneDeletionTimestamps = new HashMap<>();

        for (int catalogVersion = earliestCatalogVersion; catalogVersion <= latestCatalogVersion; catalogVersion++) {
            Catalog catalog = catalogService.catalog(catalogVersion);

            for (Iterator<Integer> iterator = existingZoneIds.iterator(); iterator.hasNext(); ) {
                Integer zoneId = iterator.next();

                if (catalog.zone(zoneId) == null) {
                    zoneDeletionTimestamps.put(zoneId, catalog.time());

                    iterator.remove();
                }
            }

            for (CatalogZoneDescriptor zone : catalog.zones()) {
                existingZoneIds.add(zone.id());
            }
        }

        return zoneDeletionTimestamps;
    }

    static long ceilTime(NavigableMap<Long, CatalogZoneDescriptor> zoneDescriptors, long timestamp, long latestTimestamp) {
        // We determine the "next" zone version, the one that comes after the version that corresponds to "timestamp".
        // "ceilingKey" accepts an inclusive boundary, while we have an exclusive one. "+ 1" converts ">=" into ">".
        Long ceilingKey = zoneDescriptors.ceilingKey(timestamp + 1);

        // While having it, we either decrement it to get "previous moment in time", or if there's no "next" version then we use "safeTime".
        return ceilingKey == null ? latestTimestamp : ceilingKey - 1;
    }

    /**
     * Detects the specific zone version, associated with the most recent entry of the assignments set, and returns the highest possible
     * timestamp that can be used to read that zone from the catalog. Returns {@code latestTimestamp} if that timestamp corresponds to
     * {@code now}.
     */
    static long findProperTimestampForAssignments(
            Map<Integer, Assignments> assignments,
            NavigableMap<Long, CatalogZoneDescriptor> zoneDescriptors,
            long latestTimestamp
    ) {
        long timestamp = assignments.values().stream()
                .mapToLong(Assignments::timestamp)
                // Use "max", assuming that older assignments are already processed in the past, they just had not been changed later.
                .max()
                // If assignments are empty, we shall use the earliest known timestamp for the zone.
                .orElse(zoneDescriptors.firstEntry().getKey());

        return ceilTime(zoneDescriptors, timestamp, latestTimestamp);
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

    /**
     * Reads assignments from the metastorage locally. The resulting map is a {@code tableId -> {partitionId -> assignments}} mapping.
     */
    Map<Integer, Map<Integer, Assignments>> readAssignments(byte[] prefix, long appliedRevision) {
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
                        .put(partitionId, Assignments.fromBytes(entry.value()));
            }
        }

        return assignments;
    }

    Map<Integer, Long> readPendingChangeTriggerRevisions(byte[] prefix, long appliedRevision) {
        Map<Integer, Long> revisions = new HashMap<>();

        try (Cursor<Entry> entries = readLocallyByPrefix(prefix, appliedRevision)) {
            for (Entry entry : entries) {
                if (entry.empty() || entry.tombstone()) {
                    continue;
                }

                int tableId = RebalanceUtil.extractTablePartitionId(entry.key(), prefix).tableId();

                byte[] value = entry.value();
                long revision = ByteUtils.bytesToLongKeepingOrder(value);

                revisions.compute(tableId, (k, prev) -> prev == null ? revision : min(prev, revision));
            }
        }

        return revisions;
    }

    private Cursor<Entry> readLocallyByPrefix(byte[] prefix, long revision) {
        return metaStorageManager.prefixLocally(new ByteArray(prefix), revision);
    }
}
