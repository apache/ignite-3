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
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.PENDING_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.util.StringUtils.incrementLastChar;

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

    /**
     * {@inheritDoc}
     *
     * <p>How it works. We choose the minimal timestamp amongst the following:
     * <ul>
     *     <li>
     *         If there's a table, that doesn't have a full set of stable assignments, including no assignments at all, then use the
     *         timestamp that will be associated with the earliest available version of the zone in the catalog. This situation implies that
     *         table has not yet saved the initial distribution.
     *     </li>
     *     <li>
     *         If there's a table, that has a full set of stable assignments, and does not have a full set of pending assignments, including
     *         no pending assignments at all, then use the timestamp that will be associated with the version of the zone in the catalog,
     *         that corresponds to current stable assignments. This situation implies that table has not yet completed the rebalance, or
     *         there's no pending rebalance at all.
     *     </li>
     *     <li>
     *         If there's a table, that has a full set of pending assignments, then use the timestamp that will be associated with the
     *         version of the zone in the catalog, that corresponds to current pending assignments. This situation implies active ongoing
     *         rebalance.
     *     </li>
     * </ul>
     */
    @Override
    public long minimumRequiredTime() {
        // Use the same revision to read all the data, in order to guarantee consistency of data.
        long appliedRevision = metaStorageManager.appliedRevision();

        // Ignore the real safe time, having a time associated with revision is enough. Also, acquiring real safe time would be
        // unnecessarily more complicated due to possible data races.
        long metaStorageSafeTime = metaStorageManager.timestampByRevisionLocally(appliedRevision).longValue();

        long minTimestamp = metaStorageSafeTime;

        Map<Integer, Map<Integer, Assignments>> stableAssignments = readAssignments(STABLE_ASSIGNMENTS_PREFIX, appliedRevision);
        Map<Integer, Map<Integer, Assignments>> pendingAssignments = readAssignments(PENDING_ASSIGNMENTS_PREFIX, appliedRevision);

        int earliestCatalogVersion = catalogService.earliestCatalogVersion();
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        Map<Integer, Integer> tableIdToZoneIdMap = tableIdToZoneIdMap(earliestCatalogVersion, latestCatalogVersion);
        Map<Integer, NavigableMap<Long, CatalogZoneDescriptor>> allZones = allZones(earliestCatalogVersion, latestCatalogVersion);
        Map<Integer, Long> zoneDeletionTimestamps = zoneDeletionTimestamps(earliestCatalogVersion, latestCatalogVersion);

        for (Map.Entry<Integer, Integer> entry : tableIdToZoneIdMap.entrySet()) {
            Integer tableId = entry.getKey();
            Integer zoneId = entry.getValue();

            NavigableMap<Long, CatalogZoneDescriptor> zoneDescriptors = allZones.get(zoneId);
            int zonePartitions = zoneDescriptors.lastEntry().getValue().partitions();

            // Having empty maps instead of nulls greatly simplifies the code that follows.
            Map<Integer, Assignments> stableTableAssignments = stableAssignments.getOrDefault(tableId, emptyMap());
            Map<Integer, Assignments> pendingTableAssignments = pendingAssignments.getOrDefault(tableId, emptyMap());

            long latestTimestamp = zoneDeletionTimestamps.getOrDefault(zoneId, metaStorageSafeTime + 1) - 1;

            if (stableTableAssignments.size() != zonePartitions || pendingTableAssignments.size() != zonePartitions) {
                long timestamp = findProperTimestampForAssignments(stableTableAssignments, zoneDescriptors, latestTimestamp);

                minTimestamp = min(minTimestamp, timestamp);
            } else {
                assert pendingTableAssignments.size() == zonePartitions;

                long timestamp = findProperTimestampForAssignments(pendingTableAssignments, zoneDescriptors, latestTimestamp);

                minTimestamp = min(minTimestamp, timestamp);
            }
        }

        return minTimestamp;
    }

    /**
     * Detects all changes in zones configurations and arranges them in a convenient map. It maps {@code zoneId} into a sorted map, that
     * contains a {@code alterTime -> zoneDescriptor} mapping.
     */
    Map<Integer, NavigableMap<Long, CatalogZoneDescriptor>> allZones(
            int earliestCatalogVersion,
            int latestCatalogVersion
    ) {
        Map<Integer, NavigableMap<Long, CatalogZoneDescriptor>> allZones = new HashMap<>();

        for (int catalogVersion = earliestCatalogVersion; catalogVersion <= latestCatalogVersion; catalogVersion++) {
            Catalog catalog = catalogService.catalog(catalogVersion);

            for (CatalogZoneDescriptor zone : catalog.zones()) {
                NavigableMap<Long, CatalogZoneDescriptor> map = allZones.computeIfAbsent(zone.id(), id -> new TreeMap<>());

                if (map.isEmpty() || !map.lastEntry().getValue().equals(zone)) {
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

        // We determine the "next" zone version, the one that comes after the version that corresponds to "timestamp".
        // "ceilingKey" accepts an inclusive boundary, while we have an exclusive one. "+ 1" converts ">=" into ">".
        Long ceilingKey = zoneDescriptors.ceilingKey(timestamp + 1);
        // While having it, we either decrement it to get "previous moment in time", or if there's no "next" version then we use "safeTime".
        return ceilingKey == null ? latestTimestamp : ceilingKey - 1;
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
    Map<Integer, Map<Integer, Assignments>> readAssignments(String prefix, long appliedRevision) {
        Map<Integer, Map<Integer, Assignments>> assignments = new HashMap<>();

        try (Cursor<Entry> entries = readLocallyByPrefix(prefix, appliedRevision)) {
            for (Entry entry : entries) {
                if (entry.empty() || entry.tombstone()) {
                    continue;
                }

                int tableId = RebalanceUtil.extractTableId(entry.key(), prefix);
                int partitionId = RebalanceUtil.extractPartitionNumber(entry.key());

                assignments.computeIfAbsent(tableId, id -> new HashMap<>())
                        .put(partitionId, Assignments.fromBytes(entry.value()));
            }
        }

        return assignments;
    }

    private Cursor<Entry> readLocallyByPrefix(String prefix, long revision) {
        return metaStorageManager.getLocally(
                ByteArray.fromString(prefix),
                ByteArray.fromString(incrementLastChar(prefix)),
                revision
        );
    }
}
