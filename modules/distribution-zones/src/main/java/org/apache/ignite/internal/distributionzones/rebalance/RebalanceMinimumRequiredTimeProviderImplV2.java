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
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceMinimumRequiredTimeProviderImpl.findProperTimestampForAssignments;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.PENDING_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignments;

/**
 * {@link RebalanceMinimumRequiredTimeProvider} implementation for the future implementation of assignments. Assumes that tables share zone
 * assignments.
 */
public class RebalanceMinimumRequiredTimeProviderImplV2 implements RebalanceMinimumRequiredTimeProvider {
    private final MetaStorageManager metaStorageManager;
    private final CatalogService catalogService;

    /** Alternative implementation is only used for its helper methods. */
    private final RebalanceMinimumRequiredTimeProviderImpl delegate;

    public RebalanceMinimumRequiredTimeProviderImplV2(MetaStorageManager metaStorageManager, CatalogService catalogService) {
        this.metaStorageManager = metaStorageManager;
        this.catalogService = catalogService;
        this.delegate = new RebalanceMinimumRequiredTimeProviderImpl(metaStorageManager, catalogService);
    }

    /**
     * {@inheritDoc}
     *
     * <p>How it works. We choose the minimal timestamp amongst the following:
     * <ul>
     *     <li>
     *         If there's a zone, that doesn't have a full set of stable assignments, including no assignments at all, then use the
     *         timestamp that will be associated with the earliest available version of the zone in the catalog. This situation implies that
     *         zone has not yet saved the initial distribution.
     *     </li>
     *     <li>
     *         If there's a zone, that has a full set of stable assignments, and does not have a full set of pending assignments, including
     *         no pending assignments at all, then use the timestamp that will be associated with the version of the zone in the catalog,
     *         that corresponds to current stable assignments. This situation implies that zone has not yet completed the rebalance, or
     *         there's no pending rebalance at all.
     *     </li>
     *     <li>
     *         If there's a zone, that has a full set of pending assignments, then use the timestamp that will be associated with the
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

        Map<Integer, Map<Integer, Assignments>> stableAssignments = delegate.readAssignments(STABLE_ASSIGNMENTS_PREFIX, appliedRevision);
        Map<Integer, Map<Integer, Assignments>> pendingAssignments = delegate.readAssignments(PENDING_ASSIGNMENTS_PREFIX, appliedRevision);

        int earliestCatalogVersion = catalogService.earliestCatalogVersion();
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        Map<Integer, NavigableMap<Long, CatalogZoneDescriptor>> allZones = delegate.allZones(earliestCatalogVersion, latestCatalogVersion);
        Map<Integer, Long> zoneDeletionTimestamps = delegate.zoneDeletionTimestamps(earliestCatalogVersion, latestCatalogVersion);

        for (Entry<Integer, NavigableMap<Long, CatalogZoneDescriptor>> entry : allZones.entrySet()) {
            Integer zoneId = entry.getKey();
            NavigableMap<Long, CatalogZoneDescriptor> zoneDescriptors = entry.getValue();

            int zonePartitions = zoneDescriptors.lastEntry().getValue().partitions();

            // Having empty maps instead of nulls greatly simplifies the code that follows.
            Map<Integer, Assignments> stableZoneAssignments = stableAssignments.getOrDefault(zoneId, emptyMap());
            Map<Integer, Assignments> pendingZoneAssignments = pendingAssignments.getOrDefault(zoneId, emptyMap());

            long latestTimestamp = zoneDeletionTimestamps.getOrDefault(zoneId, metaStorageSafeTime + 1) - 1;

            if (stableZoneAssignments.size() != zonePartitions || pendingZoneAssignments.size() != zonePartitions) {
                long timestamp = findProperTimestampForAssignments(stableZoneAssignments, zoneDescriptors, latestTimestamp);

                minTimestamp = min(minTimestamp, timestamp);
            } else {
                assert pendingZoneAssignments.size() == zonePartitions;

                long timestamp = findProperTimestampForAssignments(pendingZoneAssignments, zoneDescriptors, latestTimestamp);

                minTimestamp = min(minTimestamp, timestamp);
            }
        }

        return minTimestamp;
    }
}
