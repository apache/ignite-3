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

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.BaseDistributionZoneManagerTest;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link RebalanceMinimumRequiredTimeProviderImplV2}.
 */
class RebalanceMinimumRequiredTimeProviderImplV2Test extends BaseDistributionZoneManagerTest {
    private RebalanceMinimumRequiredTimeProvider minimumRequiredTimeProvider;

    @BeforeEach
    void initProvider() {
        minimumRequiredTimeProvider = new RebalanceMinimumRequiredTimeProviderImplV2(metaStorageManager, catalogManager);
    }

    @Test
    void testNoAssignments() throws Exception {
        startDistributionZoneManager();

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() > 0, 1000L));

        long timeBefore = currentSafeTime();

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();

        long timeAfter = currentSafeTime();

        assertThat(timeBefore, is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(timeAfter)));
    }

    @Test
    void testOldStableAssignments() throws Exception {
        startDistributionZoneManager();

        int zoneId = createZone(ZONE_NAME);

        Catalog earliestCatalog = latestCatalogVersion();
        alterZone(ZONE_NAME, 0, 0, null);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(ZONE_NAME, zoneId, earliestCatalog.time(), true);

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(currentSafeTime())));

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    @Test
    void testStableAssignmentsDroppedZone() throws Exception {
        startDistributionZoneManager();

        int zoneId = createZone(ZONE_NAME);

        Catalog earliestCatalog = latestCatalogVersion();
        saveStableAssignments(ZONE_NAME, zoneId, earliestCatalog.time(), true);

        dropZone(ZONE_NAME);
        Catalog latestCatalog = latestCatalogVersion();

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(currentSafeTime())));

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    @Test
    void testNewStableAssignments() throws Exception {
        startDistributionZoneManager();

        int zoneId = createZone(ZONE_NAME);

        Catalog earliestCatalog = latestCatalogVersion();
        alterZone(ZONE_NAME, 0, 0, null);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(ZONE_NAME, zoneId, earliestCatalog.time(), true);
        saveStableAssignments(ZONE_NAME, zoneId, latestCatalog.time(), false);

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(currentSafeTime())));

        assertThat(latestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
    }

    @Test
    void testPendingAssignmentsNotAllPartitions() throws Exception {
        startDistributionZoneManager();

        int zoneId = createZone(ZONE_NAME);

        Catalog earliestCatalog = latestCatalogVersion();
        alterZone(ZONE_NAME, 0, 0, null);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(ZONE_NAME, zoneId, earliestCatalog.time(), true);
        savePendingAssignments(ZONE_NAME, zoneId, latestCatalog.time(), false);

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(currentSafeTime())));

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    @Test
    void testPendingAssignmentsAllPartitions() throws Exception {
        startDistributionZoneManager();

        int zoneId = createZone(ZONE_NAME);

        Catalog earliestCatalog = latestCatalogVersion();
        alterZone(ZONE_NAME, 0, 0, null);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(ZONE_NAME, zoneId, earliestCatalog.time(), true);
        savePendingAssignments(ZONE_NAME, zoneId, latestCatalog.time(), true);

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(currentSafeTime())));

        assertThat(latestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
    }

    private long currentSafeTime() {
        return metaStorageManager.clusterTime().currentSafeTime().longValue();
    }

    private Catalog latestCatalogVersion() {
        Catalog latestCatalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        assertThat(latestCatalog, is(notNullValue()));
        assertThat(latestCatalog.time(), is(lessThanOrEqualTo(currentSafeTime())));

        return latestCatalog;
    }

    private int createZone(String zoneName) {
        createZone(zoneName, 10_000, 10_000, null);

        Catalog catalog = latestCatalogVersion();

        return catalog.zone(zoneName).id();
    }

    private void saveStableAssignments(String defaultZoneName, int zoneId, long timestamp, boolean allPartitions) throws Exception {
        saveAssignments(defaultZoneName, zoneId, timestamp, ZoneRebalanceUtil::stablePartAssignmentsKey, allPartitions);
    }

    private void savePendingAssignments(String zoneName, int zoneId, long timestamp, boolean allPartitions) throws Exception {
        saveAssignments(zoneName, zoneId, timestamp, ZoneRebalanceUtil::pendingPartAssignmentsKey, allPartitions);
    }

    private void saveAssignments(
            String zoneName,
            int zoneId,
            long timestamp,
            Function<ZonePartitionId, ByteArray> keyFunction,
            boolean allPartitions
    ) throws Exception {
        CatalogZoneDescriptor catalogZoneDescriptor = catalogManager.zone(zoneName, currentSafeTime());

        assertNotNull(catalogZoneDescriptor);

        Map<ByteArray, byte[]> zoneAssignments = IntStream.range(0, catalogZoneDescriptor.partitions())
                .filter(partitionId -> allPartitions || partitionId % 2 == 0)
                .boxed()
                .collect(toMap(
                        partitionId -> keyFunction.apply(new ZonePartitionId(zoneId, partitionId)),
                        partitionId -> Assignments.of(timestamp, Assignment.forPeer("nodeName")).toBytes(),
                        (l, r) -> r
                ));

        CompletableFuture<Void> zoneAssignmentsFuture = metaStorageManager.putAll(zoneAssignments);

        assertThat(zoneAssignmentsFuture, willCompleteSuccessfully());

        assertTrue(waitForCondition(() -> {
            long appliedRevision = metaStorageManager.appliedRevision();

            @Nullable Entry<ByteArray, byte[]> first = CollectionUtils.first(zoneAssignments.entrySet());

            assertNotNull(first);

            return Arrays.equals(metaStorageManager.getLocally(first.getKey(), appliedRevision).value(), first.getValue());
        }, 10, 5000));
    }
}