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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogApplyResult;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.BaseDistributionZoneManagerTest;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.partitiondistribution.AssignmentsQueue;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link RebalanceMinimumRequiredTimeProviderImpl}.
 */
class RebalanceMinimumRequiredTimeProviderImplTest extends BaseDistributionZoneManagerTest {
    private static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;
    private static final String TABLE_NAME = "tableName";

    private static final String UPDATED_FILTER_1 = "$..*.*";
    private static final String UPDATED_FILTER_2 = "$..*.*.*";

    private RebalanceMinimumRequiredTimeProvider minimumRequiredTimeProvider;

    @BeforeEach
    void initProvider() {
        minimumRequiredTimeProvider = new RebalanceMinimumRequiredTimeProviderImpl(metaStorageManager, catalogManager);
    }

    private long getMinimumRequiredTime() {
        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();

        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(currentSafeTime())));

        return minimumRequiredTime;
    }

    /**
     * "No tables" scenario means that no tables exist in zone. In this case, we should return safe time of latest meta-storage revision.
     */
    @Test
    void testNoTables() throws Exception {
        startDistributionZoneManager();

        // Wait for default zone update to be applied.
        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() > 0, 1000L));

        long timeBefore = currentSafeTime();

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();

        long timeAfter = currentSafeTime();

        // This check is fine, because there's no idle safe time propagation happening in these tests. Otherwise this check would fail
        // randomly: provider's time corresponds to a revision, which might actually be earlier than the safe time we read.
        assertThat(timeBefore, is(lessThanOrEqualTo(minimumRequiredTime)));

        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(timeAfter)));
    }

    /**
     * Scenario where table exists, but assignments are not yet saved. In this case, we should return safe time that corresponds to earliest
     * zone version.
     */
    @Test
    void testNoAssignments() throws Exception {
        startDistributionZoneManager();

        createTable();

        String defaultZoneName = getDefaultZone().name();

        Catalog earliestCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, null, null, "$..*.*");
        Catalog latestCatalog = latestCatalogVersion();

        long minimumRequiredTime = getMinimumRequiredTime();

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    /**
     * Scenario where table exists, its initial assignments are saved, and we updated the zone after that. In this case, we should return
     * time, that corresponds to zone before its update.
     */
    @Test
    void testOldStableAssignments1() throws Exception {
        startDistributionZoneManager();

        int tableId = createTable();

        String defaultZoneName = getDefaultZone().name();

        Catalog earliestCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, null, null, UPDATED_FILTER_1);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog, true);

        long minimumRequiredTime = getMinimumRequiredTime();

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    /**
     * Scenario where table exists, its initial assignments are saved, and we updated the zone after that. In this case, we should return
     * time, that corresponds to zone before its update.
     */
    @Test
    void testOldStableAssignments2() throws Exception {
        startDistributionZoneManager();

        int tableId = createTable();

        String defaultZoneName = getDefaultZone().name();

        Catalog earliestCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, 1, 1, null);
        Catalog intermediateCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, null, null, UPDATED_FILTER_1);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog, true);

        long minimumRequiredTime = getMinimumRequiredTime();

        assertThat(intermediateCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    /**
     * Scenario where zone is already dropped, but there are still assignments. In this case we return latest available timestamp, in which
     * zone is still present.
     */
    @Test
    void testStableAssignmentsDroppedZone() throws Exception {
        startDistributionZoneManager();

        String zoneName = "zoneName";
        createZone(zoneName, null, null, null);

        int tableId = createTable(zoneName);

        Catalog earliestCatalog = latestCatalogVersion();
        saveStableAssignments(zoneName, tableId, earliestCatalog, true);

        dropTable(TABLE_NAME);
        dropZone(zoneName);
        Catalog latestCatalog = latestCatalogVersion();

        long minimumRequiredTime = getMinimumRequiredTime();

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    /**
     * Scenario where zone is already dropped and there are no assignments. In this case we return latest available timestamp.
     */
    @Test
    void testNoAssignmentsDroppedZone() throws Exception {
        startDistributionZoneManager();

        String zoneName = "zoneName";
        createZone(zoneName, null, null, null);
        createTable(zoneName);

        dropTable(TABLE_NAME);
        dropZone(zoneName);

        Catalog earliestCatalog = latestCatalogVersion();
        createZone(zoneName + "1", null, null, null);
        Catalog latestCatalog = latestCatalogVersion();

        long minimumRequiredTime = getMinimumRequiredTime();

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(latestCatalog.time())));
    }

    /**
     * Scenario where not all stable assignments are recalculated, and there are no pending assignments. Doesn't happen in real environment,
     * test-only case. Shows that we use earliest zone version amongst known stable assignments.
     */
    @Test
    void testNewStableAssignments() throws Exception {
        startDistributionZoneManager();

        int tableId = createTable();

        String defaultZoneName = getDefaultZone().name();

        Catalog earliestCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, null, null, UPDATED_FILTER_1);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog, true);
        saveStableAssignments(defaultZoneName, tableId, latestCatalog, false);

        long minimumRequiredTime = getMinimumRequiredTime();

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    /**
     * Scenario where pending assignments are only partially saved. In this case we're waiting for the rest of pending assignments to appear
     * and should use the timestamp that corresponds to stable assignments.
     */
    @Test
    void testPendingAssignmentsNotAllPartitions() throws Exception {
        startDistributionZoneManager();

        int tableId = createTable();

        String defaultZoneName = getDefaultZone().name();

        Catalog earliestCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, null, null, UPDATED_FILTER_1);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog, true);
        savePendingAssignments(defaultZoneName, tableId, latestCatalog, false);

        long minimumRequiredTime = getMinimumRequiredTime();

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    /**
     * Scenario where pending assignments are fully saved and we no longer need catalog version associated with stable assignments.
     */
    @Test
    void testPendingAssignmentsAllPartitions() throws Exception {
        startDistributionZoneManager();

        int tableId = createTable();

        String defaultZoneName = getDefaultZone().name();

        Catalog earliestCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, null, null, UPDATED_FILTER_1);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog, true);
        savePendingAssignments(defaultZoneName, tableId, latestCatalog, true);

        long minimumRequiredTime = getMinimumRequiredTime();

        assertThat(latestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
    }

    /**
     * Scenario where planned assignments are only partially saved. Pending assignments are not fully saved. This is a test-only case that
     * shows that we will chose the timestamp that corresponds to stable assignments, because they are still not fully updated.
     */
    @Test
    void testPlannedAssignmentsNotAllPartitions() throws Exception {
        startDistributionZoneManager();

        int tableId = createTable();

        String defaultZoneName = getDefaultZone().name();

        Catalog earliestCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, null, null, UPDATED_FILTER_1);
        Catalog intermediateCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, null, null, UPDATED_FILTER_2);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog, true);
        savePendingAssignments(defaultZoneName, tableId, intermediateCatalog, false);
        savePlannedAssignments(defaultZoneName, tableId, latestCatalog, false);

        long minimumRequiredTime = getMinimumRequiredTime();

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(intermediateCatalog.time())));
    }

    /**
     * Scenario where planned assignments are fully saved. We should still use the timestamp associated with pending assignments, just in
     * case we want to use it. Timestamp of pending assignments is considered to have a priority.
     */
    @Test
    void testPlannedAssignmentsAllPartitions() throws Exception {
        startDistributionZoneManager();

        int tableId = createTable();

        String defaultZoneName = getDefaultZone().name();

        Catalog earliestCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, null, null, UPDATED_FILTER_1);
        Catalog intermediateCatalog = latestCatalogVersion();

        alterZone(defaultZoneName, null, null, UPDATED_FILTER_2);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog, true);
        savePendingAssignments(defaultZoneName, tableId, intermediateCatalog, true);
        savePlannedAssignments(defaultZoneName, tableId, latestCatalog, true);

        long minimumRequiredTime = getMinimumRequiredTime();

        assertThat(intermediateCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    private long currentSafeTime() {
        return metaStorageManager.clusterTime().currentSafeTime().longValue();
    }

    private Catalog latestCatalogVersion() throws Exception {
        Catalog latestCatalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        assertThat(latestCatalog, is(notNullValue()));

        assertTrue(waitForCondition(() -> latestCatalog.time() <= currentSafeTime(), 10, 5000));

        return latestCatalog;
    }

    private int createTable() throws Exception {
        return createTable(null);
    }

    private int createTable(@Nullable String zoneName) throws Exception {
        CompletableFuture<CatalogApplyResult> tableFuture = catalogManager.execute(CreateTableCommand.builder()
                .tableName(TABLE_NAME)
                .schemaName(SCHEMA_NAME)
                .columns(List.of(ColumnParams.builder().name("key").type(ColumnType.INT32).build()))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key")).build())
                .zone(zoneName)
                .build()
        );

        assertThat(tableFuture, willCompleteSuccessfully());

        int catalogVersion = tableFuture.get().getCatalogVersion();

        Catalog catalog = catalogManager.catalog(catalogVersion);
        assertNotNull(catalog);

        CatalogTableDescriptor table = catalog.table(SCHEMA_NAME, TABLE_NAME);
        assertNotNull(table);

        return table.id();
    }

    private void dropTable(String tableName) {
        CompletableFuture<CatalogApplyResult> future = catalogManager.execute(DropTableCommand.builder()
                .tableName(tableName)
                .schemaName(SCHEMA_NAME)
                .build()
        );

        assertThat(future, willCompleteSuccessfully());
    }

    private void saveStableAssignments(String defaultZoneName,
            int tableId,
            Catalog catalog,
            boolean allPartitions
    ) throws Exception {
        saveAssignments(true, defaultZoneName, tableId, catalog, RebalanceUtil::stablePartAssignmentsKey, allPartitions);
    }

    private void savePendingAssignments(String zoneName,
            int tableId,
            Catalog catalog,
            boolean allPartitions
    ) throws Exception {
        Function<Long, byte[]> valueFunction = timestamp -> new AssignmentsQueue(
                Assignments.of(timestamp, Assignment.forPeer("nodeName"))
        ).toBytes();

        saveAssignments(false, zoneName, tableId, catalog, RebalanceUtil::pendingPartAssignmentsQueueKey, valueFunction, allPartitions);
    }

    private void savePlannedAssignments(String zoneName,
            int tableId,
            Catalog catalog,
            boolean allPartitions
    ) throws Exception {
        saveAssignments(false, zoneName, tableId, catalog, RebalanceUtil::plannedPartAssignmentsKey, allPartitions);
    }

    private void saveAssignments(
            boolean stable,
            String zoneName,
            int tableId,
            Catalog catalog,
            Function<TablePartitionId, ByteArray> keyFunction,
            boolean allPartitions
    ) throws Exception {
        Function<Long, byte[]> valueFunction = timestamp -> Assignments.of(timestamp, Assignment.forPeer("nodeName")).toBytes();
        saveAssignments(stable, zoneName, tableId, catalog, keyFunction, valueFunction, allPartitions);
    }

    private void saveAssignments(
            boolean stable,
            String zoneName,
            int tableId,
            Catalog catalog,
            Function<TablePartitionId, ByteArray> keyFunction,
            Function<Long, byte[]> valueFunction,
            boolean allPartitions
    ) throws Exception {
        long timestamp = catalog.time();

        CatalogZoneDescriptor catalogZoneDescriptor = catalog.zone(zoneName);
        assertNotNull(catalogZoneDescriptor);

        HybridTimestamp msTimestamp = catalogZoneDescriptor.updateTimestamp();
        byte[] msTimestampBytes = ByteUtils.longToBytesKeepingOrder(msTimestamp.longValue());

        assertNotNull(catalogZoneDescriptor);

        Map<ByteArray, byte[]> metaStorageData = new HashMap<>();

        for (int partitionId = 0; partitionId < catalogZoneDescriptor.partitions(); partitionId++) {
            if (allPartitions || partitionId % 2 == 0) {
                TablePartitionId tablePartitionId = new TablePartitionId(tableId, partitionId);

                if (!stable) {
                    metaStorageData.put(
                            RebalanceUtil.pendingChangeTriggerKey(tablePartitionId),
                            msTimestampBytes
                    );
                }

                metaStorageData.put(
                        keyFunction.apply(tablePartitionId),
                        valueFunction.apply(timestamp)
                );
            }
        }

        CompletableFuture<Void> tableAssignmentsFuture = metaStorageManager.putAll(metaStorageData);

        assertThat(tableAssignmentsFuture, willCompleteSuccessfully());

        assertTrue(waitForCondition(() -> {
            long appliedRevision = metaStorageManager.appliedRevision();

            @Nullable Entry<ByteArray, byte[]> first = CollectionUtils.first(metaStorageData.entrySet());

            assertNotNull(first);

            return Arrays.equals(metaStorageManager.getLocally(first.getKey(), appliedRevision).value(), first.getValue());
        }, 10, 5000));
    }
}
