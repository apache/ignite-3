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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.catalog.annotations.Table;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.BaseDistributionZoneManagerTest;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link RebalanceMinimumRequiredTimeProviderImpl}.
 */
class RebalanceMinimumRequiredTimeProviderImplTest extends BaseDistributionZoneManagerTest {
    private static final String TABLE_NAME = "tableName";

    private RebalanceMinimumRequiredTimeProvider minimumRequiredTimeProvider;

    @BeforeEach
    void initProvider() {
        minimumRequiredTimeProvider = new RebalanceMinimumRequiredTimeProviderImpl(metaStorageManager, catalogManager);
    }

    @Test
    void testNoTables() throws Exception {
        startDistributionZoneManager();

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() > 0, 1000L));

        long timeBefore = currentSafeTime();

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();

        long timeAfter = currentSafeTime();

        assertThat(timeBefore, is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(timeAfter)));
    }

    @Test
    void testNoAssignments() throws Exception {
        startDistributionZoneManager();

        String defaultZoneName = getDefaultZone().name();

        createTable(defaultZoneName);

        long timeBefore = currentSafeTime();

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();

        long timeAfter = currentSafeTime();

        assertThat(timeBefore, is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(timeAfter)));
    }

    @Test
    void testOldStableAssignments() throws Exception {
        startDistributionZoneManager();

        String defaultZoneName = getDefaultZone().name();

        int tableId = createTable(defaultZoneName);

        Catalog earliestCatalog = latestCatalogVersion();
        alterZone(defaultZoneName, 0, 0, null);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog.time(), true);

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(currentSafeTime())));

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    @Test
    void testStableAssignmentsDroppedZone() throws Exception {
        startDistributionZoneManager();

        String zoneName = "zoneName";
        createZone(zoneName, null, null, null);

        int tableId = createTable(zoneName);

        Catalog earliestCatalog = latestCatalogVersion();
        saveStableAssignments(zoneName, tableId, earliestCatalog.time(), true);

        dropTable(TABLE_NAME);
        dropZone(zoneName);
        Catalog latestCatalog = latestCatalogVersion();

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(currentSafeTime())));

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    @Test
    void testNewStableAssignments() throws Exception {
        startDistributionZoneManager();

        String defaultZoneName = getDefaultZone().name();

        int tableId = createTable(defaultZoneName);

        Catalog earliestCatalog = latestCatalogVersion();
        alterZone(defaultZoneName, 0, 0, null);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog.time(), true);
        saveStableAssignments(defaultZoneName, tableId, latestCatalog.time(), false);

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(currentSafeTime())));

        assertThat(latestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
    }

    @Test
    void testPendingAssignmentsNotAllPartitions() throws Exception {
        startDistributionZoneManager();

        String defaultZoneName = getDefaultZone().name();

        int tableId = createTable(defaultZoneName);

        Catalog earliestCatalog = latestCatalogVersion();
        alterZone(defaultZoneName, 0, 0, null);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog.time(), true);
        savePendingAssignments(defaultZoneName, tableId, latestCatalog.time(), false);

        long minimumRequiredTime = minimumRequiredTimeProvider.minimumRequiredTime();
        assertThat(minimumRequiredTime, is(lessThanOrEqualTo(currentSafeTime())));

        assertThat(earliestCatalog.time(), is(lessThanOrEqualTo(minimumRequiredTime)));
        assertThat(minimumRequiredTime, is(lessThan(latestCatalog.time())));
    }

    @Test
    void testPendingAssignmentsAllPartitions() throws Exception {
        startDistributionZoneManager();

        String defaultZoneName = getDefaultZone().name();

        int tableId = createTable(defaultZoneName);

        Catalog earliestCatalog = latestCatalogVersion();
        alterZone(defaultZoneName, 0, 0, null);
        Catalog latestCatalog = latestCatalogVersion();

        saveStableAssignments(defaultZoneName, tableId, earliestCatalog.time(), true);
        savePendingAssignments(defaultZoneName, tableId, latestCatalog.time(), true);

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

    private int createTable(String defaultZoneName) throws InterruptedException, ExecutionException {
        CompletableFuture<Integer> tableFuture = catalogManager.execute(CreateTableCommand.builder()
                .tableName(TABLE_NAME)
                .schemaName(Table.DEFAULT_SCHEMA)
                .zone(defaultZoneName)
                .columns(List.of(ColumnParams.builder().name("key").type(ColumnType.INT32).build()))
                .primaryKey(TableHashPrimaryKey.builder().columns(List.of("key")).build())
                .build()
        );

        assertThat(tableFuture, willCompleteSuccessfully());

        int catalogVersion = tableFuture.get();

        Catalog catalog = catalogManager.catalog(catalogVersion);

        CatalogTableDescriptor table = catalog.table(Table.DEFAULT_SCHEMA, TABLE_NAME);

        return table.id();
    }

    private void dropTable(String tableName) {
        CompletableFuture<Integer> future = catalogManager.execute(DropTableCommand.builder()
                .tableName(tableName)
                .schemaName(Table.DEFAULT_SCHEMA)
                .build()
        );

        assertThat(future, willCompleteSuccessfully());
    }

    private void saveStableAssignments(String defaultZoneName, int tableId, long timestamp, boolean allPartitions) throws Exception {
        saveAssignments(defaultZoneName, tableId, timestamp, RebalanceUtil::stablePartAssignmentsKey, allPartitions);
    }

    private void savePendingAssignments(String zoneName, int tableId, long timestamp, boolean allPartitions) throws Exception {
        saveAssignments(zoneName, tableId, timestamp, RebalanceUtil::pendingPartAssignmentsKey, allPartitions);
    }

    private void saveAssignments(
            String zoneName,
            int tableId,
            long timestamp,
            Function<TablePartitionId, ByteArray> keyFunction,
            boolean allPartitions
    ) throws Exception {
        CatalogZoneDescriptor catalogZoneDescriptor = catalogManager.zone(zoneName, currentSafeTime());

        assertNotNull(catalogZoneDescriptor);

        Map<ByteArray, byte[]> tableAssignments = IntStream.range(0, catalogZoneDescriptor.partitions())
                .filter(partitionId -> allPartitions || partitionId % 2 == 0)
                .boxed()
                .collect(toMap(
                        partitionId -> keyFunction.apply(new TablePartitionId(tableId, partitionId)),
                        partitionId -> Assignments.of(timestamp, Assignment.forPeer("nodeName")).toBytes(),
                        (l, r) -> r
                ));

        CompletableFuture<Void> tableAssignmentsFuture = metaStorageManager.putAll(tableAssignments);

        assertThat(tableAssignmentsFuture, willCompleteSuccessfully());

        assertTrue(waitForCondition(() -> {
            long appliedRevision = metaStorageManager.appliedRevision();

            @Nullable Entry<ByteArray, byte[]> first = CollectionUtils.first(tableAssignments.entrySet());

            assertNotNull(first);

            return Arrays.equals(metaStorageManager.getLocally(first.getKey(), appliedRevision).value(), first.getValue());
        }, 10, 5000));
    }
}
