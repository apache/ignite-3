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

package org.apache.ignite.internal.catalog.commands;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.index;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_DATA_REGION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STORAGE_ENGINE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.collectIndexes;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParamsAndPreviousValue;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** For {@link CatalogUtils} testing. */
public class CatalogUtilsTest extends BaseIgniteAbstractTest {
    private static final String ZONE_NAME = "test_zone";

    private static final String TABLE_NAME = "test_table";

    private static final String INDEX_NAME = "test_index";

    private static final String PK_INDEX_NAME = pkIndexName(TABLE_NAME);

    private static final String COLUMN_NAME = "key";

    @Test
    void testFromParamsCreateZoneWithoutAutoAdjustFields() {
        CreateZoneParams params = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(2)
                .replicas(3)
                .filter("test_filter")
                .dataStorage(DataStorageParams.builder().engine("test_engine").dataRegion("test_region").build())
                .build();

        CatalogZoneDescriptor descriptor = fromParams(1, params);

        assertEquals(1, descriptor.id());
        assertEquals(ZONE_NAME, descriptor.name());
        assertEquals(2, descriptor.partitions());
        assertEquals(3, descriptor.replicas());
        assertEquals("test_filter", descriptor.filter());
        assertEquals("test_engine", descriptor.dataStorage().engine());
        assertEquals("test_region", descriptor.dataStorage().dataRegion());
    }

    @Test
    void testFromParamsCreateZoneWithDefaults() {
        CatalogZoneDescriptor descriptor = fromParams(2, CreateZoneParams.builder().zoneName(ZONE_NAME).build());

        assertEquals(2, descriptor.id());
        assertEquals(ZONE_NAME, descriptor.name());
        assertEquals(DEFAULT_PARTITION_COUNT, descriptor.partitions());
        assertEquals(DEFAULT_REPLICA_COUNT, descriptor.replicas());
        assertEquals(INFINITE_TIMER_VALUE, descriptor.dataNodesAutoAdjust());
        assertEquals(IMMEDIATE_TIMER_VALUE, descriptor.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, descriptor.dataNodesAutoAdjustScaleDown());
        assertEquals(DEFAULT_FILTER, descriptor.filter());
        assertEquals(DEFAULT_STORAGE_ENGINE, descriptor.dataStorage().engine());
        assertEquals(DEFAULT_DATA_REGION, descriptor.dataStorage().dataRegion());
    }

    @Test
    void testFromParamsCreateZoneWithAutoAdjustFields() {
        checkAutoAdjustParams(
                fromParams(1, createZoneParams(1, 2, 3)),
                1, 2, 3
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(1, null, null)),
                1, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(1, 2, null)),
                1, 2, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(1, null, 3)),
                1, INFINITE_TIMER_VALUE, 3
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(null, 2, null)),
                INFINITE_TIMER_VALUE, 2, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(null, null, 3)),
                INFINITE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, 3
        );

        checkAutoAdjustParams(
                fromParams(1, createZoneParams(null, 2, 3)),
                INFINITE_TIMER_VALUE, 2, 3
        );
    }

    @Test
    void testFromParamsAndPreviousValueWithoutAutoAdjustFields() {
        AlterZoneParams params = AlterZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(2)
                .replicas(3)
                .filter("test_filter")
                .dataStorage(DataStorageParams.builder().engine("test_engine").dataRegion("test_region").build())
                .build();

        CatalogZoneDescriptor descriptor = fromParamsAndPreviousValue(params, createPreviousZoneWithDefaults());

        assertEquals(1, descriptor.id());
        assertEquals(ZONE_NAME, descriptor.name());
        assertEquals(2, descriptor.partitions());
        assertEquals(3, descriptor.replicas());
        assertEquals("test_filter", descriptor.filter());
        assertEquals("test_engine", descriptor.dataStorage().engine());
        assertEquals("test_region", descriptor.dataStorage().dataRegion());
    }

    @Test
    void testFromParamsAndPreviousValueWithPreviousValues() {
        CreateZoneParams createZoneParams = CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .partitions(11)
                .replicas(22)
                .dataNodesAutoAdjust(33)
                .dataNodesAutoAdjustScaleUp(44)
                .dataNodesAutoAdjustScaleDown(55)
                .filter("previous_filter")
                .dataStorage(DataStorageParams.builder().engine("previous_engine").dataRegion("previous_region").build())
                .build();

        CatalogZoneDescriptor previous = fromParams(10, createZoneParams);

        CatalogZoneDescriptor descriptor = fromParamsAndPreviousValue(AlterZoneParams.builder().zoneName(ZONE_NAME).build(), previous);

        assertEquals(10, descriptor.id());
        assertEquals(ZONE_NAME, descriptor.name());
        assertEquals(11, descriptor.partitions());
        assertEquals(22, descriptor.replicas());
        assertEquals(33, descriptor.dataNodesAutoAdjust());
        assertEquals(44, descriptor.dataNodesAutoAdjustScaleUp());
        assertEquals(55, descriptor.dataNodesAutoAdjustScaleDown());
        assertEquals("previous_filter", descriptor.filter());
        assertEquals("previous_engine", descriptor.dataStorage().engine());
        assertEquals("previous_region", descriptor.dataStorage().dataRegion());
    }

    @Test
    void testFromParamsAndPreviousValueWithAutoAdjustFields() {
        checkAutoAdjustParams(
                fromParamsAndPreviousValue(alterZoneParams(1, 2, 3), createPreviousZoneWithDefaults()),
                1, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParamsAndPreviousValue(alterZoneParams(1, null, null), createPreviousZoneWithDefaults()),
                1, INFINITE_TIMER_VALUE, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParamsAndPreviousValue(alterZoneParams(null, 2, null), createPreviousZoneWithDefaults()),
                INFINITE_TIMER_VALUE, 2, INFINITE_TIMER_VALUE
        );

        checkAutoAdjustParams(
                fromParamsAndPreviousValue(alterZoneParams(null, null, 3), createPreviousZoneWithDefaults()),
                INFINITE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, 3
        );

        checkAutoAdjustParams(
                fromParamsAndPreviousValue(alterZoneParams(null, 2, 3), createPreviousZoneWithDefaults()),
                INFINITE_TIMER_VALUE, 2, 3
        );
    }

    @Test
    void testCollectIndexesAfterCreateTable() throws Exception {
        withCatalogManager(catalogManager -> {
            createTable(catalogManager, TABLE_NAME);

            int latestCatalogVersion = catalogManager.latestCatalogVersion();
            int earliestCatalogVersion = catalogManager.earliestCatalogVersion();

            int tableId = tableId(catalogManager, latestCatalogVersion, TABLE_NAME);

            assertThat(
                    collectIndexes(catalogManager, tableId, latestCatalogVersion, latestCatalogVersion),
                    contains(index(catalogManager, latestCatalogVersion, PK_INDEX_NAME))
            );

            assertThat(
                    collectIndexes(catalogManager, tableId, earliestCatalogVersion, latestCatalogVersion),
                    contains(index(catalogManager, latestCatalogVersion, PK_INDEX_NAME))
            );
        });
    }

    @Test
    void testCollectIndexesAfterCreateIndex() throws Exception {
        withCatalogManager(catalogManager -> {
            createTable(catalogManager, TABLE_NAME);
            createIndex(catalogManager, TABLE_NAME, INDEX_NAME);

            int latestCatalogVersion = catalogManager.latestCatalogVersion();
            int earliestCatalogVersion = catalogManager.earliestCatalogVersion();

            int tableId = tableId(catalogManager, latestCatalogVersion, TABLE_NAME);

            assertThat(
                    collectIndexes(catalogManager, tableId, latestCatalogVersion, latestCatalogVersion),
                    contains(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, latestCatalogVersion, INDEX_NAME)
                    )
            );

            assertThat(
                    collectIndexes(catalogManager, tableId, earliestCatalogVersion, latestCatalogVersion),
                    contains(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, latestCatalogVersion, INDEX_NAME)
                    )
            );
        });
    }

    @Test
    void testCollectIndexesAfterCreateIndexAndMakeAvailableIndex() throws Exception {
        withCatalogManager(catalogManager -> {
            String indexName0 = INDEX_NAME + 0;
            String indexName1 = INDEX_NAME + 1;

            createTable(catalogManager, TABLE_NAME);
            createIndex(catalogManager, TABLE_NAME, indexName0);
            createIndex(catalogManager, TABLE_NAME, indexName1);

            makeIndexAvailable(catalogManager, indexName1);

            int latestCatalogVersion = catalogManager.latestCatalogVersion();
            int earliestCatalogVersion = catalogManager.earliestCatalogVersion();

            int tableId = tableId(catalogManager, latestCatalogVersion, TABLE_NAME);

            assertThat(
                    collectIndexes(catalogManager, tableId, latestCatalogVersion, latestCatalogVersion),
                    contains(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, latestCatalogVersion, indexName0),
                            index(catalogManager, latestCatalogVersion, indexName1)
                    )
            );

            assertThat(
                    collectIndexes(catalogManager, tableId, earliestCatalogVersion, latestCatalogVersion),
                    contains(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, latestCatalogVersion, indexName0),
                            index(catalogManager, latestCatalogVersion, indexName1)
                    )
            );
        });
    }

    @Test
    void testCollectIndexesAfterDropIndexes() throws Exception {
        withCatalogManager(catalogManager -> {
            String indexName0 = INDEX_NAME + 0;
            String indexName1 = INDEX_NAME + 1;

            createTable(catalogManager, TABLE_NAME);
            createIndex(catalogManager, TABLE_NAME, indexName0);
            createIndex(catalogManager, TABLE_NAME, indexName1);

            makeIndexAvailable(catalogManager, indexName1);

            int catalogVersionBeforeDropIndex0 = catalogManager.latestCatalogVersion();

            dropIndex(catalogManager, indexName0);

            int catalogVersionBeforeDropIndex1 = catalogManager.latestCatalogVersion();

            dropIndex(catalogManager, indexName1);

            int latestCatalogVersion = catalogManager.latestCatalogVersion();
            int earliestCatalogVersion = catalogManager.earliestCatalogVersion();

            int tableId = tableId(catalogManager, latestCatalogVersion, TABLE_NAME);

            assertThat(
                    collectIndexes(catalogManager, tableId, latestCatalogVersion, latestCatalogVersion),
                    contains(index(catalogManager, latestCatalogVersion, PK_INDEX_NAME))
            );

            assertThat(
                    collectIndexes(catalogManager, tableId, earliestCatalogVersion, latestCatalogVersion),
                    contains(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, catalogVersionBeforeDropIndex0, indexName0),
                            index(catalogManager, catalogVersionBeforeDropIndex1, indexName1)
                    )
            );
        });
    }

    @Test
    void testCollectIndexesComplexCase() throws Exception {
        withCatalogManager(catalogManager -> {
            String indexName0 = INDEX_NAME + 0;
            String indexName1 = INDEX_NAME + 1;
            String indexName2 = INDEX_NAME + 2;
            String indexName3 = INDEX_NAME + 3;

            createTable(catalogManager, TABLE_NAME);
            createIndex(catalogManager, TABLE_NAME, indexName0);
            createIndex(catalogManager, TABLE_NAME, indexName1);
            createIndex(catalogManager, TABLE_NAME, indexName2);
            createIndex(catalogManager, TABLE_NAME, indexName3);

            makeIndexAvailable(catalogManager, indexName0);
            makeIndexAvailable(catalogManager, indexName2);

            int catalogVersionBeforeDropIndex0 = catalogManager.latestCatalogVersion();

            dropIndex(catalogManager, indexName0);

            int catalogVersionBeforeDropIndex3 = catalogManager.latestCatalogVersion();

            dropIndex(catalogManager, indexName3);

            int latestCatalogVersion = catalogManager.latestCatalogVersion();
            int earliestCatalogVersion = catalogManager.earliestCatalogVersion();

            int tableId = tableId(catalogManager, latestCatalogVersion, TABLE_NAME);

            assertThat(
                    collectIndexes(catalogManager, tableId, latestCatalogVersion, latestCatalogVersion),
                    contains(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, latestCatalogVersion, indexName1),
                            index(catalogManager, latestCatalogVersion, indexName2)
                    )
            );

            assertThat(
                    collectIndexes(catalogManager, tableId, earliestCatalogVersion, latestCatalogVersion),
                    contains(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, catalogVersionBeforeDropIndex0, indexName0),
                            index(catalogManager, latestCatalogVersion, indexName1),
                            index(catalogManager, latestCatalogVersion, indexName2),
                            index(catalogManager, catalogVersionBeforeDropIndex3, indexName3)
                    )
            );
        });
    }

    private static CreateZoneParams createZoneParams(@Nullable Integer autoAdjust, @Nullable Integer scaleUp, @Nullable Integer scaleDown) {
        return CreateZoneParams.builder()
                .zoneName(ZONE_NAME)
                .dataNodesAutoAdjust(autoAdjust)
                .dataNodesAutoAdjustScaleUp(scaleUp)
                .dataNodesAutoAdjustScaleDown(scaleDown)
                .build();
    }

    private static AlterZoneParams alterZoneParams(@Nullable Integer autoAdjust, @Nullable Integer scaleUp, @Nullable Integer scaleDown) {
        return AlterZoneParams.builder()
                .zoneName(ZONE_NAME)
                .dataNodesAutoAdjust(autoAdjust)
                .dataNodesAutoAdjustScaleUp(scaleUp)
                .dataNodesAutoAdjustScaleDown(scaleDown)
                .build();
    }

    private static void checkAutoAdjustParams(CatalogZoneDescriptor descriptor, int expAutoAdjust, int expScaleUp, int expScaleDown) {
        assertEquals(expAutoAdjust, descriptor.dataNodesAutoAdjust());
        assertEquals(expScaleUp, descriptor.dataNodesAutoAdjustScaleUp());
        assertEquals(expScaleDown, descriptor.dataNodesAutoAdjustScaleDown());
    }

    private static CatalogZoneDescriptor createPreviousZoneWithDefaults() {
        return fromParams(1, CreateZoneParams.builder().zoneName(ZONE_NAME).build());
    }

    private static void withCatalogManager(Consumer<CatalogManager> fun) throws Exception {
        CatalogManager catalogManager = createTestCatalogManager("test", new HybridClockImpl());

        try {
            catalogManager.start();

            fun.accept(catalogManager);
        } finally {
            catalogManager.stop();
        }
    }

    private static void createTable(CatalogManager catalogManager, String tableName) {
        CatalogCommand catalogCommand = CreateTableCommand.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .zone(DEFAULT_ZONE_NAME)
                .tableName(tableName)
                .columns(List.of(ColumnParams.builder().name(COLUMN_NAME).type(INT32).build()))
                .primaryKeyColumns(List.of(COLUMN_NAME))
                .colocationColumns(List.of(COLUMN_NAME))
                .build();

        assertThat(catalogManager.execute(catalogCommand), willCompleteSuccessfully());
    }

    private static void createIndex(CatalogManager catalogManager, String tableName, String indexName) {
        CatalogCommand catalogCommand = CreateHashIndexCommand.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .tableName(tableName)
                .indexName(indexName)
                .columns(List.of(COLUMN_NAME))
                .unique(false)
                .build();

        assertThat(catalogManager.execute(catalogCommand), willCompleteSuccessfully());
    }

    private static void makeIndexAvailable(CatalogManager catalogManager, String indexName) {
        CatalogIndexDescriptor index = index(catalogManager, catalogManager.latestCatalogVersion(), indexName);

        CatalogCommand catalogCommand = MakeIndexAvailableCommand.builder()
                .indexId(index.id())
                .build();

        assertThat(catalogManager.execute(catalogCommand), willCompleteSuccessfully());
    }

    private static void dropIndex(CatalogManager catalogManager, String indexName) {
        CatalogCommand catalogCommand = DropIndexCommand.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .indexName(indexName)
                .build();

        assertThat(catalogManager.execute(catalogCommand), willCompleteSuccessfully());
    }

    private static int tableId(CatalogService catalogService, int catalogVersion, String tableName) {
        CatalogTableDescriptor tableDescriptor = catalogService.tables(catalogVersion).stream()
                .filter(table -> tableName.equals(table.name()))
                .findFirst()
                .orElse(null);

        assertNotNull(tableDescriptor, "catalogVersion=" + catalogVersion + ", tableName=" + tableName);

        return tableDescriptor.id();
    }
}
