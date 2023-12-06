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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.collectIndexes;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
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
import org.junit.jupiter.api.Test;

/** For {@link CatalogUtils} testing. */
public class CatalogUtilsTest extends BaseIgniteAbstractTest {
    private static final String ZONE_NAME = "test_zone";

    private static final String TABLE_NAME = "test_table";

    private static final String INDEX_NAME = "test_index";

    private static final String PK_INDEX_NAME = pkIndexName(TABLE_NAME);

    private static final String COLUMN_NAME = "key";

    @Test
    void testCollectIndexesAfterCreateTable() throws Exception {
        withCatalogManager(catalogManager -> {
            createTable(catalogManager, TABLE_NAME);

            int latestCatalogVersion = catalogManager.latestCatalogVersion();
            int earliestCatalogVersion = catalogManager.earliestCatalogVersion();

            int tableId = tableId(catalogManager, latestCatalogVersion, TABLE_NAME);

            assertThat(
                    collectIndexes(catalogManager, tableId, latestCatalogVersion, latestCatalogVersion),
                    hasItems(index(catalogManager, latestCatalogVersion, PK_INDEX_NAME))
            );

            assertThat(
                    collectIndexes(catalogManager, tableId, earliestCatalogVersion, latestCatalogVersion),
                    hasItems(index(catalogManager, latestCatalogVersion, PK_INDEX_NAME))
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
                    hasItems(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, latestCatalogVersion, INDEX_NAME)
                    )
            );

            assertThat(
                    collectIndexes(catalogManager, tableId, earliestCatalogVersion, latestCatalogVersion),
                    hasItems(
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
                    hasItems(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, latestCatalogVersion, indexName0),
                            index(catalogManager, latestCatalogVersion, indexName1)
                    )
            );

            assertThat(
                    collectIndexes(catalogManager, tableId, earliestCatalogVersion, latestCatalogVersion),
                    hasItems(
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
                    hasItems(index(catalogManager, latestCatalogVersion, PK_INDEX_NAME))
            );

            assertThat(
                    collectIndexes(catalogManager, tableId, earliestCatalogVersion, latestCatalogVersion),
                    hasItems(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, catalogVersionBeforeDropIndex0, indexName0),
                            index(catalogManager, catalogVersionBeforeDropIndex1, indexName1)
                    )
            );
        });
    }

    /**
     * Tests the more complex case of getting indexes.
     *
     * <p>Consider the following versions of the directory with its contents:</p>
     * <pre>
     *     Catalog versions and entity IDs have been simplified.
     *
     *     0 : T0 Ipk(A)
     *     1 : T0 Ipk(A) I0(R) I1(R) I2(R) I3(R)
     *     2 : T0 Ipk(A) I0(A) I1(R) I2(A) I3(R)
     *     3 : T0 Ipk(A) I1(R) I2(A)
     * </pre>
     *
     * <p>Expected indexes for range version:</p>
     * <pre>
     *     3 -> 3 : Ipk(A) I1(R) I2(A)
     *     0 -> 3 : Ipk(A) I0(A) I1(R) I2(A) I3(R)
     * </pre>
     */
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
                    hasItems(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, latestCatalogVersion, indexName1),
                            index(catalogManager, latestCatalogVersion, indexName2)
                    )
            );

            assertThat(
                    collectIndexes(catalogManager, tableId, earliestCatalogVersion, latestCatalogVersion),
                    hasItems(
                            index(catalogManager, latestCatalogVersion, PK_INDEX_NAME),
                            index(catalogManager, catalogVersionBeforeDropIndex0, indexName0),
                            index(catalogManager, latestCatalogVersion, indexName1),
                            index(catalogManager, latestCatalogVersion, indexName2),
                            index(catalogManager, catalogVersionBeforeDropIndex3, indexName3)
                    )
            );
        });
    }

    private static void checkAutoAdjustParams(CatalogZoneDescriptor descriptor, int expAutoAdjust, int expScaleUp, int expScaleDown) {
        assertEquals(expAutoAdjust, descriptor.dataNodesAutoAdjust());
        assertEquals(expScaleUp, descriptor.dataNodesAutoAdjustScaleUp());
        assertEquals(expScaleDown, descriptor.dataNodesAutoAdjustScaleDown());
    }

    private static CatalogZoneDescriptor createPreviousZoneWithDefaults() {
        return fromParams(1, ZONE_NAME);
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
