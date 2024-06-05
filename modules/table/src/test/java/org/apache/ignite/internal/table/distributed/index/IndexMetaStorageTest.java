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

package org.apache.ignite.internal.table.distributed.index;

import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.table.TableTestUtils.INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.PK_INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.dropSimpleIndex;
import static org.apache.ignite.internal.table.TableTestUtils.dropSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.makeIndexAvailable;
import static org.apache.ignite.internal.table.TableTestUtils.removeIndex;
import static org.apache.ignite.internal.table.TableTestUtils.renameSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.startBuildingIndex;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatusEnum.AVAILABLE;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatusEnum.BUILDING;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatusEnum.READ_ONLY;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatusEnum.REGISTERED;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatusEnum.REMOVED;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatusEnum.STOPPING;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** For {@link IndexMetaStorage} testing. */
public class IndexMetaStorageTest {
    private final HybridClock clock = new HybridClockImpl();

    private final CatalogManager catalogManager = createTestCatalogManager("test", clock);

    private final TestLowWatermark lowWatermark = new TestLowWatermark();

    private final VaultManager vaultManager = new VaultManager(new InMemoryVaultService());

    private final IndexMetaStorage indexMetaStorage = new IndexMetaStorage(catalogManager, lowWatermark, vaultManager);

    @BeforeEach
    void setUp() {
        var componentContext = new ComponentContext();

        assertThat(startAsync(componentContext, vaultManager, catalogManager, indexMetaStorage), willCompleteSuccessfully());

        assertThat(catalogManager.catalogInitializationFuture(), willCompleteSuccessfully());

        createSimpleTable(catalogManager, TABLE_NAME);
    }

    @AfterEach
    void tearDown() throws Exception {
        var componentContext = new ComponentContext();

        IgniteUtils.closeAll(
                indexMetaStorage::beforeNodeStop,
                catalogManager::beforeNodeStop,
                vaultManager::beforeNodeStop,
                () -> assertThat(stopAsync(componentContext, indexMetaStorage, catalogManager, vaultManager), willCompleteSuccessfully())
        );
    }

    @Test
    void testIndexNotExists() {
        assertNull(indexMetaStorage.indexMeta(Integer.MAX_VALUE));
    }

    @Test
    void testPkIndex() {
        int indexId = indexId(PK_INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int catalogVersion = catalogManager.latestCatalogVersion();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

        checkFields(indexMeta, indexId, tableId, PK_INDEX_NAME, AVAILABLE, Map.of(AVAILABLE, toChangeInfo(catalogVersion)));
    }

    @Test
    void testRenameTable() {
        int indexId = indexId(PK_INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int catalogVersion = catalogManager.latestCatalogVersion();

        String newTableName = TABLE_NAME + "_FOO";
        String newPkIndexName = pkIndexName(newTableName);

        renameSimpleTable(catalogManager, TABLE_NAME, newTableName);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

        checkFields(indexMeta, indexId, tableId, newPkIndexName, AVAILABLE, Map.of(AVAILABLE, toChangeInfo(catalogVersion)));
    }

    @Test
    void testRegisteredIndex() {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int catalogVersion = catalogManager.latestCatalogVersion();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, REGISTERED, Map.of(REGISTERED, toChangeInfo(catalogVersion)));
    }

    @Test
    void testBuildingIndex() {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int catalogVersion = catalogManager.latestCatalogVersion();

        startBuildingIndex(catalogManager, indexId);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

        Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(catalogVersion),
                BUILDING, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, BUILDING, expectedStatuses);
    }

    @Test
    void testAvailableIndex() {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int registeredIndexCatalogVersion = catalogManager.latestCatalogVersion();

        startBuildingIndex(catalogManager, indexId);

        int buildingIndexCatalogVersion = catalogManager.latestCatalogVersion();

        makeIndexAvailable(catalogManager, indexId);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

        Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                AVAILABLE, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, AVAILABLE, expectedStatuses);
    }

    @Test
    void testStoppingIndex() {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int registeredIndexCatalogVersion = catalogManager.latestCatalogVersion();

        startBuildingIndex(catalogManager, indexId);

        int buildingIndexCatalogVersion = catalogManager.latestCatalogVersion();

        makeIndexAvailable(catalogManager, indexId);

        int availableIndexCatalogVersion = catalogManager.latestCatalogVersion();

        dropSimpleIndex(catalogManager, INDEX_NAME);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

        Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                AVAILABLE, toChangeInfo(availableIndexCatalogVersion),
                STOPPING, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, STOPPING, expectedStatuses);
    }

    @Test
    void testRemoveRegisteredIndex() {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int registeredIndexCatalogVersion = catalogManager.latestCatalogVersion();

        dropSimpleIndex(catalogManager, INDEX_NAME);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

        Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                REMOVED, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, REMOVED, expectedStatuses);
    }

    @Test
    void testRemoveBuildingIndex() {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int registeredIndexCatalogVersion = catalogManager.latestCatalogVersion();

        startBuildingIndex(catalogManager, indexId);

        int buildingIndexCatalogVersion = catalogManager.latestCatalogVersion();

        dropSimpleIndex(catalogManager, INDEX_NAME);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

        Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                REMOVED, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, REMOVED, expectedStatuses);
    }

    @Test
    void testRemoveStoppingIndex() {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int registeredIndexCatalogVersion = catalogManager.latestCatalogVersion();

        startBuildingIndex(catalogManager, indexId);

        int buildingIndexCatalogVersion = catalogManager.latestCatalogVersion();

        makeIndexAvailable(catalogManager, indexId);

        int availableIndexCatalogVersion = catalogManager.latestCatalogVersion();

        dropSimpleIndex(catalogManager, INDEX_NAME);

        int stoppingIndexCatalogVersion = catalogManager.latestCatalogVersion();

        removeIndex(catalogManager, indexId);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

        Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                AVAILABLE, toChangeInfo(availableIndexCatalogVersion),
                STOPPING, toChangeInfo(stoppingIndexCatalogVersion),
                READ_ONLY, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, READ_ONLY, expectedStatuses);
    }

    @Test
    void testRemoveTable() {
        int indexId = indexId(PK_INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int afterCreateTableCatalogVersion = catalogManager.latestCatalogVersion();

        dropSimpleTable(catalogManager, TABLE_NAME);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);

        Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> expectedStatuses = Map.of(
                AVAILABLE, toChangeInfo(afterCreateTableCatalogVersion),
                READ_ONLY, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, PK_INDEX_NAME, READ_ONLY, expectedStatuses);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRemoveRegisteredIndexAndUpdateLwm(boolean updateLwmBeforeOrAfterDropIndex) {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        if (updateLwmBeforeOrAfterDropIndex) {
            assertThat(lowWatermark.updateAndNotify(clock.now().addPhysicalTime(1_000_000)), willCompleteSuccessfully());
        }

        dropSimpleIndex(catalogManager, INDEX_NAME);

        if (!updateLwmBeforeOrAfterDropIndex) {
            assertThat(lowWatermark.updateAndNotify(clock.now()), willCompleteSuccessfully());
        }

        assertNull(indexMetaStorage.indexMeta(indexId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRemoveBuildingIndexAndUpdateLwm(boolean updateLwmBeforeOrAfterDropIndex) {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(catalogManager, indexId);

        if (updateLwmBeforeOrAfterDropIndex) {
            assertThat(lowWatermark.updateAndNotify(clock.now().addPhysicalTime(1_000_000)), willCompleteSuccessfully());
        }

        dropSimpleIndex(catalogManager, INDEX_NAME);

        if (!updateLwmBeforeOrAfterDropIndex) {
            assertThat(lowWatermark.updateAndNotify(clock.now()), willCompleteSuccessfully());
        }

        assertNull(indexMetaStorage.indexMeta(indexId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRemoveAvailableIndexAndUpdateLwm(boolean updateLwmBeforeOrAfterDropIndex) {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(catalogManager, indexId);
        makeIndexAvailable(catalogManager, indexId);

        if (updateLwmBeforeOrAfterDropIndex) {
            assertThat(lowWatermark.updateAndNotify(clock.now().addPhysicalTime(1_000_000)), willCompleteSuccessfully());
        }

        dropSimpleIndex(catalogManager, INDEX_NAME);

        if (!updateLwmBeforeOrAfterDropIndex) {
            assertThat(lowWatermark.updateAndNotify(clock.now()), willCompleteSuccessfully());
        }

        assertNotNull(indexMetaStorage.indexMeta(indexId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRemoveStoppingIndexAndUpdateLwm(boolean updateLwmBeforeOrAfterDropIndex) {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(catalogManager, indexId);
        makeIndexAvailable(catalogManager, indexId);
        dropSimpleIndex(catalogManager, INDEX_NAME);

        if (updateLwmBeforeOrAfterDropIndex) {
            assertThat(lowWatermark.updateAndNotify(clock.now().addPhysicalTime(1_000_000)), willCompleteSuccessfully());
        }

        removeIndex(catalogManager, indexId);

        if (!updateLwmBeforeOrAfterDropIndex) {
            assertThat(lowWatermark.updateAndNotify(clock.now()), willCompleteSuccessfully());
        }

        assertNull(indexMetaStorage.indexMeta(indexId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRemoveTableIndexAndUpdateLwm(boolean updateLwmBeforeOrAfterDropIndex) {
        int indexId = indexId(PK_INDEX_NAME);

        if (updateLwmBeforeOrAfterDropIndex) {
            assertThat(lowWatermark.updateAndNotify(clock.now().addPhysicalTime(1_000_000)), willCompleteSuccessfully());
        }

        dropSimpleTable(catalogManager, TABLE_NAME);

        if (!updateLwmBeforeOrAfterDropIndex) {
            assertThat(lowWatermark.updateAndNotify(clock.now()), willCompleteSuccessfully());
        }

        assertNull(indexMetaStorage.indexMeta(indexId));
    }

    private int indexId(String indexName) {
        return getIndexIdStrict(catalogManager, indexName, clock.nowLong());
    }

    private int tableId(String tableName) {
        return getTableIdStrict(catalogManager, tableName, clock.nowLong());
    }

    private MetaIndexStatusChangeInfo toChangeInfo(int catalogVersion) {
        Catalog catalog = catalogManager.catalog(catalogVersion);

        assertNotNull(catalog, "catalogVersion=" + catalogVersion);

        return new MetaIndexStatusChangeInfo(catalog.version(), catalog.time());
    }

    private static void checkFields(
            @Nullable IndexMeta indexMeta,
            int expIndexId,
            int expTableId,
            String expIndexName,
            MetaIndexStatusEnum expStatus,
            Map<MetaIndexStatusEnum, MetaIndexStatusChangeInfo> expStatuses
    ) {
        assertNotNull(indexMeta);

        assertEquals(expIndexId, indexMeta.indexId());
        assertEquals(expTableId, indexMeta.tableId());
        assertEquals(expIndexName, indexMeta.indexName());
        assertEquals(expStatus, indexMeta.status());
        assertEquals(expStatuses, indexMeta.statuses());
    }
}
