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

import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManagerWithInterceptor;
import static org.apache.ignite.internal.table.TableTestUtils.INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.PK_INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.dropSimpleIndex;
import static org.apache.ignite.internal.table.TableTestUtils.dropSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.makeIndexAvailable;
import static org.apache.ignite.internal.table.TableTestUtils.removeIndex;
import static org.apache.ignite.internal.table.TableTestUtils.renameSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.startBuildingIndex;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.BUILDING;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.READ_ONLY;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REGISTERED;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.REMOVED;
import static org.apache.ignite.internal.table.distributed.index.MetaIndexStatus.STOPPING;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.TestRocksDbKeyValueStorage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** For testing recovery of {@link IndexMetaStorage}. */
@ExtendWith(WorkDirectoryExtension.class)
public class IndexMetaStorageRecoveryTest extends BaseIndexMetaStorageTest {
    @WorkDirectory
    private Path workDir;

    private final TestUpdateHandlerInterceptor interceptor = new TestUpdateHandlerInterceptor();

    @Override
    MetaStorageManager createMetastore() {
        var keyValueStorage = new TestRocksDbKeyValueStorage(NODE_NAME, workDir);

        return StandaloneMetaStorageManager.create(keyValueStorage);
    }

    @Override
    CatalogManager createCatalogManager() {
        return createTestCatalogManagerWithInterceptor(NODE_NAME, clock, metastore, interceptor);
    }

    @Test
    void testMissingNewIndex() throws Exception {
        executeCatalogUpdateWithDropEvents(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        assertThat(allIndexNamesFromSnapshotIndexMetas(), contains(PK_INDEX_NAME));

        restartComponents();

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int catalogVersion = catalogManager.latestCatalogVersion();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromVault = fromVault(indexId);

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, REGISTERED, Map.of(REGISTERED, toChangeInfo(catalogVersion)));
        checkFields(fromVault, indexId, tableId, INDEX_NAME, REGISTERED, Map.of(REGISTERED, toChangeInfo(catalogVersion)));
    }

    @Test
    void testMissingRenameTable() throws Exception {
        int indexId = indexId(PK_INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int catalogVersion = catalogManager.latestCatalogVersion();

        executeCatalogUpdateWithDropEvents(() -> renameSimpleTable(catalogManager, TABLE_NAME, NEW_TABLE_NAME));

        assertEquals(PK_INDEX_NAME, indexMetaStorage.indexMeta(indexId).indexName());

        restartComponents();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromVault = fromVault(indexId);

        checkFields(indexMeta, indexId, tableId, NEW_PK_INDEX_NAME, AVAILABLE, Map.of(AVAILABLE, toChangeInfo(catalogVersion)));
        checkFields(fromVault, indexId, tableId, NEW_PK_INDEX_NAME, AVAILABLE, Map.of(AVAILABLE, toChangeInfo(catalogVersion)));
    }

    @Test
    void testMissingBuildingIndex() throws Exception {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        executeCatalogUpdateWithDropEvents(() -> startBuildingIndex(catalogManager, indexId));

        assertEquals(REGISTERED, indexMetaStorage.indexMeta(indexId).status());

        restartComponents();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromVault = fromVault(indexId);

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, BUILDING, expectedStatuses);
        checkFields(fromVault, indexId, tableId, INDEX_NAME, BUILDING, expectedStatuses);
    }

    @Test
    void testMissingAvailableIndex() throws Exception {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        int buildingIndexCatalogVersion = executeCatalogUpdate(() -> startBuildingIndex(catalogManager, indexId));

        executeCatalogUpdateWithDropEvents(() -> makeIndexAvailable(catalogManager, indexId));

        assertEquals(BUILDING, indexMetaStorage.indexMeta(indexId).status());

        restartComponents();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromVault = fromVault(indexId);

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                AVAILABLE, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, AVAILABLE, expectedStatuses);
        checkFields(fromVault, indexId, tableId, INDEX_NAME, AVAILABLE, expectedStatuses);
    }

    @Test
    void testMissingStoppingIndex() throws Exception {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        int buildingIndexCatalogVersion = executeCatalogUpdate(() -> startBuildingIndex(catalogManager, indexId));
        int availableIndexCatalogVersion = executeCatalogUpdate(() -> makeIndexAvailable(catalogManager, indexId));

        executeCatalogUpdateWithDropEvents(() -> dropSimpleIndex(catalogManager, INDEX_NAME));

        assertEquals(AVAILABLE, indexMetaStorage.indexMeta(indexId).status());

        restartComponents();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromVault = fromVault(indexId);

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                AVAILABLE, toChangeInfo(availableIndexCatalogVersion),
                STOPPING, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, STOPPING, expectedStatuses);
        checkFields(fromVault, indexId, tableId, INDEX_NAME, STOPPING, expectedStatuses);
    }

    @Test
    void testMissingRemovingStoppingIndex() throws Exception {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        int buildingIndexCatalogVersion = executeCatalogUpdate(() -> startBuildingIndex(catalogManager, indexId));
        int availableIndexCatalogVersion = executeCatalogUpdate(() -> makeIndexAvailable(catalogManager, indexId));
        int stoppingIndexCatalogVersion = executeCatalogUpdate(() -> dropSimpleIndex(catalogManager, INDEX_NAME));

        executeCatalogUpdateWithDropEvents(() -> removeIndex(catalogManager, indexId));

        assertEquals(STOPPING, indexMetaStorage.indexMeta(indexId).status());

        restartComponents();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromVault = fromVault(indexId);

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                AVAILABLE, toChangeInfo(availableIndexCatalogVersion),
                STOPPING, toChangeInfo(stoppingIndexCatalogVersion),
                READ_ONLY, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, READ_ONLY, expectedStatuses);
        checkFields(fromVault, indexId, tableId, INDEX_NAME, READ_ONLY, expectedStatuses);
    }

    @Test
    void testMissingDropTable() throws Exception {
        int indexId = indexId(PK_INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int availableIndexCatalogVersion = catalogManager.latestCatalogVersion();

        executeCatalogUpdateWithDropEvents(() -> dropSimpleTable(catalogManager, TABLE_NAME));

        assertEquals(AVAILABLE, indexMetaStorage.indexMeta(indexId).status());

        restartComponents();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromVault = fromVault(indexId);

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                AVAILABLE, toChangeInfo(availableIndexCatalogVersion),
                READ_ONLY, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, PK_INDEX_NAME, READ_ONLY, expectedStatuses);
        checkFields(fromVault, indexId, tableId, PK_INDEX_NAME, READ_ONLY, expectedStatuses);
    }

    @Test
    void testMissingDropRegisteredIndex() throws Exception {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        executeCatalogUpdateWithDropEvents(() -> dropSimpleIndex(catalogManager, INDEX_NAME));

        assertEquals(REGISTERED, indexMetaStorage.indexMeta(indexId).status());

        restartComponents();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromVault = fromVault(indexId);

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                REMOVED, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, REMOVED, expectedStatuses);
        checkFields(fromVault, indexId, tableId, INDEX_NAME, REMOVED, expectedStatuses);
    }

    @Test
    void testMissingDropBuildingIndex() throws Exception {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        int buildingIndexCatalogVersion = executeCatalogUpdate(() -> startBuildingIndex(catalogManager, indexId));

        executeCatalogUpdateWithDropEvents(() -> dropSimpleIndex(catalogManager, INDEX_NAME));

        assertEquals(BUILDING, indexMetaStorage.indexMeta(indexId).status());

        restartComponents();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromVault = fromVault(indexId);

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                REMOVED, toChangeInfo(catalogManager.latestCatalogVersion())
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, REMOVED, expectedStatuses);
        checkFields(fromVault, indexId, tableId, INDEX_NAME, REMOVED, expectedStatuses);
    }

    @Test
    void testMissingDropRegisteredIndexWithUpdateLwmBeforeRestart() throws Exception {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        executeCatalogUpdateWithDropEvents(() -> dropSimpleIndex(catalogManager, INDEX_NAME));

        updateLwm(clock.now().addPhysicalTime(DELTA_TO_TRIGGER_DESTROY));

        restartComponents();

        assertNull(indexMetaStorage.indexMeta(indexId));
        assertNull(fromVault(indexId));
    }

    @Test
    void testMissingDropBuildingIndexWithUpdateLwmBeforeRestart() throws Exception {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(catalogManager, indexId);

        executeCatalogUpdateWithDropEvents(() -> dropSimpleIndex(catalogManager, INDEX_NAME));

        updateLwm(clock.now().addPhysicalTime(DELTA_TO_TRIGGER_DESTROY));

        restartComponents();

        assertNull(indexMetaStorage.indexMeta(indexId));
        assertNull(fromVault(indexId));
    }

    @Test
    void testMissingDropAvailableIndexWithUpdateLwmBeforeRestart() throws Exception {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(catalogManager, indexId);
        makeIndexAvailable(catalogManager, indexId);

        executeCatalogUpdateWithDropEvents(() -> dropSimpleIndex(catalogManager, INDEX_NAME));

        updateLwm(clock.now().addPhysicalTime(DELTA_TO_TRIGGER_DESTROY));

        restartComponents();

        assertNotNull(indexMetaStorage.indexMeta(indexId));
        assertNotNull(fromVault(indexId));
    }

    @Test
    void testMissingRemovingStoppingIndexWithUpdateLwmBeforeRestart() throws Exception {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(catalogManager, indexId);
        makeIndexAvailable(catalogManager, indexId);
        dropSimpleIndex(catalogManager, INDEX_NAME);

        executeCatalogUpdateWithDropEvents(() -> removeIndex(catalogManager, indexId));

        updateLwm(clock.now().addPhysicalTime(DELTA_TO_TRIGGER_DESTROY));

        restartComponents();

        assertNull(indexMetaStorage.indexMeta(indexId));
        assertNull(fromVault(indexId));
    }

    @Test
    void testMissingDropTableWithUpdateLwmBeforeRestart() throws Exception {
        int indexId = indexId(PK_INDEX_NAME);

        executeCatalogUpdateWithDropEvents(() -> dropSimpleTable(catalogManager, TABLE_NAME));

        updateLwm(clock.now().addPhysicalTime(DELTA_TO_TRIGGER_DESTROY));

        restartComponents();

        assertNull(indexMetaStorage.indexMeta(indexId));
        assertNull(fromVault(indexId));
    }

    private void executeCatalogUpdateWithDropEvents(RunnableX task) {
        CompletableFuture<Void> startDropEventsFuture = interceptor.startDropEvents();

        runAsync(task);

        assertThat(startDropEventsFuture, willCompleteSuccessfully());
    }

    private void restartComponents() throws Exception {
        var componentContext = new ComponentContext();

        IgniteUtils.closeAll(
                indexMetaStorage == null ? null : indexMetaStorage::beforeNodeStop,
                catalogManager == null ? null : catalogManager::beforeNodeStop,
                metastore == null ? null : metastore::beforeNodeStop,
                () -> assertThat(stopAsync(componentContext, indexMetaStorage, catalogManager, metastore), willCompleteSuccessfully())
        );

        interceptor.stopDropEvents();

        createComponents();

        assertThat(startAsync(componentContext, metastore, catalogManager), willCompleteSuccessfully());

        assertThat(metastore.deployWatches(), willCompleteSuccessfully());

        assertThat(catalogManager.catalogInitializationFuture(), willCompleteSuccessfully());

        assertThat(startAsync(componentContext, indexMetaStorage), willCompleteSuccessfully());
    }
}
