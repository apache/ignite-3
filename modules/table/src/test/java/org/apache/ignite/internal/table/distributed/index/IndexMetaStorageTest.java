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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.impl.StandaloneMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** For {@link IndexMetaStorage} testing. */
public class IndexMetaStorageTest extends BaseIndexMetaStorageTest {
    @Override
    MetaStorageManager createMetastore() {
        return StandaloneMetaStorageManager.create(new SimpleInMemoryKeyValueStorage(NODE_NAME));
    }

    @Override
    CatalogManager createCatalogManager() {
        return createTestCatalogManager(NODE_NAME, clock, metastore);
    }

    @Test
    void testIndexNotExists() {
        assertNull(indexMetaStorage.indexMeta(Integer.MAX_VALUE));
    }

    @Test
    void testPkIndex() {
        int indexId = indexId(PK_INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int catalogVersion = latestCatalogVersion();

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromMetastore = fromMetastore(indexId);

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(AVAILABLE, toChangeInfo(catalogVersion));

        checkFields(indexMeta, indexId, tableId, PK_INDEX_NAME, AVAILABLE, expectedStatuses, catalogVersion);
        checkFields(fromMetastore, indexId, tableId, PK_INDEX_NAME, AVAILABLE, expectedStatuses, catalogVersion);
    }

    @Test
    void testRenameTable() {
        int indexId = indexId(PK_INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int catalogVersion = latestCatalogVersion();

        renameSimpleTable(catalogManager, TABLE_NAME, NEW_TABLE_NAME);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromMetastore = fromMetastore(indexId);

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(AVAILABLE, toChangeInfo(catalogVersion));

        int latestCatalogVersion = latestCatalogVersion();

        checkFields(indexMeta, indexId, tableId, NEW_PK_INDEX_NAME, AVAILABLE, expectedStatuses, latestCatalogVersion);
        checkFields(fromMetastore, indexId, tableId, NEW_PK_INDEX_NAME, AVAILABLE, expectedStatuses, latestCatalogVersion);
    }

    @Test
    void testRegisteredIndex() {
        int catalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromMetastore = fromMetastore(indexId);

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(REGISTERED, toChangeInfo(catalogVersion));

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, REGISTERED, expectedStatuses, catalogVersion);
        checkFields(fromMetastore, indexId, tableId, INDEX_NAME, REGISTERED, expectedStatuses, catalogVersion);
    }

    @Test
    void testBuildingIndex() {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        startBuildingIndex(catalogManager, indexId);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromMetastore = fromMetastore(indexId);

        int latestCatalogVersion = latestCatalogVersion();

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(latestCatalogVersion)
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, BUILDING, expectedStatuses, latestCatalogVersion);
        checkFields(fromMetastore, indexId, tableId, INDEX_NAME, BUILDING, expectedStatuses, latestCatalogVersion);
    }

    @Test
    void testAvailableIndex() {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        int buildingIndexCatalogVersion = executeCatalogUpdate(() -> startBuildingIndex(catalogManager, indexId));

        makeIndexAvailable(catalogManager, indexId);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromMetastore = fromMetastore(indexId);

        int latestCatalogVersion = latestCatalogVersion();

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                AVAILABLE, toChangeInfo(latestCatalogVersion)
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, AVAILABLE, expectedStatuses, latestCatalogVersion);
        checkFields(fromMetastore, indexId, tableId, INDEX_NAME, AVAILABLE, expectedStatuses, latestCatalogVersion);
    }

    @Test
    void testStoppingIndex() {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        int buildingIndexCatalogVersion = executeCatalogUpdate(() -> startBuildingIndex(catalogManager, indexId));
        int availableIndexCatalogVersion = executeCatalogUpdate(() -> makeIndexAvailable(catalogManager, indexId));

        dropSimpleIndex(catalogManager, INDEX_NAME);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromMetastore = fromMetastore(indexId);

        int latestCatalogVersion = latestCatalogVersion();

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                AVAILABLE, toChangeInfo(availableIndexCatalogVersion),
                STOPPING, toChangeInfo(latestCatalogVersion)
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, STOPPING, expectedStatuses, latestCatalogVersion);
        checkFields(fromMetastore, indexId, tableId, INDEX_NAME, STOPPING, expectedStatuses, latestCatalogVersion);
    }

    @Test
    void testRemoveRegisteredIndex() {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        dropSimpleIndex(catalogManager, INDEX_NAME);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromMetastore = fromMetastore(indexId);

        int latestCatalogVersion = latestCatalogVersion();

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                REMOVED, toChangeInfo(latestCatalogVersion)
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, REMOVED, expectedStatuses, latestCatalogVersion);
        checkFields(fromMetastore, indexId, tableId, INDEX_NAME, REMOVED, expectedStatuses, latestCatalogVersion);
    }

    @Test
    void testRemoveBuildingIndex() {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        int buildingIndexCatalogVersion = executeCatalogUpdate(() -> startBuildingIndex(catalogManager, indexId));

        dropSimpleIndex(catalogManager, INDEX_NAME);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromMetastore = fromMetastore(indexId);

        int latestCatalogVersion = latestCatalogVersion();

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                REMOVED, toChangeInfo(latestCatalogVersion)
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, REMOVED, expectedStatuses, latestCatalogVersion);
        checkFields(fromMetastore, indexId, tableId, INDEX_NAME, REMOVED, expectedStatuses, latestCatalogVersion);
    }

    @Test
    void testRemoveStoppingIndex() {
        int registeredIndexCatalogVersion = executeCatalogUpdate(() -> createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME));

        int indexId = indexId(INDEX_NAME);
        int tableId = tableId(TABLE_NAME);

        int buildingIndexCatalogVersion = executeCatalogUpdate(() -> startBuildingIndex(catalogManager, indexId));
        int availableIndexCatalogVersion = executeCatalogUpdate(() -> makeIndexAvailable(catalogManager, indexId));
        int stoppingIndexCatalogVersion = executeCatalogUpdate(() -> dropSimpleIndex(catalogManager, INDEX_NAME));

        removeIndex(catalogManager, indexId);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromMetastore = fromMetastore(indexId);

        int latestCatalogVersion = latestCatalogVersion();

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                REGISTERED, toChangeInfo(registeredIndexCatalogVersion),
                BUILDING, toChangeInfo(buildingIndexCatalogVersion),
                AVAILABLE, toChangeInfo(availableIndexCatalogVersion),
                STOPPING, toChangeInfo(stoppingIndexCatalogVersion),
                READ_ONLY, toChangeInfo(latestCatalogVersion)
        );

        checkFields(indexMeta, indexId, tableId, INDEX_NAME, READ_ONLY, expectedStatuses, latestCatalogVersion);
        checkFields(fromMetastore, indexId, tableId, INDEX_NAME, READ_ONLY, expectedStatuses, latestCatalogVersion);
    }

    @Test
    void testRemoveTable() {
        int indexId = indexId(PK_INDEX_NAME);
        int tableId = tableId(TABLE_NAME);
        int afterCreateTableCatalogVersion = latestCatalogVersion();

        dropSimpleTable(catalogManager, TABLE_NAME);

        IndexMeta indexMeta = indexMetaStorage.indexMeta(indexId);
        IndexMeta fromMetastore = fromMetastore(indexId);

        int latestCatalogVersion = latestCatalogVersion();

        Map<MetaIndexStatus, MetaIndexStatusChange> expectedStatuses = Map.of(
                AVAILABLE, toChangeInfo(afterCreateTableCatalogVersion),
                READ_ONLY, toChangeInfo(latestCatalogVersion)
        );

        checkFields(indexMeta, indexId, tableId, PK_INDEX_NAME, READ_ONLY, expectedStatuses, latestCatalogVersion);
        checkFields(fromMetastore, indexId, tableId, PK_INDEX_NAME, READ_ONLY, expectedStatuses, latestCatalogVersion);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRemoveRegisteredIndexAndUpdateLwm(boolean updateLwmBeforeOrAfterDropIndex) {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        if (updateLwmBeforeOrAfterDropIndex) {
            updateLwm(clock.now().addPhysicalTime(DELTA_TO_TRIGGER_DESTROY));
        }

        dropSimpleIndex(catalogManager, INDEX_NAME);

        if (!updateLwmBeforeOrAfterDropIndex) {
            updateLwm(clock.now());
        }

        assertNull(indexMetaStorage.indexMeta(indexId));
        assertNull(fromMetastore(indexId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRemoveBuildingIndexAndUpdateLwm(boolean updateLwmBeforeOrAfterDropIndex) {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(catalogManager, indexId);

        if (updateLwmBeforeOrAfterDropIndex) {
            updateLwm(clock.now().addPhysicalTime(DELTA_TO_TRIGGER_DESTROY));
        }

        dropSimpleIndex(catalogManager, INDEX_NAME);

        if (!updateLwmBeforeOrAfterDropIndex) {
            updateLwm(clock.now());
        }

        assertNull(indexMetaStorage.indexMeta(indexId));
        assertNull(fromMetastore(indexId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRemoveAvailableIndexAndUpdateLwm(boolean updateLwmBeforeOrAfterDropIndex) {
        createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);

        int indexId = indexId(INDEX_NAME);

        startBuildingIndex(catalogManager, indexId);
        makeIndexAvailable(catalogManager, indexId);

        if (updateLwmBeforeOrAfterDropIndex) {
            updateLwm(clock.now().addPhysicalTime(DELTA_TO_TRIGGER_DESTROY));
        }

        dropSimpleIndex(catalogManager, INDEX_NAME);

        if (!updateLwmBeforeOrAfterDropIndex) {
            updateLwm(clock.now());
        }

        assertNotNull(indexMetaStorage.indexMeta(indexId));
        assertNotNull(fromMetastore(indexId));
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
            updateLwm(clock.now().addPhysicalTime(DELTA_TO_TRIGGER_DESTROY));
        }

        removeIndex(catalogManager, indexId);

        if (!updateLwmBeforeOrAfterDropIndex) {
            updateLwm(clock.now());
        }

        assertNull(indexMetaStorage.indexMeta(indexId));
        assertNull(fromMetastore(indexId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRemoveTableIndexAndUpdateLwm(boolean updateLwmBeforeOrAfterDropIndex) {
        int indexId = indexId(PK_INDEX_NAME);

        if (updateLwmBeforeOrAfterDropIndex) {
            updateLwm(clock.now().addPhysicalTime(DELTA_TO_TRIGGER_DESTROY));
        }

        dropSimpleTable(catalogManager, TABLE_NAME);

        if (!updateLwmBeforeOrAfterDropIndex) {
            updateLwm(clock.now());
        }

        assertNull(indexMetaStorage.indexMeta(indexId));
        assertNull(fromMetastore(indexId));
    }
}
