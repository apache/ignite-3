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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.BUILDING;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.table.TableTestUtils.INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.PK_INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.makeIndexAvailable;
import static org.apache.ignite.internal.table.TableTestUtils.startBuildingIndex;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** For {@link FullStateTransferIndexChooser} testing. */
public class FullStateTransferIndexChooserTest extends BaseIgniteAbstractTest {
    private static final String REGISTERED_INDEX_NAME = INDEX_NAME + "_" + REGISTERED;

    private static final String BUILDING_INDEX_NAME = INDEX_NAME + "_" + BUILDING;

    private static final String AVAILABLE_INDEX_NAME = INDEX_NAME + "_" + AVAILABLE;

    private static final String STOPPING_INDEX_NAME = INDEX_NAME + "_" + STOPPING;

    private static final String READ_ONLY_INDEX_NAME = INDEX_NAME + "_READ_ONLY";

    private final HybridClock clock = new HybridClockImpl();

    private CatalogManager catalogManager;

    private FullStateTransferIndexChooser indexChooser;

    @BeforeEach
    void setUp() {
        catalogManager = CatalogTestUtils.createTestCatalogManager("test", clock);

        indexChooser = new FullStateTransferIndexChooser(catalogManager);

        assertThat(catalogManager.start(), willCompleteSuccessfully());

        indexChooser.start();

        createSimpleTable(catalogManager, TABLE_NAME);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(
                indexChooser,
                catalogManager::beforeNodeStop,
                catalogManager::stop
        );
    }

    @Test
    void chooseForAddWriteWithSecondaryAndWithoutReadOnlyIndexes() {
        int pkIndexId = indexId(PK_INDEX_NAME);
        assertThat(chooseForAddWriteLatest(), contains(pkIndexId));

        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        int registeredIndexId = indexId(REGISTERED_INDEX_NAME);
        assertThat(chooseForAddWriteLatest(), contains(pkIndexId, registeredIndexId));

        createSimpleBuildingIndex(BUILDING_INDEX_NAME);
        int buildingIndexId = indexId(BUILDING_INDEX_NAME);
        assertThat(chooseForAddWriteLatest(), contains(pkIndexId, registeredIndexId, buildingIndexId));

        createSimpleAvailableIndex(AVAILABLE_INDEX_NAME);
        int availableIndexId = indexId(AVAILABLE_INDEX_NAME);
        assertThat(chooseForAddWriteLatest(), contains(pkIndexId, registeredIndexId, buildingIndexId, availableIndexId));

        createSimpleStoppingIndex(STOPPING_INDEX_NAME);
        int stoppingIndexId = indexId(STOPPING_INDEX_NAME);
        assertThat(chooseForAddWriteLatest(), contains(pkIndexId, registeredIndexId, buildingIndexId, availableIndexId, stoppingIndexId));
    }

    @Test
    void chooseForAddWriteWithSecondaryAndWithoutReadOnlyAndRegisteredIndexes() {
        HybridTimestamp beginTsBeforeCreateRegisteredIndex = clock.now();

        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);

        int pkIndexId = indexId(PK_INDEX_NAME);
        int registeredIndexId = indexId(REGISTERED_INDEX_NAME);

        assertThat(chooseForAddWriteLatest(beginTsBeforeCreateRegisteredIndex), contains(pkIndexId));

        int catalogVersionBeforeStartBuildingIndex = latestCatalogVersion();

        startBuildingIndex(catalogManager, registeredIndexId);

        assertThat(
                indexChooser.chooseForAddWrite(catalogVersionBeforeStartBuildingIndex, tableId(TABLE_NAME), clock.now()),
                contains(pkIndexId)
        );
    }

    @Test
    void chooseForAddWriteCommittedWithSecondaryAndWithoutReadOnlyIndexes() {
        int pkIndexId = indexId(PK_INDEX_NAME);
        assertThat(chooseForAddWriteCommittedLatest(), contains(pkIndexId));

        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        assertThat(chooseForAddWriteCommittedLatest(), contains(pkIndexId));

        createSimpleBuildingIndex(BUILDING_INDEX_NAME);
        int buildingIndexId = indexId(BUILDING_INDEX_NAME);
        assertThat(chooseForAddWriteCommittedLatest(), contains(pkIndexId, buildingIndexId));

        createSimpleAvailableIndex(AVAILABLE_INDEX_NAME);
        int availableIndexId = indexId(AVAILABLE_INDEX_NAME);
        assertThat(chooseForAddWriteCommittedLatest(), contains(pkIndexId, buildingIndexId, availableIndexId));

        createSimpleStoppingIndex(STOPPING_INDEX_NAME);
        int stoppingIndexId = indexId(STOPPING_INDEX_NAME);
        assertThat(chooseForAddWriteCommittedLatest(), contains(pkIndexId, buildingIndexId, availableIndexId, stoppingIndexId));
    }

    @ParameterizedTest(name = "recovery = {0}")
    @ValueSource(booleans = {false, true})
    void chooseForAddWriteCommittedWithSecondaryAndReadOnlyIndexes(boolean recovery) {
        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        createSimpleBuildingIndex(BUILDING_INDEX_NAME);

        HybridTimestamp commitTsBeforeStoppingIndex = clock.now();

        createSimpleStoppingIndex(READ_ONLY_INDEX_NAME);

        HybridTimestamp commitTsOnStoppingIndex = latestCatalogVersionActivationTs();

        int pkIndexId = indexId(PK_INDEX_NAME);
        int readOnlyIndexId = indexId(READ_ONLY_INDEX_NAME);

        dropIndex(REGISTERED_INDEX_NAME);
        dropIndex(BUILDING_INDEX_NAME);
        removeIndex(READ_ONLY_INDEX_NAME);

        if (recovery) {
            recoverIndexChooser();
        }

        assertThat(chooseForAddWriteCommittedLatest(commitTsBeforeStoppingIndex), contains(pkIndexId, readOnlyIndexId));
        assertThat(chooseForAddWriteCommittedLatest(commitTsOnStoppingIndex), contains(pkIndexId));
        assertThat(chooseForAddWriteCommittedLatest(clock.now()), contains(pkIndexId));
    }

    @ParameterizedTest(name = "recovery = {0}")
    @ValueSource(booleans = {false, true})
    void chooseForAddWriteWithSecondaryAndReadOnlyIndexes(boolean recovery) {
        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        createSimpleBuildingIndex(BUILDING_INDEX_NAME);

        HybridTimestamp beginTsBeforeStoppingIndex = clock.now();

        createSimpleStoppingIndex(READ_ONLY_INDEX_NAME);

        HybridTimestamp beginTsOnStoppingIndex = latestCatalogVersionActivationTs();

        int pkIndexId = indexId(PK_INDEX_NAME);
        int readOnlyIndexId = indexId(READ_ONLY_INDEX_NAME);

        dropIndex(REGISTERED_INDEX_NAME);
        dropIndex(BUILDING_INDEX_NAME);
        removeIndex(READ_ONLY_INDEX_NAME);

        if (recovery) {
            recoverIndexChooser();
        }

        assertThat(chooseForAddWriteLatest(beginTsBeforeStoppingIndex), contains(pkIndexId, readOnlyIndexId));
        assertThat(chooseForAddWriteLatest(beginTsOnStoppingIndex), contains(pkIndexId));
        assertThat(chooseForAddWriteLatest(clock.now()), contains(pkIndexId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void chooseForAddWriteCommittedForDroppedTable(boolean recovery) {
        HybridTimestamp commitTsBeforeCreateIndexes = clock.now();

        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        createSimpleBuildingIndex(BUILDING_INDEX_NAME);
        createSimpleAvailableIndex(AVAILABLE_INDEX_NAME);
        createSimpleStoppingIndex(STOPPING_INDEX_NAME);

        int pkIndexId = indexId(PK_INDEX_NAME);
        int availableIndexId = indexId(AVAILABLE_INDEX_NAME);
        int stoppingIndexId = indexId(STOPPING_INDEX_NAME);

        int tableId = tableId(TABLE_NAME);

        HybridTimestamp commitTsBeforeDropTable = clock.now();

        dropTable();

        if (recovery) {
            recoverIndexChooser();
        }

        assertThat(
                chooseForAddWriteCommittedLatest(tableId, commitTsBeforeCreateIndexes),
                contains(pkIndexId, availableIndexId, stoppingIndexId)
        );

        assertThat(
                chooseForAddWriteCommittedLatest(tableId, commitTsBeforeDropTable),
                contains(pkIndexId, availableIndexId)
        );

        assertThat(chooseForAddWriteCommittedLatest(tableId, clock.now()), empty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void chooseForAddWriteForDroppedTable(boolean recovery) {
        HybridTimestamp beginTsBeforeCreateIndexes = clock.now();

        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        createSimpleBuildingIndex(BUILDING_INDEX_NAME);
        createSimpleAvailableIndex(AVAILABLE_INDEX_NAME);
        createSimpleStoppingIndex(STOPPING_INDEX_NAME);

        int pkIndexId = indexId(PK_INDEX_NAME);
        int availableIndexId = indexId(AVAILABLE_INDEX_NAME);
        int stoppingIndexId = indexId(STOPPING_INDEX_NAME);

        int tableId = tableId(TABLE_NAME);

        HybridTimestamp beginTsBeforeDropTable = clock.now();

        dropTable();

        if (recovery) {
            recoverIndexChooser();
        }

        assertThat(
                chooseForAddWriteLatest(tableId, beginTsBeforeCreateIndexes),
                contains(pkIndexId, availableIndexId, stoppingIndexId)
        );

        assertThat(
                chooseForAddWriteLatest(tableId, beginTsBeforeDropTable),
                contains(pkIndexId, availableIndexId)
        );

        assertThat(chooseForAddWriteLatest(tableId, clock.now()), empty());
    }

    private List<Integer> chooseForAddWriteCommittedLatest(int tableId, HybridTimestamp commitTs) {
        return indexChooser.chooseForAddWriteCommitted(latestCatalogVersion(), tableId, commitTs);
    }

    private List<Integer> chooseForAddWriteCommittedLatest(HybridTimestamp commitTs) {
        return chooseForAddWriteCommittedLatest(tableId(TABLE_NAME), commitTs);
    }

    private List<Integer> chooseForAddWriteCommittedLatest() {
        return chooseForAddWriteCommittedLatest(HybridTimestamp.MAX_VALUE);
    }

    private List<Integer> chooseForAddWriteLatest(int tableId, HybridTimestamp beginTs) {
        return indexChooser.chooseForAddWrite(latestCatalogVersion(), tableId, beginTs);
    }

    private List<Integer> chooseForAddWriteLatest(HybridTimestamp beginTs) {
        return chooseForAddWriteLatest(tableId(TABLE_NAME), beginTs);
    }

    private List<Integer> chooseForAddWriteLatest() {
        return chooseForAddWriteLatest(HybridTimestamp.MAX_VALUE);
    }

    private void createSimpleRegisteredIndex(String indexName) {
        createSimpleHashIndex(catalogManager, TABLE_NAME, indexName);
    }

    private void createSimpleBuildingIndex(String indexName) {
        createSimpleHashIndex(catalogManager, TABLE_NAME, indexName);
        startBuildingIndex(catalogManager, indexId(indexName));
    }

    private void createSimpleAvailableIndex(String indexName) {
        createSimpleBuildingIndex(indexName);
        makeIndexAvailable(catalogManager, indexId(indexName));
    }

    private void createSimpleStoppingIndex(String indexName) {
        createSimpleAvailableIndex(indexName);
        dropIndex(indexName);
    }

    private void removeIndex(String indexName) {
        TableTestUtils.removeIndex(catalogManager, indexName);
    }

    private void dropIndex(String indexName) {
        TableTestUtils.dropIndex(catalogManager, DEFAULT_SCHEMA_NAME, indexName);
    }

    private int latestCatalogVersion() {
        return catalogManager.latestCatalogVersion();
    }

    private HybridTimestamp latestCatalogVersionActivationTs() {
        int catalogVersion = catalogManager.latestCatalogVersion();

        Catalog catalog = catalogManager.catalog(catalogVersion);

        assertNotNull(catalog, "catalogVersion=" + catalogVersion);

        return hybridTimestamp(catalog.time());
    }

    private int tableId(String tableName) {
        return getTableIdStrict(catalogManager, tableName, clock.nowLong());
    }

    private int indexId(String indexName) {
        return getIndexIdStrict(catalogManager, indexName, clock.nowLong());
    }

    private void recoverIndexChooser() {
        indexChooser.close();
        indexChooser = new FullStateTransferIndexChooser(catalogManager);
        indexChooser.start();
    }

    private void dropTable() {
        TableTestUtils.dropTable(catalogManager, DEFAULT_SCHEMA_NAME, TABLE_NAME);
    }
}
