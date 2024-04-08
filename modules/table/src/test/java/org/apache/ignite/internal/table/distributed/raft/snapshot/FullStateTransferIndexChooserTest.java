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
import static org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor.INITIAL_TABLE_VERSION;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.table.TableTestUtils.COLUMN_NAME;
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
import static org.apache.ignite.internal.util.CollectionUtils.view;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnCommand;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
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

    private final TestLowWatermark lowWatermark = new TestLowWatermark();

    private FullStateTransferIndexChooser indexChooser;

    @BeforeEach
    void setUp() {
        catalogManager = CatalogTestUtils.createTestCatalogManager("test", clock);

        indexChooser = new FullStateTransferIndexChooser(catalogManager, lowWatermark);

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
        assertThat(indexIds(chooseForAddWriteLatest()), contains(pkIndexId));

        int registeredIndexId = createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        assertThat(indexIds(chooseForAddWriteLatest()), contains(pkIndexId, registeredIndexId));

        int buildingIndexId = createSimpleBuildingIndex(BUILDING_INDEX_NAME);
        assertThat(indexIds(chooseForAddWriteLatest()), contains(pkIndexId, registeredIndexId, buildingIndexId));

        int availableIndexId = createSimpleAvailableIndex(AVAILABLE_INDEX_NAME);
        assertThat(indexIds(chooseForAddWriteLatest()), contains(pkIndexId, registeredIndexId, buildingIndexId, availableIndexId));

        int stoppingIndexId = createSimpleStoppingIndex(STOPPING_INDEX_NAME);
        assertThat(
                indexIds(chooseForAddWriteLatest()),
                contains(pkIndexId, registeredIndexId, buildingIndexId, availableIndexId, stoppingIndexId)
        );
    }

    @Test
    void chooseForAddWriteWithSecondaryAndWithoutReadOnlyAndRegisteredIndexes() {
        HybridTimestamp beginTsBeforeCreateRegisteredIndex = clock.now();

        int registeredIndexId = createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);

        int pkIndexId = indexId(PK_INDEX_NAME);

        assertThat(indexIds(chooseForAddWriteLatest(beginTsBeforeCreateRegisteredIndex)), contains(pkIndexId));

        int catalogVersionBeforeStartBuildingIndex = latestCatalogVersion();

        startBuildingIndex(catalogManager, registeredIndexId);

        assertThat(
                indexIds(indexChooser.chooseForAddWrite(catalogVersionBeforeStartBuildingIndex, tableId(TABLE_NAME), clock.now())),
                contains(pkIndexId)
        );
    }

    @Test
    void chooseForAddWriteCommittedWithSecondaryAndWithoutReadOnlyIndexes() {
        int pkIndexId = indexId(PK_INDEX_NAME);
        assertThat(indexIds(chooseForAddWriteCommittedLatest()), contains(pkIndexId));

        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        assertThat(indexIds(chooseForAddWriteCommittedLatest()), contains(pkIndexId));

        int buildingIndexId = createSimpleBuildingIndex(BUILDING_INDEX_NAME);
        assertThat(indexIds(chooseForAddWriteCommittedLatest()), contains(pkIndexId, buildingIndexId));

        int availableIndexId = createSimpleAvailableIndex(AVAILABLE_INDEX_NAME);
        assertThat(indexIds(chooseForAddWriteCommittedLatest()), contains(pkIndexId, buildingIndexId, availableIndexId));

        int stoppingIndexId = createSimpleStoppingIndex(STOPPING_INDEX_NAME);
        assertThat(indexIds(chooseForAddWriteCommittedLatest()), contains(pkIndexId, buildingIndexId, availableIndexId, stoppingIndexId));
    }

    @ParameterizedTest(name = "recovery = {0}")
    @ValueSource(booleans = {false, true})
    void chooseForAddWriteCommittedWithSecondaryAndReadOnlyIndexes(boolean recovery) {
        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        createSimpleBuildingIndex(BUILDING_INDEX_NAME);

        HybridTimestamp commitTsBeforeStoppingIndex = clock.now();

        int readOnlyIndexId = createSimpleStoppingIndex(READ_ONLY_INDEX_NAME);

        HybridTimestamp commitTsOnStoppingIndex = latestCatalogVersionActivationTs();

        int pkIndexId = indexId(PK_INDEX_NAME);

        dropIndex(REGISTERED_INDEX_NAME);
        dropIndex(BUILDING_INDEX_NAME);
        removeIndex(readOnlyIndexId);

        if (recovery) {
            recoverIndexChooser();
        }

        assertThat(indexIds(chooseForAddWriteCommittedLatest(commitTsBeforeStoppingIndex)), contains(pkIndexId, readOnlyIndexId));
        assertThat(indexIds(chooseForAddWriteCommittedLatest(commitTsOnStoppingIndex)), contains(pkIndexId));
        assertThat(indexIds(chooseForAddWriteCommittedLatest(clock.now())), contains(pkIndexId));
    }

    @ParameterizedTest(name = "recovery = {0}")
    @ValueSource(booleans = {false, true})
    void chooseForAddWriteWithSecondaryAndReadOnlyIndexes(boolean recovery) {
        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        createSimpleBuildingIndex(BUILDING_INDEX_NAME);

        HybridTimestamp beginTsBeforeStoppingIndex = clock.now();

        int readOnlyIndexId = createSimpleStoppingIndex(READ_ONLY_INDEX_NAME);

        HybridTimestamp beginTsOnStoppingIndex = latestCatalogVersionActivationTs();

        int pkIndexId = indexId(PK_INDEX_NAME);

        dropIndex(REGISTERED_INDEX_NAME);
        dropIndex(BUILDING_INDEX_NAME);
        removeIndex(readOnlyIndexId);

        if (recovery) {
            recoverIndexChooser();
        }

        assertThat(indexIds(chooseForAddWriteLatest(beginTsBeforeStoppingIndex)), contains(pkIndexId, readOnlyIndexId));
        assertThat(indexIds(chooseForAddWriteLatest(beginTsOnStoppingIndex)), contains(pkIndexId));
        assertThat(indexIds(chooseForAddWriteLatest(clock.now())), contains(pkIndexId));
    }

    @ParameterizedTest(name = "recovery = {0}")
    @ValueSource(booleans = {false, true})
    void chooseForAddWriteCommittedForDroppedTable(boolean recovery) {
        HybridTimestamp commitTsBeforeCreateIndexes = clock.now();

        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        createSimpleBuildingIndex(BUILDING_INDEX_NAME);
        int availableIndexId = createSimpleAvailableIndex(AVAILABLE_INDEX_NAME);
        int stoppingIndexId = createSimpleStoppingIndex(STOPPING_INDEX_NAME);

        int pkIndexId = indexId(PK_INDEX_NAME);

        int tableId = tableId(TABLE_NAME);

        HybridTimestamp commitTsBeforeDropTable = clock.now();

        dropTable();

        if (recovery) {
            recoverIndexChooser();
        }

        assertThat(
                indexIds(chooseForAddWriteCommittedLatest(tableId, commitTsBeforeCreateIndexes)),
                contains(pkIndexId, availableIndexId, stoppingIndexId)
        );

        assertThat(
                indexIds(chooseForAddWriteCommittedLatest(tableId, commitTsBeforeDropTable)),
                contains(pkIndexId, availableIndexId)
        );

        assertThat(indexIds(chooseForAddWriteCommittedLatest(tableId, clock.now())), empty());
    }

    @ParameterizedTest(name = "recovery = {0}")
    @ValueSource(booleans = {false, true})
    void chooseForAddWriteForDroppedTable(boolean recovery) {
        HybridTimestamp beginTsBeforeCreateIndexes = clock.now();

        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        createSimpleBuildingIndex(BUILDING_INDEX_NAME);
        int availableIndexId = createSimpleAvailableIndex(AVAILABLE_INDEX_NAME);
        int stoppingIndexId = createSimpleStoppingIndex(STOPPING_INDEX_NAME);

        int pkIndexId = indexId(PK_INDEX_NAME);

        int tableId = tableId(TABLE_NAME);

        HybridTimestamp beginTsBeforeDropTable = clock.now();

        dropTable();

        if (recovery) {
            recoverIndexChooser();
        }

        assertThat(
                indexIds(chooseForAddWriteLatest(tableId, beginTsBeforeCreateIndexes)),
                contains(pkIndexId, availableIndexId, stoppingIndexId)
        );

        assertThat(
                indexIds(chooseForAddWriteLatest(tableId, beginTsBeforeDropTable)),
                contains(pkIndexId, availableIndexId)
        );

        assertThat(indexIds(chooseForAddWriteLatest(tableId, clock.now())), empty());
    }

    @ParameterizedTest(name = "recovery = {0}")
    @ValueSource(booleans = {false, true})
    void chooseWithOtherTableVersionsWithoutReadOnlyIndexes(boolean recovery) {
        addTableColumn(COLUMN_NAME + "_NEW");

        createSimpleAvailableIndex(AVAILABLE_INDEX_NAME);

        var pkIndexIdAndTableVersion = new IndexIdAndTableVersion(indexId(PK_INDEX_NAME), INITIAL_TABLE_VERSION);
        var availableIndexIdAndTableVersion = new IndexIdAndTableVersion(indexId(AVAILABLE_INDEX_NAME), INITIAL_TABLE_VERSION + 1);

        if (recovery) {
            recoverIndexChooser();
        }

        assertThat(chooseForAddWriteLatest(), contains(pkIndexIdAndTableVersion, availableIndexIdAndTableVersion));
        assertThat(chooseForAddWriteCommittedLatest(), contains(pkIndexIdAndTableVersion, availableIndexIdAndTableVersion));
    }

    @ParameterizedTest(name = "recovery = {0}")
    @ValueSource(booleans = {false, true})
    void chooseWithOtherTableVersionsWithReadOnlyIndexes(boolean recovery) {
        addTableColumn(COLUMN_NAME + "_NEW");

        int readOnlyIndexId = createSimpleStoppingIndex(READ_ONLY_INDEX_NAME);

        var pkIndexIdAndTableVersion = new IndexIdAndTableVersion(indexId(PK_INDEX_NAME), INITIAL_TABLE_VERSION);
        var readOnlyIndexIdAndTableVersion = new IndexIdAndTableVersion(readOnlyIndexId, INITIAL_TABLE_VERSION + 1);

        if (recovery) {
            recoverIndexChooser();
        }

        assertThat(chooseForAddWriteLatest(), contains(pkIndexIdAndTableVersion, readOnlyIndexIdAndTableVersion));
        assertThat(chooseForAddWriteCommittedLatest(), contains(pkIndexIdAndTableVersion, readOnlyIndexIdAndTableVersion));
    }

    @ParameterizedTest(name = "recovery = {0}")
    @ValueSource(booleans = {false, true})
    void chooseWithRemovedNotAvailableIndexes(boolean recovery) {
        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        createSimpleBuildingIndex(BUILDING_INDEX_NAME);

        int catalogVersionBeforeRemoveIndexes = catalogManager.latestCatalogVersion();

        dropIndex(REGISTERED_INDEX_NAME);
        dropIndex(BUILDING_INDEX_NAME);

        if (recovery) {
            recoverIndexChooser();
        }

        int pkIndexId = indexId(PK_INDEX_NAME);

        assertThat(indexIds(chooseForAddWriteLatest()), contains(pkIndexId));
        assertThat(indexIds(chooseForAddWriteCommittedLatest()), contains(pkIndexId));

        int tableId = tableId(TABLE_NAME);

        assertThat(
                indexIds(indexChooser.chooseForAddWrite(catalogVersionBeforeRemoveIndexes, tableId, HybridTimestamp.MAX_VALUE)),
                contains(pkIndexId)
        );

        assertThat(
                indexIds(indexChooser.chooseForAddWriteCommitted(catalogVersionBeforeRemoveIndexes, tableId, HybridTimestamp.MAX_VALUE)),
                contains(pkIndexId)
        );
    }

    @ParameterizedTest(name = "recovery = {0}")
    @ValueSource(booleans = {false, true})
    void chooseWithUpdateLowWatermark(boolean recovery) {
        createSimpleRegisteredIndex(REGISTERED_INDEX_NAME);
        createSimpleBuildingIndex(BUILDING_INDEX_NAME);
        createSimpleAvailableIndex(AVAILABLE_INDEX_NAME);

        int availableIndexId = indexId(AVAILABLE_INDEX_NAME);

        dropIndex(AVAILABLE_INDEX_NAME);
        removeIndex(availableIndexId);

        assertThat(lowWatermark.updateAndNotify(clock.now()), willCompleteSuccessfully());

        if (recovery) {
            recoverIndexChooser();
        }

        int pkIndexId = indexId(PK_INDEX_NAME);
        int registeredIndexId = indexId(REGISTERED_INDEX_NAME);
        int buildingIndexId = indexId(BUILDING_INDEX_NAME);

        // Let's make sure that there will be no read-only indexes.
        assertThat(indexIds(chooseForAddWriteLatest()), contains(pkIndexId, registeredIndexId, buildingIndexId));
        assertThat(indexIds(chooseForAddWriteCommittedLatest()), contains(pkIndexId, buildingIndexId));
    }

    private List<IndexIdAndTableVersion> chooseForAddWriteCommittedLatest(int tableId, HybridTimestamp commitTs) {
        return indexChooser.chooseForAddWriteCommitted(latestCatalogVersion(), tableId, commitTs);
    }

    private List<IndexIdAndTableVersion> chooseForAddWriteCommittedLatest(HybridTimestamp commitTs) {
        return chooseForAddWriteCommittedLatest(tableId(TABLE_NAME), commitTs);
    }

    private List<IndexIdAndTableVersion> chooseForAddWriteCommittedLatest() {
        return chooseForAddWriteCommittedLatest(HybridTimestamp.MAX_VALUE);
    }

    private List<IndexIdAndTableVersion> chooseForAddWriteLatest(int tableId, HybridTimestamp beginTs) {
        return indexChooser.chooseForAddWrite(latestCatalogVersion(), tableId, beginTs);
    }

    private List<IndexIdAndTableVersion> chooseForAddWriteLatest(HybridTimestamp beginTs) {
        return chooseForAddWriteLatest(tableId(TABLE_NAME), beginTs);
    }

    private List<IndexIdAndTableVersion> chooseForAddWriteLatest() {
        return chooseForAddWriteLatest(HybridTimestamp.MAX_VALUE);
    }

    private int createSimpleRegisteredIndex(String indexName) {
        createSimpleHashIndex(catalogManager, TABLE_NAME, indexName);

        return indexId(indexName);
    }

    private int createSimpleBuildingIndex(String indexName) {
        int indexId = createSimpleRegisteredIndex(indexName);

        startBuildingIndex(catalogManager, indexId);

        return indexId;
    }

    private int createSimpleAvailableIndex(String indexName) {
        int indexId = createSimpleBuildingIndex(indexName);

        makeIndexAvailable(catalogManager, indexId);

        return indexId;
    }

    private int createSimpleStoppingIndex(String indexName) {
        int indexId = createSimpleAvailableIndex(indexName);

        dropIndex(indexName);

        return indexId;
    }

    private void removeIndex(int indexId) {
        TableTestUtils.removeIndex(catalogManager, indexId);
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
        indexChooser = new FullStateTransferIndexChooser(catalogManager, lowWatermark);
        indexChooser.start();
    }

    private void dropTable() {
        TableTestUtils.dropTable(catalogManager, DEFAULT_SCHEMA_NAME, TABLE_NAME);
    }

    private static List<Integer> indexIds(List<IndexIdAndTableVersion> indexIdAndTableVersionList) {
        return view(indexIdAndTableVersionList, IndexIdAndTableVersion::indexId);
    }

    private void addTableColumn(String columnName) {
        ColumnParams columnParams = ColumnParams.builder()
                .name(columnName)
                .type(INT32)
                .defaultValue(DefaultValue.constant(100500))
                .build();

        CatalogCommand command = AlterTableAddColumnCommand.builder()
                .tableName(TABLE_NAME)
                .schemaName(DEFAULT_SCHEMA_NAME)
                .columns(List.of(columnParams))
                .build();

        assertThat(catalogManager.execute(command), willCompleteSuccessfully());
    }
}
