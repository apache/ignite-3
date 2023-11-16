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

package org.apache.ignite.internal.index;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.PK_INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createTable;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.indexId;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.tableId;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * For {@link IndexCollectorForUpdateOperation} testing.
 *
 * <p>In the description of the tests below catalog versions and index IDs are not correct and are presented for simplicity.</p>
 *
 * <p>Example of notation:</p>
 * <ul>
 *     <li>v1 - first version catalog.</li>
 *     <li>2(R) - registered index with ID 2.</li>
 *     <li>3(A) - available index with ID 3.</li>
 * </ul>
 */
public class IndexCollectorForUpdateOperationTest extends BaseIgniteAbstractTest {
    private final HybridClock clock = new HybridClockImpl();

    private CatalogManager catalogManager;

    private int tableId;

    private int catalogVersionAfterCreateTable;

    @BeforeEach
    void setUp() {
        catalogManager = CatalogTestUtils.createTestCatalogManager(NODE_NAME, clock);

        catalogManager.start();

        createTable(catalogManager, TABLE_NAME, COLUMN_NAME);

        tableId = tableId(catalogManager, TABLE_NAME, clock);

        catalogVersionAfterCreateTable = catalogManager.latestCatalogVersion();
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.stopAll(catalogManager);
    }

    /**
     * Checks the correctness of the selection of indexes for the partition update operation.
     *
     * <pre>
     *     v1 : 1(A)
     *     ---------
     *     res: 1(A)
     * </pre>
     */
    @Test
    void testAfterCreateTable() {
        int catalogVersion = catalogVersionAfterCreateTable;

        List<CatalogIndexDescriptor> indexes = collectIndexes(catalogVersion, catalogVersion);

        CatalogIndexDescriptor pkIndex = index(catalogVersion, PK_INDEX_NAME);

        assertThat(toIds(indexes), equalTo(toIds(pkIndex)));
        assertThat(indexes, contains(sameInstance(pkIndex)));
    }

    /**
     * Checks the correctness of the selection of indexes for the partition update operation.
     *
     * <pre>
     *     v2 : 1(A) 2(R)
     *     --------------
     *     res: 1(A) 2(R)
     * </pre>
     */
    @Test
    void testOnlyOneVersion() {
        createIndexes(INDEX_NAME);

        int catalogVersion = catalogManager.latestCatalogVersion();

        List<CatalogIndexDescriptor> indexes = collectIndexes(catalogVersion, catalogVersion);

        CatalogIndexDescriptor pkIndex = index(catalogVersion, PK_INDEX_NAME);
        CatalogIndexDescriptor index = index(catalogVersion, INDEX_NAME);

        assertThat(toIds(indexes), equalTo(toIds(pkIndex, index)));
        assertThat(indexes, contains(sameInstance(pkIndex), sameInstance(index)));
    }

    /**
     * Checks the correctness of the selection of indexes for the partition update operation.
     *
     * <pre>
     *     V1 : 1(A)
     *     v2 : 1(A) 2(R)
     *     --------------
     *     res: 1(A) 2(R)
     * </pre>
     */
    @Test
    void testTwoVersionWithOneRegisteredIndex() {
        createIndexes(INDEX_NAME);

        int catalogVersion = catalogManager.latestCatalogVersion();

        List<CatalogIndexDescriptor> indexes = collectIndexes(catalogVersionAfterCreateTable, catalogVersion);

        CatalogIndexDescriptor pkIndex = index(catalogVersion, PK_INDEX_NAME);
        CatalogIndexDescriptor index = index(catalogVersion, INDEX_NAME);

        assertThat(toIds(indexes), equalTo(toIds(pkIndex, index)));
        assertThat(indexes, contains(sameInstance(pkIndex), sameInstance(index)));
    }

    /**
     * Checks the correctness of the selection of indexes for the partition update operation.
     *
     * <pre>
     *     V2 : 1(A) 2(R)
     *     v3 : 1(A) 2(A)
     *     --------------
     *     res: 1(A) 2(A)
     * </pre>
     */
    @Test
    void testTwoVersionWithOneAvailableIndex() {
        createIndexes(INDEX_NAME);

        int catalogVersionBeforeMakeIndexAvailable = catalogManager.latestCatalogVersion();

        makeIndexAvailable(INDEX_NAME);

        int catalogVersionAfterMakeIndexAvailable = catalogManager.latestCatalogVersion();

        List<CatalogIndexDescriptor> indexes = collectIndexes(
                catalogVersionBeforeMakeIndexAvailable,
                catalogVersionAfterMakeIndexAvailable
        );

        CatalogIndexDescriptor pkIndex = index(catalogVersionAfterMakeIndexAvailable, PK_INDEX_NAME);
        CatalogIndexDescriptor index = index(catalogVersionAfterMakeIndexAvailable, INDEX_NAME);

        assertThat(toIds(indexes), equalTo(toIds(pkIndex, index)));
        assertThat(indexes, contains(sameInstance(pkIndex), sameInstance(index)));
    }

    /**
     * Checks the correctness of the selection of indexes for the partition update operation.
     *
     * <pre>
     *     V1 : 1(A)
     *     V2 : 1(A) 2(R)
     *     v3 : 1(A) 2(A)
     *     --------------
     *     res: 1(A) 2(A)
     * </pre>
     */
    @Test
    void testThreeVersionWithOneAvailableIndex() {
        createIndexes(INDEX_NAME);
        makeIndexAvailable(INDEX_NAME);

        int catalogVersion = catalogManager.latestCatalogVersion();

        List<CatalogIndexDescriptor> indexes = collectIndexes(catalogVersionAfterCreateTable, catalogVersion);

        CatalogIndexDescriptor pkIndex = index(catalogVersion, PK_INDEX_NAME);
        CatalogIndexDescriptor index = index(catalogVersion, INDEX_NAME);

        assertThat(toIds(indexes), equalTo(toIds(pkIndex, index)));
        assertThat(indexes, contains(sameInstance(pkIndex), sameInstance(index)));
    }

    /**
     * Checks the correctness of the selection of indexes for the partition update operation.
     *
     * <pre>
     *     V1 : 1(A)
     *     V2 : 1(A) 2(R)
     *     v3 : 1(A)
     *     --------------
     *     res: 1(A)
     * </pre>
     */
    @Test
    void testRemoveRegisteredIndex() {
        createIndexes(INDEX_NAME);
        dropIndex(INDEX_NAME);

        int catalogVersion = catalogManager.latestCatalogVersion();

        List<CatalogIndexDescriptor> indexes = collectIndexes(catalogVersionAfterCreateTable, catalogVersion);

        CatalogIndexDescriptor pkIndex = index(catalogVersion, PK_INDEX_NAME);

        assertThat(toIds(indexes), equalTo(toIds(pkIndex)));
        assertThat(indexes, contains(sameInstance(pkIndex)));
    }

    /**
     * Checks the correctness of the selection of indexes for the partition update operation.
     *
     * <pre>
     *     V1 : 1(A)
     *     V2 : 1(A) 2(R)
     *     V3 : 1(A) 2(A)
     *     v4 : 1(A)
     *     --------------
     *     res: 1(A) 2(A)
     * </pre>
     */
    @Test
    void testRemoveAvailableIndex() {
        createIndexes(INDEX_NAME);
        makeIndexAvailable(INDEX_NAME);

        int catalogVersionAfterMakeIndexAvailable = catalogManager.latestCatalogVersion();

        dropIndex(INDEX_NAME);

        int catalogVersion = catalogManager.latestCatalogVersion();

        List<CatalogIndexDescriptor> indexes = collectIndexes(catalogVersionAfterCreateTable, catalogVersion);

        CatalogIndexDescriptor pkIndex = index(catalogVersion, PK_INDEX_NAME);
        CatalogIndexDescriptor index = index(catalogVersionAfterMakeIndexAvailable, INDEX_NAME);

        assertThat(toIds(indexes), equalTo(toIds(pkIndex, index)));
        assertThat(indexes, contains(sameInstance(pkIndex), sameInstance(index)));
    }

    /**
     * Checks the correctness of the selection of indexes for the partition update operation.
     *
     * <pre>
     *     V1 : 1(A)
     *     V2 : 1(A) 2(R) 3(R)
     *     V3 : 1(A)           4(R)
     *     v4 : 1(A)           4(A) 5(R)
     *     v5 : 1(A)           4(A)      6(R)
     *     v6 : 1(A)                     6(A) 7(R)
     *     --------------
     *     res: 1(A) 4(A) 6(A) 7(R)
     * </pre>
     */
    @Test
    void testComplexCase() {
        // Names have been chosen to make it easier to match with description.
        String indexName2 = INDEX_NAME + 2;
        String indexName3 = INDEX_NAME + 3;
        String indexName4 = INDEX_NAME + 4;
        String indexName5 = INDEX_NAME + 5;
        String indexName6 = INDEX_NAME + 6;
        String indexName7 = INDEX_NAME + 7;

        // V2 : 1(A) 2(R) 3(R)
        createIndexes(indexName2, indexName3);

        // V3 : 1(A)           4(R)
        executeCatalogCommands(toDropIndexCommand(indexName2), toDropIndexCommand(indexName3), toCreateHashIndexCommand(indexName4));

        // v4 : 1(A)           4(A) 5(R)
        executeCatalogCommands(toMakeAvailableIndexCommand(indexName4), toCreateHashIndexCommand(indexName5));

        // v5 : 1(A)           4(A)      6(R)
        executeCatalogCommands(toDropIndexCommand(indexName5), toCreateHashIndexCommand(indexName6));

        // v6 : 1(A)                     6(A) 7(R)
        executeCatalogCommands(
                toDropIndexCommand(indexName4),
                toMakeAvailableIndexCommand(indexName6),
                toCreateHashIndexCommand(indexName7)
        );

        int latestCatalogVersion = catalogManager.latestCatalogVersion();

        List<CatalogIndexDescriptor> indexes = collectIndexes(catalogVersionAfterCreateTable, latestCatalogVersion);

        CatalogIndexDescriptor pkIndex = index(latestCatalogVersion, PK_INDEX_NAME);
        CatalogIndexDescriptor index4 = index(latestCatalogVersion - 1, indexName4);
        CatalogIndexDescriptor index6 = index(latestCatalogVersion, indexName6);
        CatalogIndexDescriptor index7 = index(latestCatalogVersion, indexName7);

        assertThat(toIds(indexes), equalTo(toIds(pkIndex, index4, index6, index7)));
        assertThat(indexes, contains(sameInstance(pkIndex), sameInstance(index4), sameInstance(index6), sameInstance(index7)));
    }

    private void executeCatalogCommands(CatalogCommand... commands) {
        executeCatalogCommands(List.of(commands));
    }

    private void executeCatalogCommands(List<CatalogCommand> commands) {
        assertThat(catalogManager.execute(commands), willCompleteSuccessfully());
    }

    private List<CatalogIndexDescriptor> collectIndexes(int catalogVersionFrom, int catalogVersionTo) {
        return new IndexCollectorForUpdateOperation(catalogManager).collect(tableId, catalogVersionFrom, catalogVersionTo);
    }

    private void createIndexes(String... indexNames) {
        List<CatalogCommand> createHashIndexCommands = Arrays.stream(indexNames)
                .map(IndexCollectorForUpdateOperationTest::toCreateHashIndexCommand)
                .collect(toList());

        executeCatalogCommands(createHashIndexCommands);
    }

    private void makeIndexAvailable(String indexName) {
        int indexId = TestIndexManagementUtils.indexId(catalogManager, indexName, clock);

        TestIndexManagementUtils.makeIndexAvailable(catalogManager, indexId);
    }

    private void dropIndex(String indexName) {
        TableTestUtils.dropIndex(catalogManager, DEFAULT_SCHEMA_NAME, indexName);
    }

    private CatalogIndexDescriptor index(int catalogVersion, String indexName) {
        return catalogManager.indexes(catalogVersion, tableId).stream().filter(index -> indexName.equals(index.name())).findFirst().get();
    }

    private static List<Integer> toIds(CatalogObjectDescriptor... indexes) {
        return nullOrEmpty(indexes) ? List.of() : toIds(List.of(indexes));
    }

    private static List<Integer> toIds(Collection<? extends CatalogObjectDescriptor> indexes) {
        return indexes.stream().map(CatalogObjectDescriptor::id).collect(toList());
    }

    private static CatalogCommand toCreateHashIndexCommand(String indexName) {
        return CreateHashIndexCommand.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .tableName(TABLE_NAME)
                .indexName(indexName)
                .columns(List.of(COLUMN_NAME))
                .unique(false)
                .build();
    }

    private static CatalogCommand toDropIndexCommand(String indexName) {
        return DropIndexCommand.builder()
                .schemaName(DEFAULT_SCHEMA_NAME)
                .indexName(indexName)
                .build();
    }

    private CatalogCommand toMakeAvailableIndexCommand(String indexName) {
        int indexId = indexId(catalogManager, indexName, clock);

        return MakeIndexAvailableCommand.builder()
                .indexId(indexId)
                .build();
    }
}
