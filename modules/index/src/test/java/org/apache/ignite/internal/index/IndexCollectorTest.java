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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.CatalogTestUtils.createTestCatalogManager;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.COLUMN_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.NODE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.PK_INDEX_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.createTable;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.indexId;
import static org.apache.ignite.internal.index.TestIndexManagementUtils.tableId;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.MakeIndexAvailableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** For {@link IndexCollector} testing. */
public class IndexCollectorTest extends BaseIgniteAbstractTest {
    private final HybridClock clock = new HybridClockImpl();

    private final CatalogManager catalogManager = createTestCatalogManager(NODE_NAME, clock);

    private IndexCollector indexCollector = new IndexCollector(catalogManager);

    private int tableId;

    private int catalogVersionAfterCreateTable;

    @BeforeEach
    void setUp() {
        catalogManager.start();

        createTable(catalogManager, TABLE_NAME, COLUMN_NAME);

        tableId = tableId(catalogManager, TABLE_NAME, clock);

        catalogVersionAfterCreateTable = catalogManager.latestCatalogVersion();
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(catalogManager::stop, indexCollector::close);
    }

    @ParameterizedTest(name = "withRecovery = {0}")
    @ValueSource(booleans = {false, true})
    void testCollectForRwTxOperationAfterCreateTable(boolean withRecovery) {
        int catalogVersion = catalogVersionAfterCreateTable;

        if (withRecovery) {
            recoverIndexCollector();
        }

        assertThat(
                collectForRwTxOperation(catalogVersion),
                contains(index(catalogVersion, PK_INDEX_NAME))
        );
    }

    @ParameterizedTest(name = "withRecovery = {0}")
    @ValueSource(booleans = {false, true})
    void testCollectForRwTxOperationAfterCreateIndex(boolean withRecovery) {
        createIndex(INDEX_NAME);

        int catalogVersion = catalogManager.latestCatalogVersion();

        if (withRecovery) {
            recoverIndexCollector();
        }

        assertThat(
                collectForRwTxOperation(catalogVersion),
                contains(index(catalogVersion, PK_INDEX_NAME), index(catalogVersion, INDEX_NAME))
        );
    }

    @ParameterizedTest(name = "withRecovery = {0}")
    @ValueSource(booleans = {false, true})
    void testCollectForRwTxOperationAfterMakeIndexAvailable(boolean withRecovery) {
        createIndex(INDEX_NAME);
        makeIndexAvailable(INDEX_NAME);

        int catalogVersion = catalogManager.latestCatalogVersion();

        if (withRecovery) {
            recoverIndexCollector();
        }

        assertThat(
                collectForRwTxOperation(catalogVersion),
                contains(index(catalogVersion, PK_INDEX_NAME), index(catalogVersion, INDEX_NAME))
        );
    }

    @ParameterizedTest(name = "withRecovery = {0}")
    @ValueSource(booleans = {false, true})
    void testCollectForRwTxOperationAfterDropRegisteredIndex(boolean withRecovery) {
        createIndex(INDEX_NAME);
        dropIndex(INDEX_NAME);

        int catalogVersion = catalogManager.latestCatalogVersion();

        if (withRecovery) {
            recoverIndexCollector();
        }

        assertThat(
                collectForRwTxOperation(catalogManager.latestCatalogVersion()),
                contains(index(catalogVersion, PK_INDEX_NAME))
        );
    }

    @ParameterizedTest(name = "withRecovery = {0}")
    @ValueSource(booleans = {false, true})
    void testCollectForRwTxOperationAfterDropAvailableIndex(boolean withRecovery) {
        createIndex(INDEX_NAME);
        makeIndexAvailable(INDEX_NAME);

        int catalogVersionAfterMakeIndexAvailable = catalogManager.latestCatalogVersion();

        dropIndex(INDEX_NAME);

        int catalogVersion = catalogManager.latestCatalogVersion();

        if (withRecovery) {
            recoverIndexCollector();
        }

        assertThat(
                collectForRwTxOperation(catalogVersion),
                contains(index(catalogVersion, PK_INDEX_NAME), index(catalogVersionAfterMakeIndexAvailable, INDEX_NAME))
        );
    }

    @ParameterizedTest(name = "withRecovery = {0}")
    @ValueSource(booleans = {false, true})
    void testCollectForRwTxOperationComplexCase(boolean withRecovery) {
        String indexName1 = INDEX_NAME + 1;
        String indexName2 = INDEX_NAME + 2;
        String indexName3 = INDEX_NAME + 3;
        String indexName4 = INDEX_NAME + 4;
        String indexName5 = INDEX_NAME + 4;

        // after execute: I0(A) I1(R) I2(R)
        executeCatalogCommands(toCreateHashIndexCommand(indexName1), toCreateHashIndexCommand(indexName2));

        // after execute: I0(A) I1(A)
        executeCatalogCommands(toMakeAvailableIndexCommand(indexName1), toDropIndexCommand(indexName2));

        // after execute: I0(A) I1(A) I3(R) I4(R)
        executeCatalogCommands(toCreateHashIndexCommand(indexName3), toCreateHashIndexCommand(indexName4));

        // after execute: I0(A) I1(A) I3(A) I4(A)
        executeCatalogCommands(toMakeAvailableIndexCommand(indexName3), toMakeAvailableIndexCommand(indexName4));

        int catalogVersionBeforeDropIndex3And4 = catalogManager.latestCatalogVersion();

        // after execute: I0(A) I1(A)
        executeCatalogCommands(toDropIndexCommand(indexName4), toDropIndexCommand(indexName3));

        int catalogVersionBeforeDropIndex1 = catalogManager.latestCatalogVersion();

        // after execute: I0(A)
        executeCatalogCommands(toDropIndexCommand(indexName1));

        // after execute: I0(A) I5(R)
        executeCatalogCommands(toCreateHashIndexCommand(indexName5));

        // Let's check.
        int catalogVersion = catalogManager.latestCatalogVersion();

        if (withRecovery) {
            recoverIndexCollector();
        }

        assertThat(
                collectForRwTxOperation(catalogVersion),
                contains(
                        index(catalogVersion, PK_INDEX_NAME),                   // Alive available index0 (pk)
                        index(catalogVersionBeforeDropIndex1, indexName1),      // Dropped available index1
                        index(catalogVersionBeforeDropIndex3And4, indexName3),  // Dropped available index3
                        index(catalogVersionBeforeDropIndex3And4, indexName4),  // Dropped available index4
                        index(catalogVersion, indexName5)                       // Alive registered index5
                )
        );
    }

    private void createIndex(String indexName) {
        TestIndexManagementUtils.createIndex(catalogManager, TABLE_NAME, indexName, COLUMN_NAME);
    }

    private void makeIndexAvailable(String indexName) {
        int indexId = indexId(catalogManager, indexName, clock);

        TestIndexManagementUtils.makeIndexAvailable(catalogManager, indexId);
    }

    private void dropIndex(String indexName) {
        TestIndexManagementUtils.dropIndex(catalogManager, indexName);
    }

    private List<CatalogIndexDescriptor> collectForRwTxOperation(int catalogVersion) {
        return indexCollector.collectForRwTxOperation(catalogVersion, tableId);
    }

    private void executeCatalogCommands(CatalogCommand... commands) {
        assertThat(catalogManager.execute(List.of(commands)), willCompleteSuccessfully());
    }

    private CatalogIndexDescriptor index(int catalogVersion, String indexName) {
        CatalogIndexDescriptor res = catalogManager.indexes(catalogVersion, tableId).stream()
                .filter(index -> indexName.equals(index.name()))
                .findFirst()
                .orElse(null);

        assertNotNull(res, "catalogVersion=" + catalogVersion + ", indexName=" + indexName);

        return res;
    }

    private CatalogCommand toMakeAvailableIndexCommand(String indexName) {
        int indexId = indexId(catalogManager, indexName, clock);

        return MakeIndexAvailableCommand.builder()
                .indexId(indexId)
                .build();
    }

    private void recoverIndexCollector() {
        indexCollector.close();

        indexCollector = new IndexCollector(catalogManager);

        indexCollector.recover();
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
}
