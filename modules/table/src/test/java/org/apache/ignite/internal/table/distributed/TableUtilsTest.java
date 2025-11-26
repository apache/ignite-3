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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.pkIndexName;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.table.TableTestUtils.COLUMN_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.addColumnToSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.dropIndex;
import static org.apache.ignite.internal.table.TableTestUtils.dropSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.makeIndexAvailable;
import static org.apache.ignite.internal.table.TableTestUtils.removeIndex;
import static org.apache.ignite.internal.table.TableTestUtils.startBuildingIndex;
import static org.apache.ignite.internal.table.distributed.TableUtils.indexIdsAtRwTxBeginTs;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TransactionIds.transactionId;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link TableUtils} testing. */
public class TableUtilsTest extends IgniteAbstractTest {
    private final HybridClock clock = new HybridClockImpl();

    private final CatalogManager catalogManager = CatalogTestUtils.createCatalogManagerWithTestUpdateLog("test-node", clock);

    @BeforeEach
    void setUp() {
        assertThat(catalogManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(
                catalogManager::beforeNodeStop,
                () -> assertThat(catalogManager.stopAsync(new ComponentContext()), willCompleteSuccessfully())
        );
    }

    @Test
    void testIndexIdsAtRwTxBeginTs() {
        createSimpleTable(catalogManager, TABLE_NAME);

        String indexName0 = INDEX_NAME + 0;
        String indexName1 = INDEX_NAME + 1;
        String indexName2 = INDEX_NAME + 2;
        String indexName3 = INDEX_NAME + 3;
        String indexName4 = INDEX_NAME + 4;

        for (String indexName : List.of(indexName0, indexName1, indexName2, indexName3, indexName4)) {
            createSimpleHashIndex(catalogManager, TABLE_NAME, indexName);
        }

        int indexId0 = indexId(indexName0);
        int indexId1 = indexId(indexName1);
        int indexId2 = indexId(indexName2);
        int indexId3 = indexId(indexName3);
        int indexId4 = indexId(indexName4);

        for (String indexName : List.of(indexName1, indexName2, indexName3, indexName4)) {
            startBuildingIndex(catalogManager, indexId(indexName));
        }

        for (String indexName : List.of(indexName2, indexName3, indexName4)) {
            makeIndexAvailable(catalogManager, indexId(indexName));
        }

        for (String indexName : List.of(indexName3, indexName4)) {
            dropIndex(catalogManager, SqlCommon.DEFAULT_SCHEMA_NAME, indexName);
        }

        removeIndex(catalogManager, indexId4);

        CatalogManager spy = spy(catalogManager);

        HybridTimestamp beginTs = clock.now();

        int tableId = getTableIdStrict(catalogManager, TABLE_NAME, clock.nowLong());

        assertThat(
                indexIdsAtRwTxBeginTs(spy, transactionId(beginTs, 1), tableId),
                contains(
                        indexId(pkIndexName(TABLE_NAME)),
                        indexId0,
                        indexId1,
                        indexId2,
                        indexId3
                )
        );

        verify(spy).activeCatalog(eq(beginTs.longValue()));
    }

    private int indexId(String indexName) {
        return getIndexIdStrict(catalogManager, indexName, clock.nowLong());
    }

    private int tableId(String tableName) {
        return getTableIdStrict(catalogManager, tableName, clock.nowLong());
    }

    private HybridTimestamp catalogTime(int catalogVersion) {
        return hybridTimestamp(catalogManager.catalog(catalogVersion).time());
    }

    @Test
    void testAliveTables() {
        String tableName0 = TABLE_NAME + 0;
        String tableName1 = TABLE_NAME + 1;
        String tableName2 = TABLE_NAME + 2;

        createSimpleTable(catalogManager, tableName0);
        addColumnToSimpleTable(catalogManager, tableName0, COLUMN_NAME + 0, INT32);
        addColumnToSimpleTable(catalogManager, tableName0, COLUMN_NAME + 1, INT32);

        createSimpleTable(catalogManager, tableName1);
        addColumnToSimpleTable(catalogManager, tableName1, COLUMN_NAME + 2, INT32);

        createSimpleTable(catalogManager, tableName2);
        addColumnToSimpleTable(catalogManager, tableName1, COLUMN_NAME + 3, INT32);
        addColumnToSimpleTable(catalogManager, tableName1, COLUMN_NAME + 4, INT32);
        addColumnToSimpleTable(catalogManager, tableName1, COLUMN_NAME + 5, INT32);

        int tableId0 = tableId(tableName0);
        int tableId1 = tableId(tableName1);
        int tableId2 = tableId(tableName2);
        Set<Integer> allTableIds = Set.of(tableId0, tableId1, tableId2);

        int catalogVersionBeforeRemoveTable1 = catalogManager.latestCatalogVersion();
        dropSimpleTable(catalogManager, tableName1);

        int catalogVersionBeforeRemoveTable0 = catalogManager.latestCatalogVersion();
        dropSimpleTable(catalogManager, tableName0);

        assertThat(aliveTables(null), is(allTableIds));
        assertThat(aliveTables(catalogTime(catalogVersionBeforeRemoveTable1)), is(allTableIds));
        // Let's check that if the time is slightly different from the activation time, the result will be the same.
        assertThat(aliveTables(catalogTime(catalogVersionBeforeRemoveTable1).addPhysicalTime(1)), is(allTableIds));
        assertThat(aliveTables(catalogTime(catalogVersionBeforeRemoveTable0).addPhysicalTime(-1)), is(allTableIds));

        assertThat(
                aliveTables(catalogTime(catalogVersionBeforeRemoveTable0)),
                containsInAnyOrder(tableId0, tableId2)
        );

        assertThat(
                aliveTables(catalogTime(catalogVersionBeforeRemoveTable0).addPhysicalTime(1)),
                containsInAnyOrder(tableId0, tableId2)
        );

        int latestCatalogVersion = catalogManager.latestCatalogVersion();

        assertThat(
                aliveTables(catalogTime(latestCatalogVersion)),
                contains(tableId2)
        );

        assertThat(
                aliveTables(catalogTime(latestCatalogVersion).addPhysicalTime(1)),
                contains(tableId2)
        );
    }

    private Set<Integer> aliveTables(@Nullable HybridTimestamp lwm) {
        return TableUtils.aliveTables(catalogManager, lwm);
    }
}
