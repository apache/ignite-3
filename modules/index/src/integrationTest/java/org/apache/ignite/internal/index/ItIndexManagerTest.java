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
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** For {@link IndexManager} testing. */
public class ItIndexManagerTest extends ClusterPerClassIntegrationTest {
    private static final String ZONE_NAME = "ZONE_TABLE";

    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String INDEX_NAME = "TEST_INDEX";

    @AfterEach
    void tearDown() {
        if (node() != null) {
            sql("DROP TABLE IF EXISTS " + TABLE_NAME);
            sql("DROP ZONE IF EXISTS " + ZONE_NAME);
        }
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testStartActiveAndDroppedIndexOnNodeRecovery() throws Exception {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1);

        insertPeople(TABLE_NAME, createPeopleBatch(100));

        String indexName0 = INDEX_NAME + 0;
        String indexName1 = INDEX_NAME + 1;

        createIndex(TABLE_NAME, indexName0, "NAME");
        createIndex(TABLE_NAME, indexName1, "SALARY");

        awaitIndexesBecomeAvailable(node(), indexName0, indexName1);

        dropIndex(indexName1);

        CLUSTER.restartNode(0);

        TableImpl tableImpl = getTableImpl(node(), TABLE_NAME);

        assertThat(
                collectIndexIdsFromTable(tableImpl, 0),
                equalTo(collectIndexIdsFromCatalogForRecovery(node(), tableImpl))
        );
    }

    private static IgniteImpl node() {
        return unwrapIgniteImpl(CLUSTER.node(0));
    }

    private static Person[] createPeopleBatch(int batchSize) {
        return IntStream.range(0, batchSize)
                .mapToObj(personId -> new Person(personId, "person" + personId, 10.0 + personId))
                .toArray(Person[]::new);
    }

    private static TableImpl getTableImpl(IgniteImpl ignite, String tableName) {
        // IgniteTables#table is not used because under the hood CompletableFuture#join is used to NOT freeze the test using an async call.
        CompletableFuture<Table> tableFuture = ignite.tables().tableAsync(tableName);

        assertThat(tableFuture, willCompleteSuccessfully());

        return unwrapTableImpl(tableFuture.join());
    }

    private static List<Integer> collectIndexIdsFromTable(TableImpl table, int partitionId) {
        // Under the hood, TableIndexStoragesSupplier#get uses CompletableFuture#join for all indexes to NOT freeze the test using an
        // asynchronous call.
        CompletableFuture<List<Integer>> future = runAsync(() -> table.indexStorageAdapters(partitionId).get())
                .thenApply(indexStorageByIndexId -> indexStorageByIndexId.keySet().stream().sorted().collect(toList()));

        assertThat(future, willCompleteSuccessfully());

        return future.join();
    }

    private static List<Integer> collectIndexIdsFromCatalogForRecovery(IgniteImpl ignite, TableImpl table) {
        CatalogManager catalogManager = ignite.catalogManager();

        int earliestCatalogVersion = catalogManager.earliestCatalogVersion();
        int latestCatalogVersion = catalogManager.latestCatalogVersion();

        return IntStream.rangeClosed(earliestCatalogVersion, latestCatalogVersion)
                .mapToObj(catalogVersion -> catalogManager.indexes(catalogVersion, table.tableId()))
                .flatMap(Collection::stream)
                .map(CatalogObjectDescriptor::id)
                .distinct()
                .sorted()
                .collect(toList());
    }
}
