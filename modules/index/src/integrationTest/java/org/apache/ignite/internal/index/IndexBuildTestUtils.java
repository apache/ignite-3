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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.index.ItBuildIndexTest.getIndexDescriptor;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexStrict;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.Table;
import org.jetbrains.annotations.Nullable;

class IndexBuildTestUtils {
    private static final IgniteLogger LOG = Loggers.forClass(IndexBuildTestUtils.class);

    private static final String ZONE_NAME = "ZONE_TABLE";

    static final String TABLE_NAME = "TEST_TABLE";

    static final String INDEX_NAME = "TEST_INDEX";

    static void createTestTable(Cluster cluster, int replicas, int partitions) {
        IgniteSql sql = cluster.node(0).sql();

        sql.executeScript(format("CREATE ZONE IF NOT EXISTS {} (REPLICAS {}, PARTITIONS {}) STORAGE PROFILES ['{}']",
                ZONE_NAME, replicas, partitions, DEFAULT_STORAGE_PROFILE
        ));
        sql.executeScript(format(
                "CREATE TABLE {} (i0 INTEGER PRIMARY KEY, i1 INTEGER) ZONE {}",
                TABLE_NAME, ZONE_NAME
        ));
    }

    static void createIndex(Cluster cluster, String indexName) {
        // We execute this operation asynchronously, because some tests block network messages, which makes the underlying code
        // stuck with timeouts. We don't need to wait for the operation to complete, as we wait for the necessary invariants further
        // below.
        cluster.aliveNode().sql()
                .executeAsync(null, format("CREATE INDEX {} ON {} (i1)", indexName, TABLE_NAME))
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        LOG.error("Failed to create index", ex);
                    }
                });

        waitForIndex(cluster, indexName);
    }

    /**
     * Waits for all nodes in the cluster to have the given index in the Catalog.
     *
     * @param indexName Name of an index to wait for.
     */
    static void waitForIndex(Cluster cluster, String indexName) {
        await().atMost(10, SECONDS).until(
                () -> cluster.runningNodes()
                        .map(TestWrappers::unwrapIgniteImpl)
                        .map(node -> getIndexDescriptor(node, indexName))
                        .allMatch(Objects::nonNull)
        );
    }

    static void verifyNoNodesHaveAnythingInIndex(Cluster cluster, int initialNodes) {
        for (int nodeIndex = 0; nodeIndex < initialNodes; nodeIndex++) {
            Ignite node = cluster.node(nodeIndex);
            verifyNodeHasNothingInIndex(node);
        }
    }

    private static void verifyNodeHasNothingInIndex(Ignite node) {
        assertThat("Nothing should have been put to the index, but it was found on node " + node.name(), rowsInIndex(node), is(0));
    }

    static int rowsInIndex(Ignite node) {
        IgniteImpl ignite = unwrapIgniteImpl(node);

        CatalogIndexDescriptor indexDescriptor = indexDescriptor(INDEX_NAME, ignite);
        SortedIndexStorage indexStorage = (SortedIndexStorage) indexStorage(indexDescriptor, 0, ignite);

        if (indexStorage != null) {
            try (Cursor<IndexRow> indexRows = indexStorage.readOnlyScan(null, null, 0)) {
                int count = 0;

                for (IndexRow ignored : indexRows) {
                    count++;
                }

                return count;
            }
        } else {
            return 0;
        }
    }

    private static CatalogIndexDescriptor indexDescriptor(String indexName, IgniteImpl ignite) {
        return getIndexStrict(ignite.catalogManager(), indexName, ignite.clock().nowLong());
    }

    private static @Nullable IndexStorage indexStorage(CatalogIndexDescriptor indexDescriptor, int partitionId, IgniteImpl ignite) {
        TableViewInternal tableViewInternal = tableViewInternal(indexDescriptor.tableId(), ignite);

        int indexId = indexDescriptor.id();

        IndexStorage indexStorage;
        try {
            indexStorage = tableViewInternal.internalTable().storage().getIndex(partitionId, indexId);
        } catch (StorageException e) {
            if (e.getMessage().contains("Partition ID " + partitionId + " does not exist")) {
                return null;
            }

            throw e;
        }

        assertNotNull(indexStorage, String.format("No index storage exists for indexId=%s, partitionId=%s", indexId, partitionId));

        return indexStorage;
    }

    private static TableViewInternal tableViewInternal(int tableId, Ignite ignite) {
        CompletableFuture<List<Table>> tablesFuture = ignite.tables().tablesAsync();

        assertThat(tablesFuture, willCompleteSuccessfully());

        TableViewInternal tableViewInternal = tablesFuture.join().stream()
                .map(TestWrappers::unwrapTableViewInternal)
                .filter(table -> table.tableId() == tableId)
                .findFirst()
                .orElse(null);

        assertNotNull(tableViewInternal, "No table object found for tableId=" + tableId);

        return tableViewInternal;
    }
}
