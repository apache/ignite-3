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
import static org.apache.ignite.internal.ClusterPerClassIntegrationTest.isIndexAvailable;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.index.IndexBuildTestUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.IndexBuildTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.IndexBuildTestUtils.createTestTable;
import static org.apache.ignite.internal.index.WriteIntentSwitchControl.disableWriteIntentSwitchExecution;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.index.message.IsNodeFinishedRwTransactionsStartedBeforeRequest;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

class ItBuildIndexWriteIntentsHandlingTest extends ClusterPerTestIntegrationTest {
    @Test
    void writeIntentFromTxAbandonedBeforeShouldNotBeIndexed() {
        createTestTable(cluster, 1, 1);

        disableWriteIntentSwitchExecution(cluster);

        // Create and abandon a transaction.
        int txCoordinatorOrdinal = 2;
        Transaction tx = cluster.node(txCoordinatorOrdinal).transactions().begin();
        insertDataInTransaction(tx, TABLE_NAME, List.of("I0", "I1"), new Object[]{1, 1});

        cluster.restartNode(txCoordinatorOrdinal);

        createIndex(INDEX_NAME);
        await("Index did not become available in time")
                .atMost(10, SECONDS)
                .until(() -> isIndexAvailable(unwrapIgniteImpl(cluster.aliveNode()), INDEX_NAME));

        verifyNoNodesHaveAnythingInIndex();
    }

    @Test
    void writeIntentFromTxAbandonedWhileWaitingForTransactionsToFinishShouldNotBeIndexed() {
        createTestTable(cluster, 1, 1);

        // Both disable write intent switch execution and track when we start waiting for transactions to finish before index build.
        CompletableFuture<Void> startedWaitForPreIndexTxsToFinish = new CompletableFuture<>();
        cluster.nodes().forEach(node -> {
            unwrapIgniteImpl(node).dropMessages((recipientId, message) -> {
                if (message instanceof WriteIntentSwitchReplicaRequest) {
                    return true;
                }

                if (message instanceof IsNodeFinishedRwTransactionsStartedBeforeRequest) {
                    startedWaitForPreIndexTxsToFinish.complete(null);
                }

                return false;
            });
        });

        // Create and abandon a transaction.
        int txCoordinatorOrdinal = 2;
        Transaction tx = cluster.node(txCoordinatorOrdinal).transactions().begin();
        insertDataInTransaction(tx, TABLE_NAME, List.of("I0", "I1"), new Object[]{1, 1});

        createIndex(INDEX_NAME);
        assertThat(startedWaitForPreIndexTxsToFinish, willCompleteSuccessfully());

        // The index pre-build wait has started, let's restart the coordinator to abandon the transaction and abruptly terminate
        // the pre-build wait.
        cluster.restartNode(txCoordinatorOrdinal);

        await("Index did not become available in time")
                .atMost(10, SECONDS)
                .until(() -> isIndexAvailable(unwrapIgniteImpl(cluster.aliveNode()), INDEX_NAME));

        verifyNoNodesHaveAnythingInIndex();
    }

    private void verifyNoNodesHaveAnythingInIndex() {
        for (int nodeIndex = 0; nodeIndex < initialNodes(); nodeIndex++) {
            IgniteImpl ignite = unwrapIgniteImpl(node(nodeIndex));

            CatalogIndexDescriptor indexDescriptor = indexDescriptor(INDEX_NAME, ignite);
            SortedIndexStorage indexStorage = (SortedIndexStorage) indexStorage(indexDescriptor, 0, ignite);

            if (indexStorage != null) {
                try (Cursor<IndexRow> indexRows = indexStorage.readOnlyScan(null, null, 0)) {
                    assertFalse(indexRows.hasNext(), "Nothing should have been put to the index, but it was found on node " + nodeIndex);
                }
            }
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

    private void createTable(int replicas, int partitions) {
        IgniteSql sql = cluster.node(0).sql();

        sql.executeScript(format("CREATE ZONE IF NOT EXISTS {} (REPLICAS {}, PARTITIONS {}) STORAGE PROFILES ['{}']",
                ZONE_NAME, replicas, partitions, DEFAULT_STORAGE_PROFILE
        ));
        sql.executeScript(format(
                "CREATE TABLE {} (i0 INTEGER PRIMARY KEY, i1 INTEGER) ZONE {}",
                TABLE_NAME, ZONE_NAME
        ));
    }

    private void createIndex(String indexName) {
        // We execute this operation asynchronously, because some tests block network messages, which makes the underlying code
        // stuck with timeouts. We don't need to wait for the operation to complete, as we wait for the necessary invariants further
        // below.
        cluster.aliveNode().sql()
                .executeAsync(format("CREATE INDEX {} ON {} (i1)", indexName, TABLE_NAME))
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        log.error("Failed to create index", ex);
                    }
                });

        waitForIndex(indexName);
    }

    /**
     * Waits for all nodes in the cluster to have the given index in the Catalog.
     *
     * @param indexName Name of an index to wait for.
     */
    private void waitForIndex(String indexName) {
        await().atMost(10, SECONDS).until(
                () -> cluster.runningNodes()
                        .map(TestWrappers::unwrapIgniteImpl)
                        .map(node -> getIndexDescriptor(node, indexName))
                        .allMatch(Objects::nonNull)
        );
    }

    private void insertDataInTransaction(Transaction tx, String tblName, List<String> columnNames, Object[]... tuples) {
        String insertStmt = "INSERT INTO " + tblName + "(" + String.join(", ", columnNames) + ")"
                + " VALUES (" + ", ?".repeat(columnNames.size()).substring(2) + ")";

        node(0).sql().execute(tx, insertStmt, args).close();
    }

    private void verifyNoNodesHaveAnythingInIndex() {
        IndexBuildTestUtils.verifyNoNodesHaveAnythingInIndex(cluster, initialNodes());
    }

    private void createIndex(String indexName) {
        IndexBuildTestUtils.createIndex(cluster, indexName);
    }
}
