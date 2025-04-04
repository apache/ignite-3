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

package org.apache.ignite.internal.partition.replicator;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// TODO: https://issues.apache.org/jira/browse/IGNITE-22522 remove this test after the switching to zone-based replication
/**
 * Tests catalog compaction for colocation track.
 */
@Timeout(60)
public class ItCatalogCompactionTest extends ItAbstractColocationTest {
    /**
     * Tests catalog compaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCatalogCompaction() throws Exception {
        // How often we update the low water mark.
        long lowWatermarkUpdateInterval = 500;
        updateLowWatermarkConfiguration(lowWatermarkUpdateInterval * 2, lowWatermarkUpdateInterval);

        // Prepare a single node cluster.
        startCluster(1);
        Node node = getNode(0);

        forceCheckpoint(node, "initial-checkpoint");

        String zoneName = "test-zone";
        createZone(node, zoneName, 1, 1);

        int catalogVersion1 = getLatestCatalogVersion(node);

        String tableName1 = "test_table_1";
        createTable(node, zoneName, tableName1);

        String tableName2 = "test_table_2";
        createTable(node, zoneName, tableName2);

        int tableId = TableTestUtils.getTableId(node.catalogManager, tableName2, node.hybridClock.nowLong());
        TableViewInternal tableViewInternal = node.tableManager.table(tableId);
        KeyValueView<Long, Integer> tableView = tableViewInternal.keyValueView(Long.class, Integer.class);

        // Write 2 rows to the table.
        Map<Long, Integer> valuesToPut = Map.of(0L, 0, 1L, 1);
        assertDoesNotThrow(() -> tableView.putAll(null, valuesToPut));

        int catalogVersion2 = getLatestCatalogVersion(node);
        assertThat("The catalog version did not changed [initial=" + catalogVersion1 + ", latest=" + catalogVersion2 + "].",
                catalogVersion2, greaterThan(catalogVersion1));

        expectEarliestCatalogVersion(node, catalogVersion2 - 1);
    }

    private static void expectEarliestCatalogVersion(Node node, int expectedVersion) throws Exception {
        boolean result = waitForCondition(() -> {
            forceCheckpoint(node, "force-checkpoint");

            return getEarliestCatalogVersion(node) == expectedVersion;
        }, 500, 10_000);

        assertTrue(result,
                "Failed to wait for the expected catalog version [expected=" + expectedVersion
                        + ", earliest=" + getEarliestCatalogVersion(node)
                        + ", latest=" + getLatestCatalogVersion(node) + "].");
    }

    private static int getLatestCatalogVersion(Node node) {
        Catalog catalog = getLatestCatalog(node);

        return catalog.version();
    }

    private static int getEarliestCatalogVersion(Node node) {
        CatalogManager catalogManager = node.catalogManager;

        int ver = catalogManager.earliestCatalogVersion();

        Catalog catalog = catalogManager.catalog(ver);

        Objects.requireNonNull(catalog);

        return catalog.version();
    }

    private static Catalog getLatestCatalog(Node node) {
        CatalogManager catalogManager = node.catalogManager;

        int ver = catalogManager.activeCatalogVersion(node.hybridClock.nowLong());

        Catalog catalog = catalogManager.catalog(ver);

        Objects.requireNonNull(catalog);

        return catalog;
    }

    /**
     * Start the new checkpoint immediately on the provided node.
     *
     * @param node Node to start the checkpoint on.
     */
    private static void forceCheckpoint(Node node, String reason) {
        PersistentPageMemoryStorageEngine storageEngine = (PersistentPageMemoryStorageEngine) node
                .dataStorageManager()
                .engineByStorageProfile(DEFAULT_STORAGE_PROFILE);

        assertThat(storageEngine.checkpointManager().forceCheckpoint(reason).futureFor(FINISHED),
                willSucceedIn(10, SECONDS));
    }

    /**
     * Update low water mark configuration.
     *
     * @param dataAvailabilityTime Data availability time.
     * @param updateInterval Update interval.
     */
    private void updateLowWatermarkConfiguration(long dataAvailabilityTime, long updateInterval) {
        CompletableFuture<?> updateFuture = gcConfiguration.lowWatermark().change(change -> {
            change.changeDataAvailabilityTimeMillis(dataAvailabilityTime);
            change.changeUpdateIntervalMillis(updateInterval);
        });

        assertThat(updateFuture, willSucceedFast());
    }
}
