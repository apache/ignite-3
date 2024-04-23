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

package org.apache.ignite.internal.table.distributed.replicator;

import static org.apache.ignite.internal.table.TableTestUtils.INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.makeIndexAvailable;
import static org.apache.ignite.internal.table.TableTestUtils.startBuildingIndex;
import static org.apache.ignite.internal.table.distributed.replicator.ReplicatorUtils.beginRwTxTs;
import static org.apache.ignite.internal.table.distributed.replicator.ReplicatorUtils.latestIndexDescriptorInBuildingStatus;
import static org.apache.ignite.internal.table.distributed.replicator.ReplicatorUtils.rwTxActiveCatalogVersion;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TransactionIds.transactionId;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteReplicaRequest;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** For {@link ReplicatorUtils} testing. */
public class ReplicatorUtilsTest extends IgniteAbstractTest {
    private final HybridClock clock = new HybridClockImpl();

    @Test
    void testBeginRwTxTs() {
        HybridTimestamp beginTs = clock.now();

        UUID txId = transactionId(beginTs, 10);

        assertEquals(beginTs, beginRwTxTs(readWriteReplicaRequest(txId)));
    }

    @Test
    void testRwTxActiveCatalogVersion() {
        HybridTimestamp beginTs = clock.now();

        UUID txId = transactionId(beginTs, 10);

        CatalogService catalogService = mock(CatalogService.class);

        when(catalogService.activeCatalogVersion(anyLong())).thenReturn(666);

        assertEquals(666, rwTxActiveCatalogVersion(catalogService, readWriteReplicaRequest(txId)));

        verify(catalogService).activeCatalogVersion(eq(beginTs.longValue()));
    }

    @Test
    void testLatestIndexDescriptorInBuildingStatus() throws Exception {
        withCatalogManager(catalogManager -> {
            createSimpleTable(catalogManager, TABLE_NAME);

            int tableId = tableId(catalogManager, TABLE_NAME);

            assertNull(latestIndexDescriptorInBuildingStatus(catalogManager, tableId));

            createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);
            assertNull(latestIndexDescriptorInBuildingStatus(catalogManager, tableId));

            int indexId = indexId(catalogManager, INDEX_NAME);

            startBuildingIndex(catalogManager, indexId);
            assertEquals(indexId, latestIndexDescriptorInBuildingStatus(catalogManager, tableId).id());

            makeIndexAvailable(catalogManager, indexId);
            assertEquals(indexId, latestIndexDescriptorInBuildingStatus(catalogManager, tableId).id());

            String otherIndexName = INDEX_NAME + 1;

            createSimpleHashIndex(catalogManager, TABLE_NAME, otherIndexName);
            assertEquals(indexId, latestIndexDescriptorInBuildingStatus(catalogManager, tableId).id());

            int otherIndexId = indexId(catalogManager, otherIndexName);

            startBuildingIndex(catalogManager, otherIndexId);
            assertEquals(otherIndexId, latestIndexDescriptorInBuildingStatus(catalogManager, tableId).id());
        });
    }

    @Test
    void testLatestIndexDescriptorInBuildingStatusForOtherTable() throws Exception {
        withCatalogManager(catalogManager -> {
            String otherTableName = TABLE_NAME + 1;

            createSimpleTable(catalogManager, TABLE_NAME);
            createSimpleTable(catalogManager, otherTableName);

            createSimpleHashIndex(catalogManager, TABLE_NAME, INDEX_NAME);
            startBuildingIndex(catalogManager, indexId(catalogManager, INDEX_NAME));

            assertNull(latestIndexDescriptorInBuildingStatus(catalogManager, tableId(catalogManager, otherTableName)));
        });
    }

    private static ReadWriteReplicaRequest readWriteReplicaRequest(UUID txId) {
        ReadWriteReplicaRequest request = mock(ReadWriteReplicaRequest.class);

        when(request.transactionId()).thenReturn(txId);

        return request;
    }

    private void withCatalogManager(Consumer<CatalogManager> consumer) throws Exception {
        CatalogManager catalogManager = CatalogTestUtils.createCatalogManagerWithTestUpdateLog("test-node", clock);

        assertThat(catalogManager.start(), willCompleteSuccessfully());

        try {
            consumer.accept(catalogManager);
        } finally {
            closeAll(catalogManager::beforeNodeStop, catalogManager::stop);
        }
    }

    private int indexId(CatalogService catalogService, String indexName) {
        return getIndexIdStrict(catalogService, indexName, clock.nowLong());
    }

    private int tableId(CatalogService catalogService, String tableName) {
        return getTableIdStrict(catalogService, tableName, clock.nowLong());
    }
}
