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

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import static org.apache.ignite.internal.catalog.CatalogTestUtils.createCatalogManagerWithTestUpdateLog;
import static org.apache.ignite.internal.table.TableTestUtils.INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableZoneIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.startBuildingIndex;
import static org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.SnapshotMetaUtils.collectNextRowIdToBuildIndexes;
import static org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.SnapshotMetaUtils.snapshotMetaAt;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.quality.Strictness.LENIENT;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SnapshotMetaUtilsTest extends BaseIgniteAbstractTest {
    @Test
    void buildsSnapshotMeta() {
        RaftGroupConfiguration config = new RaftGroupConfiguration(
                List.of("peer1:3000", "peer2:3000"), List.of("learner1:3000", "learner2:3000"),
                List.of("peer1:3000"), List.of("learner1:3000")
        );

        UUID nextRowIdToBuild = UUID.randomUUID();
        int indexId = 1;

        SnapshotMeta meta = snapshotMetaAt(100, 3, config, 42, Map.of(indexId, nextRowIdToBuild));

        assertThat(meta.lastIncludedIndex(), is(100L));
        assertThat(meta.lastIncludedTerm(), is(3L));
        assertThat(meta.peersList(), is(List.of("peer1:3000", "peer2:3000")));
        assertThat(meta.learnersList(), is(List.of("learner1:3000", "learner2:3000")));
        assertThat(meta.oldPeersList(), is(List.of("peer1:3000")));
        assertThat(meta.oldLearnersList(), is(List.of("learner1:3000")));
        assertThat(meta.requiredCatalogVersion(), is(42));
        assertThat(meta.nextRowIdToBuildByIndexId(), is(Map.of(indexId, nextRowIdToBuild)));
    }

    @Test
    void doesNotIncludeOldConfigWhenItIsNotThere() {
        SnapshotMeta meta = snapshotMetaAt(100, 3, new RaftGroupConfiguration(List.of(), List.of(), null, null), 42, Map.of());

        assertThat(meta.oldPeersList(), is(nullValue()));
        assertThat(meta.oldLearnersList(), is(nullValue()));
    }

    @Test
    void testCollectNextRowIdToBuildIndexes() throws Exception {
        HybridClock clock = new HybridClockImpl();

        CatalogManager catalogManager = createCatalogManagerWithTestUpdateLog("test", clock);

        try {
            assertThat(catalogManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

            String indexName0 = INDEX_NAME + 0;
            String indexName1 = INDEX_NAME + 1;
            String indexName2 = INDEX_NAME + 2;

            createSimpleTable(catalogManager, TABLE_NAME);

            createSimpleHashIndex(catalogManager, TABLE_NAME, indexName0);
            createSimpleHashIndex(catalogManager, TABLE_NAME, indexName1);
            createSimpleHashIndex(catalogManager, TABLE_NAME, indexName2);

            int indexId1 = getIndexIdStrict(catalogManager, indexName1, clock.nowLong());
            int indexId2 = getIndexIdStrict(catalogManager, indexName2, clock.nowLong());

            startBuildingIndex(catalogManager, indexId1);
            startBuildingIndex(catalogManager, indexId2);

            PartitionAccess partitionAccess = mock(PartitionAccess.class, withSettings().strictness(LENIENT));

            int zoneId = getTableZoneIdStrict(catalogManager, TABLE_NAME, clock.nowLong());
            int tableId = getTableIdStrict(catalogManager, TABLE_NAME, clock.nowLong());
            int partitionId = 0;

            var nextRowIdToBuildIndex2 = new RowId(partitionId);

            when(partitionAccess.partitionKey()).thenReturn(new PartitionKey(zoneId, tableId, partitionId));
            when(partitionAccess.getNextRowIdToBuildIndex(eq(indexId2))).thenReturn(nextRowIdToBuildIndex2);

            assertThat(
                    collectNextRowIdToBuildIndexes(catalogManager, partitionAccess, catalogManager.latestCatalogVersion()),
                    is(Map.of(indexId2, nextRowIdToBuildIndex2.uuid()))
            );
        } finally {
            closeAll(
                    catalogManager::beforeNodeStop,
                    () -> assertThat(catalogManager.stopAsync(new ComponentContext()), willCompleteSuccessfully())
            );
        }
    }
}
