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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing;

import static org.apache.ignite.internal.catalog.CatalogTestUtils.createCatalogManagerWithTestUpdateLog;
import static org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.SnapshotMetaUtils.collectNextRowIdToBuildIndexes;
import static org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.SnapshotMetaUtils.snapshotMetaAt;
import static org.apache.ignite.internal.table.TableTestUtils.INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.startBuildingIndex;
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
import org.apache.ignite.internal.partition.replicator.network.raft.PartitionSnapshotMeta;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SnapshotMetaUtilsTest extends BaseIgniteAbstractTest {
    @Test
    void buildsSnapshotMeta() {
        RaftGroupConfiguration config = new RaftGroupConfiguration(
                13L,
                37L,
                111L,
                110L,
                List.of("peer1:3000", "peer2:3000"), List.of("learner1:3000", "learner2:3000"),
                List.of("peer1:3000"), List.of("learner1:3000")
        );

        UUID nextRowIdToBuild = UUID.randomUUID();
        int indexId = 1;

        var leaseInfo = new LeaseInfo(777L, new UUID(1, 2), "primary");

        PartitionSnapshotMeta meta = snapshotMetaAt(
                100,
                3,
                config,
                42,
                Map.of(indexId, nextRowIdToBuild),
                leaseInfo
        );

        assertThat(meta.cfgIndex(), is(13L));
        assertThat(meta.cfgTerm(), is(37L));
        assertThat(meta.lastIncludedIndex(), is(100L));
        assertThat(meta.lastIncludedTerm(), is(3L));
        assertThat(meta.peersList(), is(List.of("peer1:3000", "peer2:3000")));
        assertThat(meta.learnersList(), is(List.of("learner1:3000", "learner2:3000")));
        assertThat(meta.oldPeersList(), is(List.of("peer1:3000")));
        assertThat(meta.oldLearnersList(), is(List.of("learner1:3000")));
        assertThat(meta.requiredCatalogVersion(), is(42));
        assertThat(meta.nextRowIdToBuildByIndexId(), is(Map.of(indexId, nextRowIdToBuild)));
        assertThat(meta.leaseStartTime(), is(777L));
        assertThat(meta.primaryReplicaNodeId(), is(new UUID(1, 2)));
        assertThat(meta.primaryReplicaNodeName(), is("primary"));
    }

    @Test
    void doesNotIncludeOldConfigWhenItIsNotThere() {
        PartitionSnapshotMeta meta = snapshotMetaAt(
                100,
                3,
                new RaftGroupConfiguration(13, 37, 111L, 110L, List.of(), List.of(), null, null),
                42,
                Map.of(),
                null
        );

        assertThat(meta.oldPeersList(), is(nullValue()));
        assertThat(meta.oldLearnersList(), is(nullValue()));
    }

    @Test
    void testCollectNextRowIdToBuildIndexes() throws Exception {
        HybridClock clock = new HybridClockImpl();

        CatalogManager catalogManager = createCatalogManagerWithTestUpdateLog("test", clock);

        try {
            assertThat(catalogManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

            String tableName0 = TABLE_NAME + 0;
            String tableName1 = TABLE_NAME + 1;

            String indexName0 = INDEX_NAME + 0;
            String indexName1 = INDEX_NAME + 1;
            String indexName2 = INDEX_NAME + 2;
            String indexName3 = INDEX_NAME + 3;

            createSimpleTable(catalogManager, tableName0);
            createSimpleTable(catalogManager, tableName1);

            createSimpleHashIndex(catalogManager, tableName0, indexName0);
            createSimpleHashIndex(catalogManager, tableName0, indexName1);
            createSimpleHashIndex(catalogManager, tableName0, indexName2);
            createSimpleHashIndex(catalogManager, tableName1, indexName3);

            int indexId1 = getIndexIdStrict(catalogManager, indexName1, clock.nowLong());
            int indexId2 = getIndexIdStrict(catalogManager, indexName2, clock.nowLong());
            int indexId3 = getIndexIdStrict(catalogManager, indexName3, clock.nowLong());

            startBuildingIndex(catalogManager, indexId1);
            startBuildingIndex(catalogManager, indexId2);
            startBuildingIndex(catalogManager, indexId3);

            PartitionMvStorageAccess partitionAccess0 = mock(PartitionMvStorageAccess.class, withSettings().strictness(LENIENT));
            PartitionMvStorageAccess partitionAccess1 = mock(PartitionMvStorageAccess.class, withSettings().strictness(LENIENT));

            int tableId0 = getTableIdStrict(catalogManager, tableName0, clock.nowLong());
            int tableId1 = getTableIdStrict(catalogManager, tableName1, clock.nowLong());
            int partitionId = 0;

            var nextRowIdToBuildIndex2 = new RowId(partitionId);
            var nextRowIdToBuildIndex3 = new RowId(partitionId);

            when(partitionAccess0.tableId()).thenReturn(tableId0);
            when(partitionAccess0.getNextRowIdToBuildIndex(eq(indexId2))).thenReturn(nextRowIdToBuildIndex2);

            when(partitionAccess1.tableId()).thenReturn(tableId1);
            when(partitionAccess1.getNextRowIdToBuildIndex(eq(indexId3))).thenReturn(nextRowIdToBuildIndex3);

            assertThat(
                    collectNextRowIdToBuildIndexes(
                            catalogManager,
                            List.of(partitionAccess0, partitionAccess1),
                            catalogManager.latestCatalogVersion()
                    ),
                    is(Map.of(indexId2, nextRowIdToBuildIndex2.uuid(), indexId3, nextRowIdToBuildIndex3.uuid()))
            );
        } finally {
            closeAll(
                    catalogManager::beforeNodeStop,
                    () -> assertThat(catalogManager.stopAsync(new ComponentContext()), willCompleteSuccessfully())
            );
        }
    }
}
