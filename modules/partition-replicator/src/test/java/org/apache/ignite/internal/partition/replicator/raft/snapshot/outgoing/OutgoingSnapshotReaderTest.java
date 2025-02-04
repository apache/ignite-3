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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.IOException;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.junit.jupiter.api.Test;

/**
 * For {@link OutgoingSnapshotReader} testing.
 */
public class OutgoingSnapshotReaderTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID_1 = 1;
    private static final int TABLE_ID_2 = 2;

    @Test
    void testForChoosingMaximumAppliedIndexForMeta() throws IOException {
        PartitionStorageAccess partitionAccess1 = mock(PartitionStorageAccess.class);
        PartitionStorageAccess partitionAccess2 = mock(PartitionStorageAccess.class);

        when(partitionAccess1.tableId()).thenReturn(TABLE_ID_1);
        when(partitionAccess2.tableId()).thenReturn(TABLE_ID_2);
        when(partitionAccess2.committedGroupConfiguration()).thenReturn(mock(RaftGroupConfiguration.class));

        OutgoingSnapshotsManager outgoingSnapshotsManager = mock(OutgoingSnapshotsManager.class);
        doAnswer(invocation -> {
            OutgoingSnapshot snapshot = invocation.getArgument(1);

            snapshot.freezeScopeUnderMvLock();

            return null;
        }).when(outgoingSnapshotsManager).startOutgoingSnapshot(any(), any());

        CatalogService catalogService = mock(CatalogService.class);
        when(catalogService.catalog(anyInt())).thenReturn(mock(Catalog.class));

        PartitionTxStateAccess txStateAccess = mock(PartitionTxStateAccess.class);

        var partitionsByTableId = new Int2ObjectOpenHashMap<PartitionStorageAccess>();

        partitionsByTableId.put(TABLE_ID_1, partitionAccess1);
        partitionsByTableId.put(TABLE_ID_2, partitionAccess2);

        PartitionSnapshotStorage snapshotStorage = new PartitionSnapshotStorage(
                new ZonePartitionKey(0, 0),
                mock(TopologyService.class),
                outgoingSnapshotsManager,
                "",
                mock(RaftOptions.class),
                partitionsByTableId,
                txStateAccess,
                catalogService,
                mock(SnapshotMeta.class),
                mock(Executor.class)
        );

        when(partitionAccess1.lastAppliedIndex()).thenReturn(5L);
        when(partitionAccess2.lastAppliedIndex()).thenReturn(6L);
        when(txStateAccess.lastAppliedIndex()).thenReturn(10L);

        when(txStateAccess.lastAppliedTerm()).thenReturn(1L);
        when(partitionAccess1.lastAppliedTerm()).thenReturn(2L);
        when(partitionAccess2.lastAppliedTerm()).thenReturn(3L);

        try (var reader = new OutgoingSnapshotReader(snapshotStorage)) {
            SnapshotMeta meta = reader.load();
            assertEquals(10L, meta.lastIncludedIndex());
            assertEquals(3L, meta.lastIncludedTerm());
        }
    }
}
