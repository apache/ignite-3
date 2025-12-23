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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.LogStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.metrics.RaftSnapshotsMetricsSource;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * For {@link OutgoingSnapshotReader} testing.
 */
@ExtendWith(MockitoExtension.class)
public class OutgoingSnapshotReaderTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID_1 = 1;
    private static final int TABLE_ID_2 = 2;

    @Test
    void testForChoosingMaximumAppliedIndexForMeta(
            @Mock CatalogService catalogService,
            @Mock Catalog catalog,
            @Mock RaftGroupConfiguration raftGroupConfiguration,
            @Mock PartitionMvStorageAccess partitionAccess1,
            @Mock PartitionMvStorageAccess partitionAccess2,
            @Mock OutgoingSnapshotsManager outgoingSnapshotsManager,
            @Mock PartitionTxStateAccess txStateAccess
    ) throws IOException {
        when(catalogService.catalog(anyInt())).thenReturn(catalog);

        when(partitionAccess1.tableId()).thenReturn(TABLE_ID_1);
        when(partitionAccess2.tableId()).thenReturn(TABLE_ID_2);

        when(txStateAccess.committedGroupConfiguration()).thenReturn(raftGroupConfiguration);

        doAnswer(invocation -> {
            OutgoingSnapshot snapshot = invocation.getArgument(1);

            snapshot.freezeScopeUnderMvLock();

            return null;
        }).when(outgoingSnapshotsManager).startOutgoingSnapshot(any(), any());

        var partitionKey = new ZonePartitionKey(0, 0);

        var snapshotMetricsSource = new RaftSnapshotsMetricsSource();

        var snapshotStorage = new PartitionSnapshotStorage(
                partitionKey,
                mock(TopologyService.class),
                outgoingSnapshotsManager,
                txStateAccess,
                catalogService,
                mock(FailureProcessor.class),
                mock(Executor.class),
                mock(LogStorageAccess.class),
                snapshotMetricsSource
        );

        snapshotStorage.addMvPartition(TABLE_ID_1, partitionAccess1);
        snapshotStorage.addMvPartition(TABLE_ID_2, partitionAccess2);

        when(partitionAccess1.lastAppliedIndex()).thenReturn(5L);
        when(partitionAccess2.lastAppliedIndex()).thenReturn(6L);
        when(txStateAccess.lastAppliedIndex()).thenReturn(10L);

        when(txStateAccess.lastAppliedTerm()).thenReturn(1L);
        lenient().when(partitionAccess1.lastAppliedTerm()).thenReturn(2L);
        lenient().when(partitionAccess2.lastAppliedTerm()).thenReturn(3L);

        UUID snapshotId = UUID.randomUUID();

        try (var reader = new OutgoingSnapshotReader(snapshotId, snapshotStorage, snapshotMetricsSource)) {
            SnapshotMeta meta = reader.load();
            assertEquals(10L, meta.lastIncludedIndex());
            assertEquals(1L, meta.lastIncludedTerm());
        }
    }
}
