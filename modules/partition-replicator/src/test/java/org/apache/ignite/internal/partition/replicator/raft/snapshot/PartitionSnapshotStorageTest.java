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

package org.apache.ignite.internal.partition.replicator.raft.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.metrics.NoOpMetricManager;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * For testing {@link PartitionSnapshotStorageFactory}.
 */
@ExtendWith(MockitoExtension.class)
public class PartitionSnapshotStorageTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID_1 = 1;
    private static final int TABLE_ID_2 = 2;

    private final PartitionSnapshotStorage snapshotStorage = new PartitionSnapshotStorage(
            new ZonePartitionKey(1, 1),
            mock(TopologyService.class),
            mock(OutgoingSnapshotsManager.class),
            mock(PartitionTxStateAccess.class),
            mock(CatalogService.class),
            mock(FailureProcessor.class),
            mock(Executor.class),
            mock(LogStorageAccess.class),
            new NoOpMetricManager()
    );

    @Test
    void choosesMinimalIndexFromPartitionStorage(
            @Mock PartitionMvStorageAccess partitionAccess1,
            @Mock PartitionMvStorageAccess partitionAccess2
    ) {
        when(partitionAccess1.lastAppliedIndex()).thenReturn(5L);
        when(partitionAccess2.lastAppliedIndex()).thenReturn(3L);
        when(snapshotStorage.txState().lastAppliedIndex()).thenReturn(10L);

        lenient().when(partitionAccess1.lastAppliedTerm()).thenReturn(5L);
        when(partitionAccess2.lastAppliedTerm()).thenReturn(1L);
        lenient().when(snapshotStorage.txState().lastAppliedTerm()).thenReturn(10L);

        when(partitionAccess2.committedGroupConfiguration()).thenReturn(mock(RaftGroupConfiguration.class));

        snapshotStorage.addMvPartition(TABLE_ID_1, partitionAccess1);
        snapshotStorage.addMvPartition(TABLE_ID_2, partitionAccess2);

        SnapshotMeta startupSnapshotMeta = snapshotStorage.readStartupSnapshotMeta();

        assertEquals(3L, startupSnapshotMeta.lastIncludedIndex());
        assertEquals(1L, startupSnapshotMeta.lastIncludedTerm());
    }

    @Test
    void choosesMinimalIndexFromTxStorage(@Mock PartitionMvStorageAccess partitionAccess) {
        when(partitionAccess.lastAppliedIndex()).thenReturn(5L);
        when(snapshotStorage.txState().lastAppliedIndex()).thenReturn(3L);

        lenient().when(partitionAccess.lastAppliedTerm()).thenReturn(5L);
        when(snapshotStorage.txState().lastAppliedTerm()).thenReturn(2L);

        when(snapshotStorage.txState().committedGroupConfiguration()).thenReturn(mock(RaftGroupConfiguration.class));

        snapshotStorage.addMvPartition(TABLE_ID_1, partitionAccess);

        SnapshotMeta startupSnapshotMeta = snapshotStorage.readStartupSnapshotMeta();

        assertEquals(3L, startupSnapshotMeta.lastIncludedIndex());
        assertEquals(2L, startupSnapshotMeta.lastIncludedTerm());
    }

    @Test
    void partitionCanBeRemovedWithoutSnapshotOperations(@Mock PartitionMvStorageAccess partitionAccess) {
        snapshotStorage.addMvPartition(TABLE_ID_1, partitionAccess);

        CompletableFuture<Void> removeFuture = snapshotStorage.removeMvPartition(TABLE_ID_1);

        assertThat(removeFuture.isDone(), is(true));

        assertThat(snapshotStorage.partitionsByTableId(), is(anEmptyMap()));
    }

    @Test
    void partitionCanBeRemovedOnlyWithoutOutgoingSnapshot(@Mock PartitionMvStorageAccess partitionAccess) throws IOException {
        snapshotStorage.addMvPartition(TABLE_ID_1, partitionAccess);

        CompletableFuture<Void> removeFuture;

        try (SnapshotReader ignored = snapshotStorage.startOutgoingSnapshot()) {
            removeFuture = snapshotStorage.removeMvPartition(TABLE_ID_1);

            assertThat(removeFuture.isDone(), is(false));

            assertThat(snapshotStorage.partitionsByTableId(), is(not(anEmptyMap())));
        }

        assertThat(removeFuture.isDone(), is(true));

        assertThat(snapshotStorage.partitionsByTableId(), is(anEmptyMap()));
    }

    @Test
    void partitionCanBeRemovedOnlyWithoutIncomingSnapshot(@Mock PartitionMvStorageAccess partitionAccess) throws IOException {
        snapshotStorage.addMvPartition(TABLE_ID_1, partitionAccess);

        String snapshotUri = SnapshotUri.toStringUri(UUID.randomUUID(), "node");

        CompletableFuture<Void> removeFuture;

        try (SnapshotCopier ignored = snapshotStorage.startIncomingSnapshot(snapshotUri)) {
            removeFuture = snapshotStorage.removeMvPartition(TABLE_ID_1);

            assertThat(removeFuture.isDone(), is(false));

            assertThat(snapshotStorage.partitionsByTableId(), is(not(anEmptyMap())));
        }

        assertThat(removeFuture.isDone(), is(true));

        assertThat(snapshotStorage.partitionsByTableId(), is(anEmptyMap()));
    }
}
