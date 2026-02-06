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

import static it.unimi.dsi.fastutil.ints.Int2ObjectMaps.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionMvStorageAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionTxStateAccess;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.metrics.RaftSnapshotsMetricsSource;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OutgoingSnapshotsManagerTest extends BaseIgniteAbstractTest {
    private static final int ZONE_ID = 1;
    private static final int TABLE_ID = 2;

    @InjectMocks
    private OutgoingSnapshotsManager manager;

    @Mock
    private PartitionMvStorageAccess partitionAccess;

    @Mock
    private CatalogService catalogService;

    private final PartitionKey partitionKey = new PartitionKey(ZONE_ID, 1);

    @SuppressWarnings("EmptyTryBlock")
    @Test
    void readLockOnPartitionSnapshotsWorks() {
        PartitionSnapshots snapshots = manager.partitionSnapshots(partitionKey);

        snapshots.acquireReadLock();
        snapshots.releaseReadLock();
    }

    @Test
    void emptyOngoingSnapshotsIfNoSnapshotWasRegistered() {
        PartitionSnapshots snapshots = manager.partitionSnapshots(partitionKey);

        snapshots.acquireReadLock();
        try {
            assertThat(snapshots.ongoingSnapshots(), is(empty()));
        } finally {
            snapshots.releaseReadLock();
        }
    }

    @Test
    void startsSnapshot() {
        when(partitionAccess.tableId()).thenReturn(TABLE_ID);
        when(partitionAccess.committedGroupConfiguration()).thenReturn(mock(RaftGroupConfiguration.class));

        when(catalogService.catalog(anyInt())).thenReturn(mock(Catalog.class));

        UUID snapshotId = UUID.randomUUID();

        OutgoingSnapshot snapshot = new OutgoingSnapshot(
                snapshotId,
                partitionKey,
                singleton(TABLE_ID, partitionAccess),
                mock(PartitionTxStateAccess.class),
                catalogService,
                new RaftSnapshotsMetricsSource()
        );

        assertDoesNotThrow(() -> manager.startOutgoingSnapshot(UUID.randomUUID(), snapshot));
    }

    @Test
    void finishesSnapshot() {
        UUID snapshotId = startSnapshot();

        manager.finishOutgoingSnapshot(snapshotId);
    }

    private UUID startSnapshot() {
        UUID snapshotId = UUID.randomUUID();
        OutgoingSnapshot snapshot = mock(OutgoingSnapshot.class);
        lenient().when(snapshot.id()).thenReturn(snapshotId);
        doReturn(partitionKey).when(snapshot).partitionKey();

        manager.startOutgoingSnapshot(snapshotId, snapshot);
        return snapshotId;
    }

    @Test
    void removesPartitionsCollection() {
        startSnapshot();

        manager.cleanupOutgoingSnapshots(partitionKey);

        PartitionSnapshots snapshots = manager.partitionSnapshots(partitionKey);

        snapshots.acquireReadLock();
        try {
            assertThat(snapshots.ongoingSnapshots(), is(empty()));
        } finally {
            snapshots.releaseReadLock();
        }
    }
}
