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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.ignite.internal.storage.GroupConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.network.MessagingService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OutgoingSnapshotsManagerTest {
    @Mock
    private MessagingService messagingService;

    @InjectMocks
    private OutgoingSnapshotsManager manager;

    @Mock
    private PartitionAccess partitionAccess;

    private final PartitionKey partitionKey = new PartitionKey(UUID.randomUUID(), 1);

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

        assertThat(snapshots.ongoingSnapshots(), is(empty()));
    }

    @Test
    void startsSnapshot() {
        MvPartitionStorage mvPartitionStorage = mock(MvPartitionStorage.class);

        when(partitionAccess.partitionKey()).thenReturn(partitionKey);
        when(partitionAccess.mvPartitionStorage()).thenReturn(mvPartitionStorage);
        when(partitionAccess.txStatePartitionStorage()).thenReturn(mock(TxStateStorage.class));

        when(mvPartitionStorage.committedGroupConfiguration()).thenReturn(mock(GroupConfiguration.class));

        OutgoingSnapshot snapshot = new OutgoingSnapshot(UUID.randomUUID(), partitionAccess);

        manager.startOutgoingSnapshot(UUID.randomUUID(), snapshot);
    }

    @Test
    void finishesSnapshot() {
        UUID snapshotId = startSnapshot();

        manager.finishOutgoingSnapshot(snapshotId);
    }

    private UUID startSnapshot() {
        UUID snapshotId = UUID.randomUUID();
        OutgoingSnapshot snapshot = mock(OutgoingSnapshot.class);
        doReturn(partitionKey).when(snapshot).partitionKey();

        manager.startOutgoingSnapshot(snapshotId, snapshot);
        return snapshotId;
    }

    @Test
    void removesPartitionsCollection() {
        startSnapshot();

        manager.removeSnapshots(partitionKey);

        assertThat(manager.partitionSnapshots(partitionKey).ongoingSnapshots(), is(empty()));
    }
}
