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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.junit.jupiter.api.Test;

/**
 * For {@link OutgoingSnapshotReader} testing.
 */
public class OutgoingSnapshotReaderTest {
    @Test
    void testForChoosingMaximumAppliedIndexForMeta() {
        MvPartitionStorage mvPartitionStorage = new TestMvPartitionStorage(0);
        TxStateStorage txStateStorage = new TestTxStateStorage();

        PartitionAccess partitionAccess = mock(PartitionAccess.class);

        when(partitionAccess.mvPartitionStorage()).thenReturn(mvPartitionStorage);
        when(partitionAccess.txStatePartitionStorage()).thenReturn(txStateStorage);
        when(partitionAccess.partitionKey()).thenReturn(new PartitionKey(UUID.randomUUID(), 0));
        when(partitionAccess.committedGroupConfiguration()).thenReturn(mock(RaftGroupConfiguration.class));

        OutgoingSnapshotsManager outgoingSnapshotsManager = mock(OutgoingSnapshotsManager.class);
        doAnswer(invocation -> {
            OutgoingSnapshot snapshot = invocation.getArgument(1);

            snapshot.freezeScopeUnderMvLock();

            return null;
        }).when(outgoingSnapshotsManager).startOutgoingSnapshot(any(), any());

        PartitionSnapshotStorage snapshotStorage = new PartitionSnapshotStorage(
                mock(TopologyService.class),
                outgoingSnapshotsManager,
                "",
                mock(RaftOptions.class),
                partitionAccess,
                mock(SnapshotMeta.class),
                mock(Executor.class)
        );

        mvPartitionStorage.lastApplied(10L, 2L);
        txStateStorage.lastApplied(5L, 1L);

        SnapshotMeta meta1 = new OutgoingSnapshotReader(snapshotStorage).load();
        assertEquals(10L, meta1.lastIncludedIndex());
        assertEquals(2L, meta1.lastIncludedTerm());

        mvPartitionStorage.lastApplied(1L, 1L);
        txStateStorage.lastApplied(2L, 2L);

        SnapshotMeta meta2 = new OutgoingSnapshotReader(snapshotStorage).load();
        assertEquals(2L, meta2.lastIncludedIndex());
        assertEquals(2L, meta2.lastIncludedTerm());
    }
}
