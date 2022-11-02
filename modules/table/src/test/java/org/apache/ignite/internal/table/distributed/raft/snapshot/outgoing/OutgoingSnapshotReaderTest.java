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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogManager;
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

        OutgoingSnapshotsManager outgoingSnapshotsManager = mock(OutgoingSnapshotsManager.class);
        doAnswer(invocation -> {
            OutgoingSnapshot snapshot = invocation.getArgument(1);

            snapshot.acquireMvLock();

            try {
                snapshot.freezeScope();
            } finally {
                snapshot.releaseMvLock();
            }

            return null;
        }).when(outgoingSnapshotsManager).startOutgoingSnapshot(any(), any());

        LogManager logManager = mock(LogManager.class);
        when(logManager.getConfiguration(anyLong())).thenReturn(new ConfigurationEntry(
                new LogId(1, 1),
                new Configuration(List.of(), List.of()),
                null
        ));

        PartitionSnapshotStorage snapshotStorage = new PartitionSnapshotStorage(
                mock(TopologyService.class),
                outgoingSnapshotsManager,
                "",
                mock(RaftOptions.class),
                partitionAccess,
                mock(SnapshotMeta.class),
                mock(Executor.class),
                logManager
        );

        mvPartitionStorage.lastAppliedIndex(10L);
        txStateStorage.lastAppliedIndex(5L);

        assertEquals(10L, new OutgoingSnapshotReader(snapshotStorage, logManager).load().lastIncludedIndex());

        mvPartitionStorage.lastAppliedIndex(1L);
        txStateStorage.lastAppliedIndex(2L);

        assertEquals(2L, new OutgoingSnapshotReader(snapshotStorage, logManager).load().lastIncludedIndex());
    }
}
