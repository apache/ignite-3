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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestConcurrentHashMapTxStateStorage;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.junit.jupiter.api.Test;

/**
 * For testing {@link PartitionSnapshotStorageFactory}.
 */
public class PartitionSnapshotStorageFactoryTest {
    @Test
    void testForChoosingMinimumAppliedIndexForMeta() {
        MvPartitionStorage mvPartitionStorage = new TestMvPartitionStorage(0);
        TxStateStorage txStateStorage = new TestConcurrentHashMapTxStateStorage();

        PartitionAccess partitionAccess = mock(PartitionAccess.class);

        when(partitionAccess.mvPartitionStorage()).thenReturn(mvPartitionStorage);
        when(partitionAccess.txStatePartitionStorage()).thenReturn(txStateStorage);

        mvPartitionStorage.lastAppliedIndex(10L);
        txStateStorage.lastAppliedIndex(5L);

        PartitionSnapshotStorageFactory partitionSnapshotStorageFactory = new PartitionSnapshotStorageFactory(
                mock(TopologyService.class),
                mock(OutgoingSnapshotsManager.class),
                partitionAccess,
                List.of(),
                List.of(),
                mock(Executor.class)
        );

        PartitionSnapshotStorage snapshotStorage = partitionSnapshotStorageFactory.createSnapshotStorage("", mock(RaftOptions.class));

        assertEquals(5L, snapshotStorage.startupSnapshotMeta().lastIncludedIndex());

        mvPartitionStorage.lastAppliedIndex(1L);
        txStateStorage.lastAppliedIndex(2L);

        partitionSnapshotStorageFactory = new PartitionSnapshotStorageFactory(
                mock(TopologyService.class),
                mock(OutgoingSnapshotsManager.class),
                partitionAccess,
                List.of(),
                List.of(),
                mock(Executor.class)
        );

        snapshotStorage = partitionSnapshotStorageFactory.createSnapshotStorage("", mock(RaftOptions.class));

        assertEquals(1L, snapshotStorage.startupSnapshotMeta().lastIncludedIndex());
    }
}
