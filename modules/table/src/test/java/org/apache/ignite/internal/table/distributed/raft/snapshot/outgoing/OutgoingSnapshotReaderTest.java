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

import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.junit.jupiter.api.Test;

/**
 * For {@link OutgoingSnapshotReader} testing.
 */
public class OutgoingSnapshotReaderTest extends BaseIgniteAbstractTest {
    @Test
    void testForChoosingMaximumAppliedIndexForMeta() {
        PartitionAccess partitionAccess = mock(PartitionAccess.class);

        when(partitionAccess.partitionKey()).thenReturn(new PartitionKey(1, 0));
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
                mock(CatalogService.class),
                mock(SnapshotMeta.class),
                mock(Executor.class)
        );

        when(partitionAccess.minLastAppliedIndex()).thenReturn(5L);
        when(partitionAccess.maxLastAppliedIndex()).thenReturn(10L);

        when(partitionAccess.minLastAppliedTerm()).thenReturn(1L);
        when(partitionAccess.maxLastAppliedTerm()).thenReturn(2L);

        SnapshotMeta meta = new OutgoingSnapshotReader(snapshotStorage).load();
        assertEquals(10L, meta.lastIncludedIndex());
        assertEquals(2L, meta.lastIncludedTerm());
    }
}
