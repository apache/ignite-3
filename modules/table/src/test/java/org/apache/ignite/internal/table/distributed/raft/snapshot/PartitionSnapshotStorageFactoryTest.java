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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * For testing {@link PartitionSnapshotStorageFactory}.
 */
@ExtendWith(MockitoExtension.class)
public class PartitionSnapshotStorageFactoryTest extends BaseIgniteAbstractTest {
    @Mock
    private PartitionAccess partitionAccess;

    @Test
    void testForChoosingMinimumAppliedIndexForMeta() {
        when(partitionAccess.minLastAppliedIndex()).thenReturn(5L);

        when(partitionAccess.minLastAppliedTerm()).thenReturn(1L);

        when(partitionAccess.committedGroupConfiguration()).thenReturn(mock(RaftGroupConfiguration.class));

        CatalogService catalogService = mock(CatalogService.class);
        when(catalogService.indexes(anyInt(), anyInt())).thenReturn(List.of());

        when(partitionAccess.partitionKey()).thenReturn(new PartitionKey(11, 1, 1));

        PartitionSnapshotStorageFactory partitionSnapshotStorageFactory = new PartitionSnapshotStorageFactory(
                mock(TopologyService.class),
                mock(OutgoingSnapshotsManager.class),
                partitionAccess,
                catalogService,
                mock(Executor.class)
        );

        PartitionSnapshotStorage snapshotStorage = partitionSnapshotStorageFactory.createSnapshotStorage("", mock(RaftOptions.class));

        assertEquals(5L, snapshotStorage.startupSnapshotMeta().lastIncludedIndex());
        assertEquals(1L, snapshotStorage.startupSnapshotMeta().lastIncludedTerm());
    }

    @Test
    void storageThrowsOnAttemptToGetStartupMetaOnEmptyStorage() {
        var factory = new PartitionSnapshotStorageFactory(
                mock(TopologyService.class),
                mock(OutgoingSnapshotsManager.class),
                partitionAccess,
                mock(CatalogService.class),
                mock(Executor.class)
        );

        PartitionSnapshotStorage snapshotStorage = factory.createSnapshotStorage("", mock(RaftOptions.class));

        IllegalStateException ex = assertThrows(IllegalStateException.class, snapshotStorage::startupSnapshotMeta);
        assertThat(ex.getMessage(), is("Storage is empty, so startup snapshot should not be read"));
    }
}
