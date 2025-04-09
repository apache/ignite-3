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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
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
    private static final int TABLE_ID_1 = 1;
    private static final int TABLE_ID_2 = 2;

    @Test
    void choosesMinimalIndexFromPartitionStorage(
            @Mock PartitionMvStorageAccess partitionAccess1,
            @Mock PartitionMvStorageAccess partitionAccess2,
            @Mock PartitionTxStateAccess txStateAccess
    ) {
        when(partitionAccess1.lastAppliedIndex()).thenReturn(5L);
        when(partitionAccess2.lastAppliedIndex()).thenReturn(3L);
        when(txStateAccess.lastAppliedIndex()).thenReturn(10L);

        lenient().when(partitionAccess1.lastAppliedTerm()).thenReturn(5L);
        when(partitionAccess2.lastAppliedTerm()).thenReturn(1L);
        lenient().when(txStateAccess.lastAppliedTerm()).thenReturn(10L);

        when(partitionAccess2.committedGroupConfiguration()).thenReturn(mock(RaftGroupConfiguration.class));

        PartitionSnapshotStorageFactory partitionSnapshotStorageFactory = new PartitionSnapshotStorageFactory(
                new ZonePartitionKey(0, 0),
                mock(TopologyService.class),
                mock(OutgoingSnapshotsManager.class),
                txStateAccess,
                mock(CatalogService.class),
                mock(FailureProcessor.class),
                mock(Executor.class)
        );

        partitionSnapshotStorageFactory.addMvPartition(TABLE_ID_1, partitionAccess1);
        partitionSnapshotStorageFactory.addMvPartition(TABLE_ID_2, partitionAccess2);

        PartitionSnapshotStorage snapshotStorage = partitionSnapshotStorageFactory.createSnapshotStorage("", mock(RaftOptions.class));

        assertEquals(3L, snapshotStorage.startupSnapshotMeta().lastIncludedIndex());
        assertEquals(1L, snapshotStorage.startupSnapshotMeta().lastIncludedTerm());
    }

    @Test
    void choosesMinimalIndexFromTxStorage(
            @Mock PartitionMvStorageAccess partitionAccess,
            @Mock PartitionTxStateAccess txStateAccess
    ) {
        when(partitionAccess.lastAppliedIndex()).thenReturn(5L);
        when(txStateAccess.lastAppliedIndex()).thenReturn(3L);

        lenient().when(partitionAccess.lastAppliedTerm()).thenReturn(5L);
        when(txStateAccess.lastAppliedTerm()).thenReturn(2L);

        when(txStateAccess.committedGroupConfiguration()).thenReturn(mock(RaftGroupConfiguration.class));

        PartitionSnapshotStorageFactory partitionSnapshotStorageFactory = new PartitionSnapshotStorageFactory(
                new ZonePartitionKey(0, 0),
                mock(TopologyService.class),
                mock(OutgoingSnapshotsManager.class),
                txStateAccess,
                mock(CatalogService.class),
                mock(FailureProcessor.class),
                mock(Executor.class)
        );

        partitionSnapshotStorageFactory.addMvPartition(TABLE_ID_1, partitionAccess);

        PartitionSnapshotStorage snapshotStorage = partitionSnapshotStorageFactory.createSnapshotStorage("", mock(RaftOptions.class));

        assertEquals(3L, snapshotStorage.startupSnapshotMeta().lastIncludedIndex());
        assertEquals(2L, snapshotStorage.startupSnapshotMeta().lastIncludedTerm());
    }

    @Test
    void returnsNullForEmptyPartitionStorage(
            @Mock PartitionMvStorageAccess partitionAccess,
            @Mock PartitionTxStateAccess txStateAccess
    ) {
        when(partitionAccess.lastAppliedIndex()).thenReturn(0L);
        lenient().when(txStateAccess.lastAppliedIndex()).thenReturn(5L);

        PartitionSnapshotStorageFactory partitionSnapshotStorageFactory = new PartitionSnapshotStorageFactory(
                new ZonePartitionKey(0, 0),
                mock(TopologyService.class),
                mock(OutgoingSnapshotsManager.class),
                txStateAccess,
                mock(CatalogService.class),
                mock(FailureProcessor.class),
                mock(Executor.class)
        );

        partitionSnapshotStorageFactory.addMvPartition(TABLE_ID_1, partitionAccess);

        PartitionSnapshotStorage snapshotStorage = partitionSnapshotStorageFactory.createSnapshotStorage("", mock(RaftOptions.class));

        assertThat(snapshotStorage.open(), is(nullValue()));
    }

    @Test
    void returnsNullForEmptyTxStorage(
            @Mock PartitionMvStorageAccess partitionAccess,
            @Mock PartitionTxStateAccess txStateAccess
    ) {
        when(partitionAccess.lastAppliedIndex()).thenReturn(5L);
        when(txStateAccess.lastAppliedIndex()).thenReturn(0L);

        PartitionSnapshotStorageFactory partitionSnapshotStorageFactory = new PartitionSnapshotStorageFactory(
                new ZonePartitionKey(0, 0),
                mock(TopologyService.class),
                mock(OutgoingSnapshotsManager.class),
                txStateAccess,
                mock(CatalogService.class),
                mock(FailureProcessor.class),
                mock(Executor.class)
        );

        partitionSnapshotStorageFactory.addMvPartition(TABLE_ID_1, partitionAccess);

        PartitionSnapshotStorage snapshotStorage = partitionSnapshotStorageFactory.createSnapshotStorage("", mock(RaftOptions.class));

        assertThat(snapshotStorage.open(), is(nullValue()));
    }

    @Test
    void storageThrowsOnAttemptToGetStartupMetaOnEmptyStorage() {
        var factory = new PartitionSnapshotStorageFactory(
                new ZonePartitionKey(0, 0),
                mock(TopologyService.class),
                mock(OutgoingSnapshotsManager.class),
                mock(PartitionTxStateAccess.class),
                mock(CatalogService.class),
                mock(FailureProcessor.class),
                mock(Executor.class)
        );

        PartitionSnapshotStorage snapshotStorage = factory.createSnapshotStorage("", mock(RaftOptions.class));

        IllegalStateException ex = assertThrows(IllegalStateException.class, snapshotStorage::startupSnapshotMeta);
        assertThat(ex.getMessage(), is("Storage is empty, so startup snapshot should not be read"));
    }
}
