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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.startup.StartupPartitionSnapshotReader;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PartitionSnapshotStorageFactoryTest extends BaseIgniteAbstractTest {
    private final PartitionSnapshotStorage snapshotStorage = new PartitionSnapshotStorage(
            new ZonePartitionKey(1, 1),
            mock(TopologyService.class),
            mock(OutgoingSnapshotsManager.class),
            mock(PartitionTxStateAccess.class),
            mock(CatalogService.class),
            mock(FailureProcessor.class),
            mock(Executor.class),
            mock(LogStorageAccess.class)
    );

    @Mock
    private RaftOptions raftOptions;

    @Test
    void returnsNullWhenOpeningOnCleanStorage() {
        var storageFactory = new PartitionSnapshotStorageFactory(snapshotStorage);

        SnapshotStorage storage = storageFactory.createSnapshotStorage("", raftOptions);

        assertThat(storage.open(), is(nullValue()));
    }

    @Test
    void returnsNullForEmptyPartitionStorage(@Mock PartitionMvStorageAccess partitionAccess) {
        when(partitionAccess.lastAppliedIndex()).thenReturn(0L);
        lenient().when(snapshotStorage.txState().lastAppliedIndex()).thenReturn(5L);

        snapshotStorage.addMvPartition(0, partitionAccess);

        var storageFactory = new PartitionSnapshotStorageFactory(snapshotStorage);

        SnapshotStorage snapshotStorage = storageFactory.createSnapshotStorage("", raftOptions);

        assertThat(snapshotStorage.open(), is(nullValue()));
    }

    @Test
    void returnsNullForEmptyTxStorage(@Mock PartitionMvStorageAccess partitionAccess) {
        when(partitionAccess.lastAppliedIndex()).thenReturn(5L);
        when(snapshotStorage.txState().lastAppliedIndex()).thenReturn(0L);

        snapshotStorage.addMvPartition(0, partitionAccess);

        var storageFactory = new PartitionSnapshotStorageFactory(snapshotStorage);

        SnapshotStorage snapshotStorage = storageFactory.createSnapshotStorage("", raftOptions);

        assertThat(snapshotStorage.open(), is(nullValue()));
    }

    @Test
    void returnsStartupReaderWhenOpeningOnStorageHavingSomething(
            @Mock PartitionMvStorageAccess storageAccess,
            @Mock RaftGroupConfiguration configuration
    ) {
        when(storageAccess.lastAppliedIndex()).thenReturn(1L);
        when(storageAccess.lastAppliedTerm()).thenReturn(1L);
        when(storageAccess.committedGroupConfiguration()).thenReturn(configuration);

        when(snapshotStorage.txState().lastAppliedIndex()).thenReturn(2L);
        when(snapshotStorage.txState().lastAppliedTerm()).thenReturn(2L);

        snapshotStorage.addMvPartition(0, storageAccess);

        var storageFactory = new PartitionSnapshotStorageFactory(snapshotStorage);

        SnapshotStorage storage = storageFactory.createSnapshotStorage("", raftOptions);

        assertThat(storage.open(), is(instanceOf(StartupPartitionSnapshotReader.class)));
    }
}
