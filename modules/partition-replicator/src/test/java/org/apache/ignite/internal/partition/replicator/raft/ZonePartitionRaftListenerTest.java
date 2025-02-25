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

package org.apache.ignite.internal.partition.replicator.raft;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateMinimumActiveTxBeginTimeCommand;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionSnapshots;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotAwarePartitionDataStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStatePartitionStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ZonePartitionRaftListenerTest extends BaseIgniteAbstractTest {
    private static final int ZONE_ID = 0;

    private static final int PARTITION_ID = 0;

    private static final ZonePartitionKey ZONE_PARTITION_KEY = new ZonePartitionKey(ZONE_ID, PARTITION_ID);

    private ZonePartitionRaftListener listener;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private OutgoingSnapshotsManager outgoingSnapshotsManager;

    @Mock
    private TxManager txManager;

    @Spy
    private TxStatePartitionStorage txStatePartitionStorage = new TestTxStatePartitionStorage();

    @Mock
    private MvPartitionStorage mvPartitionStorage;

    @BeforeEach
    void setUp() {
        listener = new ZonePartitionRaftListener(
                new ZonePartitionId(ZONE_ID, PARTITION_ID),
                txStatePartitionStorage,
                txManager,
                new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE),
                new PendingComparableValuesTracker<>(0L),
                outgoingSnapshotsManager
        );
    }

    @AfterEach
    void tearDown() {
        txStatePartitionStorage.close();
    }

    @Test
    void closesOngoingSnapshots(@Mock PartitionSnapshots partitionSnapshots) {
        var tablePartition1 = new TablePartitionId(1, PARTITION_ID);
        var tablePartition2 = new TablePartitionId(2, PARTITION_ID);

        listener.addTableProcessor(tablePartition1, partitionListener(tablePartition1));
        listener.addTableProcessor(tablePartition2, partitionListener(tablePartition2));

        listener.onShutdown();

        verify(outgoingSnapshotsManager).cleanupOutgoingSnapshots(ZONE_PARTITION_KEY);
    }

    @Test
    void savesIndexAndTermOnSnapshotSave(
            @Mock CommandClosure<WriteCommand> writeCommandClosure,
            @Mock UpdateMinimumActiveTxBeginTimeCommand command
    ) {
        when(writeCommandClosure.command()).thenReturn(command);
        when(writeCommandClosure.index()).thenReturn(25L);
        when(writeCommandClosure.term()).thenReturn(42L);

        listener.onWrite(List.of(writeCommandClosure).iterator());

        var future = new CompletableFuture<Void>();

        listener.onSnapshotSave(Path.of("foo"), throwable -> {
            if (throwable == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(throwable);
            }
        });

        assertThat(future, willCompleteSuccessfully());

        verify(txStatePartitionStorage).lastApplied(25L, 42L);
    }

    @Test
    void loadsIndexAndTermOnSnapshotLoad() {
        listener.onSnapshotLoad(Path.of("foo"));

        verify(txStatePartitionStorage).lastAppliedIndex();
        verify(txStatePartitionStorage).lastAppliedTerm();
    }

    @Test
    void propagatesRaftMetaToPartitionListeners(
            @Mock CommandClosure<WriteCommand> writeCommandClosure,
            @Mock PrimaryReplicaChangeCommand command,
            @Mock RaftGroupConfiguration raftGroupConfiguration,
            @Mock Locker locker
    ) {
        when(writeCommandClosure.command()).thenReturn(command);
        when(writeCommandClosure.index()).thenReturn(1L);
        when(writeCommandClosure.term()).thenReturn(1L);

        UUID id = UUID.randomUUID();

        when(command.leaseStartTime()).thenReturn(123L);
        when(command.primaryReplicaNodeId()).thenReturn(id);
        when(command.primaryReplicaNodeName()).thenReturn("foo");

        when(mvPartitionStorage.runConsistently(any())).thenAnswer(invocation -> {
            WriteClosure<?> closure = invocation.getArgument(0);

            return closure.execute(locker);
        });

        listener.onWrite(List.of(writeCommandClosure).iterator());

        listener.onConfigurationCommitted(raftGroupConfiguration, 2L, 3L);

        var tablePartitionId = new TablePartitionId(1, PARTITION_ID);

        PartitionListener partitionListener = partitionListener(tablePartitionId);

        listener.addTableProcessor(tablePartitionId, partitionListener);

        verify(mvPartitionStorage).lastApplied(2L, 3L);
        verify(mvPartitionStorage).committedGroupConfiguration(any());
        verify(mvPartitionStorage).updateLease(123L, id, "foo");
    }

    private PartitionListener partitionListener(TablePartitionId tablePartitionId) {
        return new PartitionListener(
                txManager,
                new SnapshotAwarePartitionDataStorage(
                        tablePartitionId.tableId(),
                        mvPartitionStorage,
                        outgoingSnapshotsManager,
                        ZONE_PARTITION_KEY
                ),
                mock(StorageUpdateHandler.class),
                txStatePartitionStorage,
                new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE),
                new PendingComparableValuesTracker<>(0L),
                mock(CatalogService.class),
                mock(SchemaRegistry.class),
                mock(IndexMetaStorage.class),
                UUID.randomUUID(),
                mock(MinimumRequiredTimeCollectorService.class)
        );
    }
}
