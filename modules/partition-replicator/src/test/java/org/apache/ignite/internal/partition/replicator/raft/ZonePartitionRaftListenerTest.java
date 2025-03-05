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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateMinimumActiveTxBeginTimeCommand;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionSnapshots;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotAwarePartitionDataStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStatePartitionStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
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
        listener.addTableProcessor(1, partitionListener(1));
        listener.addTableProcessor(2, partitionListener(2));

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

        PartitionListener partitionListener = partitionListener(1);

        listener.addTableProcessor(1, partitionListener);

        verify(mvPartitionStorage).lastApplied(2L, 3L);
        verify(mvPartitionStorage).committedGroupConfiguration(any());
        verify(mvPartitionStorage).updateLease(123L, id, "foo");
    }

    @RepeatedTest(10)
    void checkRacesOnCommittedConfigurationInitialization(@InjectExecutorService ExecutorService executor) {
        long index = 1;
        long term = 2;

        var raftGroupConfiguration = new RaftGroupConfiguration(index, term, List.of("foo"), List.of("bar"), null, null);

        var tableProcessor = new TestRaftTableProcessor();

        CompletableFuture<Void> f1 = runAsync(() -> listener.onConfigurationCommitted(raftGroupConfiguration, index, term), executor);

        CompletableFuture<Void> f2 = runAsync(() -> listener.addTableProcessor(1, tableProcessor), executor);

        assertThat(allOf(f1, f2), willCompleteSuccessfully());

        assertThat(tableProcessor.raftGroupConfiguration, is(raftGroupConfiguration));
        assertThat(tableProcessor.lastAppliedIndex, is(index));
        assertThat(tableProcessor.lastAppliedTerm, is(term));
    }

    @RepeatedTest(10)
    void checkRacesOnLeaseInitialization(@InjectExecutorService ExecutorService executor) {
        long index = 1;
        long term = 2;

        PrimaryReplicaChangeCommand command = new ReplicaMessagesFactory()
                .primaryReplicaChangeCommand()
                .leaseStartTime(123)
                .primaryReplicaNodeId(UUID.randomUUID())
                .primaryReplicaNodeName("foo")
                .build();

        List<CommandClosure<WriteCommand>> closure = List.of(new CommandClosure<>() {
            @Override
            public WriteCommand command() {
                return command;
            }

            @Override
            public long index() {
                return index;
            }

            @Override
            public long term() {
                return term;
            }

            @Override
            public void result(@Nullable Serializable res) {
            }
        });

        var tableProcessor = new TestRaftTableProcessor();

        CompletableFuture<Void> f1 = runAsync(() -> listener.onWrite(closure.iterator()), executor);

        CompletableFuture<Void> f2 = runAsync(() -> listener.addTableProcessor(1, tableProcessor), executor);

        assertThat(allOf(f1, f2), willCompleteSuccessfully());

        var expectedLeaseInfo = new LeaseInfo(
                command.leaseStartTime(),
                command.primaryReplicaNodeId(),
                command.primaryReplicaNodeName()
        );

        assertThat(tableProcessor.leaseInfo, is(expectedLeaseInfo));
        assertThat(tableProcessor.lastAppliedIndex, is(index));
        assertThat(tableProcessor.lastAppliedTerm, is(term));
    }

    private PartitionListener partitionListener(int tableId) {
        return new PartitionListener(
                txManager,
                new SnapshotAwarePartitionDataStorage(
                        tableId,
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

    private static class TestRaftTableProcessor implements RaftTableProcessor {
        @Nullable
        private RaftGroupConfiguration raftGroupConfiguration;

        @Nullable
        private LeaseInfo leaseInfo;

        private long lastAppliedIndex;

        private long lastAppliedTerm;

        @Override
        public synchronized IgniteBiTuple<Serializable, Boolean> processCommand(WriteCommand command, long commandIndex, long commandTerm,
                @Nullable HybridTimestamp safeTimestamp) {
            if (command instanceof PrimaryReplicaChangeCommand) {
                PrimaryReplicaChangeCommand primaryReplicaChangeCommand = (PrimaryReplicaChangeCommand) command;

                leaseInfo = new LeaseInfo(
                        primaryReplicaChangeCommand.leaseStartTime(),
                        primaryReplicaChangeCommand.primaryReplicaNodeId(),
                        primaryReplicaChangeCommand.primaryReplicaNodeName()
                );
            }

            lastAppliedIndex = commandIndex;
            lastAppliedTerm = commandTerm;

            return new IgniteBiTuple<>(commandIndex, true);
        }

        @Override
        public synchronized void onConfigurationCommitted(RaftGroupConfiguration config, long lastAppliedIndex, long lastAppliedTerm) {
            this.raftGroupConfiguration = config;
            this.lastAppliedIndex = lastAppliedIndex;
            this.lastAppliedTerm = lastAppliedTerm;
        }

        @Override
        public synchronized void initialize(@Nullable RaftGroupConfiguration config, @Nullable LeaseInfo leaseInfo, long lastAppliedIndex,
                long lastAppliedTerm) {
            this.raftGroupConfiguration = config;
            this.leaseInfo = leaseInfo;
            this.lastAppliedIndex = lastAppliedIndex;
            this.lastAppliedTerm = lastAppliedTerm;
        }

        @Override
        public synchronized long lastAppliedIndex() {
            return lastAppliedIndex;
        }

        @Override
        public synchronized long lastAppliedTerm() {
            return lastAppliedTerm;
        }

        @Override
        public synchronized void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
            this.lastAppliedIndex = lastAppliedIndex;
            this.lastAppliedTerm = lastAppliedTerm;
        }

        @Override
        public CompletableFuture<Void> flushStorage() {
            return nullCompletedFuture();
        }

        @Override
        public void onShutdown() {
        }
    }
}
