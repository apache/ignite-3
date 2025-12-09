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

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.partition.replicator.raft.CommandResult.EMPTY_APPLIED_RESULT;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup.Commands;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommandV2;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommandV2;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionSnapshots;
import org.apache.ignite.internal.placementdriver.LeasePlacementDriver;
import org.apache.ignite.internal.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.raft.RaftGroupConfigurationSerializer;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaChangeCommand;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.Locker;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
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
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.UpdateCommandResult;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStatePartitionStorage;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
class ZonePartitionRaftListenerTest extends BaseIgniteAbstractTest {
    private static final int ZONE_ID = 0;

    private static final int PARTITION_ID = 0;

    private static final int TABLE_ID = 1;

    private static final ZonePartitionKey ZONE_PARTITION_KEY = new ZonePartitionKey(ZONE_ID, PARTITION_ID);

    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    @Captor
    private ArgumentCaptor<UpdateCommandResult> updateCommandClosureResultCaptor;

    @Captor
    private ArgumentCaptor<Throwable> commandClosureResultCaptor;

    private ZonePartitionRaftListener listener;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private OutgoingSnapshotsManager outgoingSnapshotsManager;

    @Mock
    private TxManager txManager;

    @Spy
    private TxStatePartitionStorage txStatePartitionStorage = new TestTxStatePartitionStorage();

    @Mock
    private MvPartitionStorage mvPartitionStorage;

    @InjectExecutorService
    private ExecutorService executor;

    @Mock
    private SafeTimeValuesTracker safeTimeTracker;

    private final HybridClock clock = new HybridClockImpl();

    @BeforeEach
    void setUp() {
        listener = createListener();
    }

    private ZonePartitionRaftListener createListener() {
        return new ZonePartitionRaftListener(
                new ZonePartitionId(ZONE_ID, PARTITION_ID),
                txStatePartitionStorage,
                txManager,
                new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE),
                new PendingComparableValuesTracker<>(0L),
                outgoingSnapshotsManager,
                executor
        );
    }

    @AfterEach
    void tearDown() {
        txStatePartitionStorage.close();
    }

    @Test
    void closesOngoingSnapshots(@Mock PartitionSnapshots partitionSnapshots) {
        listener.onShutdown();

        verify(outgoingSnapshotsManager).cleanupOutgoingSnapshots(ZONE_PARTITION_KEY);
    }

    @Test
    void savesSnapshotInfoToTxStorageOnSnapshotSave(
            @Mock CommandClosure<WriteCommand> writeCommandClosure,
            @Mock PrimaryReplicaChangeCommand command,
            @Mock RaftTableProcessor tableProcessor
    ) {
        when(writeCommandClosure.command()).thenReturn(command);
        when(writeCommandClosure.index()).thenReturn(25L);
        when(writeCommandClosure.term()).thenReturn(42L);

        var leaseInfo = new LeaseInfo(123L, randomUUID(), "foo");

        when(command.leaseStartTime()).thenReturn(leaseInfo.leaseStartTime());
        when(command.primaryReplicaNodeId()).thenReturn(leaseInfo.primaryReplicaNodeId());
        when(command.primaryReplicaNodeName()).thenReturn(leaseInfo.primaryReplicaNodeName());

        listener.onWrite(List.of(writeCommandClosure).iterator());

        var config = new RaftGroupConfiguration(26, 43, 111L, 110L, List.of("foo"), List.of("bar"), null, null);

        listener.onConfigurationCommitted(config, config.index(), config.term());

        when(tableProcessor.flushStorage()).thenReturn(nullCompletedFuture());

        listener.addTableProcessor(42, tableProcessor);

        var future = new CompletableFuture<Void>();

        listener.onSnapshotSave(Path.of("foo"), throwable -> {
            if (throwable == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(throwable);
            }
        });

        assertThat(future, willCompleteSuccessfully());

        var snapshotInfo = new PartitionSnapshotInfo(
                config.index(),
                config.term(),
                leaseInfo,
                VersionedSerialization.toBytes(config, RaftGroupConfigurationSerializer.INSTANCE),
                Set.of(42)
        );

        byte[] snapshotInfoBytes = VersionedSerialization.toBytes(snapshotInfo, PartitionSnapshotInfoSerializer.INSTANCE);

        verify(txStatePartitionStorage).snapshotInfo(snapshotInfoBytes);
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

        var leaseInfo = new LeaseInfo(123L, randomUUID(), "foo");

        when(command.leaseStartTime()).thenReturn(leaseInfo.leaseStartTime());
        when(command.primaryReplicaNodeId()).thenReturn(leaseInfo.primaryReplicaNodeId());
        when(command.primaryReplicaNodeName()).thenReturn(leaseInfo.primaryReplicaNodeName());

        when(mvPartitionStorage.runConsistently(any())).thenAnswer(invocation -> {
            WriteClosure<?> closure = invocation.getArgument(0);

            return closure.execute(locker);
        });

        listener.onWrite(List.of(writeCommandClosure).iterator());

        listener.onConfigurationCommitted(raftGroupConfiguration, 2L, 3L);

        PartitionListener partitionListener = partitionListener(TABLE_ID);

        listener.addTableProcessor(TABLE_ID, partitionListener);

        verify(mvPartitionStorage).lastApplied(2L, 3L);
        verify(mvPartitionStorage).committedGroupConfiguration(any());
        verify(mvPartitionStorage).updateLease(leaseInfo);
    }

    @RepeatedTest(10)
    void checkRacesOnCommittedConfigurationInitialization(@InjectExecutorService ExecutorService executor) {
        long index = 1;
        long term = 2;

        var raftGroupConfiguration = new RaftGroupConfiguration(index, term, 111L, 110L, List.of("foo"), List.of("bar"), null, null);

        var tableProcessor = new TestRaftTableProcessor();

        CompletableFuture<Void> f1 = runAsync(() -> listener.onConfigurationCommitted(raftGroupConfiguration, index, term), executor);

        CompletableFuture<Void> f2 = runAsync(() -> listener.addTableProcessor(TABLE_ID, tableProcessor), executor);

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
                .primaryReplicaNodeId(randomUUID())
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

        CompletableFuture<Void> f2 = runAsync(() -> listener.addTableProcessor(TABLE_ID, tableProcessor), executor);

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

    @Test
    void usesSnapshotInfoForRecovery(
            @Mock RaftTableProcessor tableProcessor1,
            @Mock RaftTableProcessor tableProcessor2,
            @Mock RaftTableProcessor tableProcessor3,
            @Mock CommandClosure<WriteCommand> writeCommandClosure,
            @Mock PrimaryReplicaChangeCommand command
    ) {
        when(writeCommandClosure.command()).thenReturn(command);
        when(writeCommandClosure.index()).thenReturn(1L);
        when(writeCommandClosure.term()).thenReturn(1L);

        var leaseInfo = new LeaseInfo(123L, randomUUID(), "foo");

        when(command.leaseStartTime()).thenReturn(leaseInfo.leaseStartTime());
        when(command.primaryReplicaNodeId()).thenReturn(leaseInfo.primaryReplicaNodeId());
        when(command.primaryReplicaNodeName()).thenReturn(leaseInfo.primaryReplicaNodeName());

        listener.onWrite(List.of(writeCommandClosure).iterator());

        var config = new RaftGroupConfiguration(26, 43, 111L, 110L, List.of("foo"), List.of("bar"), null, null);

        listener.onConfigurationCommitted(config, config.index(), config.term());

        when(tableProcessor1.flushStorage()).thenReturn(nullCompletedFuture());

        listener.addTableProcessor(42, tableProcessor1);

        var future = new CompletableFuture<Void>();

        listener.onSnapshotSave(Path.of("foo"), throwable -> {
            if (throwable == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(throwable);
            }
        });

        assertThat(future, willCompleteSuccessfully());

        // Emulate a restart.
        listener = createListener();

        // Adding a table processor that participated in the snapshot.
        listener.addTableProcessorOnRecovery(42, tableProcessor2);
        // Adding a table processor that did not participate in the snapshot.
        listener.addTableProcessorOnRecovery(43, tableProcessor3);

        verify(tableProcessor2, never()).initialize(any(), any(), anyLong(), anyLong());
        verify(tableProcessor3).initialize(config, leaseInfo, config.index(), config.term());
    }

    @Test
    void processorsAreNotInitializedWithoutSnapshot(@Mock RaftTableProcessor tableProcessor) {
        listener.addTableProcessorOnRecovery(42, tableProcessor);

        verify(tableProcessor, never()).initialize(any(), any(), anyLong(), anyLong());
    }

    @Test
    void testSkipWriteCommandByAppliedIndex() {
        mvPartitionStorage = spy(new TestMvPartitionStorage(PARTITION_ID));

        PartitionListener tableProcessor = partitionListener(TABLE_ID);

        listener.addTableProcessor(TABLE_ID, tableProcessor);
        // Update(All)Command handling requires both information about raft group topology and the primary replica,
        // thus onConfigurationCommited and primaryReplicaChangeCommand are called.
        AtomicInteger raftIndex = new AtomicInteger();

        long index = raftIndex.incrementAndGet();

        listener.onConfigurationCommitted(
                new RaftGroupConfiguration(
                        index,
                        1,
                        111L,
                        110L,
                        List.of("foo"),
                        List.of("bar"),
                        null,
                        null
                ),
                index,
                1
        );

        PrimaryReplicaChangeCommand command = REPLICA_MESSAGES_FACTORY.primaryReplicaChangeCommand()
                .primaryReplicaNodeName("primary")
                .primaryReplicaNodeId(randomUUID())
                .leaseStartTime(HybridTimestamp.MIN_VALUE.addPhysicalTime(1).longValue())
                .build();

        listener.onWrite(List.of(writeCommandClosure(raftIndex.incrementAndGet(), 1, command, null, null)).iterator());

        mvPartitionStorage.lastApplied(10L, 1L);

        UpdateCommandV2 updateCommand = mock(UpdateCommandV2.class);
        when(updateCommand.tableId()).thenReturn(TABLE_ID);

        WriteIntentSwitchCommand writeIntentSwitchCommand = mock(WriteIntentSwitchCommand.class);

        SafeTimeSyncCommand safeTimeSyncCommand = mock(SafeTimeSyncCommand.class);

        FinishTxCommandV2 finishTxCommand = mock(FinishTxCommandV2.class);
        when(finishTxCommand.groupType()).thenReturn(PartitionReplicationMessageGroup.GROUP_TYPE);
        when(finishTxCommand.messageType()).thenReturn(Commands.FINISH_TX_V2);

        PrimaryReplicaChangeCommand primaryReplicaChangeCommand = mock(PrimaryReplicaChangeCommand.class);

        // Checks for MvPartitionStorage.
        listener.onWrite(List.of(
                writeCommandClosure(3, 1, updateCommand, updateCommandClosureResultCaptor, clock.now()),
                writeCommandClosure(10, 1, updateCommand, updateCommandClosureResultCaptor, clock.now()),
                writeCommandClosure(4, 1, writeIntentSwitchCommand, commandClosureResultCaptor, clock.now()),
                writeCommandClosure(5, 1, safeTimeSyncCommand, commandClosureResultCaptor, clock.now()),
                writeCommandClosure(6, 1, primaryReplicaChangeCommand, commandClosureResultCaptor, null)
        ).iterator());

        // Expected runConsistently calls: the first inside listener#onConfigurationCommitted and the second inside listener#onWrite.
        verify(mvPartitionStorage, times(2)).runConsistently(any(WriteClosure.class));
        // Expected lastApplied calls places are the same as runConsistently but with the extra one mvPartitionStorage#lastApplied later in
        // in the test to set to high boundaries index and term for later listener#onWrite with different commands.
        verify(mvPartitionStorage, times(3)).lastApplied(anyLong(), anyLong());

        List<UpdateCommandResult> allValues = updateCommandClosureResultCaptor.getAllValues();
        assertThat(allValues, containsInAnyOrder(new Throwable[]{null, null}));
        assertThat(commandClosureResultCaptor.getAllValues(), containsInAnyOrder(new Throwable[]{null, null, null}));

        // Checks for TxStateStorage.
        mvPartitionStorage.lastApplied(1L, 1L);
        txStatePartitionStorage.lastApplied(10L, 2L);

        commandClosureResultCaptor = ArgumentCaptor.forClass(Throwable.class);

        listener.onWrite(List.of(
                writeCommandClosure(2, 1, finishTxCommand, commandClosureResultCaptor, clock.now()),
                writeCommandClosure(10, 1, finishTxCommand, commandClosureResultCaptor, clock.now())
        ).iterator());

        verify(txStatePartitionStorage, never())
                .compareAndSet(any(UUID.class), any(TxState.class), any(TxMeta.class), anyLong(), anyLong());
        verify(txStatePartitionStorage, times(1)).lastApplied(anyLong(), anyLong());

        assertThat(commandClosureResultCaptor.getAllValues(), containsInAnyOrder(new Throwable[]{null, null}));

        listener.removeTableProcessor(TABLE_ID);
    }

    @Test
    void updatesLastAppliedForFinishTxCommands() {
        safeTimeTracker.update(clock.now(), null);

        FinishTxCommand command = PARTITION_REPLICATION_MESSAGES_FACTORY.finishTxCommandV2()
                .txId(TestTransactionIds.newTransactionId())
                .initiatorTime(clock.now())
                .partitions(List.of())
                .build();

        listener.onWrite(List.of(
                writeCommandClosure(3, 2, command)
        ).iterator());

        assertThat(txStatePartitionStorage.lastAppliedIndex(), is(3L));
        assertThat(txStatePartitionStorage.lastAppliedTerm(), is(2L));
    }

    private CommandClosure<WriteCommand> writeCommandClosure(
            long index,
            long term,
            WriteCommand writeCommand
    ) {
        return writeCommandClosure(index, term, writeCommand, null, clock.now());
    }

    /**
     * Create a command closure.
     *
     * @param index Index of the RAFT command.
     * @param term Term of RAFT command.
     * @param writeCommand Write command.
     * @param resultClosureCaptor Captor for {@link CommandClosure#result(Serializable)}
     * @param safeTimestamp The safe timestamp.
     */
    private static CommandClosure<WriteCommand> writeCommandClosure(
            long index,
            long term,
            WriteCommand writeCommand,
            @Nullable ArgumentCaptor<? extends Serializable> resultClosureCaptor,
            @Nullable HybridTimestamp safeTimestamp
    ) {
        CommandClosure<WriteCommand> commandClosure = mock(CommandClosure.class);

        when(commandClosure.index()).thenReturn(index);
        when(commandClosure.term()).thenReturn(term);
        when(commandClosure.command()).thenReturn(writeCommand);
        when(commandClosure.safeTimestamp()).thenReturn(safeTimestamp);

        if (resultClosureCaptor != null) {
            doNothing().when(commandClosure).result(resultClosureCaptor.capture());
        }

        return commandClosure;
    }

    private PartitionListener partitionListener(int tableId) {
        LeasePlacementDriver placementDriver = mock(LeasePlacementDriver.class);
        lenient().when(placementDriver.getCurrentPrimaryReplica(any(), any())).thenReturn(null);

        ClockService clockService = mock(ClockService.class);
        lenient().when(clockService.current()).thenReturn(clock.current());

        return new PartitionListener(
                txManager,
                new SnapshotAwarePartitionDataStorage(
                        tableId,
                        mvPartitionStorage,
                        outgoingSnapshotsManager,
                        ZONE_PARTITION_KEY
                ),
                mock(StorageUpdateHandler.class),
                new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE),
                mock(CatalogService.class),
                mock(SchemaRegistry.class),
                mock(IndexMetaStorage.class),
                randomUUID(),
                mock(MinimumRequiredTimeCollectorService.class),
                mock(Executor.class),
                placementDriver,
                clockService,
                new ZonePartitionId(ZONE_ID, PARTITION_ID)
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
        public synchronized CommandResult processCommand(WriteCommand command, long commandIndex, long commandTerm,
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

            return EMPTY_APPLIED_RESULT;
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
