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

package org.apache.ignite.internal.table.distributed.raft;

import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.CommittedConfiguration;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.command.SafeTimePropagatingCommand;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommandBuilder;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.distributed.LowWatermark;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.command.BuildIndexCommand;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for the table command listener.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class PartitionCommandListenerTest {
    /** Key count. */
    private static final int KEY_COUNT = 100;

    /** Partition id. */
    private static final int PARTITION_ID = 0;

    /** Schema. */
    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT32, false)},
            new Column[]{new Column("value", NativeTypes.INT32, false)}
    );

    /** Table command listener. */
    private PartitionListener commandListener;

    /** RAFT index. */
    private final AtomicLong raftIndex = new AtomicLong();

    /** Primary index. */
    private final TableSchemaAwareIndexStorage pkStorage = new TableSchemaAwareIndexStorage(
            1,
            new TestHashIndexStorage(PARTITION_ID, null),
            BinaryRowConverter.keyExtractor(SCHEMA)
    );

    /** Partition storage. */
    private final MvPartitionStorage mvPartitionStorage = spy(new TestMvPartitionStorage(PARTITION_ID));

    private final PartitionDataStorage partitionDataStorage = spy(new TestPartitionDataStorage(mvPartitionStorage));

    /** Transaction meta storage. */
    private final TxStateStorage txStateStorage = spy(new TestTxStateStorage());

    /** Work directory. */
    @WorkDirectory
    private Path workDir;

    /** Factory for command messages. */
    private final TableMessagesFactory msgFactory = new TableMessagesFactory();

    /** Factory for replica messages. */
    private final ReplicaMessagesFactory replicaMessagesFactory = new ReplicaMessagesFactory();

    /** Hybrid clock. */
    private final HybridClock hybridClock = new HybridClockImpl();

    /** Safe time tracker. */
    private PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker;

    @Captor
    private ArgumentCaptor<Throwable> commandClosureResultCaptor;

    private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

    private IndexUpdateHandler indexUpdateHandler;

    private StorageUpdateHandler storageUpdateHandler;

    /**
     * Initializes a table listener before tests.
     */
    @BeforeEach
    public void before(@InjectConfiguration GcConfiguration gcConfig) {
        NetworkAddress addr = new NetworkAddress("127.0.0.1", 5003);

        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

        when(clusterService.topologyService().localMember().address()).thenReturn(addr);

        safeTimeTracker = new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0));

        indexUpdateHandler = spy(new IndexUpdateHandler(
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(pkStorage.id(), pkStorage))
        ));

        storageUpdateHandler = spy(new StorageUpdateHandler(
                PARTITION_ID,
                partitionDataStorage,
                gcConfig,
                mock(LowWatermark.class),
                indexUpdateHandler,
                new GcUpdateHandler(partitionDataStorage, safeTimeTracker, indexUpdateHandler)
        ));

        commandListener = new PartitionListener(
                partitionDataStorage,
                storageUpdateHandler,
                txStateStorage,
                safeTimeTracker,
                new PendingComparableValuesTracker<>(0L)
        );
    }

    /**
     * Inserts rows and checks them.
     */
    @Test
    public void testInsertCommands() {
        readAndCheck(false);

        insert();

        readAndCheck(true);

        delete();
    }

    /**
     * Upserts rows and checks them.
     */
    @Test
    public void testUpdateValues() {
        readAndCheck(false);

        insert();

        readAndCheck(true);

        update(integer -> integer + 1);

        readAndCheck(true, integer -> integer + 1);

        delete();

        readAndCheck(false);
    }

    /**
     * The test checks a batch upsert command.
     */
    @Test
    public void testUpsertRowsBatchedAndCheck() {
        readAndCheck(false);

        insertAll();

        readAndCheck(true);

        updateAll(integer -> integer + 2);

        readAndCheck(true, integer -> integer + 2);

        deleteAll();

        readAndCheck(false);
    }

    /**
     * The test checks a batch insert command.
     */
    @Test
    public void testInsertRowsBatchedAndCheck() {
        readAndCheck(false);

        insertAll();

        readAndCheck(true);

        deleteAll();

        readAndCheck(false);
    }

    /**
     * The test checks that {@link PartitionListener#onSnapshotSave(Path, Consumer)} propagates
     * the maximal last applied index among storages to all storages.
     */
    @Test
    public void testOnSnapshotSavePropagateLastAppliedIndexAndTerm(@InjectConfiguration GcConfiguration gcConfig) {
        TestPartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(mvPartitionStorage);

        IndexUpdateHandler indexUpdateHandler1 = new IndexUpdateHandler(
                DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of(pkStorage.id(), pkStorage))
        );

        StorageUpdateHandler storageUpdateHandler = new StorageUpdateHandler(
                PARTITION_ID,
                partitionDataStorage,
                gcConfig,
                mock(LowWatermark.class),
                indexUpdateHandler1,
                new GcUpdateHandler(partitionDataStorage, safeTimeTracker, indexUpdateHandler1)
        );

        PartitionListener testCommandListener = new PartitionListener(
                partitionDataStorage,
                storageUpdateHandler,
                txStateStorage,
                safeTimeTracker,
                new PendingComparableValuesTracker<>(0L)
        );

        txStateStorage.lastApplied(3L, 1L);

        partitionDataStorage.lastApplied(5L, 2L);

        AtomicLong counter = new AtomicLong(0);

        testCommandListener.onSnapshotSave(workDir, (throwable) -> counter.incrementAndGet());

        assertEquals(1L, counter.get());

        assertEquals(5L, partitionDataStorage.lastAppliedIndex());
        assertEquals(2L, partitionDataStorage.lastAppliedTerm());

        assertEquals(5L, txStateStorage.lastAppliedIndex());
        assertEquals(2L, txStateStorage.lastAppliedTerm());

        txStateStorage.lastApplied(10L, 2L);

        partitionDataStorage.lastApplied(7L, 1L);

        testCommandListener.onSnapshotSave(workDir, (throwable) -> counter.incrementAndGet());

        assertEquals(2L, counter.get());

        assertEquals(10L, partitionDataStorage.lastAppliedIndex());
        assertEquals(2L, partitionDataStorage.lastAppliedTerm());

        assertEquals(10L, txStateStorage.lastAppliedIndex());
        assertEquals(2L, txStateStorage.lastAppliedTerm());
    }

    @Test
    void testSkipWriteCommandByAppliedIndex() {
        mvPartitionStorage.lastApplied(10L, 1L);

        HybridTimestamp timestamp = hybridClock.now();

        UpdateCommand updateCommand = mock(UpdateCommand.class);
        when(updateCommand.safeTime()).thenReturn(timestamp);

        TxCleanupCommand txCleanupCommand = mock(TxCleanupCommand.class);
        when(txCleanupCommand.safeTime()).thenReturn(timestamp);

        SafeTimeSyncCommand safeTimeSyncCommand = mock(SafeTimeSyncCommand.class);
        when(safeTimeSyncCommand.safeTime()).thenReturn(timestamp);

        FinishTxCommand finishTxCommand = mock(FinishTxCommand.class);
        when(finishTxCommand.safeTime()).thenReturn(timestamp);

        // Checks for MvPartitionStorage.
        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, 1, updateCommand, commandClosureResultCaptor),
                writeCommandCommandClosure(10, 1, updateCommand, commandClosureResultCaptor),
                writeCommandCommandClosure(4, 1, txCleanupCommand, commandClosureResultCaptor),
                writeCommandCommandClosure(5, 1, safeTimeSyncCommand, commandClosureResultCaptor)
        ).iterator());

        verify(mvPartitionStorage, never()).runConsistently(any(WriteClosure.class));
        verify(mvPartitionStorage, times(1)).lastApplied(anyLong(), anyLong());

        assertThat(commandClosureResultCaptor.getAllValues(), containsInAnyOrder(new Throwable[]{null, null, null, null}));

        // Checks for TxStateStorage.
        mvPartitionStorage.lastApplied(1L, 1L);
        txStateStorage.lastApplied(10L, 2L);

        commandClosureResultCaptor = ArgumentCaptor.forClass(Throwable.class);

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(2, 1, finishTxCommand, commandClosureResultCaptor),
                writeCommandCommandClosure(10, 1, finishTxCommand, commandClosureResultCaptor)
        ).iterator());

        verify(txStateStorage, never()).compareAndSet(any(UUID.class), any(TxState.class), any(TxMeta.class), anyLong(), anyLong());
        verify(txStateStorage, times(1)).lastApplied(anyLong(), anyLong());

        assertThat(commandClosureResultCaptor.getAllValues(), containsInAnyOrder(new Throwable[]{null, null}));
    }

    @Test
    void updatesLastAppliedForSafeTimeSyncCommands() {
        SafeTimeSyncCommand safeTimeSyncCommand = new ReplicaMessagesFactory()
                .safeTimeSyncCommand()
                .safeTimeLong(hybridClock.nowLong())
                .build();

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, 2, safeTimeSyncCommand, commandClosureResultCaptor)
        ).iterator());

        verify(mvPartitionStorage).lastApplied(3, 2);
    }

    @Test
    void locksOnCommandApplication() {
        SafeTimeSyncCommandBuilder safeTimeSyncCommand = new ReplicaMessagesFactory()
                .safeTimeSyncCommand()
                .safeTimeLong(hybridClock.nowLong());

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, 2, safeTimeSyncCommand.build(), commandClosureResultCaptor)
        ).iterator());

        InOrder inOrder = inOrder(partitionDataStorage);

        inOrder.verify(partitionDataStorage).acquirePartitionSnapshotsReadLock();
        inOrder.verify(partitionDataStorage).lastApplied(3, 2);
        inOrder.verify(partitionDataStorage).releasePartitionSnapshotsReadLock();
    }

    @Test
    void updatesGroupConfigurationOnConfigCommit() {
        commandListener.onConfigurationCommitted(new CommittedConfiguration(
                1, 2, List.of("peer"), List.of("learner"), List.of("old-peer"), List.of("old-learner")
        ));

        RaftGroupConfiguration expectedConfig = new RaftGroupConfiguration(
                List.of("peer"),
                List.of("learner"),
                List.of("old-peer"),
                List.of("old-learner")
        );
        verify(mvPartitionStorage).committedGroupConfiguration(
                raftGroupConfigurationConverter.toBytes(expectedConfig)
        );
    }

    @Test
    void updatesLastAppliedIndexAndTermOnConfigCommit() {
        commandListener.onConfigurationCommitted(new CommittedConfiguration(
                1, 2, List.of("peer"), List.of("learner"), List.of("old-peer"), List.of("old-learner")
        ));

        verify(mvPartitionStorage).lastApplied(1, 2);
    }

    @Test
    void skipsUpdatesOnConfigCommitIfIndexIsStale() {
        mvPartitionStorage.lastApplied(10, 3);

        commandListener.onConfigurationCommitted(new CommittedConfiguration(
                1, 2, List.of("peer"), List.of("learner"), List.of("old-peer"), List.of("old-learner")
        ));

        verify(mvPartitionStorage, never()).committedGroupConfiguration(any());
        verify(mvPartitionStorage, never()).lastApplied(eq(1L), anyLong());
    }

    @Test
    void locksOnConfigCommit() {
        commandListener.onConfigurationCommitted(new CommittedConfiguration(
                1, 2, List.of("peer"), List.of("learner"), List.of("old-peer"), List.of("old-learner")
        ));

        InOrder inOrder = inOrder(partitionDataStorage);

        inOrder.verify(partitionDataStorage).acquirePartitionSnapshotsReadLock();
        inOrder.verify(partitionDataStorage).lastApplied(1, 2);
        inOrder.verify(partitionDataStorage).releasePartitionSnapshotsReadLock();
    }

    @Test
    public void testSafeTime() {
        HybridClock testClock = new TestHybridClock(() -> 1);

        applySafeTimeCommand(SafeTimeSyncCommand.class, testClock.now());
        applySafeTimeCommand(SafeTimeSyncCommand.class, testClock.now());
    }

    @Test
    void testBuildIndexCommand() {
        int indexId = 1;

        doNothing().when(indexUpdateHandler).buildIndex(eq(indexId), any(Stream.class), any());

        RowId row0 = new RowId(PARTITION_ID);
        RowId row1 = new RowId(PARTITION_ID);
        RowId row2 = new RowId(PARTITION_ID);

        InOrder inOrder = inOrder(partitionDataStorage, indexUpdateHandler);

        commandListener.handleBuildIndexCommand(createBuildIndexCommand(indexId, List.of(row0.uuid()), false), 10, 1);

        inOrder.verify(indexUpdateHandler).buildIndex(eq(indexId), any(Stream.class), eq(row0.increment()));
        inOrder.verify(partitionDataStorage).lastApplied(10, 1);

        commandListener.handleBuildIndexCommand(createBuildIndexCommand(indexId, List.of(row1.uuid()), true), 20, 2);

        inOrder.verify(indexUpdateHandler).buildIndex(eq(indexId), any(Stream.class), eq(null));
        inOrder.verify(partitionDataStorage).lastApplied(20, 2);

        // Let's check that the command with a lower commandIndex than in the storage will not be executed.
        commandListener.handleBuildIndexCommand(createBuildIndexCommand(indexId, List.of(row2.uuid()), false), 5, 1);

        inOrder.verify(indexUpdateHandler, never()).buildIndex(eq(indexId), any(Stream.class), eq(row2.increment()));
        inOrder.verify(partitionDataStorage, never()).lastApplied(5, 1);
    }

    private BuildIndexCommand createBuildIndexCommand(int indexId, List<UUID> rowUuids, boolean finish) {
        return msgFactory.buildIndexCommand()
                .tablePartitionId(
                        msgFactory.tablePartitionIdMessage()
                                .tableId(1)
                                .partitionId(PARTITION_ID)
                                .build()
                )
                .indexId(indexId)
                .rowIds(rowUuids)
                .finish(finish)
                .build();
    }

    private void applySafeTimeCommand(Class<? extends SafeTimePropagatingCommand> cls, HybridTimestamp timestamp) {
        SafeTimePropagatingCommand command = mock(cls);
        when(command.safeTime()).thenReturn(timestamp);

        CommandClosure<WriteCommand> closure = writeCommandCommandClosure(1, 1, command, commandClosureResultCaptor);
        commandListener.onWrite(asList(closure).iterator());
        assertEquals(timestamp, safeTimeTracker.current());
    }

    /**
     * Crate a command closure.
     *
     * @param index Index of the RAFT command.
     * @param writeCommand Write command.
     * @param resultClosureCaptor Captor for {@link CommandClosure#result(Serializable)}
     */
    private static CommandClosure<WriteCommand> writeCommandCommandClosure(
            long index,
            long term,
            WriteCommand writeCommand,
            ArgumentCaptor<Throwable> resultClosureCaptor
    ) {
        CommandClosure<WriteCommand> commandClosure = mock(CommandClosure.class);

        when(commandClosure.index()).thenReturn(index);
        when(commandClosure.term()).thenReturn(term);
        when(commandClosure.command()).thenReturn(writeCommand);

        doNothing().when(commandClosure).result(resultClosureCaptor.capture());

        return commandClosure;
    }

    /**
     * Prepares a closure iterator for a specific batch operation.
     *
     * @param func The function prepare a closure for the operation.
     * @param <T>  Type of the operation.
     * @return Closure iterator.
     */
    private <T extends Command> Iterator<CommandClosure<T>> batchIterator(Consumer<CommandClosure<T>> func) {
        return new Iterator<>() {
            boolean moved;

            @Override
            public boolean hasNext() {
                return !moved;
            }

            @Override
            public CommandClosure<T> next() {
                CommandClosure<T> clo = mock(CommandClosure.class);

                func.accept(clo);

                moved = true;

                return clo;
            }
        };
    }

    /**
     * Prepares a closure iterator for a specific operation.
     *
     * @param func The function prepare a closure for the operation.
     * @param <T>  Type of the operation.
     * @return Closure iterator.
     */
    private <T extends Command> Iterator<CommandClosure<T>> iterator(BiConsumer<Integer, CommandClosure<T>> func) {
        return new Iterator<>() {
            /** Iteration. */
            private int it = 0;

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                return it < KEY_COUNT;
            }

            /** {@inheritDoc} */
            @Override
            public CommandClosure<T> next() {
                CommandClosure<T> clo = mock(CommandClosure.class);

                func.accept(it, clo);

                it++;

                return clo;
            }
        };
    }

    /**
     * Inserts all rows.
     */
    private void insertAll() {
        Map<UUID, ByteBuffer> rows = new HashMap<>(KEY_COUNT);
        UUID txId = TestTransactionIds.newTransactionId();
        var commitPartId = new TablePartitionId(1, PARTITION_ID);

        for (int i = 0; i < KEY_COUNT; i++) {
            Row row = getTestRow(i, i);

            rows.put(TestTransactionIds.newTransactionId(), row.byteBuffer());
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        invokeBatchedCommand(msgFactory.updateAllCommand()
                .tablePartitionId(
                        msgFactory.tablePartitionIdMessage()
                                .tableId(commitPartId.tableId())
                                .partitionId(commitPartId.partitionId())
                                .build())
                .rowsToUpdate(rows)
                .txId(txId)
                .safeTimeLong(hybridClock.nowLong())
                .build());

        invokeBatchedCommand(msgFactory.txCleanupCommand()
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTimestamp.longValue())
                .safeTimeLong(hybridClock.nowLong())
                .build());
    }

    /**
     * Update values from the listener in the batch operation.
     *
     * @param keyValueMapper Mep a value to update to the iter number.
     */
    private void updateAll(Function<Integer, Integer> keyValueMapper) {
        UUID txId = TestTransactionIds.newTransactionId();
        var commitPartId = new TablePartitionId(1, PARTITION_ID);
        Map<UUID, ByteBuffer> rows = new HashMap<>(KEY_COUNT);

        for (int i = 0; i < KEY_COUNT; i++) {
            Row row = getTestRow(i, keyValueMapper.apply(i));

            rows.put(readRow(row).uuid(), row.byteBuffer());
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        invokeBatchedCommand(msgFactory.updateAllCommand()
                .tablePartitionId(
                        msgFactory.tablePartitionIdMessage()
                                .tableId(commitPartId.tableId())
                                .partitionId(commitPartId.partitionId())
                                .build())
                .rowsToUpdate(rows)
                .txId(txId)
                .safeTimeLong(hybridClock.nowLong())
                .build());

        invokeBatchedCommand(msgFactory.txCleanupCommand()
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTimestamp.longValue())
                .safeTimeLong(hybridClock.nowLong())
                .build());
    }

    /**
     * Deletes all rows.
     */
    private void deleteAll() {
        UUID txId = TestTransactionIds.newTransactionId();
        var commitPartId = new TablePartitionId(1, PARTITION_ID);
        Map<UUID, ByteBuffer> keyRows = new HashMap<>(KEY_COUNT);

        for (int i = 0; i < KEY_COUNT; i++) {
            Row row = getTestRow(i, i);

            keyRows.put(readRow(row).uuid(), null);
        }

        HybridTimestamp commitTimestamp = hybridClock.now();

        invokeBatchedCommand(msgFactory.updateAllCommand()
                .tablePartitionId(
                        msgFactory.tablePartitionIdMessage()
                                .tableId(commitPartId.tableId())
                                .partitionId(commitPartId.partitionId())
                                .build())
                .rowsToUpdate(keyRows)
                .txId(txId)
                .safeTimeLong(hybridClock.nowLong())
                .build());

        invokeBatchedCommand(msgFactory.txCleanupCommand()
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTimestamp.longValue())
                .safeTimeLong(hybridClock.nowLong())
                .build());
    }

    /**
     * Update rows.
     *
     * @param keyValueMapper Mep a value to update to the iter number.
     */
    private void update(Function<Integer, Integer> keyValueMapper) {
        List<UUID> txIds = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = TestTransactionIds.newTransactionId();
            Row row = getTestRow(i, keyValueMapper.apply(i));
            RowId rowId = readRow(row);

            assertNotNull(rowId);

            txIds.add(txId);

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(
                    msgFactory.updateCommand()
                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                    .tableId(1)
                                    .partitionId(PARTITION_ID).build())
                            .rowUuid(rowId.uuid())
                            .rowBuffer(row.byteBuffer())
                            .txId(txId)
                            .safeTimeLong(hybridClock.nowLong())
                            .build());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        HybridTimestamp commitTimestamp = hybridClock.now();

        txIds.forEach(txId -> invokeBatchedCommand(msgFactory.txCleanupCommand()
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTimestamp.longValue())
                .safeTimeLong(hybridClock.nowLong())
                .build()));
    }

    /**
     * Deletes row.
     */
    private void delete() {
        List<UUID> txIds = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = TestTransactionIds.newTransactionId();
            Row row = getTestRow(i, i);
            RowId rowId = readRow(row);

            assertNotNull(rowId);

            txIds.add(txId);

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(
                    msgFactory.updateCommand()
                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                    .tableId(1)
                                    .partitionId(PARTITION_ID).build())
                            .rowUuid(rowId.uuid())
                            .txId(txId)
                            .safeTimeLong(hybridClock.nowLong())
                            .build());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        HybridTimestamp commitTimestamp = hybridClock.now();

        txIds.forEach(txId -> invokeBatchedCommand(msgFactory.txCleanupCommand()
                .txId(txId)
                .commit(true)
                .commitTimestampLong(commitTimestamp.longValue())
                .safeTimeLong(hybridClock.nowLong())
                .build()));
    }

    /**
     * Reads rows from the listener and checks them.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void readAndCheck(boolean existed) {
        readAndCheck(existed, i -> i);
    }

    /**
     * Reads rows from the listener and checks values as expected by a mapper.
     *
     * @param existed        True if rows are existed, false otherwise.
     * @param keyValueMapper Mapper a key to the value which will be expected.
     */
    private void readAndCheck(boolean existed, Function<Integer, Integer> keyValueMapper) {
        for (int i = 0; i < KEY_COUNT; i++) {
            Row keyRow = getTestKey(i);

            RowId rowId = readRow(keyRow);

            if (existed) {
                ReadResult readResult = mvPartitionStorage.read(rowId, HybridTimestamp.MAX_VALUE);

                Row row = new Row(SCHEMA, readResult.binaryRow());

                assertEquals(i, row.intValue(0));
                assertEquals(keyValueMapper.apply(i), row.intValue(1));
            } else {
                assertNull(rowId);
            }
        }
    }

    /**
     * Inserts row.
     */
    private void insert() {
        List<UUID> txIds = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = TestTransactionIds.newTransactionId();
            Row row = getTestRow(i, i);
            txIds.add(txId);

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(
                    msgFactory.updateCommand()
                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                    .tableId(1)
                                    .partitionId(PARTITION_ID).build())
                            .rowUuid(UUID.randomUUID())
                            .rowBuffer(row.byteBuffer())
                            .txId(txId)
                            .safeTimeLong(hybridClock.nowLong())
                            .build());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        long commitTimestamp = hybridClock.nowLong();

        txIds.forEach(txId -> invokeBatchedCommand(
                msgFactory.txCleanupCommand()
                        .txId(txId)
                        .commit(true)
                        .commitTimestampLong(commitTimestamp)
                        .safeTimeLong(hybridClock.nowLong())
                        .build()));
    }

    /**
     * Prepares a test row which contains only key field.
     *
     * @return Row.
     */
    private Row getTestKey(int key) {
        RowAssembler rowBuilder = RowAssembler.keyAssembler(SCHEMA);

        rowBuilder.appendInt(key);

        return new Row(SCHEMA, rowBuilder.build());
    }

    /**
     * Prepares a test binary row which contains key and value fields.
     *
     * @return Row.
     */
    private Row getTestRow(int key, int val) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA);

        rowBuilder.appendInt(key);
        rowBuilder.appendInt(val);

        return new Row(SCHEMA, rowBuilder.build());
    }

    private void invokeBatchedCommand(WriteCommand cmd) {
        commandListener.onWrite(batchIterator(clo -> {
            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());

            when(clo.command()).thenReturn(cmd);
        }));
    }

    private RowId readRow(BinaryRow binaryRow) {
        try (Cursor<RowId> cursor = pkStorage.get(binaryRow)) {
            while (cursor.hasNext()) {
                RowId rowId = cursor.next();

                ReadResult readResult = mvPartitionStorage.read(rowId, HybridTimestamp.MAX_VALUE);

                if (!readResult.isEmpty() && readResult.binaryRow() != null) {
                    return rowId;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }
}
