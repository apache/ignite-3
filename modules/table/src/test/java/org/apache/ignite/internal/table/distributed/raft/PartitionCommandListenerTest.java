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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
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
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.command.SafeTimeSyncCommand;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.command.TxCleanupCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * Tests for the table command listener.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class PartitionCommandListenerTest {
    /** Key count. */
    public static final int KEY_COUNT = 100;

    /** Partition id. */
    public static final int PARTITION_ID = 0;

    /** Schema. */
    public static SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT32, false)},
            new Column[]{new Column("value", NativeTypes.INT32, false)}
    );

    /** Hybrid clock. */
    private static final HybridClock CLOCK = new HybridClockImpl();

    /** Table command listener. */
    private PartitionListener commandListener;

    /** RAFT index. */
    private final AtomicLong raftIndex = new AtomicLong();

    /** Primary index. */
    private final TableSchemaAwareIndexStorage pkStorage = new TableSchemaAwareIndexStorage(
            UUID.randomUUID(),
            new TestHashIndexStorage(null),
            tableRow -> new BinaryTuple(
                    BinaryTupleSchema.create(new Element[]{
                            new Element(NativeTypes.BYTES, false)
                    }),
                    tableRow.keySlice()
            )
    );

    /** Partition storage. */
    private final MvPartitionStorage mvPartitionStorage = spy(new TestMvPartitionStorage(PARTITION_ID));

    /** Transaction meta storage. */
    private final TxStateStorage txStateStorage = spy(new TestTxStateStorage());

    /** Work directory. */
    @WorkDirectory
    private Path workDir;

    /** Factory for command messages. */
    private TableMessagesFactory msgFactory = new TableMessagesFactory();

    /**
     * Initializes a table listener before tests.
     */
    @BeforeEach
    public void before() {
        NetworkAddress addr = new NetworkAddress("127.0.0.1", 5003);

        ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

        when(clusterService.topologyService().localMember().address()).thenReturn(addr);

        ReplicaService replicaService = mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        commandListener = new PartitionListener(
                new TestPartitionDataStorage(mvPartitionStorage),
                txStateStorage,
                new TxManagerImpl(replicaService, new HeapLockManager(), new HybridClockImpl()),
                () -> Map.of(pkStorage.id(), pkStorage),
                PARTITION_ID
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
    public void testOnSnapshotSavePropagateLastAppliedIndex() {
        ReplicaService replicaService = mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        TestPartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(mvPartitionStorage);

        PartitionListener testCommandListener = new PartitionListener(
                partitionDataStorage,
                txStateStorage,
                new TxManagerImpl(replicaService, new HeapLockManager(), new HybridClockImpl()),
                () -> Map.of(pkStorage.id(), pkStorage),
                PARTITION_ID
        );

        txStateStorage.lastAppliedIndex(3L);

        partitionDataStorage.lastAppliedIndex(5L);

        AtomicLong counter = new AtomicLong(0);

        testCommandListener.onSnapshotSave(workDir, (throwable) -> counter.incrementAndGet());

        assertEquals(1L, counter.get());

        assertEquals(5L, partitionDataStorage.lastAppliedIndex());

        assertEquals(5L, txStateStorage.lastAppliedIndex());

        txStateStorage.lastAppliedIndex(10L);

        partitionDataStorage.lastAppliedIndex(7L);

        testCommandListener.onSnapshotSave(workDir, (throwable) -> counter.incrementAndGet());

        assertEquals(2L, counter.get());

        assertEquals(10L, partitionDataStorage.lastAppliedIndex());

        assertEquals(10L, txStateStorage.lastAppliedIndex());
    }

    @Test
    void testSkipWriteCommandByAppliedIndex() {
        mvPartitionStorage.lastAppliedIndex(10L);

        ArgumentCaptor<Throwable> commandClosureResultCaptor = ArgumentCaptor.forClass(Throwable.class);

        // Checks for MvPartitionStorage.
        commandListener.onWrite(List.of(
                writeCommandCommandClosure(3, mock(UpdateCommand.class), commandClosureResultCaptor),
                writeCommandCommandClosure(10, mock(UpdateCommand.class), commandClosureResultCaptor),
                writeCommandCommandClosure(4, mock(TxCleanupCommand.class), commandClosureResultCaptor),
                writeCommandCommandClosure(5, mock(SafeTimeSyncCommand.class), commandClosureResultCaptor)
        ).iterator());

        verify(mvPartitionStorage, never()).runConsistently(any(WriteClosure.class));
        verify(mvPartitionStorage, times(1)).lastAppliedIndex(anyLong());

        assertThat(commandClosureResultCaptor.getAllValues(), containsInAnyOrder(new Throwable[]{null, null, null, null}));

        // Checks for TxStateStorage.
        mvPartitionStorage.lastAppliedIndex(1L);
        txStateStorage.lastAppliedIndex(10L);

        commandClosureResultCaptor = ArgumentCaptor.forClass(Throwable.class);

        commandListener.onWrite(List.of(
                writeCommandCommandClosure(2, mock(FinishTxCommand.class), commandClosureResultCaptor),
                writeCommandCommandClosure(10, mock(FinishTxCommand.class), commandClosureResultCaptor)
        ).iterator());

        verify(txStateStorage, never()).compareAndSet(any(UUID.class), any(TxState.class), any(TxMeta.class), anyLong());
        verify(txStateStorage, times(1)).lastAppliedIndex(anyLong());

        assertThat(commandClosureResultCaptor.getAllValues(), containsInAnyOrder(new Throwable[]{null, null}));
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
            WriteCommand writeCommand,
            ArgumentCaptor<Throwable> resultClosureCaptor
    ) {
        CommandClosure<WriteCommand> commandClosure = mock(CommandClosure.class);

        when(commandClosure.index()).thenReturn(index);
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
        HashMap<UUID, ByteString> rows = new HashMap<>(KEY_COUNT);
        UUID txId = Timestamp.nextVersion().toUuid();
        var commitPartId = new TablePartitionId(txId, PARTITION_ID);

        for (int i = 0; i < KEY_COUNT; i++) {
            Row row = getTestRow(i, i);

            rows.put(Timestamp.nextVersion().toUuid(), new ByteString(row.byteBuffer()));
        }

        HybridTimestamp commitTimestamp = CLOCK.now();

        invokeBatchedCommand(msgFactory.updateAllCommand()
                .tablePartitionId(
                        msgFactory.tablePartitionIdMessage()
                                .tableId(commitPartId.getTableId())
                                .partitionId(commitPartId.getPartId())
                                .build())
                .rowsToUpdate(rows)
                .txId(txId)
                .build());
        invokeBatchedCommand(msgFactory.txCleanupCommand()
                .txId(txId)
                .commit(true)
                .commitTimestamp(msgFactory.hybridTimestampMessage()
                        .physical(commitTimestamp.getPhysical())
                        .logical(commitTimestamp.getLogical())
                        .build())
                .build());
    }

    /**
     * Update values from the listener in the batch operation.
     *
     * @param keyValueMapper Mep a value to update to the iter number.
     */
    private void updateAll(Function<Integer, Integer> keyValueMapper) {
        UUID txId = Timestamp.nextVersion().toUuid();
        var commitPartId = new TablePartitionId(txId, PARTITION_ID);
        HashMap<UUID, ByteString> rows = new HashMap<>(KEY_COUNT);

        for (int i = 0; i < KEY_COUNT; i++) {
            Row row = getTestRow(i, keyValueMapper.apply(i));

            rows.put(readRow(row).uuid(), new ByteString(row.byteBuffer()));
        }

        HybridTimestamp commitTimestamp = CLOCK.now();

        invokeBatchedCommand(msgFactory.updateAllCommand()
                .tablePartitionId(
                        msgFactory.tablePartitionIdMessage()
                                .tableId(commitPartId.getTableId())
                                .partitionId(commitPartId.getPartId())
                                .build())
                .rowsToUpdate(rows)
                .txId(txId)
                .build());
        invokeBatchedCommand(msgFactory.txCleanupCommand()
                .txId(txId)
                .commit(true)
                .commitTimestamp(msgFactory.hybridTimestampMessage()
                        .physical(commitTimestamp.getPhysical())
                        .logical(commitTimestamp.getLogical())
                        .build())
                .build());
    }

    /**
     * Deletes all rows.
     */
    private void deleteAll() {
        UUID txId = Timestamp.nextVersion().toUuid();
        var commitPartId = new TablePartitionId(txId, PARTITION_ID);
        Map<UUID, ByteString> keyRows = new HashMap<>(KEY_COUNT);

        for (int i = 0; i < KEY_COUNT; i++) {
            Row row = getTestRow(i, i);

            keyRows.put(readRow(row).uuid(), null);
        }

        HybridTimestamp commitTimestamp = CLOCK.now();

        invokeBatchedCommand(msgFactory.updateAllCommand()
                .tablePartitionId(
                        msgFactory.tablePartitionIdMessage()
                                .tableId(commitPartId.getTableId())
                                .partitionId(commitPartId.getPartId())
                                .build())
                .rowsToUpdate(keyRows)
                .txId(txId)
                .build());
        invokeBatchedCommand(msgFactory.txCleanupCommand()
                .txId(txId)
                .commit(true)
                .commitTimestamp(msgFactory.hybridTimestampMessage()
                        .physical(commitTimestamp.getPhysical())
                        .logical(commitTimestamp.getLogical())
                        .build())
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
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, keyValueMapper.apply(i));
            RowId rowId = readRow(row);

            assertNotNull(rowId);

            txIds.add(txId);

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(
                    msgFactory.updateCommand()
                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                    .tableId(txId)
                                    .partitionId(PARTITION_ID).build())
                            .rowUuid(new UUID(rowId.mostSignificantBits(), rowId.leastSignificantBits()))
                            .rowBuffer(new ByteString(row.byteBuffer()))
                            .txId(txId)
                            .build());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        HybridTimestamp commitTimestamp = CLOCK.now();

        txIds.forEach(txId -> invokeBatchedCommand(msgFactory.txCleanupCommand()
                .txId(txId)
                .commit(true)
                .commitTimestamp(msgFactory.hybridTimestampMessage()
                        .physical(commitTimestamp.getPhysical())
                        .logical(commitTimestamp.getLogical())
                        .build())
                .build()));
    }

    /**
     * Deletes row.
     */
    private void delete() {
        List<UUID> txIds = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i);
            RowId rowId = readRow(row);

            assertNotNull(rowId);

            txIds.add(txId);

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(
                    msgFactory.updateCommand()
                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                    .tableId(txId)
                                    .partitionId(PARTITION_ID).build())
                            .rowUuid(new UUID(rowId.mostSignificantBits(), rowId.leastSignificantBits()))
                            .txId(txId)
                            .build());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        HybridTimestamp commitTimestamp = CLOCK.now();

        txIds.forEach(txId -> invokeBatchedCommand(msgFactory.txCleanupCommand()
                .txId(txId)
                .commit(true)
                .commitTimestamp(msgFactory.hybridTimestampMessage()
                        .physical(commitTimestamp.getPhysical())
                        .logical(commitTimestamp.getLogical())
                        .build())
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
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i);
            var commitPartId = new TablePartitionId(txId, PARTITION_ID);
            txIds.add(txId);

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(
                    msgFactory.updateCommand()
                            .tablePartitionId(msgFactory.tablePartitionIdMessage()
                                    .tableId(txId)
                                    .partitionId(PARTITION_ID).build())
                            .rowUuid(Timestamp.nextVersion().toUuid())
                            .rowBuffer(new ByteString(row.byteBuffer()))
                            .txId(txId)
                            .build());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        HybridTimestamp now = CLOCK.now();



        txIds.forEach(txId -> invokeBatchedCommand(
                msgFactory.txCleanupCommand()
                        .txId(txId)
                        .commit(true)
                        .commitTimestamp(msgFactory.hybridTimestampMessage()
                                .physical(now.getPhysical())
                                .logical(now.getLogical())
                                .build())
                        .build()));
    }

    /**
     * Prepares a test row which contains only key field.
     *
     * @return Row.
     */
    private Row getTestKey(int key) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendInt(key);

        return new Row(SCHEMA, rowBuilder.build());
    }

    /**
     * Prepares a test row which contains key and value fields.
     *
     * @return Row.
     */
    private Row getTestRow(int key, int val) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

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

    private RowId readRow(BinaryRow tableRow) {
        try (Cursor<RowId> cursor = pkStorage.get(tableRow)) {
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
