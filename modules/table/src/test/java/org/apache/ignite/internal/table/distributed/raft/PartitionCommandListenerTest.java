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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactCommand;
import org.apache.ignite.internal.table.distributed.command.GetAllCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndDeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndUpsertCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceIfExistCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.MultiRowsResponse;
import org.apache.ignite.internal.table.distributed.command.response.SingleRowResponse;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for the table command listener.
 */
public class PartitionCommandListenerTest {
    /** Key count. */
    public static final int KEY_COUNT = 100;

    /** Schema. */
    public static SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT32, false)},
            new Column[]{new Column("value", NativeTypes.INT32, false)}
    );

    /** Table command listener. */
    private PartitionListener commandListener;

    /** Versioned row store. */
    private VersionedRowStore versionedRowStore;

    /** RAFT index. */
    private AtomicLong raftIndex = new AtomicLong();

    /**
     * Initializes a table listener before tests.
     */
    @BeforeEach
    public void before() {
        ClusterService clusterService = Mockito.mock(ClusterService.class, RETURNS_DEEP_STUBS);
        NetworkAddress addr = new NetworkAddress("127.0.0.1", 5003);
        Mockito.when(clusterService.topologyService().localMember().address()).thenReturn(addr);

        versionedRowStore = new VersionedRowStore(
                new TestMvPartitionStorage(0),
                new TxManagerImpl(clusterService, new HeapLockManager())
        );

        commandListener = new PartitionListener(
                UUID.randomUUID(),
                versionedRowStore
        );
    }

    /**
     * Inserts rows and checks them.
     */
    @Test
    public void testInsertCommands() {
        readAndCheck(false);

        delete(false);

        insert(false);

        insert(true);

        readAndCheck(true);

        delete(true);
    }

    /**
     * Upserts rows and checks them.
     */
    @Test
    public void testUpsertValues() {
        readAndCheck(false);

        upsert();

        readAndCheck(true);

        delete(true);

        readAndCheck(false);
    }

    /**
     * Adds rows, replaces and checks them.
     */
    @Test
    public void testReplaceCommand() {
        upsert();

        deleteExactValues(false);

        replaceValues(true);

        readAndCheck(true, i -> i + 1);

        replaceValues(false);

        readAndCheck(true, i -> i + 1);

        deleteExactValues(true);

        readAndCheck(false);
    }

    /**
     * The test checks PutIfExist command.
     */
    @Test
    public void testPutIfExistCommand() {
        putIfExistValues(false);

        readAndCheck(false);

        upsert();

        putIfExistValues(true);

        readAndCheck(true, i -> i + 1);

        getAndDeleteValues(true);

        readAndCheck(false);

        getAndDeleteValues(false);
    }

    /**
     * The test checks GetAndReplace command.
     */
    @Test
    public void testGetAndReplaceCommand() {
        readAndCheck(false);

        getAndUpsertValues(false);

        readAndCheck(true);

        getAndReplaceValues(true);

        readAndCheck(true, i -> i + 1);

        getAndUpsertValues(true);

        readAndCheck(true);

        deleteExactAllValues(true);

        readAndCheck(false);

        getAndReplaceValues(false);

        deleteExactAllValues(false);
    }

    /**
     * The test checks a batch upsert command.
     */
    @Test
    public void testUpsertRowsBatchedAndCheck() {
        readAll(false);

        deleteAll(false);

        upsertAll();

        readAll(true);

        deleteAll(true);

        readAll(false);
    }

    /**
     * The test checks a batch insert command.
     */
    @Test
    public void testInsertRowsBatchedAndCheck() {
        readAll(false);

        deleteAll(false);

        insertAll(false);

        readAll(true);

        insertAll(true);

        deleteAll(true);

        readAll(false);
    }

    /**
     * Prepares a closure iterator for a specific batch operation.
     *
     * @param func The function prepare a closure for the operation.
     * @param <T>  Type of the operation.
     * @return Closure iterator.
     */
    private <T extends Command> Iterator<CommandClosure<T>> batchIterator(Consumer<CommandClosure<T>> func) {
        return new Iterator<CommandClosure<T>>() {
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
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void insertAll(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(batchIterator(clo -> {
            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            doAnswer(invocation -> {
                MultiRowsResponse resp = invocation.getArgument(0);

                if (existed) {
                    assertEquals(KEY_COUNT, resp.getValues().size());

                    for (BinaryRow binaryRow : resp.getValues()) {
                        Row row = new Row(SCHEMA, binaryRow);

                        int keyVal = row.intValue(0);

                        assertTrue(keyVal < KEY_COUNT);
                        assertEquals(keyVal, row.intValue(1));
                    }
                } else {
                    assertTrue(resp.getValues().isEmpty());
                }

                return null;
            }).when(clo).result(any(MultiRowsResponse.class));

            Set<BinaryRow> rows = new HashSet<>(KEY_COUNT);
            UUID txId = Timestamp.nextVersion().toUuid();

            for (int i = 0; i < KEY_COUNT; i++) {
                Row row = getTestRow(i, i);

                rows.add(row);

                txs.add(new IgniteBiTuple<>(row, txId));
            }

            when(clo.command()).thenReturn(new InsertAllCommand(rows, txId));
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Upserts values from the listener in the batch operation.
     */
    private void upsertAll() {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(batchIterator(clo -> {
            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());

            Set<BinaryRow> rows = new HashSet<>(KEY_COUNT);

            UUID txId = Timestamp.nextVersion().toUuid();

            for (int i = 0; i < KEY_COUNT; i++) {
                Row row = getTestRow(i, i);

                rows.add(row);

                txs.add(new IgniteBiTuple<>(row, txId));
            }

            when(clo.command()).thenReturn(new UpsertAllCommand(rows, Timestamp.nextVersion().toUuid()));
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Deletes all rows.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void deleteAll(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(batchIterator(clo -> {
            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            doAnswer(invocation -> {
                MultiRowsResponse resp = invocation.getArgument(0);

                if (!existed) {
                    assertEquals(KEY_COUNT, resp.getValues().size());

                    for (BinaryRow binaryRow : resp.getValues()) {
                        Row row = new Row(SCHEMA, binaryRow);

                        int keyVal = row.intValue(0);

                        assertTrue(keyVal < KEY_COUNT);
                    }
                } else {
                    assertTrue(resp.getValues().isEmpty());
                }

                return null;
            }).when(clo).result(any(MultiRowsResponse.class));

            Set<BinaryRow> keyRows = new HashSet<>(KEY_COUNT);

            UUID txId = Timestamp.nextVersion().toUuid();

            for (int i = 0; i < KEY_COUNT; i++) {
                Row row = getTestRow(i, i);

                keyRows.add(row);

                txs.add(new IgniteBiTuple<>(row, txId));
            }

            when(clo.command()).thenReturn(new DeleteAllCommand(keyRows, Timestamp.nextVersion().toUuid()));
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Reads all rows.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void readAll(boolean existed) {
        commandListener.onRead(batchIterator(clo -> {
            doAnswer(invocation -> {
                MultiRowsResponse resp = invocation.getArgument(0);

                if (existed) {
                    assertEquals(KEY_COUNT, resp.getValues().size());

                    for (BinaryRow binaryRow : resp.getValues()) {
                        Row row = new Row(SCHEMA, binaryRow);

                        int keyVal = row.intValue(0);

                        assertTrue(keyVal < KEY_COUNT);
                        assertEquals(keyVal, row.intValue(1));
                    }
                } else {
                    assertTrue(resp.getValues().isEmpty() || resp.getValues().stream()
                            .allMatch(r -> r == null));
                }

                return null;
            }).when(clo).result(any(MultiRowsResponse.class));

            Set<BinaryRow> keyRows = new HashSet<>(KEY_COUNT);

            for (int i = 0; i < KEY_COUNT; i++) {
                keyRows.add(getTestKey(i));
            }

            when(clo.command()).thenReturn(new GetAllCommand(keyRows, Timestamp.nextVersion().toUuid()));
        }));
    }

    /**
     * Upserts rows.
     */
    private void upsert() {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i);
            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new UpsertCommand(row, txId));

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Deletes row.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void delete(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i);
            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new DeleteCommand(row, txId));

            doAnswer(invocation -> {
                assertEquals(existed, invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
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
        commandListener.onRead(iterator((i, clo) -> {
            when(clo.command()).thenReturn(new GetCommand(getTestKey(i), Timestamp.nextVersion().toUuid()));

            doAnswer(invocation -> {
                SingleRowResponse resp = invocation.getArgument(0);

                if (existed) {
                    assertNotNull(resp.getValue());

                    assertTrue(resp.getValue().hasValue());

                    Row row = new Row(SCHEMA, resp.getValue());

                    assertEquals(i, row.intValue(0));
                    assertEquals(keyValueMapper.apply(i), row.intValue(1));
                } else {
                    assertNull(resp.getValue());
                }

                return null;
            }).when(clo).result(any(SingleRowResponse.class));
        }));
    }

    /**
     * Inserts row.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void insert(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i);
            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new InsertCommand(row, txId));

            doAnswer(mock -> {
                assertEquals(!existed, mock.getArgument(0));

                return null;
            }).when(clo).result(!existed);
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Deletes exact rows.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void deleteExactAllValues(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(batchIterator(clo -> {
            UUID txId = Timestamp.nextVersion().toUuid();

            HashSet rows = new HashSet(KEY_COUNT);

            for (int i = 0; i < KEY_COUNT; i++) {
                Row row = getTestRow(i, i);

                rows.add(row);

                txs.add(new IgniteBiTuple<>(row, txId));
            }

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new DeleteExactAllCommand(rows, txId));

            doAnswer(invocation -> {
                MultiRowsResponse resp = invocation.getArgument(0);

                if (!existed) {
                    assertEquals(KEY_COUNT, resp.getValues().size());

                    for (BinaryRow binaryRow : resp.getValues()) {
                        Row row = new Row(SCHEMA, binaryRow);

                        int keyVal = row.intValue(0);

                        assertTrue(keyVal < KEY_COUNT);

                        assertEquals(keyVal, row.intValue(1));
                    }
                } else {
                    assertTrue(resp.getValues().isEmpty());
                }

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Gets and replaces rows.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void getAndReplaceValues(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i + 1);
            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new GetAndReplaceCommand(row, txId));

            doAnswer(invocation -> {
                SingleRowResponse resp = invocation.getArgument(0);

                if (existed) {
                    Row row0 = new Row(SCHEMA, resp.getValue());

                    assertEquals(i, row0.intValue(0));
                    assertEquals(i, row0.intValue(1));
                } else {
                    assertNull(resp.getValue());
                }

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Gets an upserts rows.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void getAndUpsertValues(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i);
            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new GetAndUpsertCommand(row, txId));

            doAnswer(invocation -> {
                SingleRowResponse resp = invocation.getArgument(0);

                if (existed) {
                    Row row0 = new Row(SCHEMA, resp.getValue());

                    assertEquals(i, row0.intValue(0));
                    assertEquals(i + 1, row0.intValue(1));
                } else {
                    assertNull(resp.getValue());
                }

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Gets and deletes rows.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void getAndDeleteValues(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i);
            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new GetAndDeleteCommand(row, txId));

            doAnswer(invocation -> {
                SingleRowResponse resp = invocation.getArgument(0);

                if (existed) {
                    Row row0 = new Row(SCHEMA, resp.getValue());

                    assertEquals(i, row0.intValue(0));
                    assertEquals(i + 1, row0.intValue(1));
                } else {
                    assertNull(resp.getValue());
                }

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Puts rows if exists.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void putIfExistValues(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i + 1);
            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new ReplaceIfExistCommand(row, Timestamp.nextVersion().toUuid()));

            doAnswer(invocation -> {
                boolean result = invocation.getArgument(0);

                assertEquals(existed, result);

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Deletes exact rows.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void deleteExactValues(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i + 1);
            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new DeleteExactCommand(row, txId));

            doAnswer(invocation -> {
                boolean result = invocation.getArgument(0);

                assertEquals(existed, result);

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Replaces rows.
     *
     * @param existed True if rows are existed, false otherwise.
     */
    private void replaceValues(boolean existed) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i);
            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new ReplaceCommand(row, getTestRow(i, i + 1), txId));

            doAnswer(invocation -> {
                assertTrue(invocation.getArgument(0) instanceof Boolean);

                boolean result = invocation.getArgument(0);

                assertEquals(existed, result);

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> versionedRowStore.commitWrite(tuple.getKey().keySlice(), tuple.getValue()));
    }

    /**
     * Prepares a test row which contains only key field.
     *
     * @return Row.
     */
    @NotNull
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
    @NotNull
    private Row getTestRow(int key, int val) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendInt(key);
        rowBuilder.appendInt(val);

        return new Row(SCHEMA, rowBuilder.build());
    }
}
