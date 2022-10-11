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
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.command.UpdateAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpdateCommand;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.test.TestConcurrentHashMapTxStateStorage;
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

    /** Partition id. */
    public static final int PARTITION_ID = 0;

    /** Schema. */
    public static SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT32, false)},
            new Column[]{new Column("value", NativeTypes.INT32, false)}
    );

    /** Hybrid clock. */
    private static final HybridClock CLOCK = new HybridClock();

    /** Table command listener. */
    private PartitionListener commandListener;

    /** RAFT index. */
    private AtomicLong raftIndex = new AtomicLong();

    /** Primary index. */
    private ConcurrentHashMap<ByteBuffer, RowId> primaryIndex = new ConcurrentHashMap<>();

    /** Partition storage. */
    private MvPartitionStorage mvPartitionStorage = new TestMvPartitionStorage(PARTITION_ID);

    /**
     * Initializes a table listener before tests.
     */
    @BeforeEach
    public void before() {
        ClusterService clusterService = Mockito.mock(ClusterService.class, RETURNS_DEEP_STUBS);
        NetworkAddress addr = new NetworkAddress("127.0.0.1", 5003);
        Mockito.when(clusterService.topologyService().localMember().address()).thenReturn(addr);

        ReplicaService replicaService = Mockito.mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        commandListener = new PartitionListener(
                mvPartitionStorage,
                new TestConcurrentHashMapTxStateStorage(),
                new TxManagerImpl(replicaService, new HeapLockManager(), new HybridClock()),
                primaryIndex
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
     */
    private void insertAll() {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(batchIterator(clo -> {
            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());

            HashMap<RowId, BinaryRow> rows = new HashMap<>(KEY_COUNT);
            UUID txId = Timestamp.nextVersion().toUuid();

            for (int i = 0; i < KEY_COUNT; i++) {
                Row row = getTestRow(i, i);

                rows.put(new RowId(PARTITION_ID), row);

                txs.add(new IgniteBiTuple<>(row, txId));
            }

            when(clo.command()).thenReturn(new UpdateAllCommand(rows, txId));
        }));

        txs.forEach(tuple -> mvPartitionStorage.commitWrite(primaryIndex.get(tuple.getKey().keySlice()), CLOCK.now()));
    }

    /**
     * Update values from the listener in the batch operation.
     *
     * @param keyValueMapper Mep a value to update to the iter number.
     */
    private void updateAll(Function<Integer, Integer> keyValueMapper) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(batchIterator(clo -> {
            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());

            HashMap<RowId, BinaryRow> rows = new HashMap<>(KEY_COUNT);

            UUID txId = Timestamp.nextVersion().toUuid();

            for (int i = 0; i < KEY_COUNT; i++) {
                Row row = getTestRow(i, keyValueMapper.apply(i));

                rows.put(primaryIndex.get(row.keySlice()), row);

                txs.add(new IgniteBiTuple<>(row, txId));
            }

            when(clo.command()).thenReturn(new UpdateAllCommand(rows, txId));
        }));

        txs.forEach(tuple -> mvPartitionStorage.commitWrite(primaryIndex.get(tuple.getKey().keySlice()), CLOCK.now()));
    }

    /**
     * Deletes all rows.
     */
    private void deleteAll() {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(batchIterator(clo -> {
            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());

            Set<RowId> keyRows = new HashSet<>(KEY_COUNT);

            UUID txId = Timestamp.nextVersion().toUuid();

            for (int i = 0; i < KEY_COUNT; i++) {
                Row row = getTestRow(i, i);

                keyRows.add(primaryIndex.get(row.keySlice()));

                txs.add(new IgniteBiTuple<>(row, txId));
            }

            when(clo.command()).thenReturn(new UpdateAllCommand(keyRows, txId));
        }));

        txs.forEach(
                tuple -> mvPartitionStorage.commitWrite(primaryIndex.remove(tuple.getKey().keySlice()), CLOCK.now()));
    }

    /**
     * Update rows.
     *
     * @param keyValueMapper Mep a value to update to the iter number.
     */
    private void update(Function<Integer, Integer> keyValueMapper) {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, keyValueMapper.apply(i));
            RowId rowId = primaryIndex.get(row.keySlice());

            assertNotNull(rowId);

            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new UpdateCommand(rowId, row, txId));

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> mvPartitionStorage.commitWrite(primaryIndex.get(tuple.getKey().keySlice()), CLOCK.now()));
    }

    /**
     * Deletes row.
     */
    private void delete() {
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i);
            RowId rowId = primaryIndex.get(row.keySlice());

            assertNotNull(rowId);

            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new UpdateCommand(rowId, txId));

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(
                tuple -> mvPartitionStorage.commitWrite(primaryIndex.remove(tuple.getKey().keySlice()), CLOCK.now()));
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

            RowId rowId = primaryIndex.get(keyRow.keySlice());

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
        List<IgniteBiTuple<Row, UUID>> txs = new ArrayList<>();

        commandListener.onWrite(iterator((i, clo) -> {
            UUID txId = Timestamp.nextVersion().toUuid();
            Row row = getTestRow(i, i);
            txs.add(new IgniteBiTuple<>(row, txId));

            when(clo.index()).thenReturn(raftIndex.incrementAndGet());

            when(clo.command()).thenReturn(new UpdateCommand(new RowId(PARTITION_ID), row, txId));

            doAnswer(invocation -> {
                assertNull(invocation.getArgument(0));

                return null;
            }).when(clo).result(any());
        }));

        txs.forEach(tuple -> mvPartitionStorage.commitWrite(primaryIndex.get(tuple.getKey().keySlice()), CLOCK.now()));
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
