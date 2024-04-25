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

package org.apache.ignite.internal.table.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.configuration.StorageUpdateConfiguration;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.replicator.TimedBinaryRow;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test write intent cleanups via {@link StorageUpdateHandler}.
 */
public class StorageCleanupTest extends BaseMvStoragesTest {

    private static final HybridClock CLOCK = new HybridClockImpl();

    protected static final int PARTITION_ID = 0;

    private static final BinaryTupleSchema TUPLE_SCHEMA = BinaryTupleSchema.createRowSchema(SCHEMA_DESCRIPTOR);

    private static final BinaryTupleSchema PK_INDEX_SCHEMA = BinaryTupleSchema.createKeySchema(SCHEMA_DESCRIPTOR);

    private static final ColumnsExtractor PK_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, PK_INDEX_SCHEMA);

    private static final int[] USER_INDEX_COLS = {
            SCHEMA_DESCRIPTOR.column("INTVAL").positionInRow(),
            SCHEMA_DESCRIPTOR.column("STRVAL").positionInRow()
    };

    private static final BinaryTupleSchema USER_INDEX_SCHEMA = BinaryTupleSchema.createSchema(SCHEMA_DESCRIPTOR, USER_INDEX_COLS);

    private static final ColumnsExtractor USER_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, USER_INDEX_SCHEMA);

    private TestHashIndexStorage pkInnerStorage;
    private TestSortedIndexStorage sortedInnerStorage;
    private TestHashIndexStorage hashInnerStorage;
    private TestMvPartitionStorage storage;
    private StorageUpdateHandler storageUpdateHandler;
    private IndexUpdateHandler indexUpdateHandler;

    @InjectConfiguration
    private StorageUpdateConfiguration storageUpdateConfiguration;

    @BeforeEach
    void setUp() {
        int tableId = 1;
        int pkIndexId = 2;
        int sortedIndexId = 3;
        int hashIndexId = 4;

        pkInnerStorage = new TestHashIndexStorage(
                PARTITION_ID,
                new StorageHashIndexDescriptor(
                        pkIndexId,
                        List.of(
                                new StorageHashIndexColumnDescriptor("INTKEY", NativeTypes.INT32, false),
                                new StorageHashIndexColumnDescriptor("STRKEY", NativeTypes.STRING, false)
                        ),
                        true
                )
        );

        TableSchemaAwareIndexStorage pkStorage = new TableSchemaAwareIndexStorage(
                pkIndexId,
                pkInnerStorage,
                PK_INDEX_BINARY_TUPLE_CONVERTER
        );

        sortedInnerStorage = new TestSortedIndexStorage(
                PARTITION_ID,
                new StorageSortedIndexDescriptor(
                        sortedIndexId,
                        List.of(
                                new StorageSortedIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false, true),
                                new StorageSortedIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false, true)
                        ),
                        false
                ),
                mock(CatalogIndexStatusSupplier.class)
        );

        TableSchemaAwareIndexStorage sortedIndexStorage = new TableSchemaAwareIndexStorage(
                sortedIndexId,
                sortedInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER
        );

        hashInnerStorage = new TestHashIndexStorage(
                PARTITION_ID,
                new StorageHashIndexDescriptor(
                        hashIndexId,
                        List.of(
                                new StorageHashIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false),
                                new StorageHashIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false)
                        ),
                        false
                )
        );

        TableSchemaAwareIndexStorage hashIndexStorage = new TableSchemaAwareIndexStorage(
                hashIndexId,
                hashInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER
        );

        storage = spy(new TestMvPartitionStorage(PARTITION_ID));

        Map<Integer, TableSchemaAwareIndexStorage> indexes = Map.of(
                pkIndexId, pkStorage,
                sortedIndexId, sortedIndexStorage,
                hashIndexId, hashIndexStorage
        );

        TestPartitionDataStorage partitionDataStorage = new TestPartitionDataStorage(tableId, PARTITION_ID, storage);

        indexUpdateHandler = spy(new IndexUpdateHandler(DummyInternalTableImpl.createTableIndexStoragesSupplier(indexes)));

        storageUpdateHandler = new StorageUpdateHandler(
                PARTITION_ID,
                partitionDataStorage,
                indexUpdateHandler,
                storageUpdateConfiguration
        );
    }

    @Test
    void testSimpleAbort() {
        UUID txUuid = UUID.randomUUID();

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));
        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));
        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row1, true, null, null, null, null);
        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row2, true, null, null, null, null);
        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row3, true, null, null, null, null);

        assertEquals(3, storage.rowsCount());

        storageUpdateHandler.switchWriteIntents(txUuid, false, null, null);

        assertEquals(0, storage.rowsCount());
    }

    @Test
    void testSimpleCommit() {
        UUID txUuid = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));
        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));
        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row1, true, null, null, null, null);
        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row2, true, null, null, null, null);
        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row3, true, null, null, null, null);

        assertEquals(3, storage.rowsCount());
        // We have three writes to the storage.
        verify(storage, times(3)).addWrite(any(), any(), any(), anyInt(), anyInt());

        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        assertEquals(3, storage.rowsCount());
        // Those writes resulted in three commits.
        verify(storage, times(3)).commitWrite(any(), any());

        // Now reset the invocation counter.
        clearInvocations(storage);

        // And run cleanup again for the same transaction.
        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        assertEquals(3, storage.rowsCount());
        // And no invocation after, meaning idempotence of the cleanup.
        verify(storage, never()).commitWrite(any(), any());
    }

    @Test
    void testSimpleCommitBatch() {
        UUID txUuid = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));
        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));
        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        TimedBinaryRow tb1 = new TimedBinaryRow(row1, null);
        TimedBinaryRow tb2 = new TimedBinaryRow(row2, null);
        TimedBinaryRow tb3 = new TimedBinaryRow(row3, null);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        UUID id3 = UUID.randomUUID();

        Map<UUID, TimedBinaryRow> rowsToUpdate = Map.of(
                id1, tb1,
                id2, tb2,
                id3, tb3
        );
        storageUpdateHandler.handleUpdateAll(txUuid, rowsToUpdate, partitionId, true, null, null, null);

        assertEquals(3, storage.rowsCount());
        // We have three writes to the storage.
        verify(storage, times(3)).addWrite(any(), any(), any(), anyInt(), anyInt());

        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        assertEquals(3, storage.rowsCount());
        // Those writes resulted in three commits.
        verify(storage, times(3)).commitWrite(any(), any());

        // Now reset the invocation counter.
        clearInvocations(storage);

        // And run cleanup again for the same transaction.
        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        assertEquals(3, storage.rowsCount());
        // And no invocation after, meaning idempotence of the cleanup.
        verify(storage, never()).commitWrite(any(), any());

        ReadResult result1 = storage.read(new RowId(partitionId.partitionId(), id1), HybridTimestamp.MAX_VALUE);
        assertEquals(row1, result1.binaryRow());

        ReadResult result2 = storage.read(new RowId(partitionId.partitionId(), id2), HybridTimestamp.MAX_VALUE);
        assertEquals(row2, result2.binaryRow());

        ReadResult result3 = storage.read(new RowId(partitionId.partitionId(), id3), HybridTimestamp.MAX_VALUE);
        assertEquals(row3, result3.binaryRow());

        // Reset the invocation counter.
        clearInvocations(storage);

        // Now delete rows with a batch
        Map<UUID, TimedBinaryRow> rowsToDelete = new HashMap<>();
        rowsToDelete.put(id2, null);
        rowsToDelete.put(id3, null);

        storageUpdateHandler.handleUpdateAll(txUuid, rowsToDelete, partitionId, true, null, null, null);

        // We have three writes to the storage.
        verify(storage, times(2)).addWrite(any(), any(), any(), anyInt(), anyInt());

        // And run cleanup again for the same transaction.
        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        ReadResult resultAfterDelete1 = storage.read(new RowId(partitionId.partitionId(), id1), HybridTimestamp.MAX_VALUE);
        assertEquals(row1, resultAfterDelete1.binaryRow());

        ReadResult resultAfterDelete2 = storage.read(new RowId(partitionId.partitionId(), id2), HybridTimestamp.MAX_VALUE);
        assertNull(resultAfterDelete2.binaryRow());

        ReadResult resultAfterDelete3 = storage.read(new RowId(partitionId.partitionId(), id3), HybridTimestamp.MAX_VALUE);
        assertNull(resultAfterDelete3.binaryRow());
    }

    @Test
    void testCleanupAndUpdateRow() {
        UUID txUuid = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));
        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));
        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // Do not track write intents to simulate the loss of a volatile state.
        UUID row1Id = UUID.randomUUID();
        UUID row2Id = UUID.randomUUID();
        UUID row3Id = UUID.randomUUID();

        storageUpdateHandler.handleUpdate(txUuid, row1Id, partitionId, row1, false, null, null, null, null);
        storageUpdateHandler.handleUpdate(txUuid, row2Id, partitionId, row2, false, null, null, null, null);
        storageUpdateHandler.handleUpdate(txUuid, row3Id, partitionId, row3, false, null, null, null, null);

        assertEquals(3, storage.rowsCount());

        // Now run cleanup.
        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        // But the loss of the state results in no cleanup, and the entries are still write intents.
        verify(storage, never()).commitWrite(any(), any());

        // Now imagine we have another transaction that resolves the row, does the cleanup and commits its own data.

        // Resolve one of the rows affected by the committed transaction.
        storageUpdateHandler.handleWriteIntentRead(txUuid, new RowId(PARTITION_ID, row1Id));

        // Run the cleanup.
        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        // Only the discovered write intent was committed, the other two are still write intents.
        verify(storage, times(1)).commitWrite(any(), any());

        BinaryRow row4 = binaryRow(new TestKey(1, "foo1"), new TestValue(20, "bar20"));

        UUID newTxUuid = UUID.randomUUID();

        // Insert new write intent in the new transaction.
        storageUpdateHandler.handleUpdate(newTxUuid, row1Id, partitionId, row4, true, null, null, null, null);

        // And concurrently the other two intents were also resolved and scheduled for cleanup.
        storageUpdateHandler.handleWriteIntentRead(txUuid, new RowId(PARTITION_ID, row2Id));
        storageUpdateHandler.handleWriteIntentRead(txUuid, new RowId(PARTITION_ID, row3Id));

        // Now reset the invocation counter.
        clearInvocations(storage);

        // Run cleanup for the original transaction
        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        // Only those two entries will be affected.
        verify(storage, times(2)).commitWrite(any(), any());
    }

    @Test
    void testCleanupAndUpdateRowBatch() {
        UUID txUuid = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));
        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));
        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);


        UUID row1Id = UUID.randomUUID();
        UUID row2Id = UUID.randomUUID();
        UUID row3Id = UUID.randomUUID();

        TimedBinaryRow tb1 = new TimedBinaryRow(row1, null);
        TimedBinaryRow tb2 = new TimedBinaryRow(row2, null);
        TimedBinaryRow tb3 = new TimedBinaryRow(row3, null);

        Map<UUID, TimedBinaryRow> rowsToUpdate = Map.of(
                row1Id, tb1,
                row2Id, tb2,
                row3Id, tb3
        );
        // Do not track write intents to simulate the loss of a volatile state.
        storageUpdateHandler.handleUpdateAll(txUuid, rowsToUpdate, partitionId, false, null, null, null);

        assertEquals(3, storage.rowsCount());

        // Now run cleanup.
        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        // But the loss of the state results in no cleanup, and the entries are still write intents.
        verify(storage, never()).commitWrite(any(), any());

        // Now imagine we have another transaction that resolves the row, does the cleanup and commits its own data.

        // Resolve one of the rows affected by the committed transaction.
        storageUpdateHandler.handleWriteIntentRead(txUuid, new RowId(PARTITION_ID, row1Id));

        // Run the cleanup.
        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        // Only the discovered write intent was committed, the other two are still write intents.
        verify(storage, times(1)).commitWrite(any(), any());

        BinaryRow row4 = binaryRow(new TestKey(1, "foo1"), new TestValue(20, "bar20"));

        UUID newTxUuid = UUID.randomUUID();

        // Insert new write intent in the new transaction.
        TimedBinaryRow tb4 = new TimedBinaryRow(row4, null);

        Map<UUID, TimedBinaryRow> rowsToUpdate2 = Map.of(
                row1Id, tb4
        );
        storageUpdateHandler.handleUpdateAll(newTxUuid, rowsToUpdate2, partitionId, true, null, null, null);

        // And concurrently the other two intents were also resolved and scheduled for cleanup.
        storageUpdateHandler.handleWriteIntentRead(txUuid, new RowId(PARTITION_ID, row2Id));
        storageUpdateHandler.handleWriteIntentRead(txUuid, new RowId(PARTITION_ID, row3Id));

        // Now reset the invocation counter.
        clearInvocations(storage);

        // Run cleanup for the original transaction
        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        // Only those two entries will be affected.
        verify(storage, times(2)).commitWrite(any(), any());
    }

    @Test
    void testCleanupBeforeUpdateNoData() {
        UUID runningTx = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        storageUpdateHandler.handleUpdate(runningTx, rowId, partitionId, row1, false, null, null, commitTs, null);

        verify(storage, never()).commitWrite(any(), any());
        verify(storage, never()).abortWrite(any());
        verify(indexUpdateHandler, never()).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateNoWriteIntent() {
        UUID committedTx = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // First commit a row

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        storageUpdateHandler.handleUpdate(committedTx, rowId, partitionId, row1, true, null, null, null, null);

        storageUpdateHandler.switchWriteIntents(committedTx, true, commitTs, null);

        assertEquals(1, storage.rowsCount());

        assertFalse(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Now create a new write intent over the committed data. No cleanup should be triggered.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        UUID runningTx = UUID.randomUUID();

        clearInvocations(storage, indexUpdateHandler);

        storageUpdateHandler.handleUpdate(runningTx, rowId, partitionId, row2, true, null, null, commitTs, null);

        verify(storage, never()).commitWrite(any(), any());
        verify(storage, never()).abortWrite(any());
        verify(indexUpdateHandler, never()).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateNoWriteIntentBatch() {
        UUID committedTx = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // First commit a row

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        Map<UUID, TimedBinaryRow> rowsToUpdate = Map.of(
                rowId, new TimedBinaryRow(row1, null)
        );

        storageUpdateHandler.handleUpdateAll(committedTx, rowsToUpdate, partitionId, true, null, null, null);

        storageUpdateHandler.switchWriteIntents(committedTx, true, commitTs, null);

        assertEquals(1, storage.rowsCount());

        assertFalse(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Now create a new write intent over the committed data. No cleanup should be triggered.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        UUID runningTx = UUID.randomUUID();

        clearInvocations(storage, indexUpdateHandler);

        Map<UUID, TimedBinaryRow> rowsToUpdate2 = Map.of(
                rowId, new TimedBinaryRow(row2, commitTs)
        );
        storageUpdateHandler.handleUpdateAll(runningTx, rowsToUpdate2, partitionId, true, null, null, null);

        verify(storage, never()).commitWrite(any(), any());
        verify(storage, never()).abortWrite(any());
        verify(indexUpdateHandler, never()).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateSameTxOnlyWriteIntent() {
        UUID runningTx = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // Create a write intent.

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        storageUpdateHandler.handleUpdate(runningTx, rowId, partitionId, row1, true, null, null, null, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Then create another one for the same row in the same transaction. The entry will be replaced.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        clearInvocations(storage, indexUpdateHandler);

        storageUpdateHandler.handleUpdate(runningTx, rowId, partitionId, row2, true, null, null, commitTs, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        verify(storage, never()).commitWrite(any(), any());
        verify(storage, never()).abortWrite(any());
        verify(indexUpdateHandler, times(1)).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateSameTxOnlyWriteIntentBatch() {
        UUID runningTx = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // Create a write intent.

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        Map<UUID, TimedBinaryRow> rowsToUpdate = Map.of(
                rowId, new TimedBinaryRow(row1, null)
        );

        storageUpdateHandler.handleUpdateAll(runningTx, rowsToUpdate, partitionId, true, null, null, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Then create another one for the same row in the same transaction. The entry will be replaced.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        clearInvocations(storage, indexUpdateHandler);

        Map<UUID, TimedBinaryRow> rowsToUpdate2 = Map.of(
                rowId, new TimedBinaryRow(row2, commitTs)
        );
        // Do not track write intents to simulate the loss of a volatile state.
        storageUpdateHandler.handleUpdateAll(runningTx, rowsToUpdate2, partitionId, true, null, null, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        verify(storage, never()).commitWrite(any(), any());
        verify(storage, never()).abortWrite(any());
        verify(indexUpdateHandler, times(1)).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateDifferentTxOnlyWriteIntent() {
        UUID runningTx = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // Create a new write intent.

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        storageUpdateHandler.handleUpdate(runningTx, rowId, partitionId, row1, true, null, null, null, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Create another one and pass `last commit time`. The previous value should be committed automatically.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        clearInvocations(storage, indexUpdateHandler);

        UUID runningTx2 = UUID.randomUUID();

        // Previous value will be committed even though the cleanup was not called explicitly.
        storageUpdateHandler.handleUpdate(runningTx2, rowId, partitionId, row2, true, null, null, commitTs, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        verify(storage, times(1)).commitWrite(any(), any());
        verify(storage, never()).abortWrite(any());
        verify(indexUpdateHandler, never()).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateDifferentTxOnlyWriteIntentBatch() {
        UUID runningTx = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // Create a new write intent.

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        Map<UUID, TimedBinaryRow> rowsToUpdate = Map.of(
                rowId, new TimedBinaryRow(row1, null)
        );

        storageUpdateHandler.handleUpdateAll(runningTx, rowsToUpdate, partitionId, true, null, null, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Create another one and pass `last commit time`. The previous value should be committed automatically.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        clearInvocations(storage, indexUpdateHandler);

        UUID runningTx2 = UUID.randomUUID();

        // Previous value will be committed even though the cleanup was not called explicitly.
        Map<UUID, TimedBinaryRow> rowsToUpdate2 = Map.of(
                rowId, new TimedBinaryRow(row2, commitTs)
        );

        storageUpdateHandler.handleUpdateAll(runningTx2, rowsToUpdate2, partitionId, true, null, null, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        verify(storage, times(1)).commitWrite(any(), any());
        verify(storage, never()).abortWrite(any());
        verify(indexUpdateHandler, never()).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateAbortWriteIntent() {
        UUID committed1 = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // First commit an entry.

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        storageUpdateHandler.handleUpdate(committed1, rowId, partitionId, row1, true, null, null, null, null);

        storageUpdateHandler.switchWriteIntents(committed1, true, commitTs, null);

        assertEquals(1, storage.rowsCount());

        assertFalse(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Now add a new write intent.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        UUID committed2 = UUID.randomUUID();

        storageUpdateHandler.handleUpdate(committed2, rowId, partitionId, row2, true, null, null, null, null);

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Create another write intent and provide `last commit time`.

        clearInvocations(storage, indexUpdateHandler);

        UUID runningTx = UUID.randomUUID();

        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        // Last commit time equal to the time of the previously committed value => previous write intent will be reverted.
        storageUpdateHandler.handleUpdate(runningTx, rowId, partitionId, row3, true, null, null, commitTs, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        verify(storage, never()).commitWrite(any(), any());
        verify(storage, times(1)).abortWrite(any());
        verify(indexUpdateHandler, times(1)).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateAbortWriteIntentBatch() {
        UUID committed1 = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // First commit an entry.

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        Map<UUID, TimedBinaryRow> rowsToUpdate = Map.of(
                rowId, new TimedBinaryRow(row1, null)
        );

        storageUpdateHandler.handleUpdateAll(committed1, rowsToUpdate, partitionId, true, null, null, null);

        storageUpdateHandler.switchWriteIntents(committed1, true, commitTs, null);

        assertEquals(1, storage.rowsCount());

        assertFalse(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Now add a new write intent.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        UUID committed2 = UUID.randomUUID();

        Map<UUID, TimedBinaryRow> rowsToUpdate2 = Map.of(
                rowId, new TimedBinaryRow(row2, commitTs)
        );

        storageUpdateHandler.handleUpdateAll(committed2, rowsToUpdate2, partitionId, true, null, null, null);

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Create another write intent and provide `last commit time`.

        clearInvocations(storage, indexUpdateHandler);

        UUID runningTx = UUID.randomUUID();

        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        // Last commit time equal to the time of the previously committed value => previous write intent will be reverted.
        Map<UUID, TimedBinaryRow> rowsToUpdate3 = Map.of(
                rowId, new TimedBinaryRow(row3, commitTs)
        );

        storageUpdateHandler.handleUpdateAll(runningTx, rowsToUpdate3, partitionId, true, null, null, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        verify(storage, never()).commitWrite(any(), any());
        verify(storage, times(1)).abortWrite(any());
        verify(indexUpdateHandler, times(1)).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateCommitWriteIntent() {
        UUID committed1 = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // First commit an entry.

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        storageUpdateHandler.handleUpdate(committed1, rowId, partitionId, row1, true, null, null, null, null);

        storageUpdateHandler.switchWriteIntents(committed1, true, commitTs, null);

        assertEquals(1, storage.rowsCount());

        assertFalse(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Now add a new write intent.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        UUID committed2 = UUID.randomUUID();

        storageUpdateHandler.handleUpdate(committed2, rowId, partitionId, row2, true, null, null, null, null);

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Create another write intent and provide `last commit time`.

        clearInvocations(storage, indexUpdateHandler);

        UUID runningTx = UUID.randomUUID();

        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        HybridTimestamp lastCommitTs = commitTs.addPhysicalTime(100);

        // Last commit time is after the time of the previously committed value => previous write intent will be committed.
        storageUpdateHandler.handleUpdate(runningTx, rowId, partitionId, row3, true, null, null, lastCommitTs, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        verify(storage, times(1)).commitWrite(any(), any());
        verify(storage, never()).abortWrite(any());
        verify(indexUpdateHandler, never()).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateCommitWriteIntentBatch() {
        UUID committed1 = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // First commit an entry.

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        Map<UUID, TimedBinaryRow> rowsToUpdate = Map.of(
                rowId, new TimedBinaryRow(row1, null)
        );

        storageUpdateHandler.handleUpdateAll(committed1, rowsToUpdate, partitionId, true, null, null, null);

        storageUpdateHandler.switchWriteIntents(committed1, true, commitTs, null);

        assertEquals(1, storage.rowsCount());

        assertFalse(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Now add a new write intent.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        UUID committed2 = UUID.randomUUID();

        Map<UUID, TimedBinaryRow> rowsToUpdate2 = Map.of(
                rowId, new TimedBinaryRow(row2, commitTs)
        );

        storageUpdateHandler.handleUpdateAll(committed2, rowsToUpdate2, partitionId, true, null, null, null);

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Create another write intent and provide `last commit time`.

        clearInvocations(storage, indexUpdateHandler);

        UUID runningTx = UUID.randomUUID();

        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        HybridTimestamp lastCommitTs = commitTs.addPhysicalTime(100);

        // Last commit time is after the time of the previously committed value => previous write intent will be committed.
        Map<UUID, TimedBinaryRow> rowsToUpdate3 = Map.of(
                rowId, new TimedBinaryRow(row3, lastCommitTs)
        );

        storageUpdateHandler.handleUpdateAll(runningTx, rowsToUpdate3, partitionId, true, null, null, null);

        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        verify(storage, times(1)).commitWrite(any(), any());
        verify(storage, never()).abortWrite(any());
        verify(indexUpdateHandler, never()).tryRemoveFromIndexes(any(), any(), any(), any());
    }

    @Test
    void testCleanupBeforeUpdateError() {
        UUID committed1 = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        // First commit an entry.

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));

        UUID rowId = UUID.randomUUID();

        HybridTimestamp commitTs = CLOCK.now();

        storageUpdateHandler.handleUpdate(committed1, rowId, partitionId, row1, true, null, null, null, null);

        storageUpdateHandler.switchWriteIntents(committed1, true, commitTs, null);

        assertEquals(1, storage.rowsCount());

        assertFalse(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Now add a new write intent.

        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));

        UUID committed2 = UUID.randomUUID();

        storageUpdateHandler.handleUpdate(committed2, rowId, partitionId, row2, true, null, null, null, null);

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        // Create another write intent and provide `last commit time`.

        clearInvocations(storage, indexUpdateHandler);

        UUID runningTx = UUID.randomUUID();

        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        // This should lead to an exception
        HybridTimestamp lastCommitTs = commitTs.subtractPhysicalTime(100);

        // Last commit time is before the time of the previously committed value => this should not happen.
        assertThrows(AssertionError.class, () ->
                storageUpdateHandler.handleUpdate(runningTx, rowId, partitionId, row3, true, null, null, lastCommitTs, null));


        assertEquals(1, storage.rowsCount());

        assertTrue(storage.read(new RowId(PARTITION_ID, rowId), HybridTimestamp.MAX_VALUE).isWriteIntent());

        verify(storage, never()).commitWrite(any(), any());
        verify(storage, never()).abortWrite(any());
        verify(indexUpdateHandler, never()).tryRemoveFromIndexes(any(), any(), any(), any());
    }

}
