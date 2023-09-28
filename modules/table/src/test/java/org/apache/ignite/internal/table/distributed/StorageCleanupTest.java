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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test write intent cleanups via {@link StorageUpdateHandler}.
 */
@ExtendWith(ConfigurationExtension.class)
public class StorageCleanupTest extends BaseMvStoragesTest {

    private static final HybridClock CLOCK = new HybridClockImpl();

    protected static final int PARTITION_ID = 0;

    private static final BinaryTupleSchema TUPLE_SCHEMA = BinaryTupleSchema.createRowSchema(SCHEMA_DESCRIPTOR);

    private static final BinaryTupleSchema PK_INDEX_SCHEMA = BinaryTupleSchema.createKeySchema(SCHEMA_DESCRIPTOR);

    private static final ColumnsExtractor PK_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, PK_INDEX_SCHEMA);

    private static final int[] USER_INDEX_COLS = {
            SCHEMA_DESCRIPTOR.column("INTVAL").schemaIndex(),
            SCHEMA_DESCRIPTOR.column("STRVAL").schemaIndex()
    };

    private static final BinaryTupleSchema USER_INDEX_SCHEMA = BinaryTupleSchema.createSchema(SCHEMA_DESCRIPTOR, USER_INDEX_COLS);

    private static final ColumnsExtractor USER_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, USER_INDEX_SCHEMA);

    private TestHashIndexStorage pkInnerStorage;
    private TestSortedIndexStorage sortedInnerStorage;
    private TestHashIndexStorage hashInnerStorage;
    private TestMvPartitionStorage storage;
    private StorageUpdateHandler storageUpdateHandler;

    private GcUpdateHandler gcUpdateHandler;

    @BeforeEach
    void setUp(@InjectConfiguration GcConfiguration gcConfig) {
        int tableId = 1;
        int pkIndexId = 2;
        int sortedIndexId = 3;
        int hashIndexId = 4;

        pkInnerStorage = new TestHashIndexStorage(PARTITION_ID, new StorageHashIndexDescriptor(pkIndexId, List.of(
                new StorageHashIndexColumnDescriptor("INTKEY", NativeTypes.INT32, false),
                new StorageHashIndexColumnDescriptor("STRKEY", NativeTypes.STRING, false)
        )));

        TableSchemaAwareIndexStorage pkStorage = new TableSchemaAwareIndexStorage(
                pkIndexId,
                pkInnerStorage,
                PK_INDEX_BINARY_TUPLE_CONVERTER
        );

        sortedInnerStorage = new TestSortedIndexStorage(PARTITION_ID, new StorageSortedIndexDescriptor(sortedIndexId, List.of(
                new StorageSortedIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false, true),
                new StorageSortedIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false, true)
        )));

        TableSchemaAwareIndexStorage sortedIndexStorage = new TableSchemaAwareIndexStorage(
                sortedIndexId,
                sortedInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER
        );

        hashInnerStorage = new TestHashIndexStorage(PARTITION_ID, new StorageHashIndexDescriptor(hashIndexId, List.of(
                new StorageHashIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false),
                new StorageHashIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false)
        )));

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

        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(DummyInternalTableImpl.createTableIndexStoragesSupplier(indexes));

        gcUpdateHandler = new GcUpdateHandler(
                partitionDataStorage,
                new PendingComparableValuesTracker<>(HybridTimestamp.MAX_VALUE),
                indexUpdateHandler
        );

        storageUpdateHandler = new StorageUpdateHandler(
                PARTITION_ID,
                partitionDataStorage,
                gcConfig,
                mock(LowWatermark.class),
                indexUpdateHandler,
                gcUpdateHandler
        );
    }

    @Test
    void testSimpleAbort() {
        UUID txUuid = UUID.randomUUID();

        BinaryRow row1 = binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar"));
        BinaryRow row2 = binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz"));
        BinaryRow row3 = binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"));

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row1, true, null, null);
        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row2, true, null, null);
        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row3, true, null, null);

        assertEquals(3, storage.rowsCount());

        storageUpdateHandler.handleTransactionCleanup(txUuid, false, null);

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

        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row1, true, null, null);
        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row2, true, null, null);
        storageUpdateHandler.handleUpdate(txUuid, UUID.randomUUID(), partitionId, row3, true, null, null);

        assertEquals(3, storage.rowsCount());
        // We have three writes to the storage.
        verify(storage, times(3)).addWrite(any(), any(), any(), anyInt(), anyInt());

        storageUpdateHandler.handleTransactionCleanup(txUuid, true, commitTs);

        assertEquals(3, storage.rowsCount());
        // Those writes resulted in three commits.
        verify(storage, times(3)).commitWrite(any(), any());

        // Now reset the invocation counter.
        clearInvocations(storage);

        // And run cleanup again for the same transaction.
        storageUpdateHandler.handleTransactionCleanup(txUuid, true, commitTs);

        assertEquals(3, storage.rowsCount());
        // And no invocation after, meaning idempotence of the cleanup.
        verify(storage, never()).commitWrite(any(), any());
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

        storageUpdateHandler.handleUpdate(txUuid, row1Id, partitionId, row1, false, null, null);
        storageUpdateHandler.handleUpdate(txUuid, row2Id, partitionId, row2, false, null, null);
        storageUpdateHandler.handleUpdate(txUuid, row3Id, partitionId, row3, false, null, null);

        assertEquals(3, storage.rowsCount());

        // Now run cleanup.
        storageUpdateHandler.handleTransactionCleanup(txUuid, true, commitTs);

        // But the loss of the state results in no cleanup, and the entries are still write intents.
        verify(storage, never()).commitWrite(any(), any());

        // Now imagine we have another transaction that resolves the row, does the cleanup and commits its own data.

        // Resolve one of the rows affected by the committed transaction.
        storageUpdateHandler.handleWriteIntentRead(txUuid, new RowId(PARTITION_ID, row1Id));

        // Run the cleanup.
        storageUpdateHandler.handleTransactionCleanup(txUuid, true, commitTs);

        // Only the discovered write intent was committed, the other two are still write intents.
        verify(storage, times(1)).commitWrite(any(), any());

        BinaryRow row4 = binaryRow(new TestKey(1, "foo1"), new TestValue(20, "bar20"));

        UUID newTxUuid = UUID.randomUUID();

        // Insert new write intent in the new transaction.
        storageUpdateHandler.handleUpdate(newTxUuid, row1Id, partitionId, row4, true, null, null);

        // And concurrently the other two intents were also resolved and scheduled for cleanup.
        storageUpdateHandler.handleWriteIntentRead(txUuid, new RowId(PARTITION_ID, row2Id));
        storageUpdateHandler.handleWriteIntentRead(txUuid, new RowId(PARTITION_ID, row3Id));

        // Now reset the invocation counter.
        clearInvocations(storage);

        // Run cleanup for the original transaction
        storageUpdateHandler.handleTransactionCleanup(txUuid, true, commitTs);

        // Only those two entries will be affected.
        verify(storage, times(2)).commitWrite(any(), any());
    }
}
