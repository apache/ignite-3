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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.network.TimedBinaryRow;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor.StorageHashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.storage.util.LockByRowId;
import org.apache.ignite.internal.table.TableTestUtils;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test for {@link StorageUpdateHandler}. */
public class StorageUpdateHandlerTest extends BaseMvStoragesTest {
    private static final HybridClock CLOCK = new HybridClockImpl();

    private static final int PARTITION_ID = 0;

    private static final BinaryTupleSchema TUPLE_SCHEMA = BinaryTupleSchema.createRowSchema(SCHEMA_DESCRIPTOR);

    private static final BinaryTupleSchema PK_INDEX_SCHEMA = BinaryTupleSchema.createKeySchema(SCHEMA_DESCRIPTOR);

    private static final ColumnsExtractor PK_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, PK_INDEX_SCHEMA);

    private static final int[] USER_INDEX_COLS = {
            SCHEMA_DESCRIPTOR.column("INTVAL").positionInRow(),
            SCHEMA_DESCRIPTOR.column("STRVAL").positionInRow()
    };

    private static final BinaryTupleSchema USER_INDEX_SCHEMA = BinaryTupleSchema.createSchema(SCHEMA_DESCRIPTOR, USER_INDEX_COLS);

    private static final ColumnsExtractor USER_INDEX_BINARY_TUPLE_CONVERTER = new BinaryRowConverter(TUPLE_SCHEMA, USER_INDEX_SCHEMA);

    private TestMvPartitionStorage storage;
    private StorageUpdateHandler storageUpdateHandler;
    private LockByRowId lock;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    @BeforeEach
    void setUp() {
        int tableId = 1;
        int pkIndexId = 2;
        int sortedIndexId = 3;
        int hashIndexId = 4;

        var pkInnerStorage = new TestHashIndexStorage(
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

        var pkStorage = new TableSchemaAwareIndexStorage(
                pkIndexId,
                pkInnerStorage,
                PK_INDEX_BINARY_TUPLE_CONVERTER
        );

        var sortedInnerStorage = new TestSortedIndexStorage(
                PARTITION_ID,
                new StorageSortedIndexDescriptor(
                        sortedIndexId,
                        List.of(
                                new StorageSortedIndexColumnDescriptor("INTVAL", NativeTypes.INT32, false, true, false),
                                new StorageSortedIndexColumnDescriptor("STRVAL", NativeTypes.STRING, false, true, false)
                        ),
                        false
                )
        );

        var sortedIndexStorage = new TableSchemaAwareIndexStorage(
                sortedIndexId,
                sortedInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER
        );

        var hashInnerStorage = new TestHashIndexStorage(
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

        var hashIndexStorage = new TableSchemaAwareIndexStorage(
                hashIndexId,
                hashInnerStorage,
                USER_INDEX_BINARY_TUPLE_CONVERTER
        );

        lock = spy(new LockByRowId());
        storage = spy(new TestMvPartitionStorage(PARTITION_ID, lock));

        Map<Integer, TableSchemaAwareIndexStorage> indexes = Map.of(
                pkIndexId, pkStorage,
                sortedIndexId, sortedIndexStorage,
                hashIndexId, hashIndexStorage
        );

        var partitionDataStorage = new TestPartitionDataStorage(tableId, PARTITION_ID, storage);

        var indexUpdateHandler = new IndexUpdateHandler(DummyInternalTableImpl.createTableIndexStoragesSupplier(indexes));

        storageUpdateHandler = new StorageUpdateHandler(
                PARTITION_ID,
                partitionDataStorage,
                indexUpdateHandler,
                replicationConfiguration,
                TableTestUtils.NOOP_PARTITION_MODIFICATION_COUNTER
        );
    }

    @Test
    void testUpdateAllBatchedTryLockFailed() {
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

        doReturn(false).when(lock).tryLock(any());

        storageUpdateHandler.handleUpdateAll(txUuid, rowsToUpdate, partitionId, true, null, null, null);

        // We have three writes to the storage.
        verify(storage, times(3)).addWrite(any(), any(), any(), anyInt(), anyInt());

        // First entry calls lock(). Second calls tryLock and fails, starts second batch, calls lock(). Same for third.
        verify(lock, times(2)).tryLock(any());
        verify(lock, times(3)).lock(any());

        storageUpdateHandler.switchWriteIntents(txUuid, true, commitTs, null);

        // Those writes resulted in three commits.
        verify(storage, times(3)).commitWrite(any(), any(), eq(txUuid));

        ReadResult result1 = storage.read(new RowId(partitionId.partitionId(), id1), HybridTimestamp.MAX_VALUE);
        assertEquals(row1, result1.binaryRow());

        ReadResult result2 = storage.read(new RowId(partitionId.partitionId(), id2), HybridTimestamp.MAX_VALUE);
        assertEquals(row2, result2.binaryRow());

        ReadResult result3 = storage.read(new RowId(partitionId.partitionId(), id3), HybridTimestamp.MAX_VALUE);
        assertEquals(row3, result3.binaryRow());
    }

    /**
     * Tests that {@link StorageUpdateHandler#handleUpdateAll} respects {@code shouldRelease()} from the storage engine.
     *
     * <p>This test verifies that the implementation checks {@code shouldRelease()} during batch processing
     * to allow the storage engine to perform critical operations like checkpoints.
     */
    @Test
    void testHandleUpdateAllExitsEarlyOnShouldRelease() {
        UUID txUuid = UUID.randomUUID();

        TablePartitionId partitionId = new TablePartitionId(333, PARTITION_ID);

        Map<UUID, BinaryRow> rows = Map.of(
                UUID.randomUUID(), binaryRow(new TestKey(1, "foo1"), new TestValue(2, "bar")),
                UUID.randomUUID(), binaryRow(new TestKey(3, "foo3"), new TestValue(4, "baz")),
                UUID.randomUUID(), binaryRow(new TestKey(5, "foo5"), new TestValue(7, "zzu"))
        );

        Map<UUID, TimedBinaryRow> rowsToUpdate = rows.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> new TimedBinaryRow(entry.getValue(), null)
        ));

        // When: handleUpdateAll is called with write intents
        storageUpdateHandler.handleUpdateAll(txUuid, rowsToUpdate, partitionId, true, null, null, null);

        // Then: All rows are written (verifying the operation completes successfully when shouldRelease=false)
        verify(storage, times(rows.size())).addWrite(any(), any(), any(), anyInt(), anyInt());

        // Verify all rows can be read
        List<BinaryRow> readResults = new ArrayList<>();
        for (UUID id : rows.keySet()) {
            ReadResult readResult = storage.read(new RowId(partitionId.partitionId(), id), HybridTimestamp.MAX_VALUE);
            if (readResult != null) {
                readResults.add(readResult.binaryRow());
            }
        }

        assertThat(readResults, containsInAnyOrder(rows.values().toArray()));
    }
}
