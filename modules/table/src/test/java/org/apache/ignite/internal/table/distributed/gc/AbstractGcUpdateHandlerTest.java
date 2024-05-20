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

package org.apache.ignite.internal.table.distributed.gc;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Abstract class for testing {@link GcUpdateHandler} using different implementations of {@link MvPartitionStorage}.
 */
abstract class AbstractGcUpdateHandlerTest extends BaseMvStoragesTest {
    /** To be used in a loop. {@link RepeatedTest} has a smaller failure rate due to recreating the storage every time. */
    private static final int REPEATS = 1000;

    protected static final int TABLE_ID = 1;

    private static final int PARTITION_ID = 0;

    private MvTableStorage tableStorage;

    /**
     * Initializes the internal structures needed for tests.
     *
     * <p>This method *MUST* always be called in either subclass' constructor or setUp method.
     */
    final void initialize(MvTableStorage tableStorage) {
        this.tableStorage = tableStorage;
    }

    @ParameterizedTest(name = "strict : {0}")
    @ValueSource(booleans = {true, false})
    void testVacuum(boolean strict) {
        TestPartitionDataStorage partitionStorage = spy(createPartitionDataStorage());
        IndexUpdateHandler indexUpdateHandler = spy(createIndexUpdateHandler());

        GcUpdateHandler gcUpdateHandler = createGcUpdateHandler(partitionStorage, indexUpdateHandler);

        HybridTimestamp lowWatermark = HybridTimestamp.MAX_VALUE;

        assertFalse(gcUpdateHandler.vacuumBatch(lowWatermark, 1, strict));
        verify(partitionStorage).peek(lowWatermark);

        // Let's check that StorageUpdateHandler#vacuumBatch returns true.
        clearInvocations(partitionStorage);

        RowId rowId = new RowId(PARTITION_ID);
        BinaryRow row = binaryRow(new TestKey(0, "key"), new TestValue(0, "value"));

        addWriteCommitted(partitionStorage, rowId, row, clock.now());
        addWriteCommitted(partitionStorage, rowId, row, clock.now());

        assertTrue(gcUpdateHandler.vacuumBatch(lowWatermark, 1, strict));
        verify(partitionStorage).peek(lowWatermark);
        verify(indexUpdateHandler).tryRemoveFromIndexes(any(), eq(rowId), any(), isNull());
    }

    @ParameterizedTest(name = "strict : {0}")
    @ValueSource(booleans = {true, false})
    void testVacuumBatch(boolean strict) {
        TestPartitionDataStorage partitionStorage = spy(createPartitionDataStorage());
        IndexUpdateHandler indexUpdateHandler = spy(createIndexUpdateHandler());

        GcUpdateHandler gcUpdateHandler = createGcUpdateHandler(partitionStorage, indexUpdateHandler);

        HybridTimestamp lowWatermark = HybridTimestamp.MAX_VALUE;

        RowId rowId0 = new RowId(PARTITION_ID);
        RowId rowId1 = new RowId(PARTITION_ID);

        BinaryRow row0 = binaryRow(new TestKey(0, "key0"), new TestValue(0, "value0"));
        BinaryRow row1 = binaryRow(new TestKey(0, "key1"), new TestValue(0, "value0"));

        addWriteCommitted(partitionStorage, rowId0, row0, clock.now());
        addWriteCommitted(partitionStorage, rowId0, row0, clock.now());

        addWriteCommitted(partitionStorage, rowId1, row1, clock.now());
        addWriteCommitted(partitionStorage, rowId1, row1, clock.now());

        assertFalse(gcUpdateHandler.vacuumBatch(lowWatermark, 5, strict));

        verify(partitionStorage, times(3)).peek(lowWatermark);
        verify(indexUpdateHandler).tryRemoveFromIndexes(any(), eq(rowId0), any(), isNull());
        verify(indexUpdateHandler).tryRemoveFromIndexes(any(), eq(rowId1), any(), isNull());
    }

    @Test
    void testConcurrentVacuumBatchStrictTrue() {
        TestPartitionDataStorage partitionStorage = createPartitionDataStorage();
        IndexUpdateHandler indexUpdateHandler = createIndexUpdateHandler();

        GcUpdateHandler gcUpdateHandler = createGcUpdateHandler(partitionStorage, indexUpdateHandler);

        RowId rowId0 = new RowId(PARTITION_ID);
        RowId rowId1 = new RowId(PARTITION_ID);

        BinaryRow row0 = binaryRow(new TestKey(0, "key0"), new TestValue(0, "value0"));
        BinaryRow row1 = binaryRow(new TestKey(1, "key1"), new TestValue(1, "value1"));

        for (int i = 0; i < REPEATS; i++) {
            addWriteCommitted(partitionStorage, rowId0, row0, clock.now());
            addWriteCommitted(partitionStorage, rowId1, row1, clock.now());

            addWriteCommitted(partitionStorage, rowId0, row0, clock.now());
            addWriteCommitted(partitionStorage, rowId1, row1, clock.now());

            addWriteCommitted(partitionStorage, rowId0, null, clock.now());
            addWriteCommitted(partitionStorage, rowId1, null, clock.now());

            runRace(
                    () -> gcUpdateHandler.vacuumBatch(HybridTimestamp.MAX_VALUE, 2, true),
                    () -> gcUpdateHandler.vacuumBatch(HybridTimestamp.MAX_VALUE, 2, true)
            );

            assertNull(partitionStorage.getStorage().closestRowId(RowId.lowestRowId(PARTITION_ID)));
        }
    }

    @Test
    void testConcurrentVacuumBatchStrictFalse() {
        TestPartitionDataStorage partitionStorage = createPartitionDataStorage();
        IndexUpdateHandler indexUpdateHandler = createIndexUpdateHandler();

        GcUpdateHandler gcUpdateHandler = createGcUpdateHandler(partitionStorage, indexUpdateHandler);

        RowId rowId0 = new RowId(PARTITION_ID);
        RowId rowId1 = new RowId(PARTITION_ID);
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        BinaryRow row0 = binaryRow(new TestKey(0, "key0"), new TestValue(0, "value0"));
        BinaryRow row1 = binaryRow(new TestKey(1, "key1"), new TestValue(1, "value1"));
        BinaryRow row2 = binaryRow(new TestKey(2, "key2"), new TestValue(2, "value2"));
        BinaryRow row3 = binaryRow(new TestKey(3, "key3"), new TestValue(3, "value3"));

        for (int i = 0; i < REPEATS; i++) {
            addWriteCommitted(partitionStorage, rowId0, row0, clock.now());
            addWriteCommitted(partitionStorage, rowId1, row1, clock.now());
            addWriteCommitted(partitionStorage, rowId2, row2, clock.now());
            addWriteCommitted(partitionStorage, rowId3, row3, clock.now());

            addWriteCommitted(partitionStorage, rowId0, null, clock.now());
            addWriteCommitted(partitionStorage, rowId1, null, clock.now());
            addWriteCommitted(partitionStorage, rowId2, null, clock.now());
            addWriteCommitted(partitionStorage, rowId3, null, clock.now());

            runRace(
                    () -> gcUpdateHandler.vacuumBatch(HybridTimestamp.MAX_VALUE, 4, false),
                    () -> gcUpdateHandler.vacuumBatch(HybridTimestamp.MAX_VALUE, 4, false)
            );

            assertNull(partitionStorage.getStorage().closestRowId(RowId.lowestRowId(PARTITION_ID)));
        }
    }

    /**
     * Tests a particular scenario when some data is inserted into multiple partition storages, then removed by the GC and then inserted
     * again.
     */
    @Test
    void testVacuumThenInsert() {
        int numPartitions = 3;

        int numRows = 1000;

        IndexUpdateHandler indexUpdateHandler = createIndexUpdateHandler();

        List<TestPartitionDataStorage> partitionStorages = IntStream.range(0, numPartitions)
                .mapToObj(partId -> new TestPartitionDataStorage(TABLE_ID, partId, getOrCreateMvPartition(tableStorage, partId)))
                .collect(toList());

        List<GcUpdateHandler> gcUpdateHandlers = partitionStorages.stream()
                .map(partitionStorage -> createGcUpdateHandler(partitionStorage, indexUpdateHandler))
                .collect(toList());

        BinaryRow row = binaryRow(new TestKey(0, "key"), new TestValue(0, "value"));

        HybridTimestamp timestamp = clock.now();

        for (int i = 0; i < numPartitions; i++) {
            TestPartitionDataStorage storage = partitionStorages.get(i);

            for (int j = 0; j < numRows; j++) {
                var rowId = new RowId(i);

                addWriteCommitted(storage, rowId, row, timestamp);
                addWriteCommitted(storage, rowId, null, timestamp);
            }
        }

        for (GcUpdateHandler gcUpdateHandler : gcUpdateHandlers) {
            gcUpdateHandler.vacuumBatch(HybridTimestamp.MAX_VALUE, Integer.MAX_VALUE, true);
        }

        for (int i = 0 ; i < numPartitions; i++) {
            TestPartitionDataStorage storage = partitionStorages.get(i);

            for (int j = 0; j < numRows; j++) {
                var rowId = new RowId(i);

                addWriteCommitted(storage, rowId, row, timestamp);
            }
        }
    }

    private TestPartitionDataStorage createPartitionDataStorage() {
        return new TestPartitionDataStorage(TABLE_ID, PARTITION_ID, getOrCreateMvPartition(tableStorage, PARTITION_ID));
    }

    private static IndexUpdateHandler createIndexUpdateHandler() {
        // Don’t use mocking to avoid performance degradation for concurrent tests.
        return new IndexUpdateHandler(DummyInternalTableImpl.createTableIndexStoragesSupplier(Map.of())) {
            @Override
            public void tryRemoveFromIndexes(
                    BinaryRow rowToRemove,
                    RowId rowId,
                    Cursor<ReadResult> previousValues,
                    @Nullable List<Integer> indexIds
            ) {
                // No-op.
            }
        };
    }

    private static GcUpdateHandler createGcUpdateHandler(
            PartitionDataStorage partitionDataStorage,
            IndexUpdateHandler indexUpdateHandler
    ) {
        return new GcUpdateHandler(
                partitionDataStorage,
                new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0)),
                indexUpdateHandler
        );
    }

    private static void addWriteCommitted(PartitionDataStorage storage, RowId rowId, @Nullable BinaryRow row, HybridTimestamp timestamp) {
        storage.runConsistently(locker -> {
            locker.lock(rowId);

            storage.addWrite(rowId, row, UUID.randomUUID(), 999, PARTITION_ID);

            storage.commitWrite(rowId, timestamp);

            return null;
        });
    }
}
