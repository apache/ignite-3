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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfiguration;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.CursorUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link StorageUpdateHandler} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class StorageUpdateHandlerTest {
    private static final int PARTITION_ID = 0;

    @InjectConfiguration
    private DataStorageConfiguration dataStorageConfig;

    private final HybridClock clock = new HybridClockImpl();

    private final PendingComparableValuesTracker<HybridTimestamp> safeTimeTracker = spy(new PendingComparableValuesTracker<>(
            new HybridTimestamp(1, 0)
    ));

    @Test
    void testBuildIndex() {
        PartitionDataStorage partitionStorage = mock(PartitionDataStorage.class);

        TableSchemaAwareIndexStorage indexStorage = createIndexStorage();

        UUID indexId = UUID.randomUUID();

        TableIndexStoragesSupplier indexes = mock(TableIndexStoragesSupplier.class);

        when(indexes.get()).thenReturn(Map.of(indexId, indexStorage));

        StorageUpdateHandler storageUpdateHandler = createStorageUpdateHandler(partitionStorage, indexes);

        RowId rowId0 = new RowId(PARTITION_ID);
        RowId rowId1 = new RowId(PARTITION_ID);

        List<BinaryRow> rowVersions0 = asList(mock(BinaryRow.class), null);
        List<BinaryRow> rowVersions1 = asList(mock(BinaryRow.class), null);

        setRowVersions(partitionStorage, Map.of(rowId0.uuid(), rowVersions0, rowId1.uuid(), rowVersions1));

        storageUpdateHandler.buildIndex(indexId, List.of(rowId0.uuid(), rowId1.uuid()), false);

        verify(indexStorage).put(rowVersions0.get(0), rowId0);
        verify(indexStorage, never()).put(rowVersions0.get(1), rowId0);

        verify(indexStorage).put(rowVersions1.get(0), rowId1);
        verify(indexStorage, never()).put(rowVersions1.get(1), rowId1);

        verify(indexStorage.storage()).setNextRowIdToBuild(rowId1.increment());
        verify(indexes).addIndexToWaitIfAbsent(indexId);

        // Let's check one more batch - it will be the finishing one.
        RowId rowId2 = new RowId(PARTITION_ID, UUID.randomUUID());

        List<BinaryRow> rowVersions2 = singletonList(mock(BinaryRow.class));

        setRowVersions(partitionStorage, Map.of(rowId2.uuid(), rowVersions2));

        storageUpdateHandler.buildIndex(indexId, List.of(rowId2.uuid()), true);

        verify(indexStorage).put(rowVersions2.get(0), rowId2);

        verify(indexStorage.storage()).setNextRowIdToBuild(null);
        verify(indexes, times(2)).addIndexToWaitIfAbsent(indexId);
    }

    @Test
    void testVacuum() {
        PartitionDataStorage partitionStorage = createPartitionDataStorage();

        StorageUpdateHandler storageUpdateHandler = createStorageUpdateHandler(partitionStorage, mock(TableIndexStoragesSupplier.class));

        HybridTimestamp lowWatermark = new HybridTimestamp(100, 100);

        assertFalse(storageUpdateHandler.vacuum(lowWatermark));
        verify(partitionStorage).pollForVacuum(lowWatermark);
        // Let's check that StorageUpdateHandler#vacuumBatch returns true.
        clearInvocations(partitionStorage);

        BinaryRowAndRowId binaryRowAndRowId = new BinaryRowAndRowId(mock(BinaryRow.class), new RowId(PARTITION_ID));

        when(partitionStorage.scanVersions(any(RowId.class))).thenReturn(CursorUtils.emptyCursor());
        when(partitionStorage.pollForVacuum(lowWatermark)).thenReturn(binaryRowAndRowId);

        assertTrue(storageUpdateHandler.vacuum(lowWatermark));
        verify(partitionStorage).pollForVacuum(lowWatermark);
    }

    @Test
    void testExecuteBatchGc() {
        PartitionDataStorage partitionStorage = createPartitionDataStorage();

        StorageUpdateHandler storageUpdateHandler = createStorageUpdateHandler(partitionStorage, mock(TableIndexStoragesSupplier.class));

        // Let's check that if lwm is {@code null} then nothing will happen.
        storageUpdateHandler.executeBatchGc(null);

        verify(partitionStorage, never()).pollForVacuum(any(HybridTimestamp.class));

        // Let's check that if lvm is greater than the safe time, then nothing will happen.
        safeTimeTracker.update(new HybridTimestamp(10, 10));

        storageUpdateHandler.executeBatchGc(new HybridTimestamp(11, 1));

        verify(partitionStorage, never()).pollForVacuum(any(HybridTimestamp.class));

        // Let's check that if lvm is equal to or less than the safe time, then garbage collection will be executed.
        storageUpdateHandler.executeBatchGc(new HybridTimestamp(10, 10));

        verify(partitionStorage, times(1)).pollForVacuum(any(HybridTimestamp.class));

        storageUpdateHandler.executeBatchGc(new HybridTimestamp(9, 9));

        verify(partitionStorage, times(2)).pollForVacuum(any(HybridTimestamp.class));
    }

    private static TableSchemaAwareIndexStorage createIndexStorage() {
        TableSchemaAwareIndexStorage indexStorage = mock(TableSchemaAwareIndexStorage.class);

        IndexStorage storage = mock(IndexStorage.class);

        when(indexStorage.storage()).thenReturn(storage);

        return indexStorage;
    }

    private StorageUpdateHandler createStorageUpdateHandler(PartitionDataStorage partitionStorage, TableIndexStoragesSupplier indexes) {
        return new StorageUpdateHandler(PARTITION_ID, partitionStorage, indexes, dataStorageConfig, safeTimeTracker);
    }

    private void setRowVersions(PartitionDataStorage partitionStorage, Map<UUID, List<BinaryRow>> rowVersions) {
        for (Entry<UUID, List<BinaryRow>> entry : rowVersions.entrySet()) {
            RowId rowId = new RowId(PARTITION_ID, entry.getKey());

            List<ReadResult> readResults = entry.getValue().stream()
                    .map(binaryRow -> ReadResult.createFromCommitted(rowId, binaryRow, clock.now()))
                    .collect(toList());

            when(partitionStorage.scanVersions(rowId)).thenReturn(Cursor.fromIterable(readResults));
        }
    }

    private static PartitionDataStorage createPartitionDataStorage() {
        PartitionDataStorage partitionStorage = spy(new TestPartitionDataStorage(new TestMvPartitionStorage(0)));

        return partitionStorage;
    }
}
