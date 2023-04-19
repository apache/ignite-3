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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.configuration.storage.DataStorageConfiguration;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
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
    void testVacuumBatch() {
        PartitionDataStorage partitionStorage = mock(PartitionDataStorage.class);

        when(partitionStorage.scanVersions(any(RowId.class))).thenReturn(CursorUtils.emptyCursor());

        when(partitionStorage.runConsistently(any(WriteClosure.class)))
                .then(invocation -> ((WriteClosure<?>) invocation.getArgument(0)).execute());

        TableIndexStoragesSupplier indexes = mock(TableIndexStoragesSupplier.class);

        StorageUpdateHandler storageUpdateHandler = createStorageUpdateHandler(partitionStorage, indexes);

        HybridTimestamp lowWatermark = new HybridTimestamp(100, 100);

        CompletableFuture<Void> startWaitSafeTime = new CompletableFuture<>();
        CompletableFuture<Void> finishWaitSafeTime = new CompletableFuture<>();

        when(safeTimeTracker.waitFor(lowWatermark)).then(invocation -> {
            startWaitSafeTime.complete(null);

            return finishWaitSafeTime;
        });

        CompletableFuture<Boolean> vacuumBatchFuture = storageUpdateHandler.vacuumBatch(lowWatermark, 2);

        assertThat(startWaitSafeTime, willSucceedFast());

        assertFalse(vacuumBatchFuture.isDone());
        verify(partitionStorage, never()).pollForVacuum(lowWatermark);
        verify(indexes, never()).get();

        finishWaitSafeTime.complete(null);

        assertThat(vacuumBatchFuture, willBe(false));
        verify(partitionStorage).pollForVacuum(lowWatermark);

        // Let's check that StorageUpdateHandler#vacuumBatch returns true.
        clearInvocations(partitionStorage, indexes);

        BinaryRowAndRowId binaryRowAndRowId = new BinaryRowAndRowId(mock(BinaryRow.class), new RowId(PARTITION_ID));

        when(partitionStorage.pollForVacuum(lowWatermark)).thenReturn(binaryRowAndRowId);

        assertThat(storageUpdateHandler.vacuumBatch(lowWatermark, 3), willBe(true));
        verify(partitionStorage, times(3)).pollForVacuum(lowWatermark);
        verify(indexes, times(3)).get();
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
}
