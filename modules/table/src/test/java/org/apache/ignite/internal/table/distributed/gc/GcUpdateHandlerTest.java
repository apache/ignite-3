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

import static org.apache.ignite.internal.util.CursorUtils.emptyCursor;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.junit.jupiter.api.Test;

/**
 * For {@link GcUpdateHandler} testing.
 */
public class GcUpdateHandlerTest {
    private static final int PARTITION_ID = 0;

    @Test
    void testVacuum() {
        PartitionDataStorage partitionStorage = createPartitionDataStorage();

        IndexUpdateHandler indexUpdateHandler = spy(new IndexUpdateHandler(mock(TableIndexStoragesSupplier.class)));

        GcUpdateHandler gcUpdateHandler = createGcUpdateHandler(partitionStorage, indexUpdateHandler);

        HybridTimestamp lowWatermark = new HybridTimestamp(100, 100);

        assertFalse(gcUpdateHandler.vacuumBatch(lowWatermark, 1));
        verify(partitionStorage).peek(lowWatermark);

        // Let's check that StorageUpdateHandler#vacuumBatch returns true.
        clearInvocations(partitionStorage);

        RowId rowId = new RowId(PARTITION_ID);

        GcEntry gcEntry = new GcEntryImpl(rowId, lowWatermark);
        BinaryRow binaryRow = mock(BinaryRow.class);

        when(partitionStorage.scanVersions(any(RowId.class))).thenReturn(emptyCursor());
        when(partitionStorage.peek(lowWatermark)).thenReturn(gcEntry);
        when(partitionStorage.vacuum(gcEntry)).thenReturn(binaryRow);

        assertTrue(gcUpdateHandler.vacuumBatch(lowWatermark, 1));
        verify(partitionStorage).peek(lowWatermark);
        verify(indexUpdateHandler).tryRemoveFromIndexes(binaryRow, rowId, emptyCursor());
    }

    @Test
    void testVacuumBatch() {
        PartitionDataStorage partitionStorage = createPartitionDataStorage();

        IndexUpdateHandler indexUpdateHandler = spy(new IndexUpdateHandler(mock(TableIndexStoragesSupplier.class)));

        GcUpdateHandler gcUpdateHandler = createGcUpdateHandler(partitionStorage, indexUpdateHandler);

        HybridTimestamp lowWatermark = new HybridTimestamp(100, 100);

        RowId rowId0 = new RowId(PARTITION_ID);
        RowId rowId1 = new RowId(PARTITION_ID);

        BinaryRow binaryRow0 = mock(BinaryRow.class);
        BinaryRow binaryRow1 = mock(BinaryRow.class);

        GcEntry gcEntry0 = new GcEntryImpl(rowId0, lowWatermark);
        GcEntry gcEntry1 = new GcEntryImpl(rowId1, lowWatermark);

        when(partitionStorage.peek(lowWatermark)).thenReturn(gcEntry0).thenReturn(gcEntry1);
        when(partitionStorage.vacuum(gcEntry0)).thenReturn(binaryRow0);
        when(partitionStorage.vacuum(gcEntry1)).thenReturn(binaryRow1);

        assertTrue(gcUpdateHandler.vacuumBatch(lowWatermark, 2));

        verify(partitionStorage, times(2)).peek(lowWatermark);
        verify(partitionStorage).vacuum(gcEntry0);
        verify(partitionStorage).vacuum(gcEntry1);
    }

    private GcUpdateHandler createGcUpdateHandler(PartitionDataStorage partitionStorage, IndexUpdateHandler indexUpdateHandler) {
        return new GcUpdateHandler(
                partitionStorage,
                new PendingComparableValuesTracker<>(new HybridTimestamp(1, 0)),
                indexUpdateHandler
        );
    }

    private static PartitionDataStorage createPartitionDataStorage() {
        PartitionDataStorage partitionStorage = spy(new TestPartitionDataStorage(new TestMvPartitionStorage(PARTITION_ID)));

        return partitionStorage;
    }

    private static class GcEntryImpl implements GcEntry {
        private final RowId rowId;

        private final HybridTimestamp timestamp;

        private GcEntryImpl(RowId rowId, HybridTimestamp timestamp) {
            this.rowId = rowId;
            this.timestamp = timestamp;
        }

        @Override
        public RowId getRowId() {
            return rowId;
        }

        @Override
        public HybridTimestamp getTimestamp() {
            return timestamp;
        }
    }
}
