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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.RowId;
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

        assertFalse(gcUpdateHandler.vacuum(lowWatermark));
        verify(partitionStorage).pollForVacuum(lowWatermark);

        // Let's check that StorageUpdateHandler#vacuumBatch returns true.
        clearInvocations(partitionStorage);

        BinaryRowAndRowId binaryRowAndRowId = new BinaryRowAndRowId(mock(BinaryRow.class), new RowId(PARTITION_ID));

        when(partitionStorage.scanVersions(any(RowId.class))).thenReturn(emptyCursor());
        when(partitionStorage.pollForVacuum(lowWatermark)).thenReturn(binaryRowAndRowId);

        assertTrue(gcUpdateHandler.vacuum(lowWatermark));
        verify(partitionStorage).pollForVacuum(lowWatermark);
        verify(indexUpdateHandler).tryRemoveFromIndexes(binaryRowAndRowId.binaryRow(), binaryRowAndRowId.rowId(), emptyCursor());
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
}
