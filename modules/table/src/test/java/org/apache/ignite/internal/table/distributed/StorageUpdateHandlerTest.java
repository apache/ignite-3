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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.junit.jupiter.api.Test;

/**
 * For {@link StorageUpdateHandler} testing.
 */
public class StorageUpdateHandlerTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID = 1;

    private static final int PARTITION_ID = 0;

    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker = spy(new PendingComparableValuesTracker<>(
            new HybridTimestamp(1, 0)
    ));

    private final LowWatermark lowWatermark = mock(LowWatermark.class);

    @Test
    void testInvokeGcOnHandleUpdate() {
        PartitionDataStorage partitionStorage = createPartitionDataStorage();

        StorageUpdateHandler storageUpdateHandler = createStorageUpdateHandler(partitionStorage, mock(TableIndexStoragesSupplier.class));

        HybridTimestamp lwm = safeTimeTracker.current();

        when(lowWatermark.getLowWatermark()).thenReturn(lwm);

        storageUpdateHandler.handleUpdate(
                UUID.randomUUID(),
                UUID.randomUUID(),
                new TablePartitionId(TABLE_ID, PARTITION_ID),
                null,
                false,
                null,
                null,
                null
        );

        verify(partitionStorage).peek(lwm);
    }

    @Test
    void testInvokeGcOnHandleUpdateAll() {
        PartitionDataStorage partitionStorage = createPartitionDataStorage();

        StorageUpdateHandler storageUpdateHandler = createStorageUpdateHandler(partitionStorage, mock(TableIndexStoragesSupplier.class));

        HybridTimestamp lwm = safeTimeTracker.current();

        when(lowWatermark.getLowWatermark()).thenReturn(lwm);

        storageUpdateHandler.handleUpdateAll(
                UUID.randomUUID(),
                Map.of(),
                new TablePartitionId(TABLE_ID, PARTITION_ID),
                false,
                null,
                null
        );

        verify(partitionStorage).peek(lwm);
    }

    private StorageUpdateHandler createStorageUpdateHandler(PartitionDataStorage partitionStorage, TableIndexStoragesSupplier indexes) {
        IndexUpdateHandler indexUpdateHandler = new IndexUpdateHandler(indexes);

        return new StorageUpdateHandler(
                PARTITION_ID,
                partitionStorage,
                indexUpdateHandler
        );
    }

    private static PartitionDataStorage createPartitionDataStorage() {
        PartitionDataStorage partitionStorage = spy(
                new TestPartitionDataStorage(TABLE_ID, PARTITION_ID, new TestMvPartitionStorage(PARTITION_ID))
        );

        return partitionStorage;
    }
}
