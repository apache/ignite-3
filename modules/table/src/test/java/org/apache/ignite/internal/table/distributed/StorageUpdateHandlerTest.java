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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link StorageUpdateHandler} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class StorageUpdateHandlerTest extends BaseIgniteAbstractTest {
    private static final int PARTITION_ID = 0;

    @InjectConfiguration
    private GcConfiguration gcConfig;

    private final PendingComparableValuesTracker<HybridTimestamp, Void> safeTimeTracker = spy(new PendingComparableValuesTracker<>(
            new HybridTimestamp(1, 0)
    ));

    private final LowWatermark lowWatermark = mock(LowWatermark.class);

    @Test
    void testExecuteBatchGc() {
        PartitionDataStorage partitionStorage = createPartitionDataStorage();

        StorageUpdateHandler storageUpdateHandler = createStorageUpdateHandler(partitionStorage, mock(TableIndexStoragesSupplier.class));

        AtomicReference<HybridTimestamp> lowWatermarkReference = new AtomicReference<>();

        when(lowWatermark.getLowWatermark()).then(invocation -> lowWatermarkReference.get());

        // Let's check that if lwm is {@code null} then nothing will happen.
        storageUpdateHandler.executeBatchGc();

        verify(partitionStorage, never()).peek(any(HybridTimestamp.class));

        // Let's check that if lvm is greater than the safe time, then nothing will happen.
        safeTimeTracker.update(new HybridTimestamp(10, 10), null);

        lowWatermarkReference.set(new HybridTimestamp(11, 1));

        storageUpdateHandler.executeBatchGc();

        verify(partitionStorage, never()).peek(any(HybridTimestamp.class));

        // Let's check that if lvm is equal to or less than the safe time, then garbage collection will be executed.
        lowWatermarkReference.set(new HybridTimestamp(10, 10));

        storageUpdateHandler.executeBatchGc();

        verify(partitionStorage, times(1)).peek(any(HybridTimestamp.class));

        lowWatermarkReference.set(new HybridTimestamp(9, 9));

        storageUpdateHandler.executeBatchGc();

        verify(partitionStorage, times(2)).peek(any(HybridTimestamp.class));
    }

    @Test
    void testInvokeGcOnHandleUpdate() {
        PartitionDataStorage partitionStorage = createPartitionDataStorage();

        StorageUpdateHandler storageUpdateHandler = createStorageUpdateHandler(partitionStorage, mock(TableIndexStoragesSupplier.class));

        HybridTimestamp lwm = safeTimeTracker.current();

        when(lowWatermark.getLowWatermark()).thenReturn(lwm);

        storageUpdateHandler.handleUpdate(
                UUID.randomUUID(),
                UUID.randomUUID(),
                new TablePartitionId(1, PARTITION_ID),
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
                new TablePartitionId(1, PARTITION_ID),
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
                gcConfig,
                lowWatermark,
                indexUpdateHandler,
                new GcUpdateHandler(partitionStorage, safeTimeTracker, indexUpdateHandler)
        );
    }

    private static PartitionDataStorage createPartitionDataStorage() {
        PartitionDataStorage partitionStorage = spy(new TestPartitionDataStorage(new TestMvPartitionStorage(PARTITION_ID)));

        return partitionStorage;
    }
}
