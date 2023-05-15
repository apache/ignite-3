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

package org.apache.ignite.internal.table.distributed.storage;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Test;

/**
 * For {@link InternalTableImpl} testing.
 */
public class InternalTableImplTest {
    @Test
    void testUpdatePartitionTrackers() {
        InternalTableImpl internalTable = new InternalTableImpl(
                "test",
                UUID.randomUUID(),
                Int2ObjectMaps.emptyMap(),
                1,
                s -> mock(ClusterNode.class),
                mock(TxManager.class),
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                mock(ReplicaService.class),
                mock(HybridClock.class)
        );

        // Let's check the empty table.
        assertNull(internalTable.getPartitionSafeTimeTracker(0));
        assertNull(internalTable.getPartitionStorageIndexTracker(0));

        // Let's check the first insert.
        PendingComparableValuesTracker<HybridTimestamp, Void> safeTime0 = mock(PendingComparableValuesTracker.class);
        PendingComparableValuesTracker<Long, Void> storageIndex0 = mock(PendingComparableValuesTracker.class);

        internalTable.updatePartitionTrackers(0, safeTime0, storageIndex0);

        assertSame(safeTime0, internalTable.getPartitionSafeTimeTracker(0));
        assertSame(storageIndex0, internalTable.getPartitionStorageIndexTracker(0));

        verify(safeTime0, never()).close();
        verify(storageIndex0, never()).close();

        // Let's check the new insert.
        PendingComparableValuesTracker<HybridTimestamp, Void> safeTime1 = mock(PendingComparableValuesTracker.class);
        PendingComparableValuesTracker<Long, Void> storageIndex1 = mock(PendingComparableValuesTracker.class);

        internalTable.updatePartitionTrackers(0, safeTime1, storageIndex1);

        assertSame(safeTime1, internalTable.getPartitionSafeTimeTracker(0));
        assertSame(storageIndex1, internalTable.getPartitionStorageIndexTracker(0));

        verify(safeTime0).close();
        verify(storageIndex0).close();
    }
}
