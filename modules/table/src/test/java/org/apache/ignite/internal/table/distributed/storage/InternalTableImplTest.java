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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.collectMultiRowsResponsesWithRestoreOrder;
import static org.apache.ignite.internal.table.distributed.storage.InternalTableImpl.collectRejectedRowsResponsesWithRestoreOrder;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.SingleClusterNodeResolver;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.NullBinaryRow;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.Test;

/**
 * For {@link InternalTableImpl} testing.
 */
public class InternalTableImplTest extends BaseIgniteAbstractTest {
    @Test
    void testUpdatePartitionTrackers() {
        InternalTableImpl internalTable = new InternalTableImpl(
                "test",
                1,
                1,
                new SingleClusterNodeResolver(mock(ClusterNode.class)),
                mock(TxManager.class),
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                mock(ReplicaService.class),
                mock(HybridClock.class),
                new HybridTimestampTracker(),
                mock(PlacementDriver.class),
                new TableRaftServiceImpl("test", 1, Int2ObjectMaps.emptyMap(), new SingleClusterNodeResolver(mock(ClusterNode.class))),
                mock(TransactionInflights.class),
                3_000,
                0,
                null,
                null
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

    @Test
    void testRowBatchByPartitionId() {
        InternalTableImpl internalTable = new InternalTableImpl(
                "test",
                1,
                3,
                new SingleClusterNodeResolver(mock(ClusterNode.class)),
                mock(TxManager.class),
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                mock(ReplicaService.class),
                mock(HybridClock.class),
                new HybridTimestampTracker(),
                mock(PlacementDriver.class),
                new TableRaftServiceImpl("test", 3, Int2ObjectMaps.emptyMap(), new SingleClusterNodeResolver(mock(ClusterNode.class))),
                mock(TransactionInflights.class),
                3_000,
                0,
                null,
                null
        );

        List<BinaryRowEx> originalRows = List.of(
                // Rows for 0 partition.
                createBinaryRows(0),
                createBinaryRows(0),
                // Rows for 1 partition.
                createBinaryRows(1),
                // Rows for 2 partition.
                createBinaryRows(2),
                createBinaryRows(2),
                createBinaryRows(2)
        );

        // We will get batches for processing and check them.
        Int2ObjectMap<RowBatch> rowBatchByPartitionId = internalTable.toRowBatchByPartitionId(originalRows);

        assertThat(rowBatchByPartitionId.get(0).requestedRows, hasSize(2));
        assertThat(rowBatchByPartitionId.get(0).originalRowOrder, contains(0, 1));

        assertThat(rowBatchByPartitionId.get(1).requestedRows, hasSize(1));
        assertThat(rowBatchByPartitionId.get(1).originalRowOrder, contains(2));

        assertThat(rowBatchByPartitionId.get(2).requestedRows, hasSize(3));
        assertThat(rowBatchByPartitionId.get(2).originalRowOrder, contains(3, 4, 5));

        // Collect the result and check it.
        rowBatchByPartitionId.get(0).resultFuture = completedFuture(List.of(originalRows.get(0), originalRows.get(1)));
        rowBatchByPartitionId.get(1).resultFuture = completedFuture(List.of(originalRows.get(2)));
        rowBatchByPartitionId.get(2).resultFuture = completedFuture(List.of(originalRows.get(3), originalRows.get(4), originalRows.get(5)));

        assertThat(
                collectMultiRowsResponsesWithRestoreOrder(rowBatchByPartitionId.values()),
                willBe(equalTo(originalRows))
        );

        var part1 = new ArrayList<>(2);

        part1.add(null);
        part1.add(new NullBinaryRow());

        rowBatchByPartitionId.get(0).resultFuture = completedFuture(part1);

        var part2 = new ArrayList<>(2);

        part2.add(null);

        rowBatchByPartitionId.get(1).resultFuture = completedFuture(part2);

        var part3 = new ArrayList<>(2);

        part3.add(new NullBinaryRow());
        part3.add(null);
        part3.add(new NullBinaryRow());

        rowBatchByPartitionId.get(2).resultFuture = completedFuture(part3);

        List<BinaryRowEx> rejectedRows = List.of(
                // Rows for 0 partition.
                originalRows.get(0),
                // Rows for 1 partition.
                originalRows.get(2),
                // Rows for 2 partition.
                originalRows.get(4)
        );

        assertThat(
                collectRejectedRowsResponsesWithRestoreOrder(rowBatchByPartitionId.values()),
                willBe(equalTo(rejectedRows))
        );
    }

    private static BinaryRowEx createBinaryRows(int colocationHash) {
        BinaryRowEx rowEx = mock(BinaryRowEx.class);

        when(rowEx.colocationHash()).thenReturn(colocationHash);

        return rowEx;
    }
}
