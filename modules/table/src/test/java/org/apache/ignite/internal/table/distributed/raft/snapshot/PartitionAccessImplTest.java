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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.junit.jupiter.api.Test;

/**
 * For {@link PartitionAccessImpl} testing.
 */
public class PartitionAccessImplTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID = 1;

    private static final int TEST_PARTITION_ID = 0;

    @Test
    void testMinMaxLastAppliedIndex() {
        TestMvTableStorage mvTableStorage = new TestMvTableStorage(TABLE_ID, DEFAULT_PARTITION_COUNT);
        TestTxStateTableStorage txStateTableStorage = new TestTxStateTableStorage();

        MvPartitionStorage mvPartitionStorage = createMvPartition(mvTableStorage, TEST_PARTITION_ID);
        TxStateStorage txStateStorage = txStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION_ID);

        PartitionAccess partitionAccess = new PartitionAccessImpl(
                testPartitionKey(),
                mvTableStorage,
                txStateTableStorage,
                mock(MvGc.class),
                mock(IndexUpdateHandler.class),
                mock(GcUpdateHandler.class)
        );

        assertEquals(0, partitionAccess.minLastAppliedIndex());
        assertEquals(0, partitionAccess.maxLastAppliedIndex());

        mvPartitionStorage.runConsistently(locker -> {
            mvPartitionStorage.lastApplied(10, 1);
            txStateStorage.lastApplied(5, 1);

            return null;
        });

        assertEquals(5, partitionAccess.minLastAppliedIndex());
        assertEquals(10, partitionAccess.maxLastAppliedIndex());

        mvPartitionStorage.runConsistently(locker -> {
            mvPartitionStorage.lastApplied(15, 2);

            txStateStorage.lastApplied(20, 2);

            return null;
        });

        assertEquals(15, partitionAccess.minLastAppliedIndex());
        assertEquals(20, partitionAccess.maxLastAppliedIndex());
    }

    private static PartitionKey testPartitionKey() {
        return new PartitionKey(1, TEST_PARTITION_ID);
    }

    @Test
    void testMinMaxLastAppliedTerm() {
        TestMvTableStorage mvTableStorage = new TestMvTableStorage(TABLE_ID, DEFAULT_PARTITION_COUNT);
        TestTxStateTableStorage txStateTableStorage = new TestTxStateTableStorage();

        MvPartitionStorage mvPartitionStorage = createMvPartition(mvTableStorage, TEST_PARTITION_ID);
        TxStateStorage txStateStorage = txStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION_ID);

        PartitionAccess partitionAccess = new PartitionAccessImpl(
                testPartitionKey(),
                mvTableStorage,
                txStateTableStorage,
                mock(MvGc.class),
                mock(IndexUpdateHandler.class),
                mock(GcUpdateHandler.class)
        );

        assertEquals(0, partitionAccess.minLastAppliedTerm());
        assertEquals(0, partitionAccess.maxLastAppliedTerm());

        mvPartitionStorage.runConsistently(locker -> {
            mvPartitionStorage.lastApplied(1, 10);
            txStateStorage.lastApplied(1, 5);

            return null;
        });

        assertEquals(5, partitionAccess.minLastAppliedTerm());
        assertEquals(10, partitionAccess.maxLastAppliedTerm());

        mvPartitionStorage.runConsistently(locker -> {
            mvPartitionStorage.lastApplied(2, 15);

            txStateStorage.lastApplied(2, 20);

            return null;
        });

        assertEquals(15, partitionAccess.minLastAppliedTerm());
        assertEquals(20, partitionAccess.maxLastAppliedTerm());
    }

    @Test
    void testAddWrite() {
        TestMvTableStorage mvTableStorage = new TestMvTableStorage(TABLE_ID, DEFAULT_PARTITION_COUNT);

        MvPartitionStorage mvPartitionStorage = createMvPartition(mvTableStorage, TEST_PARTITION_ID);

        IndexUpdateHandler indexUpdateHandler = mock(IndexUpdateHandler.class);

        PartitionAccess partitionAccess = new PartitionAccessImpl(
                testPartitionKey(),
                mvTableStorage,
                new TestTxStateTableStorage(),
                mock(MvGc.class),
                indexUpdateHandler,
                mock(GcUpdateHandler.class)
        );

        RowId rowId = new RowId(TEST_PARTITION_ID);
        BinaryRow binaryRow = mock(BinaryRow.class);
        UUID txId = UUID.randomUUID();
        int commitTableId = 999;

        partitionAccess.addWrite(rowId, binaryRow, txId, commitTableId, TEST_PARTITION_ID);

        verify(mvPartitionStorage, times(1)).addWrite(eq(rowId), eq(binaryRow), eq(txId), eq(commitTableId), eq(TEST_PARTITION_ID));

        verify(indexUpdateHandler, times(1)).addToIndexes(eq(binaryRow), eq(rowId));

        // Let's check with a null binaryRow.
        binaryRow = null;

        reset(mvPartitionStorage, indexUpdateHandler);

        partitionAccess.addWrite(rowId, binaryRow, txId, commitTableId, TEST_PARTITION_ID);

        verify(mvPartitionStorage, times(1)).addWrite(eq(rowId), eq(binaryRow), eq(txId), eq(commitTableId), eq(TEST_PARTITION_ID));

        verify(indexUpdateHandler, times(1)).addToIndexes(eq(binaryRow), eq(rowId));
    }

    @Test
    void testAddWriteCommitted() {
        TestMvTableStorage mvTableStorage = new TestMvTableStorage(TABLE_ID, DEFAULT_PARTITION_COUNT);

        MvPartitionStorage mvPartitionStorage = createMvPartition(mvTableStorage, TEST_PARTITION_ID);

        IndexUpdateHandler indexUpdateHandler = mock(IndexUpdateHandler.class);

        PartitionAccess partitionAccess = new PartitionAccessImpl(
                testPartitionKey(),
                mvTableStorage,
                new TestTxStateTableStorage(),
                mock(MvGc.class),
                indexUpdateHandler,
                mock(GcUpdateHandler.class)
        );

        RowId rowId = new RowId(TEST_PARTITION_ID);
        BinaryRow binaryRow = mock(BinaryRow.class);

        partitionAccess.addWriteCommitted(rowId, binaryRow, HybridTimestamp.MAX_VALUE);

        verify(mvPartitionStorage, times(1)).addWriteCommitted(eq(rowId), eq(binaryRow), eq(HybridTimestamp.MAX_VALUE));

        verify(indexUpdateHandler, times(1)).addToIndexes(eq(binaryRow), eq(rowId));

        // Let's check with a null binaryRow.
        binaryRow = null;

        reset(mvPartitionStorage, indexUpdateHandler);

        partitionAccess.addWriteCommitted(rowId, binaryRow, HybridTimestamp.MAX_VALUE);

        verify(mvPartitionStorage, times(1)).addWriteCommitted(eq(rowId), eq(binaryRow), eq(HybridTimestamp.MAX_VALUE));

        verify(indexUpdateHandler, times(1)).addToIndexes(eq(binaryRow), eq(rowId));
    }

    private static MvPartitionStorage createMvPartition(MvTableStorage tableStorage, int partitionId) {
        CompletableFuture<MvPartitionStorage> createMvPartitionFuture = tableStorage.createMvPartition(partitionId);

        assertThat(createMvPartitionFuture, willCompleteSuccessfully());

        return createMvPartitionFuture.join();
    }
}
