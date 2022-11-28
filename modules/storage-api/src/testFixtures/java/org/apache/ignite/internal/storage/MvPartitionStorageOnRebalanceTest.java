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

package org.apache.ignite.internal.storage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * For {@link MvPartitionStorageOnRebalance} testing.
 */
@ExtendWith(MockitoExtension.class)
public class MvPartitionStorageOnRebalanceTest {
    private static final int PARTITION_ID = 0;

    @Mock
    private MvPartitionStorage mvPartitionStorage;

    private MvPartitionStorageOnRebalance mvPartitionStorageOnRebalance;

    @BeforeEach
    void setUp() {
        mvPartitionStorageOnRebalance = new MvPartitionStorageOnRebalance(mvPartitionStorage);
    }

    @Test
    void testRunConsistently(@Mock WriteClosure writeClosure) {
        mvPartitionStorageOnRebalance.runConsistently(writeClosure);

        verify(mvPartitionStorage, times(1)).runConsistently(eq(writeClosure));
    }

    @Test
    void testFlush() {
        mvPartitionStorageOnRebalance.flush();

        verify(mvPartitionStorage, times(1)).flush();
    }

    @Test
    void testAppliedIndex() {
        mvPartitionStorageOnRebalance.lastAppliedIndex();

        verify(mvPartitionStorage, times(1)).lastAppliedIndex();

        mvPartitionStorageOnRebalance.lastApplied(1L, 2L);

        verify(mvPartitionStorage, times(1)).lastApplied(eq(1L), eq(2L));

        mvPartitionStorageOnRebalance.persistedIndex();

        verify(mvPartitionStorage, times(1)).persistedIndex();
    }

    @Test
    void testRead() {
        assertThrows(
                IllegalStateException.class,
                () -> mvPartitionStorageOnRebalance.read(new RowId(PARTITION_ID), new HybridClockImpl().now())
        );
    }

    @Test
    void testWrites() {
        RowId rowId = new RowId(PARTITION_ID);
        BinaryRow row = mock(BinaryRow.class);
        UUID txId = UUID.randomUUID();
        UUID commitTableId = UUID.randomUUID();

        mvPartitionStorageOnRebalance.addWrite(rowId, row, txId, commitTableId, PARTITION_ID);

        verify(mvPartitionStorage, times(1)).addWrite(eq(rowId), eq(row), eq(txId), eq(commitTableId), eq(PARTITION_ID));

        HybridTimestamp commitTimestamp = new HybridClockImpl().now();

        mvPartitionStorageOnRebalance.addWriteCommitted(rowId, row, commitTimestamp);

        verify(mvPartitionStorage, times(1)).addWriteCommitted(eq(rowId), eq(row), eq(commitTimestamp));

        mvPartitionStorageOnRebalance.commitWrite(rowId, commitTimestamp);

        verify(mvPartitionStorage, times(1)).commitWrite(eq(rowId), eq(commitTimestamp));

        mvPartitionStorageOnRebalance.abortWrite(rowId);

        verify(mvPartitionStorage, times(1)).abortWrite(eq(rowId));
    }

    @Test
    void testScans() {
        assertThrows(IllegalStateException.class, () -> mvPartitionStorageOnRebalance.scan(new HybridClockImpl().now()));

        assertThrows(IllegalStateException.class, () -> mvPartitionStorageOnRebalance.scanVersions(new RowId(PARTITION_ID)));
    }

    @Test
    void testClosestRowId() {
        assertThrows(IllegalStateException.class, () -> mvPartitionStorageOnRebalance.closestRowId(new RowId(PARTITION_ID)));
    }

    @Test
    void testRowsCount() {
        mvPartitionStorageOnRebalance.rowsCount();

        verify(mvPartitionStorage, times(1)).rowsCount();
    }

    @Test
    void testClose() throws Exception {
        mvPartitionStorageOnRebalance.close();

        verify(mvPartitionStorage, times(1)).close();
    }
}
