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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage.WriteClosure;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DelegatingMvPartitionStorageTest {
    @Mock
    private MvPartitionStorage realStorage;

    private DelegatingMvPartitionStorage delegating;

    private final HybridClock clock = new HybridClock();

    private final RowId rowId = new RowId(1);

    @BeforeEach
    void createTestInstance() {
        delegating = new DelegatingMvPartitionStorage(realStorage) {
        };
    }

    @Test
    void delegatesRunConsistently() {
        Object token = new Object();

        when(realStorage.runConsistently(any())).then(invocation -> {
            WriteClosure<?> closure = invocation.getArgument(0);
            return closure.execute();
        });

        WriteClosure<Object> closure = () -> token;

        assertThat(delegating.runConsistently(closure), is(sameInstance(token)));
        verify(realStorage).runConsistently(closure);
    }

    @Test
    void delegatesFlush() {
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

        when(realStorage.flush()).thenReturn(future);

        assertThat(delegating.flush(), is(future));
    }

    @Test
    void delegatesLastAppliedIndexGetter() {
        when(realStorage.lastAppliedIndex()).thenReturn(42L);

        assertThat(delegating.lastAppliedIndex(), is(42L));
    }

    @Test
    void delegatesLastAppliedIndexSetter() {
        delegating.lastAppliedIndex(42L);

        verify(realStorage).lastAppliedIndex(42L);
    }

    @Test
    void delegatesPersistedIndex() {
        when(realStorage.persistedIndex()).thenReturn(42L);

        assertThat(delegating.persistedIndex(), is(42L));
    }

    @Test
    void delegatesRead() {
        ReadResult readResult = ReadResult.createFromCommitted(null, clock.now());

        when(realStorage.read(any(), any())).thenReturn(readResult);

        HybridTimestamp timestamp = clock.now();

        assertThat(delegating.read(rowId, timestamp), is(readResult));
        verify(realStorage).read(rowId, timestamp);
    }

    @Test
    void delegatesAddWrite() {
        BinaryRow resultRow = mock(BinaryRow.class);

        when(realStorage.addWrite(any(), any(), any(), any(), anyInt())).thenReturn(resultRow);

        BinaryRow argumentRow = mock(BinaryRow.class);
        UUID txId = UUID.randomUUID();
        UUID commitTableId = UUID.randomUUID();

        assertThat(delegating.addWrite(rowId, argumentRow, txId, commitTableId, 42), is(resultRow));
        verify(realStorage).addWrite(rowId, argumentRow, txId, commitTableId, 42);
    }

    @Test
    void delegatesAbortWrite() {
        BinaryRow resultRow = mock(BinaryRow.class);

        when(realStorage.abortWrite(any())).thenReturn(resultRow);

        assertThat(delegating.abortWrite(rowId), is(resultRow));
        verify(realStorage).abortWrite(rowId);
    }

    @Test
    void delegatesCommitWrite() {
        HybridTimestamp commitTs = clock.now();

        delegating.commitWrite(rowId, commitTs);

        verify(realStorage).commitWrite(rowId, commitTs);
    }

    @Test
    void delegatesAddWriteCommitted() {
        BinaryRow argumentRow = mock(BinaryRow.class);
        HybridTimestamp commitTs = clock.now();

        delegating.addWriteCommitted(rowId, argumentRow, commitTs);

        verify(realStorage).addWriteCommitted(rowId, argumentRow, commitTs);
    }

    @Test
    void delegatesScanVersions() {
        @SuppressWarnings("unchecked") Cursor<ReadResult> cursor = mock(Cursor.class);

        when(realStorage.scanVersions(any())).thenReturn(cursor);

        assertThat(delegating.scanVersions(rowId), is(cursor));
        verify(realStorage).scanVersions(rowId);
    }

    @Test
    void delegatesScan() {
        PartitionTimestampCursor cursor = mock(PartitionTimestampCursor.class);

        when(realStorage.scan(any())).thenReturn(cursor);

        HybridTimestamp scanTs = clock.now();

        assertThat(delegating.scan(scanTs), is(cursor));
        verify(realStorage).scan(scanTs);
    }

    @Test
    void delegatesClosestRowId() {
        RowId resultRowId = new RowId(1);

        when(realStorage.closestRowId(any())).thenReturn(resultRowId);

        assertThat(delegating.closestRowId(rowId), is(resultRowId));
        verify(realStorage).closestRowId(rowId);
    }

    @SuppressWarnings("deprecation")
    @Test
    void delegatesRowsCount() {
        when(realStorage.rowsCount()).thenReturn(42L);

        assertThat(delegating.rowsCount(), is(42L));
    }

    @SuppressWarnings("deprecation")
    @Test
    void delegatesForEach() {
        BiConsumer<RowId, BinaryRow> consumer = (id, row) -> {};

        delegating.forEach(consumer);

        verify(realStorage).forEach(consumer);
    }

    @Test
    void delegatesClose() throws Exception {
        delegating.close();

        verify(realStorage).close();
    }
}