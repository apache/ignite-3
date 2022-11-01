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

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lock.AutoLockup;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SnapshotAwarePartitionDataStorageTest {
    private static final int PARTITION_ID = 1;

    @Mock
    private MvPartitionStorage partitionStorage;

    @Mock
    private PartitionsSnapshots partitionsSnapshots;

    @Spy
    private final PartitionKey partitionKey = new PartitionKey(UUID.randomUUID(), PARTITION_ID);

    @InjectMocks
    private SnapshotAwarePartitionDataStorage testedStorage;

    @Mock
    private PartitionSnapshots partitionSnapshots;

    private final RowId rowId = new RowId(PARTITION_ID);

    @Mock
    private OutgoingSnapshot snapshot;

    @BeforeEach
    void configureMocks() {
        lenient().when(partitionsSnapshots.partitionSnapshots(any())).thenReturn(partitionSnapshots);
    }

    @Test
    void delegatesRunConsistently() {
        Object token = new Object();

        when(partitionStorage.runConsistently(any())).then(invocation -> {
            MvPartitionStorage.WriteClosure<?> closure = invocation.getArgument(0);
            return closure.execute();
        });

        MvPartitionStorage.WriteClosure<Object> closure = () -> token;

        assertThat(testedStorage.runConsistently(closure), is(sameInstance(token)));
        verify(partitionStorage).runConsistently(closure);
    }

    @Test
    void delegatesFlush() {
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

        when(partitionStorage.flush()).thenReturn(future);

        assertThat(testedStorage.flush(), is(future));
    }

    @Test
    void delegatesLastAppliedIndexGetter() {
        when(partitionStorage.lastAppliedIndex()).thenReturn(42L);

        assertThat(testedStorage.lastAppliedIndex(), is(42L));
    }

    @Test
    void delegatesLastAppliedIndexSetter() {
        testedStorage.lastAppliedIndex(42L);

        verify(partitionStorage).lastAppliedIndex(42L);
    }

    @Test
    void delegatesAddWrite() {
        BinaryRow resultRow = mock(BinaryRow.class);

        when(partitionStorage.addWrite(any(), any(), any(), any(), anyInt())).thenReturn(resultRow);

        BinaryRow argumentRow = mock(BinaryRow.class);
        UUID txId = UUID.randomUUID();
        UUID commitTableId = UUID.randomUUID();

        assertThat(testedStorage.addWrite(rowId, argumentRow, txId, commitTableId, 42), is(resultRow));
        verify(partitionStorage).addWrite(rowId, argumentRow, txId, commitTableId, 42);
    }

    @Test
    void delegatesAbortWrite() {
        BinaryRow resultRow = mock(BinaryRow.class);

        when(partitionStorage.abortWrite(any())).thenReturn(resultRow);

        assertThat(testedStorage.abortWrite(rowId), is(resultRow));
        verify(partitionStorage).abortWrite(rowId);
    }

    @Test
    void delegatesCommitWrite() {
        HybridTimestamp commitTs = new HybridClockImpl().now();

        testedStorage.commitWrite(rowId, commitTs);

        verify(partitionStorage).commitWrite(rowId, commitTs);
    }

    @Test
    void delegatesClose() {
        testedStorage.close();

        verify(partitionStorage).close();
    }

    @Test
    void delegatesAcquirePartitionSnapshotsReadLock() {
        AutoLockup lockup = mock(AutoLockup.class);

        when(partitionSnapshots.acquireReadLock()).thenReturn(lockup);

        assertThat(testedStorage.acquirePartitionSnapshotsReadLock(), is(lockup));
    }

    @ParameterizedTest
    @EnumSource(MvWriteAction.class)
    void notYetPassedRowIsEnqueued(MvWriteAction writeAction) {
        when(partitionSnapshots.ongoingSnapshots()).thenReturn(List.of(snapshot));

        doReturn(false).when(snapshot).alreadyPassed(any());
        doReturn(true).when(snapshot).addRowIdToSkip(any());

        writeAction.executeOn(testedStorage, rowId);

        verify(snapshot).enqueueForSending(rowId);
    }

    @ParameterizedTest
    @EnumSource(MvWriteAction.class)
    void notYetPassedRowNotEnqueuedSecondTime(MvWriteAction writeAction) {
        when(partitionSnapshots.ongoingSnapshots()).thenReturn(List.of(snapshot));

        doReturn(false).when(snapshot).alreadyPassed(any());
        doReturn(false).when(snapshot).addRowIdToSkip(any());

        writeAction.executeOn(testedStorage, rowId);

        verify(snapshot, never()).enqueueForSending(any());
    }

    @ParameterizedTest
    @EnumSource(MvWriteAction.class)
    void alreadyPassedRowNotEnqueued(MvWriteAction writeAction) {
        when(partitionSnapshots.ongoingSnapshots()).thenReturn(List.of(snapshot));

        doReturn(true).when(snapshot).alreadyPassed(any());

        writeAction.executeOn(testedStorage, rowId);

        verify(snapshot, never()).enqueueForSending(any());
    }

    @ParameterizedTest
    @EnumSource(MvWriteAction.class)
    void sendsVersionsInOldestToNewestOrder(MvWriteAction writeAction) {
        when(partitionSnapshots.ongoingSnapshots()).thenReturn(List.of(snapshot));

        configureSnapshotToLetEnqueueOutOfOrderMvRow(snapshot);

        writeAction.executeOn(testedStorage, rowId);

        verify(snapshot).enqueueForSending(rowId);
    }

    private void configureSnapshotToLetEnqueueOutOfOrderMvRow(OutgoingSnapshot snapshotToConfigure) {
        doReturn(false).when(snapshotToConfigure).alreadyPassed(any());
        doReturn(true).when(snapshotToConfigure).addRowIdToSkip(any());
    }

    @ParameterizedTest
    @EnumSource(MvWriteAction.class)
    void interceptsWritesToMvStorageOnMultipleSnapshots(MvWriteAction writeAction) {
        OutgoingSnapshot snapshot2 = mock(OutgoingSnapshot.class);

        when(partitionSnapshots.ongoingSnapshots()).thenReturn(List.of(snapshot, snapshot2));

        configureSnapshotToLetEnqueueOutOfOrderMvRow(snapshot);
        configureSnapshotToLetEnqueueOutOfOrderMvRow(snapshot2);

        writeAction.executeOn(testedStorage, rowId);

        verify(snapshot).enqueueForSending(rowId);
        verify(snapshot2).enqueueForSending(rowId);
    }

    private enum MvWriteAction {
        ADD_WRITE {
            @Override
            void executeOn(SnapshotAwarePartitionDataStorage storage, RowId rowId) {
                storage.addWrite(rowId, mock(BinaryRow.class), UUID.randomUUID(), UUID.randomUUID(), 42);
            }
        },
        ABORT_WRITE {
            @Override
            void executeOn(SnapshotAwarePartitionDataStorage storage, RowId rowId) {
                storage.abortWrite(rowId);
            }
        },
        COMMIT_WRITE {
            @Override
            void executeOn(SnapshotAwarePartitionDataStorage storage, RowId rowId) {
                storage.commitWrite(rowId, new HybridClockImpl().now());
            }
        };

        abstract void executeOn(SnapshotAwarePartitionDataStorage storage, RowId rowId);
    }
}
