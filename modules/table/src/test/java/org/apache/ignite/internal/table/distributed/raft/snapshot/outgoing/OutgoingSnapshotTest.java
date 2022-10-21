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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OutgoingSnapshotTest {
    @Mock
    private PartitionAccess partitionAccess;

    @Mock
    private OutgoingSnapshotRegistry snapshotRegistry;

    private OutgoingSnapshot snapshot;

    private final TableMessagesFactory messagesFactory = new TableMessagesFactory();

    private final RowId lowestRowId = RowId.lowestRowId(1);
    private final RowId rowId1 = Objects.requireNonNull(lowestRowId.increment());
    private final RowId rowId2 = Objects.requireNonNull(rowId1.increment());
    private final RowId rowId3 = Objects.requireNonNull(rowId2.increment());

    private final RowId rowIdOutOfOrder;

    {
        RowId id = rowId3;
        for (int i = 0; i < 100; i++) {
            id = Objects.requireNonNull(id.increment());
        }
        rowIdOutOfOrder = id;
    }

    private final HybridClock clock = new HybridClock();

    private final UUID transactionId = UUID.randomUUID();
    private final UUID commitTableId = UUID.randomUUID();

    private final PartitionKey partitionKey = new PartitionKey(UUID.randomUUID(), 1);

    @BeforeEach
    void createTestInstance() {
        lenient().when(partitionAccess.key()).thenReturn(partitionKey);

        snapshot = new OutgoingSnapshot(UUID.randomUUID(), partitionAccess, snapshotRegistry);
    }

    @Test
    void returnsKeyFromStorage() {
        assertThat(snapshot.partitionKey(), is(partitionKey));
    }

    @Test
    void sendsCommittedAndUncommittedVersionsFromStorage() throws Exception {
        ReadResult version1 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());
        ReadResult version2 = ReadResult.createFromWriteIntent(
                new ByteBufferRow(new byte[]{2}),
                transactionId,
                commitTableId,
                42,
                clock.now()
        );

        configureStorageToHaveExactlyOneRowWith(List.of(version1, version2));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(1));
        SnapshotMvDataResponse.ResponseEntry responseRow = response.rows().get(0);

        assertThat(responseRow.rowId(), is(rowId1.uuid()));
        assertThat(responseRow.txId(), is(transactionId));
        assertThat(responseRow.commitTableId(), is(commitTableId));
        assertThat(responseRow.commitPartitionId(), is(42));
        //noinspection ConstantConditions
        assertThat(responseRow.timestamps(), is(equalTo(List.of(version1.commitTimestamp()))));

        assertThat(responseRow.rowVersions(), hasSize(2));
        assertThat(responseRow.rowVersions().get(0).array(), is(new byte[]{2}));
        assertThat(responseRow.rowVersions().get(1).array(), is(new byte[]{1}));
    }

    private void configureStorageToHaveExactlyOneRowWith(List<ReadResult> versions) {
        when(partitionAccess.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess.rowVersions(rowId1)).thenReturn(versions);
        lenient().when(partitionAccess.closestRowId(rowId2)).thenReturn(null);
    }

    private SnapshotMvDataResponse getMvDataResponse(long batchSizeHint) throws InterruptedException, ExecutionException, TimeoutException {
        SnapshotMvDataRequest request = messagesFactory.snapshotMvDataRequest()
                .batchSizeHint(batchSizeHint)
                .build();

        return snapshot.handleSnapshotMvDataRequest(request).get(1, TimeUnit.SECONDS);
    }

    @Test
    void reversesOrderOfVersionsObtainedFromPartition() throws Exception {
        ReadResult version1 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{2}), clock.now());

        configureStorageToHaveExactlyOneRowWith(List.of(version1, version2));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        SnapshotMvDataResponse.ResponseEntry responseRow = response.rows().get(0);

        //noinspection ConstantConditions
        assertThat(responseRow.timestamps(), is(equalTo(List.of(version2.commitTimestamp(), version1.commitTimestamp()))));

        assertThat(responseRow.rowVersions(), hasSize(2));
        assertThat(responseRow.rowVersions().get(0).array(), is(new byte[]{2}));
        assertThat(responseRow.rowVersions().get(1).array(), is(new byte[]{1}));
    }

    @Test
    void iteratesRowsInPartition() throws Exception {
        ReadResult version1 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{2}), clock.now());

        when(partitionAccess.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess.rowVersions(rowId1)).thenReturn(List.of(version1));
        when(partitionAccess.closestRowId(rowId2)).thenReturn(rowId2);
        when(partitionAccess.rowVersions(rowId2)).thenReturn(List.of(version2));
        when(partitionAccess.closestRowId(rowId3)).thenReturn(null);

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(2));

        assertThat(response.rows().get(0).rowVersions(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions().get(0).array(), is(new byte[]{1}));
        assertThat(response.rows().get(1).rowVersions(), hasSize(1));
        assertThat(response.rows().get(1).rowVersions().get(0).array(), is(new byte[]{2}));
    }

    @Test
    void rowsWithOverwrittenIdsAreIgnored() throws Exception {
        snapshot.addOverwrittenRowId(rowId1);

        when(partitionAccess.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess.closestRowId(rowId2)).thenReturn(null);

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), is(empty()));
    }

    @Test
    void sendsCommittedAndUncommittedVersionsFromQueue() throws Exception {
        ReadResult version1 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());
        ReadResult version2 = ReadResult.createFromWriteIntent(
                new ByteBufferRow(new byte[]{2}),
                transactionId,
                commitTableId,
                42,
                clock.now()
        );

        snapshot.enqueueForSending(rowId1, List.of(version1, version2));

        configureStorageToBeEmpty();

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(1));
        SnapshotMvDataResponse.ResponseEntry responseRow = response.rows().get(0);

        assertThat(responseRow.rowId(), is(rowId1.uuid()));
        assertThat(responseRow.txId(), is(transactionId));
        assertThat(responseRow.commitTableId(), is(commitTableId));
        assertThat(responseRow.commitPartitionId(), is(42));
        //noinspection ConstantConditions
        assertThat(responseRow.timestamps(), is(equalTo(List.of(version1.commitTimestamp()))));

        assertThat(responseRow.rowVersions(), hasSize(2));
        assertThat(responseRow.rowVersions().get(0).array(), is(new byte[]{1}));
        assertThat(responseRow.rowVersions().get(1).array(), is(new byte[]{2}));
    }

    private void configureStorageToBeEmpty() {
        lenient().when(partitionAccess.closestRowId(lowestRowId)).thenReturn(null);
    }

    @Test
    void sendsOutOfOrderRowsWithHighestPriority() throws Exception {
        ReadResult version1 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{2}), clock.now());

        snapshot.enqueueForSending(rowIdOutOfOrder, List.of(version1));

        configureStorageToHaveExactlyOneRowWith(List.of(version2));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(2));

        assertThat(response.rows().get(0).rowId(), is(rowIdOutOfOrder.uuid()));
        assertThat(response.rows().get(1).rowId(), is(rowId1.uuid()));
    }

    @Test
    void sendsOutOfOrderRowsWhichAppearWhenScanning() throws Exception {
        ReadResult version1 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{2}), clock.now());

        when(partitionAccess.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess.rowVersions(rowId1)).thenReturn(List.of(version1));
        when(partitionAccess.closestRowId(rowId2)).then(invocation -> {
            snapshot.enqueueForSending(rowIdOutOfOrder, List.of(version2));
            return null;
        });

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows(), hasSize(2));

        assertThat(response.rows().get(0).rowId(), is(rowId1.uuid()));
        assertThat(response.rows().get(1).rowId(), is(rowIdOutOfOrder.uuid()));
    }

    @Test
    void sendsTombstonesWithNullBuffers() throws Exception {
        ReadResult version = ReadResult.createFromCommitted(null, clock.now());

        configureStorageToHaveExactlyOneRowWith(List.of(version));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows().get(0).rowVersions().get(0), is(nullValue()));
    }

    @Test
    void doesNotSendWriteIntentTimestamp() throws Exception {
        ReadResult version = ReadResult.createFromWriteIntent(
                new ByteBufferRow(new byte[]{1}),
                transactionId,
                commitTableId,
                42,
                clock.now()
        );

        configureStorageToHaveExactlyOneRowWith(List.of(version));

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertThat(response.rows().get(0).timestamps(), is(empty()));
    }

    @Test
    void finalMvDataChunkHasFinishTrue() throws Exception {
        configureStorageToBeEmpty();

        SnapshotMvDataResponse response = getMvDataResponse(Long.MAX_VALUE);

        assertTrue(response.finish());
    }

    @Test
    void snapshotThatSentAllDataIsFinished() throws Exception {
        configureStorageToBeEmpty();

        getMvDataResponse(Long.MAX_VALUE);

        assertTrue(snapshot.isFinished());
    }

    @Test
    void snapshotThatSentAllDataUnregistersItself() throws Exception {
        configureStorageToBeEmpty();

        getMvDataResponse(Long.MAX_VALUE);

        verify(snapshotRegistry).unregisterOutgoingSnapshot(snapshot.id());
    }

    @Test
    void mvDataHandlingRespectsBatchSizeHintForMessagesFromPartition() throws Exception {
        ReadResult version = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());

        when(partitionAccess.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess.rowVersions(rowId1)).thenReturn(List.of(version));

        SnapshotMvDataResponse response = getMvDataResponse(1);

        assertThat(response.rows(), hasSize(1));
    }

    @Test
    void mvDataHandlingRespectsBatchSizeHintForOutOfOrderMessages() throws Exception {
        ReadResult version = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());

        snapshot.enqueueForSending(rowIdOutOfOrder, List.of(version));

        configureStorageToBeEmpty();

        SnapshotMvDataResponse response = getMvDataResponse(1);

        assertThat(response.rows(), hasSize(1));
    }

    @Test
    void mvDataResponseThatIsNotLastHasFinishFalse() throws Exception {
        ReadResult version1 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{2}), clock.now());

        when(partitionAccess.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess.rowVersions(rowId1)).thenReturn(List.of(version1, version2));
        lenient().when(partitionAccess.closestRowId(rowId2)).thenReturn(rowId2);

        SnapshotMvDataResponse response = getMvDataResponse(1);

        assertFalse(response.finish());
    }

    @Test
    void sendsRowsFromPartitionBiggerThanHint() throws Exception {
        ReadResult version = ReadResult.createFromCommitted(new ByteBufferRow(new byte[1000]), clock.now());

        configureStorageToHaveExactlyOneRowWith(List.of(version));

        SnapshotMvDataResponse response = getMvDataResponse(1);

        assertThat(response.rows(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions().get(0).limit(), is(1000));
    }

    @Test
    void sendsRowsFromOutOfOrderQueueBiggerThanHint() throws Exception {
        ReadResult version = ReadResult.createFromCommitted(new ByteBufferRow(new byte[1000]), clock.now());

        snapshot.enqueueForSending(rowIdOutOfOrder, List.of(version));

        configureStorageToBeEmpty();

        SnapshotMvDataResponse response = getMvDataResponse(1);

        assertThat(response.rows(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions(), hasSize(1));
        assertThat(response.rows().get(0).rowVersions().get(0).limit(), is(1000));
    }

    @Test
    void whenNotStartedThenEvenLowestRowIdIsNotPassed() {
        assertFalse(snapshot.alreadyPassed(lowestRowId));
    }

    @Test
    void lastSentRowIdIsPassed() throws Exception {
        ReadResult version1 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{2}), clock.now());

        when(partitionAccess.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess.rowVersions(rowId1)).thenReturn(List.of(version1, version2));
        lenient().when(partitionAccess.closestRowId(rowId2)).thenReturn(rowId2);

        getMvDataResponse(1);

        assertTrue(snapshot.alreadyPassed(rowId1));
    }

    @Test
    void notYetSentRowIdIsNotPassed() throws Exception {
        ReadResult version1 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());
        ReadResult version2 = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{2}), clock.now());

        when(partitionAccess.closestRowId(lowestRowId)).thenReturn(rowId1);
        when(partitionAccess.rowVersions(rowId1)).thenReturn(List.of(version1, version2));
        lenient().when(partitionAccess.closestRowId(rowId2)).thenReturn(rowId2);

        getMvDataResponse(1);

        assertFalse(snapshot.alreadyPassed(rowId2));
    }

    @Test
    void anyRowIdIsPassedForFinishedSnapshot() throws Exception {
        ReadResult version = ReadResult.createFromCommitted(new ByteBufferRow(new byte[]{1}), clock.now());

        configureStorageToHaveExactlyOneRowWith(List.of(version));

        getMvDataResponse(Long.MAX_VALUE);

        //noinspection ConstantConditions
        assertTrue(snapshot.alreadyPassed(rowId3.increment().increment().increment()));
    }
}
