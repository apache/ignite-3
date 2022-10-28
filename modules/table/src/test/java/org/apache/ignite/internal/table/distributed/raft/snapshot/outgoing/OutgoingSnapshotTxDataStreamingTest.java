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

import static java.util.Collections.emptyIterator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lock.AutoLockup;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccess;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotTxDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotTxDataResponse;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OutgoingSnapshotTxDataStreamingTest {
    @Mock
    private PartitionAccess partitionAccess;

    @Mock
    private MvPartitionStorage mvPartitionStorage;

    @Mock
    private TxStateStorage txStateStorage;

    @Mock
    private LogManager logManager;

    private OutgoingSnapshot snapshot;

    private final TableMessagesFactory messagesFactory = new TableMessagesFactory();

    private final UUID txId1 = new UUID(0, 1);
    private final UUID txId2 = new UUID(0, 2);

    private final HybridClock clock = new HybridClockImpl();

    private final TablePartitionId partition1Id = new TablePartitionId(UUID.randomUUID(), 1);
    private final TablePartitionId partition2Id = new TablePartitionId(UUID.randomUUID(), 2);

    private final TxMeta meta1 = new TxMeta(TxState.ABORTED, List.of(partition1Id), clock.now());
    private final TxMeta meta2 = new TxMeta(TxState.COMMITED, List.of(partition1Id, partition2Id), clock.now());

    private final PartitionKey partitionKey = new PartitionKey(UUID.randomUUID(), 1);

    @BeforeEach
    void createTestInstance() {
        when(partitionAccess.partitionKey()).thenReturn(partitionKey);

        lenient().when(partitionAccess.mvPartitionStorage()).thenReturn(mvPartitionStorage);
        lenient().when(partitionAccess.txStatePartitionStorage()).thenReturn(txStateStorage);

        lenient().when(logManager.getConfiguration(anyLong())).thenReturn(new ConfigurationEntry());

        snapshot = new OutgoingSnapshot(UUID.randomUUID(), partitionAccess, logManager);
    }

    @Test
    void sendsTxDataFromStorage() {
        configureStorageToHaveExactly(txId1, meta1, txId2, meta2);

        SnapshotTxDataResponse response = getTxDataResponse(Integer.MAX_VALUE);

        assertThat(response.txIds(), is(List.of(txId1, txId2)));

        assertThat(response.txMeta(), hasSize(2));

        assertThat(response.txMeta().get(0).txState(), is(TxState.ABORTED));
        assertThat(response.txMeta().get(0).enlistedPartitions(), is(List.of(partition1Id)));
        assertThat(response.txMeta().get(0).commitTimestamp(), is(meta1.commitTimestamp()));

        assertThat(response.txMeta().get(1).txState(), is(TxState.COMMITED));
        assertThat(response.txMeta().get(1).enlistedPartitions(), is(List.of(partition1Id, partition2Id)));
        assertThat(response.txMeta().get(1).commitTimestamp(), is(meta2.commitTimestamp()));
    }

    private void configureStorageToHaveExactly(UUID txId1, TxMeta meta1, UUID txId2, TxMeta meta2) {
        when(txStateStorage.scan()).thenReturn(Cursor.fromIterator(
                List.of(new IgniteBiTuple<>(txId1, meta1), new IgniteBiTuple<>(txId2, meta2)).iterator())
        );

        freezeSnapshotScope();
    }

    private void freezeSnapshotScope() {
        try (AutoLockup ignored = snapshot.acquireMvLock()) {
            snapshot.freezeScope();
        }
    }

    private SnapshotTxDataResponse getTxDataResponse(int maxTxsInBatch) {
        SnapshotTxDataResponse response = getNullableTxDataResponse(maxTxsInBatch);

        assertThat(response, is(notNullValue()));

        return response;
    }

    @Nullable
    private SnapshotTxDataResponse getNullableTxDataResponse(int maxTxsInBatch) {
        SnapshotTxDataRequest request = messagesFactory.snapshotTxDataRequest()
                .maxTransactionsInBatch(maxTxsInBatch)
                .build();

        return snapshot.handleSnapshotTxDataRequest(request);
    }

    private void configureStorageToBeEmpty() {
        when(txStateStorage.scan()).thenReturn(Cursor.fromIterator(emptyIterator()));

        freezeSnapshotScope();
    }

    @Test
    void finalTxDataChunkHasFinishTrue() {
        configureStorageToBeEmpty();

        SnapshotTxDataResponse response = getTxDataResponse(Integer.MAX_VALUE);

        assertTrue(response.finish());
    }

    @Test
    void txDataHandlingRespectsBatchSizeHintForMessagesFromPartition() {
        configureStorageToHaveExactly(txId1, meta1, txId2, meta2);

        SnapshotTxDataResponse response = getTxDataResponse(1);

        assertThat(response.txIds(), hasSize(1));
    }

    @Test
    void txDataResponseThatIsNotLastHasFinishFalse() {
        configureStorageToHaveExactly(txId1, meta1, txId2, meta2);

        SnapshotTxDataResponse response = getTxDataResponse(1);

        assertFalse(response.finish());
    }

    @Test
    void closesCursorWhenTxDataIsExhaustedInPartition() throws Exception {
        Cursor<IgniteBiTuple<UUID, TxMeta>> cursor = spy(Cursor.fromIterator(emptyIterator()));

        when(txStateStorage.scan()).thenReturn(cursor);
        freezeSnapshotScope();

        getTxDataResponse(Integer.MAX_VALUE);

        verify(cursor).close();
    }

    @Test
    void returnsNullTxDataResponseWhenClosed() {
        snapshot.close();

        assertThat(getNullableTxDataResponse(Integer.MAX_VALUE), is(nullValue()));
    }
}
