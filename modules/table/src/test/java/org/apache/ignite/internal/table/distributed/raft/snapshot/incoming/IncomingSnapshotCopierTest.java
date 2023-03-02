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

package org.apache.ignite.internal.table.distributed.raft.snapshot.incoming;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willFailFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfiguration;
import org.apache.ignite.internal.table.distributed.raft.RaftGroupConfigurationConverter;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccessImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionKey;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotTxDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link IncomingSnapshotCopier} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class IncomingSnapshotCopierTest {
    private static final String NODE_NAME = "node";

    private static final int TEST_PARTITION = 0;

    private static final SchemaDescriptor SCHEMA_DESCRIPTOR = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.stringOf(256), false)},
            new Column[]{new Column("value", NativeTypes.stringOf(256), false)}
    );

    private static final HybridClock HYBRID_CLOCK = new HybridClockImpl();

    private static final TableMessagesFactory TABLE_MSG_FACTORY = new TableMessagesFactory();

    private static final RaftMessagesFactory RAFT_MSG_FACTORY = new RaftMessagesFactory();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @InjectConfiguration("mock.tables.foo {}")
    private TablesConfiguration tablesConfig;

    private final ClusterNode clusterNode = mock(ClusterNode.class);

    private final UUID snapshotId = UUID.randomUUID();

    private final UUID tableId = UUID.randomUUID();

    private final RaftGroupConfigurationConverter raftGroupConfigurationConverter = new RaftGroupConfigurationConverter();

    private MvGc mvGc;

    @BeforeEach
    void setUp() {
        mvGc = mock(MvGc.class);

        when(mvGc.removeStorage(any(TablePartitionId.class))).then(invocation -> completedFuture(null));
    }

    @AfterEach
    void tearDown() {
        shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS);
    }

    @Test
    void test() {
        MvPartitionStorage outgoingMvPartitionStorage = new TestMvPartitionStorage(TEST_PARTITION);
        TxStateStorage outgoingTxStatePartitionStorage = new TestTxStateStorage();

        long expLastAppliedIndex = 100500L;
        long expLastAppliedTerm = 100L;
        RaftGroupConfiguration expLastGroupConfig = generateRaftGroupConfig();

        List<RowId> rowIds = generateRowIds();
        List<UUID> txIds = generateTxIds();

        fillMvPartitionStorage(outgoingMvPartitionStorage, expLastAppliedIndex, expLastAppliedTerm, expLastGroupConfig, rowIds);
        fillTxStatePartitionStorage(outgoingTxStatePartitionStorage, expLastAppliedIndex, expLastAppliedTerm, txIds);

        MvTableStorage incomingMvTableStorage = spy(new TestMvTableStorage(getTableConfig(), tablesConfig));
        TxStateTableStorage incomingTxStateTableStorage = spy(new TestTxStateTableStorage());

        assertThat(incomingMvTableStorage.createMvPartition(TEST_PARTITION), willCompleteSuccessfully());
        incomingTxStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION);

        MessagingService messagingService = messagingServiceForSuccessScenario(outgoingMvPartitionStorage,
                outgoingTxStatePartitionStorage, expLastAppliedIndex, expLastAppliedTerm, expLastGroupConfig, rowIds, txIds, snapshotId);

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                snapshotId,
                incomingMvTableStorage,
                incomingTxStateTableStorage,
                messagingService
        );

        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startToCopyFrom(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME),
                mock(SnapshotCopierOptions.class)
        );

        assertThat(runAsync(snapshotCopier::join), willSucceedIn(1, TimeUnit.SECONDS));

        assertEquals(Status.OK().getCode(), snapshotCopier.getCode());

        TablePartitionId tablePartitionId = new TablePartitionId(tableId, TEST_PARTITION);

        verify(mvGc, times(1)).removeStorage(eq(tablePartitionId));
        verify(mvGc, times(1)).addStorage(eq(tablePartitionId), any(StorageUpdateHandler.class));

        MvPartitionStorage incomingMvPartitionStorage = incomingMvTableStorage.getMvPartition(TEST_PARTITION);
        TxStateStorage incomingTxStatePartitionStorage = incomingTxStateTableStorage.getTxStateStorage(TEST_PARTITION);

        assertEquals(expLastAppliedIndex, outgoingMvPartitionStorage.lastAppliedIndex());
        assertEquals(expLastAppliedTerm, outgoingMvPartitionStorage.lastAppliedTerm());
        assertArrayEquals(
                raftGroupConfigurationConverter.toBytes(expLastGroupConfig),
                outgoingMvPartitionStorage.committedGroupConfiguration()
        );
        assertEquals(expLastAppliedIndex, outgoingTxStatePartitionStorage.lastAppliedIndex());
        assertEquals(expLastAppliedTerm, outgoingTxStatePartitionStorage.lastAppliedTerm());

        assertEqualsMvRows(outgoingMvPartitionStorage, incomingMvPartitionStorage, rowIds);
        assertEqualsTxStates(outgoingTxStatePartitionStorage, incomingTxStatePartitionStorage, txIds);

        verify(incomingMvTableStorage, times(1)).startRebalancePartition(eq(TEST_PARTITION));
        verify(incomingTxStatePartitionStorage, times(1)).startRebalance();
    }

    private MessagingService messagingServiceForSuccessScenario(MvPartitionStorage outgoingMvPartitionStorage,
            TxStateStorage outgoingTxStatePartitionStorage, long expLastAppliedIndex, long expLastAppliedTerm,
            RaftGroupConfiguration expLastGroupConfig, List<RowId> rowIds, List<UUID> txIds, UUID snapshotId) {
        MessagingService messagingService = mock(MessagingService.class);

        when(messagingService.invoke(eq(clusterNode), any(SnapshotMetaRequest.class), anyLong())).then(answer -> {
            SnapshotMetaRequest snapshotMetaRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotMetaRequest.id());

            return completedFuture(
                    TABLE_MSG_FACTORY.snapshotMetaResponse()
                            .meta(
                                    RAFT_MSG_FACTORY.snapshotMeta()
                                            .lastIncludedIndex(expLastAppliedIndex)
                                            .lastIncludedTerm(expLastAppliedTerm)
                                            .peersList(expLastGroupConfig.peers())
                                            .learnersList(expLastGroupConfig.learners())
                                            .oldPeersList(expLastGroupConfig.oldPeers())
                                            .oldLearnersList(expLastGroupConfig.oldLearners())
                                            .build()
                            )
                            .build()
            );
        });

        when(messagingService.invoke(eq(clusterNode), any(SnapshotMvDataRequest.class), anyLong())).then(answer -> {
            SnapshotMvDataRequest snapshotMvDataRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotMvDataRequest.id());

            List<ResponseEntry> responseEntries = createSnapshotMvDataEntries(outgoingMvPartitionStorage, rowIds);

            assertThat(responseEntries, not(empty()));

            return completedFuture(TABLE_MSG_FACTORY.snapshotMvDataResponse().rows(responseEntries).finish(true).build());
        });

        when(messagingService.invoke(eq(clusterNode), any(SnapshotTxDataRequest.class), anyLong())).then(answer -> {
            SnapshotTxDataRequest snapshotTxDataRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotTxDataRequest.id());

            List<TxMeta> txMetas = txIds.stream().map(outgoingTxStatePartitionStorage::get).collect(toList());

            return completedFuture(TABLE_MSG_FACTORY.snapshotTxDataResponse().txIds(txIds).txMeta(txMetas).finish(true).build());
        });

        return messagingService;
    }

    private PartitionSnapshotStorage createPartitionSnapshotStorage(
            UUID snapshotId,
            MvTableStorage incomingTableStorage,
            TxStateTableStorage incomingTxStateTableStorage,
            MessagingService messagingService
    ) {
        TopologyService topologyService = mock(TopologyService.class);

        when(topologyService.getByConsistentId(NODE_NAME)).thenReturn(clusterNode);

        OutgoingSnapshotsManager outgoingSnapshotsManager = mock(OutgoingSnapshotsManager.class);

        when(outgoingSnapshotsManager.messagingService()).thenReturn(messagingService);

        return new PartitionSnapshotStorage(
                topologyService,
                outgoingSnapshotsManager,
                SnapshotUri.toStringUri(snapshotId, NODE_NAME),
                mock(RaftOptions.class),
                spy(new PartitionAccessImpl(
                        new PartitionKey(tableId, TEST_PARTITION),
                        incomingTableStorage,
                        incomingTxStateTableStorage,
                        mock(StorageUpdateHandler.class),
                        mvGc
                )),
                mock(SnapshotMeta.class),
                executorService
        );
    }

    private void fillMvPartitionStorage(
            MvPartitionStorage storage,
            long lastAppliedIndex,
            long lastAppliedTerm,
            RaftGroupConfiguration raftGroupConfig,
            List<RowId> rowIds
    ) {
        assertEquals(0, rowIds.size() % 2, "size=" + rowIds.size());

        storage.runConsistently(() -> {
            for (int i = 0; i < rowIds.size(); i++) {
                if (i % 2 == 0) {
                    // Writes committed version.
                    storage.addWriteCommitted(rowIds.get(i), createRow("k" + i, "v" + i), HYBRID_CLOCK.now());
                } else {
                    // Writes an intent to write (uncommitted version).
                    storage.addWrite(rowIds.get(i), createRow("k" + i, "v" + i), UUID.randomUUID(), UUID.randomUUID(), TEST_PARTITION);
                }
            }

            storage.lastApplied(lastAppliedIndex, lastAppliedTerm);

            storage.committedGroupConfiguration(raftGroupConfigurationConverter.toBytes(raftGroupConfig));

            return null;
        });
    }

    private static void fillTxStatePartitionStorage(
            TxStateStorage storage,
            long lastAppliedIndex,
            long lastAppliedTerm,
            List<UUID> txIds
    ) {
        assertEquals(0, txIds.size() % 2, "size=" + txIds.size());

        UUID tableId = UUID.randomUUID();

        for (int i = 0; i < txIds.size(); i++) {
            TxState txState = i % 2 == 0 ? COMMITED : ABORTED;

            storage.put(txIds.get(i), new TxMeta(txState, List.of(new TablePartitionId(tableId, TEST_PARTITION)), HYBRID_CLOCK.now()));
        }

        storage.lastApplied(lastAppliedIndex, lastAppliedTerm);
    }

    private static List<ResponseEntry> createSnapshotMvDataEntries(MvPartitionStorage storage, List<RowId> rowIds) {
        List<ResponseEntry> responseEntries = new ArrayList<>();

        for (RowId rowId : rowIds) {
            List<ReadResult> readResults = storage.scanVersions(rowId).stream().collect(toList());

            Collections.reverse(readResults);

            List<ByteBuffer> rowVersions = new ArrayList<>();
            List<HybridTimestamp> timestamps = new ArrayList<>();

            UUID txId = null;
            UUID commitTableId = null;
            int commitPartitionId = ReadResult.UNDEFINED_COMMIT_PARTITION_ID;

            for (ReadResult readResult : readResults) {
                rowVersions.add(readResult.binaryRow().byteBuffer());

                if (readResult.isWriteIntent()) {
                    txId = readResult.transactionId();
                    commitTableId = readResult.commitTableId();
                    commitPartitionId = readResult.commitPartitionId();
                } else {
                    timestamps.add(readResult.commitTimestamp());
                }
            }

            responseEntries.add(
                    TABLE_MSG_FACTORY.responseEntry()
                            .rowId(rowId.uuid())
                            .rowVersions(rowVersions)
                            .timestamps(timestamps)
                            .txId(txId)
                            .commitTableId(commitTableId)
                            .commitPartitionId(commitPartitionId)
                            .build()
            );
        }

        return responseEntries;
    }

    private static BinaryRow createRow(String key, String value) {
        return new RowAssembler(SCHEMA_DESCRIPTOR)
                .appendStringNotNull(key)
                .appendStringNotNull(value)
                .build();
    }

    private static void assertEqualsMvRows(MvPartitionStorage expected, MvPartitionStorage actual, List<RowId> rowIds) {
        for (RowId rowId : rowIds) {
            List<ReadResult> expReadResults = expected.scanVersions(rowId).stream().collect(toList());
            List<ReadResult> actReadResults = actual.scanVersions(rowId).stream().collect(toList());

            assertEquals(expReadResults.size(), actReadResults.size(), rowId.toString());

            for (int i = 0; i < expReadResults.size(); i++) {
                ReadResult expReadResult = expReadResults.get(i);
                ReadResult actReadResult = actReadResults.get(i);

                String msg = "RowId=" + rowId + ", i=" + i;

                BinaryTupleReader expTuple = new BinaryTupleReader(SCHEMA_DESCRIPTOR.length(), expReadResult.binaryRow().tupleSlice());
                BinaryTupleReader actTuple = new BinaryTupleReader(SCHEMA_DESCRIPTOR.length(), actReadResult.binaryRow().tupleSlice());

                assertEquals(expTuple.stringValue(0), actTuple.stringValue(0), msg);
                assertEquals(expTuple.stringValue(1), actTuple.stringValue(1), msg);

                assertEquals(expReadResult.commitTimestamp(), actReadResult.commitTimestamp(), msg);
                assertEquals(expReadResult.transactionId(), actReadResult.transactionId(), msg);
                assertEquals(expReadResult.commitTableId(), actReadResult.commitTableId(), msg);
                assertEquals(expReadResult.commitPartitionId(), actReadResult.commitPartitionId(), msg);
                assertEquals(expReadResult.isWriteIntent(), actReadResult.isWriteIntent(), msg);
            }
        }
    }

    private static void assertEqualsTxStates(TxStateStorage expected, TxStateStorage actual, List<UUID> txIds) {
        for (UUID txId : txIds) {
            assertEquals(expected.get(txId), actual.get(txId));
        }
    }

    @Test
    void cancellationMakesJoinFinishIfHangingOnNetworkCall() throws Exception {
        MvTableStorage incomingMvTableStorage = spy(new TestMvTableStorage(tablesConfig.tables().get("foo"), tablesConfig));
        TxStateTableStorage incomingTxStateTableStorage = spy(new TestTxStateTableStorage());

        assertThat(incomingMvTableStorage.createMvPartition(TEST_PARTITION), willCompleteSuccessfully());
        incomingTxStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION);

        CountDownLatch networkInvokeLatch = new CountDownLatch(1);

        MessagingService messagingService = mock(MessagingService.class);

        when(messagingService.invoke(any(ClusterNode.class), any(), anyLong())).then(invocation -> {
            networkInvokeLatch.countDown();

            return new CompletableFuture<>();
        });

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                snapshotId,
                incomingMvTableStorage,
                incomingTxStateTableStorage,
                messagingService
        );

        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startToCopyFrom(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME),
                mock(SnapshotCopierOptions.class)
        );

        networkInvokeLatch.await(1, TimeUnit.SECONDS);

        CompletableFuture<?> cancelAndJoinFuture = runAsync(() -> {
            snapshotCopier.cancel();

            snapshotCopier.join();
        });

        assertThat(cancelAndJoinFuture, willSucceedIn(1, TimeUnit.SECONDS));

        verify(partitionSnapshotStorage.partition()).abortRebalance();
    }

    @Test
    void testCancelOnMiddleRebalance() {
        MvPartitionStorage outgoingMvPartitionStorage = new TestMvPartitionStorage(TEST_PARTITION);
        TxStateStorage outgoingTxStatePartitionStorage = new TestTxStateStorage();

        long expLastAppliedIndex = 100500L;
        long expLastAppliedTerm = 100L;
        RaftGroupConfiguration expLastGroupConfig = generateRaftGroupConfig();

        List<RowId> rowIds = generateRowIds();
        List<UUID> txIds = generateTxIds();

        fillMvPartitionStorage(outgoingMvPartitionStorage, expLastAppliedIndex, expLastAppliedTerm, expLastGroupConfig, rowIds);
        fillTxStatePartitionStorage(outgoingTxStatePartitionStorage, expLastAppliedIndex, expLastAppliedTerm, txIds);

        MvTableStorage incomingMvTableStorage = spy(new TestMvTableStorage(getTableConfig(), tablesConfig));
        TxStateTableStorage incomingTxStateTableStorage = spy(new TestTxStateTableStorage());

        assertThat(incomingMvTableStorage.createMvPartition(TEST_PARTITION), willCompleteSuccessfully());
        incomingTxStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION);

        MessagingService messagingService = messagingServiceForSuccessScenario(outgoingMvPartitionStorage,
                outgoingTxStatePartitionStorage, expLastAppliedIndex, expLastAppliedTerm, expLastGroupConfig, rowIds, txIds, snapshotId);

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                snapshotId,
                incomingMvTableStorage,
                incomingTxStateTableStorage,
                messagingService
        );

        // Let's add a rebalance interruption in the middle.
        CompletableFuture<Void> startAddWriteFuture = new CompletableFuture<>();
        CompletableFuture<Void> finishAddWriteFuture = new CompletableFuture<>();

        doAnswer(answer -> {
            startAddWriteFuture.complete(null);

            assertThat(finishAddWriteFuture, willCompleteSuccessfully());

            return null;
        }).when(partitionSnapshotStorage.partition())
                .addWrite(any(RowId.class), any(BinaryRow.class), any(UUID.class), any(UUID.class), anyInt());

        // Let's start rebalancing.
        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startToCopyFrom(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME),
                mock(SnapshotCopierOptions.class)
        );

        // Let's try to cancel it in the middle of the rebalance.
        CompletableFuture<?> cancelRebalanceFuture = runAsync(() -> {
            assertThat(startAddWriteFuture, willCompleteSuccessfully());

            CompletableFuture<?> cancelCopierFuture = runAsync(() -> finishAddWriteFuture.complete(null));

            snapshotCopier.cancel();

            snapshotCopier.join();

            assertThat(cancelCopierFuture, willCompleteSuccessfully());
        });

        assertThat(cancelRebalanceFuture, willCompleteSuccessfully());

        verify(partitionSnapshotStorage.partition()).abortRebalance();
    }

    @Test
    void testErrorInProcessOfRebalance() {
        MvPartitionStorage outgoingMvPartitionStorage = new TestMvPartitionStorage(TEST_PARTITION);
        TxStateStorage outgoingTxStatePartitionStorage = new TestTxStateStorage();

        long expLastAppliedIndex = 100500L;
        long expLastAppliedTerm = 100L;
        RaftGroupConfiguration expLastGroupConfig = generateRaftGroupConfig();

        List<RowId> rowIds = generateRowIds();
        List<UUID> txIds = generateTxIds();

        fillMvPartitionStorage(outgoingMvPartitionStorage, expLastAppliedIndex, expLastAppliedTerm, expLastGroupConfig, rowIds);
        fillTxStatePartitionStorage(outgoingTxStatePartitionStorage, expLastAppliedIndex, expLastAppliedTerm, txIds);

        MvTableStorage incomingMvTableStorage = spy(new TestMvTableStorage(getTableConfig(), tablesConfig));
        TxStateTableStorage incomingTxStateTableStorage = spy(new TestTxStateTableStorage());

        assertThat(incomingMvTableStorage.createMvPartition(TEST_PARTITION), willCompleteSuccessfully());
        incomingTxStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION);

        MessagingService messagingService = messagingServiceForSuccessScenario(outgoingMvPartitionStorage,
                outgoingTxStatePartitionStorage, expLastAppliedIndex, expLastAppliedTerm, expLastGroupConfig, rowIds, txIds, snapshotId);

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                snapshotId,
                incomingMvTableStorage,
                incomingTxStateTableStorage,
                messagingService
        );

        // Let's add an error on the rebalance.
        doThrow(StorageException.class).when(partitionSnapshotStorage.partition())
                .addWrite(any(RowId.class), any(BinaryRow.class), any(UUID.class), any(UUID.class), anyInt());

        // Let's start rebalancing.
        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startToCopyFrom(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME),
                mock(SnapshotCopierOptions.class)
        );

        // Let's wait for an error on rebalancing.
        assertThat(runAsync(snapshotCopier::join), willFailFast(IllegalStateException.class));

        verify(partitionSnapshotStorage.partition()).abortRebalance();

        TablePartitionId tablePartitionId = new TablePartitionId(tableId, TEST_PARTITION);

        verify(mvGc, times(1)).removeStorage(eq(tablePartitionId));
        verify(mvGc, times(1)).addStorage(eq(tablePartitionId), any(StorageUpdateHandler.class));
    }

    private TableConfiguration getTableConfig() {
        return tablesConfig.tables().get("foo");
    }

    private static List<RowId> generateRowIds() {
        return List.of(
                new RowId(TEST_PARTITION),
                new RowId(TEST_PARTITION),
                new RowId(TEST_PARTITION),
                new RowId(TEST_PARTITION)
        );
    }

    private static List<UUID> generateTxIds() {
        return List.of(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID()
        );
    }

    private static RaftGroupConfiguration generateRaftGroupConfig() {
        return new RaftGroupConfiguration(
                List.of("peer"),
                List.of("learner"),
                List.of("old-peer"),
                List.of("old-learner")
        );
    }
}
