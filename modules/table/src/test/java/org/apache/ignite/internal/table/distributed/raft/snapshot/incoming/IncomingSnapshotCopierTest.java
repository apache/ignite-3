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
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionAccessImpl;
import org.apache.ignite.internal.table.distributed.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotUri;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
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

    private static final HybridClock HYBRID_CLOCK = new HybridClock();

    private static final TableMessagesFactory TABLE_MSG_FACTORY = new TableMessagesFactory();

    private static final RaftMessagesFactory RAFT_MSG_FACTORY = new RaftMessagesFactory();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @InjectConfiguration
    private TablesConfiguration tablesConfig;

    @InjectConfiguration
    private TableConfiguration tableConfig;

    @AfterEach
    void tearDown() {
        shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS);
    }

    @Test
    void test() throws Exception {
        MvPartitionStorage outgoingPartitionStorage = new TestMvPartitionStorage(TEST_PARTITION);

        List<RowId> rowIds = prepareOutgoingPartitionStorage(outgoingPartitionStorage);

        UUID snapshotId = UUID.randomUUID();

        MvTableStorage incomingTableStorage = spy(new TestMvTableStorage(tableConfig, tablesConfig));

        incomingTableStorage.getOrCreateMvPartition(TEST_PARTITION);

        PartitionSnapshotStorage partitionSnapshotStorage = createPartitionSnapshotStorage(
                snapshotId,
                incomingTableStorage,
                outgoingPartitionStorage,
                rowIds
        );

        SnapshotCopier snapshotCopier = partitionSnapshotStorage.startToCopyFrom(
                SnapshotUri.toStringUri(snapshotId, NODE_NAME),
                mock(SnapshotCopierOptions.class)
        );

        runAsync(snapshotCopier::join).get(1, TimeUnit.SECONDS);

        assertEquals(Status.OK().getCode(), snapshotCopier.getCode());

        MvPartitionStorage incomingPartitionStorage = incomingTableStorage.getMvPartition(TEST_PARTITION);

        assertEquals(outgoingPartitionStorage.lastAppliedIndex(), incomingPartitionStorage.lastAppliedIndex());

        assertEqualsRows(outgoingPartitionStorage, incomingPartitionStorage, rowIds);

        verify(incomingTableStorage, times(1)).destroyPartition(eq(TEST_PARTITION));
        verify(incomingTableStorage, times(2)).getOrCreateMvPartition(eq(TEST_PARTITION));
    }

    private PartitionSnapshotStorage createPartitionSnapshotStorage(
            UUID snapshotId,
            MvTableStorage incomingTableStorage,
            MvPartitionStorage outgoingPartitionStorage,
            List<RowId> rowIds
    ) {
        TopologyService topologyService = mock(TopologyService.class);

        ClusterNode clusterNode = mock(ClusterNode.class);

        when(topologyService.getByConsistentId(NODE_NAME)).thenReturn(clusterNode);

        OutgoingSnapshotsManager outgoingSnapshotsManager = mock(OutgoingSnapshotsManager.class);

        MessagingService messagingService = mock(MessagingService.class);

        when(messagingService.invoke(eq(clusterNode), any(SnapshotMetaRequest.class), anyLong())).then(answer -> {
            SnapshotMetaRequest snapshotMetaRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotMetaRequest.id());

            return completedFuture(
                    TABLE_MSG_FACTORY.snapshotMetaResponse()
                            .meta(RAFT_MSG_FACTORY.snapshotMeta().lastIncludedIndex(outgoingPartitionStorage.lastAppliedIndex()).build())
                            .build()
            );
        });

        when(messagingService.invoke(eq(clusterNode), any(SnapshotMvDataRequest.class), anyLong())).then(answer -> {
            SnapshotMvDataRequest snapshotMvDataRequest = answer.getArgument(1);

            assertEquals(snapshotId, snapshotMvDataRequest.id());

            List<ResponseEntry> responseEntries = createResponseEntries(outgoingPartitionStorage, rowIds);

            assertThat(responseEntries, not(empty()));

            return completedFuture(TABLE_MSG_FACTORY.snapshotMvDataResponse().rows(responseEntries).finish(true).build());
        });

        when(outgoingSnapshotsManager.messagingService()).thenReturn(messagingService);

        return new PartitionSnapshotStorage(
                topologyService,
                outgoingSnapshotsManager,
                SnapshotUri.toStringUri(snapshotId, NODE_NAME),
                mock(RaftOptions.class),
                // TODO: IGNITE-17894 не забудь протестировать все это добро!
                new PartitionAccessImpl(incomingTableStorage, mock(TxStateTableStorage.class), TEST_PARTITION),
                mock(SnapshotMeta.class),
                executorService
        );
    }

    private static List<RowId> prepareOutgoingPartitionStorage(MvPartitionStorage partitionStorage) {
        List<RowId> rowIds = new ArrayList<>();

        partitionStorage.runConsistently(() -> {
            // Writes committed version.
            RowId rowId0 = new RowId(TEST_PARTITION);
            RowId rowId1 = new RowId(TEST_PARTITION);

            partitionStorage.addWriteCommitted(rowId0, createBinaryRow("k0", "v0"), HYBRID_CLOCK.now());
            partitionStorage.addWriteCommitted(rowId1, createBinaryRow("k1", "v1"), HYBRID_CLOCK.now());

            RowId rowId2 = new RowId(TEST_PARTITION);

            partitionStorage.addWriteCommitted(rowId2, createBinaryRow("k20", "v20"), HYBRID_CLOCK.now());
            partitionStorage.addWriteCommitted(rowId2, createBinaryRow("k21", "v21"), HYBRID_CLOCK.now());

            // Writes an intent to write (uncommitted version).
            partitionStorage.addWrite(rowId2, createBinaryRow("k22", "v22"), UUID.randomUUID(), UUID.randomUUID(), TEST_PARTITION);

            RowId rowId3 = new RowId(TEST_PARTITION);

            partitionStorage.addWrite(
                    rowId3,
                    createBinaryRow("k3", "v3"),
                    UUID.randomUUID(),
                    UUID.randomUUID(),
                    TEST_PARTITION
            );

            rowIds.addAll(Set.of(rowId0, rowId1, rowId2, rowId3));

            partitionStorage.lastAppliedIndex(rowIds.size());

            return null;
        });

        return rowIds;
    }

    private static List<ResponseEntry> createResponseEntries(MvPartitionStorage partitionStorage, List<RowId> rowIds) {
        List<ResponseEntry> responseEntries = new ArrayList<>();

        for (RowId rowId : rowIds) {
            List<ReadResult> readResults = partitionStorage.scanVersions(rowId).stream().collect(toList());

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
                            .rowId(new UUID(rowId.mostSignificantBits(), rowId.leastSignificantBits()))
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

    private static BinaryRow createBinaryRow(String key, String value) {
        return new RowAssembler(SCHEMA_DESCRIPTOR, 1, 1).appendString(key).appendString(value).build();
    }

    private static void assertEqualsRows(MvPartitionStorage expected, MvPartitionStorage actual, List<RowId> rowIds) {
        for (RowId rowId : rowIds) {
            List<ReadResult> expReadResults = expected.scanVersions(rowId).stream().collect(toList());
            List<ReadResult> actReadResults = actual.scanVersions(rowId).stream().collect(toList());

            assertEquals(expReadResults.size(), actReadResults.size(), rowId.toString());

            for (int i = 0; i < expReadResults.size(); i++) {
                ReadResult expReadResult = expReadResults.get(i);
                ReadResult actReadResult = actReadResults.get(i);

                String msg = "RowId=" + rowId + ", i=" + i;

                Row expRow = new Row(SCHEMA_DESCRIPTOR, expReadResult.binaryRow());
                Row actRow = new Row(SCHEMA_DESCRIPTOR, actReadResult.binaryRow());

                assertEquals(expRow.stringValue(0), actRow.stringValue(0), msg);
                assertEquals(expRow.stringValue(1), actRow.stringValue(1), msg);

                assertEquals(expReadResult.commitTimestamp(), actReadResult.commitTimestamp(), msg);
                assertEquals(expReadResult.transactionId(), actReadResult.transactionId(), msg);
                assertEquals(expReadResult.commitTableId(), actReadResult.commitTableId(), msg);
                assertEquals(expReadResult.commitPartitionId(), actReadResult.commitPartitionId(), msg);
                assertEquals(expReadResult.isWriteIntent(), actReadResult.isWriteIntent(), msg);
            }
        }
    }
}
