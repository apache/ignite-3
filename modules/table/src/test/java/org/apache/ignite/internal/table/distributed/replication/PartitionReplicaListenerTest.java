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

package org.apache.ignite.internal.table.distributed.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTuplePrefixBuilder;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.MarshallerFactory;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor.HashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.SortedIndexLocker;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.replicator.LeaderOrTxState;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.replicator.PlacementDriver;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateStorage;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/** There are tests for partition replica listener. */
public class PartitionReplicaListenerTest extends IgniteAbstractTest {
    /** Tx messages factory. */
    private static final TxMessagesFactory TX_MESSAGES_FACTORY = new TxMessagesFactory();

    /** Table messages factory. */
    private static final TableMessagesFactory TABLE_MESSAGES_FACTORY = new TableMessagesFactory();

    /** Partition id. */
    private static final int partId = 0;

    /** Table id. */
    private static final UUID tblId = UUID.randomUUID();

    /** Replication group id. */
    private static final ReplicationGroupId grpId = new TablePartitionId(tblId, partId);

    /** Hybrid clock. */
    private static final HybridClock clock = new HybridClockImpl();

    /** The storage stores transaction states. */
    private static final TestTxStateStorage txStateStorage = new TestTxStateStorage();

    /** The storage stores partition data. */
    private static final TestMvPartitionStorage testMvPartitionStorage = new TestMvPartitionStorage(partId);

    /** Local cluster node. */
    private static final ClusterNode localNode = new ClusterNode("node1", "node1", NetworkAddress.from("127.0.0.1:127"));

    /** Another (not local) cluster node. */
    private static final ClusterNode anotherNode = new ClusterNode("node2", "node2", NetworkAddress.from("127.0.0.2:127"));

    private static PlacementDriver placementDriver = mock(PlacementDriver.class);

    @Mock
    private static RaftGroupService mockRaftClient = mock(RaftGroupService.class);

    @Mock
    private static TopologyService topologySrv = mock(TopologyService.class);

    /** Default reflection marshaller factory. */
    protected static MarshallerFactory marshallerFactory;

    /** Schema descriptor for tests. */
    protected static SchemaDescriptor schemaDescriptor;

    /** Key-value marshaller for tests. */
    protected static KvMarshaller<TestKey, TestValue> kvMarshaller;

    /** Partition replication listener to test. */
    private static PartitionReplicaListener partitionReplicaListener;

    /** Primary index. */
    private static Lazy<TableSchemaAwareIndexStorage> pkStorage;

    /** If true the local replica is considered leader, false otherwise. */
    private static boolean localLeader;

    /** The state is used to resolve write intent. */
    private static TxState txState;

    private static BinaryTupleSchema sortedIndexBinarySchema;

    /** Secondary sorted index. */
    private static TableSchemaAwareIndexStorage sortedIndexStorage;

    /** Secondary hash index. */
    private static TableSchemaAwareIndexStorage hashIndexStorage;

    @BeforeAll
    private static void beforeAll() {
        when(mockRaftClient.refreshAndGetLeaderWithTerm()).thenAnswer(invocationOnMock -> {
            if (!localLeader) {
                return CompletableFuture.completedFuture(new LeaderWithTerm(new Peer(anotherNode.name()), 1L));
            }

            return CompletableFuture.completedFuture(new LeaderWithTerm(new Peer(localNode.name()), 1L));
        });

        when(topologySrv.getByConsistentId(any())).thenAnswer(invocationOnMock -> {
            String consistentId = invocationOnMock.getArgument(0);
            if (consistentId.equals(anotherNode.name())) {
                return anotherNode;
            } else if (consistentId.equals(localNode.name())) {
                return localNode;
            } else {
                return null;
            }
        });

        when(topologySrv.localMember()).thenReturn(localNode);

        HybridTimestamp txFixedTimestamp = clock.now();

        when(placementDriver.sendMetaRequest(eq(grpId), any())).thenAnswer(invocationOnMock -> {
            TxMeta txMeta;

            if (txState == null) {
                txMeta = null;
            } else if (txState == TxState.COMMITED) {
                txMeta = new TxMeta(TxState.COMMITED, Collections.singletonList(grpId), txFixedTimestamp);
            } else {
                assert txState == TxState.ABORTED : "Sate is " + txState;

                txMeta = new TxMeta(TxState.ABORTED, Collections.singletonList(grpId), txFixedTimestamp);
            }
            return CompletableFuture.completedFuture(txMeta);
        });

        PendingComparableValuesTracker safeTimeClock = mock(PendingComparableValuesTracker.class);
        when(safeTimeClock.waitFor(any())).thenReturn(CompletableFuture.completedFuture(null));

        UUID pkIndexId = UUID.randomUUID();
        UUID sortedIndexId = UUID.randomUUID();
        UUID hashIndexId = UUID.randomUUID();

        BinaryTupleSchema pkSchema = BinaryTupleSchema.create(new Element[]{
                new Element(NativeTypes.BYTES, false)
        });

        Function<BinaryRow, BinaryTuple> row2tuple = tableRow -> new BinaryTuple(pkSchema, ((BinaryRow) tableRow).keySlice());

        pkStorage = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                pkIndexId,
                new TestHashIndexStorage(null),
                row2tuple
        ));

        sortedIndexStorage = new TableSchemaAwareIndexStorage(
                sortedIndexId,
                new TestSortedIndexStorage(new SortedIndexDescriptor(sortedIndexId, List.of(
                        new SortedIndexColumnDescriptor("intVal", NativeTypes.INT32, false, true)
                ))),
                row -> null
        );

        hashIndexStorage = new TableSchemaAwareIndexStorage(
                hashIndexId,
                new TestHashIndexStorage(new HashIndexDescriptor(hashIndexId, List.of(
                        new HashIndexColumnDescriptor("intVal", NativeTypes.INT32, false)
                ))),
                row -> null
        );

        LockManager lockManager = new HeapLockManager();

        IndexLocker pkLocker = new HashIndexLocker(pkIndexId, true, lockManager, row2tuple);
        IndexLocker sortedIndexLocker = new SortedIndexLocker(sortedIndexId, lockManager, null, row2tuple);
        IndexLocker hashIndexLocker = new HashIndexLocker(hashIndexId, false, lockManager, row2tuple);

        partitionReplicaListener = new PartitionReplicaListener(
                testMvPartitionStorage,
                mockRaftClient,
                mock(TxManager.class),
                lockManager,
                Runnable::run,
                partId,
                tblId,
                () -> Map.of(pkLocker.id(), pkLocker, sortedIndexId, sortedIndexLocker, hashIndexId, hashIndexLocker),
                pkStorage,
                () -> Map.of(sortedIndexId, sortedIndexStorage, hashIndexId, hashIndexStorage),
                clock,
                safeTimeClock,
                txStateStorage,
                placementDriver,
                new StorageUpdateHandler(
                        partId,
                        new TestPartitionDataStorage(testMvPartitionStorage),
                        () -> Map.of(pkStorage.get().id(), pkStorage.get())
                ),
                peer -> localNode.name().equals(peer.consistentId())
        );

        marshallerFactory = new ReflectionMarshallerFactory();

        schemaDescriptor = new SchemaDescriptor(1, new Column[]{
                new Column("intKey".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strKey".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        }, new Column[]{
                new Column("intVal".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strVal".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        });

        sortedIndexBinarySchema = BinaryTupleSchema.createSchema(schemaDescriptor, new int[]{2 /* intVal column */});

        kvMarshaller = marshallerFactory.create(schemaDescriptor, TestKey.class, TestValue.class);
    }

    @BeforeEach
    private void beforeTest() {
        localLeader = true;
        txState = null;
        ((TestHashIndexStorage) pkStorage.get().storage()).clear();
        ((TestHashIndexStorage) hashIndexStorage.storage()).clear();
        ((TestSortedIndexStorage) sortedIndexStorage.storage()).clear();
    }

    @Test
    public void testTxStateReplicaRequestEmptyState() throws Exception {
        CompletableFuture<?> fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateReplicaRequest()
                .groupId(grpId)
                .commitTimestamp(clock.now())
                .txId(Timestamp.nextVersion().toUuid())
                .build());

        LeaderOrTxState tuple = (LeaderOrTxState) fut.get(1, TimeUnit.SECONDS);

        assertNull(tuple.leaderName());
        assertNull(tuple.txMeta());
    }

    @Test
    public void testTxStateReplicaRequestCommitState() throws Exception {
        UUID txId = Timestamp.nextVersion().toUuid();

        txStateStorage.put(txId, new TxMeta(TxState.COMMITED, Collections.singletonList(grpId), clock.now()));

        HybridTimestamp readTimestamp = clock.now();

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateReplicaRequest()
                .groupId(grpId)
                .commitTimestamp(readTimestamp)
                .txId(txId)
                .build());

        LeaderOrTxState tuple = (LeaderOrTxState) fut.get(1, TimeUnit.SECONDS);

        assertEquals(TxState.COMMITED, tuple.txMeta().txState());
        assertTrue(readTimestamp.compareTo(tuple.txMeta().commitTimestamp()) > 0);
        assertNull(tuple.leaderName());
    }

    @Test
    public void testTxStateReplicaRequestMissLeaderMiss() throws Exception {
        localLeader = false;

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateReplicaRequest()
                .groupId(grpId)
                .commitTimestamp(clock.now())
                .txId(Timestamp.nextVersion().toUuid())
                .build());

        LeaderOrTxState tuple = (LeaderOrTxState) fut.get(1, TimeUnit.SECONDS);

        assertNull(tuple.txMeta());
        assertNotNull(tuple.leaderName());
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestEmptyResult() throws Exception {
        BinaryRow testBinaryKey = nextBinaryKey();

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowReplicaRequest()
                .groupId(grpId)
                .readTimestamp(clock.now())
                .binaryRow(testBinaryKey)
                .requestType(RequestType.RO_GET)
                .build());

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS);

        assertNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestCommittedResult() throws Exception {
        UUID txId = Timestamp.nextVersion().toUuid();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(partId);

        pkStorage.get().put(testBinaryKey, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, tblId, partId);
        testMvPartitionStorage.commitWrite(rowId, clock.now());

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowReplicaRequest()
                .groupId(grpId)
                .readTimestamp(clock.now())
                .binaryRow(testBinaryKey)
                .requestType(RequestType.RO_GET)
                .build());

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentCommitted() throws Exception {
        UUID txId = Timestamp.nextVersion().toUuid();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(partId);
        txState = TxState.COMMITED;

        pkStorage.get().put(testBinaryKey, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, tblId, partId);

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowReplicaRequest()
                .groupId(grpId)
                .readTimestamp(clock.now())
                .binaryRow(testBinaryKey)
                .requestType(RequestType.RO_GET)
                .build());

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentPending() throws Exception {
        UUID txId = Timestamp.nextVersion().toUuid();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(partId);

        pkStorage.get().put(testBinaryKey, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, tblId, partId);

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowReplicaRequest()
                .groupId(grpId)
                .readTimestamp(clock.now())
                .binaryRow(testBinaryKey)
                .requestType(RequestType.RO_GET)
                .build());

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS);

        assertNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentAborted() throws Exception {
        UUID txId = Timestamp.nextVersion().toUuid();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(partId);
        txState = TxState.ABORTED;

        pkStorage.get().put(testBinaryKey, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, tblId, partId);

        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowReplicaRequest()
                .groupId(grpId)
                .readTimestamp(clock.now())
                .binaryRow(testBinaryKey)
                .requestType(RequestType.RO_GET)
                .build());

        BinaryRow binaryRow = (BinaryRow) fut.get(1, TimeUnit.SECONDS);

        assertNull(binaryRow);
    }

    @Test
    public void testWriteScanRetriveBatchReplicaRequestWithSortedIndex() throws Exception {
        UUID txId = Timestamp.nextVersion().toUuid();
        UUID sortedIndexId = sortedIndexStorage.id();

        IntStream.range(0, 6).forEach(i -> {
            RowId rowId = new RowId(partId);
            int indexedVal = i % 5; // Non-uniq index.
            TestValue testValue = new TestValue(indexedVal, "val" + i);

            BinaryTuple indexedValue = new BinaryTuple(sortedIndexBinarySchema,
                    new BinaryTupleBuilder(1, false).appendInt(indexedVal).build());
            BinaryRow storeRow = binaryRow(key(nextBinaryKey()), testValue);

            testMvPartitionStorage.addWrite(rowId, storeRow, txId, tblId, partId);
            sortedIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
            testMvPartitionStorage.commitWrite(rowId, clock.now());
        });

        UUID scanTxId = Timestamp.nextVersion().toUuid();

        // Request first batch
        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .timestamp(clock.now())
                .term(1L)
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .build());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(4, rows.size());

        // Request second batch
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .timestamp(clock.now())
                .term(1L)
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Request bounded.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(Timestamp.nextVersion().toUuid())
                .timestamp(clock.now())
                .term(1L)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBound(toIndexBound(1))
                .upperBound(toIndexBound(3))
                .flags(SortedIndexStorage.LESS_OR_EQUAL)
                .batchSize(5)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Empty result.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(Timestamp.nextVersion().toUuid())
                .timestamp(clock.now())
                .term(1L)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBound(toIndexBound(5))
                .batchSize(5)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readWriteScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(Timestamp.nextVersion().toUuid())
                .timestamp(clock.now())
                .term(1L)
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(5)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());
    }

    @Test
    public void testReadOnlyScanRetriveBatchReplicaRequestSortedIndex() throws Exception {
        UUID txId = Timestamp.nextVersion().toUuid();
        UUID sortedIndexId = sortedIndexStorage.id();

        IntStream.range(0, 6).forEach(i -> {
            RowId rowId = new RowId(partId);
            int indexedVal = i % 5; // Non-uniq index.
            TestValue testValue = new TestValue(indexedVal, "val" + i);

            BinaryTuple indexedValue = new BinaryTuple(sortedIndexBinarySchema,
                    new BinaryTupleBuilder(1, false).appendInt(indexedVal).build());
            BinaryRow storeRow = binaryRow(key(nextBinaryKey()), testValue);

            testMvPartitionStorage.addWrite(rowId, storeRow, txId, tblId, partId);
            sortedIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
            testMvPartitionStorage.commitWrite(rowId, clock.now());
        });

        UUID scanTxId = Timestamp.nextVersion().toUuid();

        // Request first batch
        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .readTimestamp(clock.now())
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .build());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(4, rows.size());

        // Request second batch
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .readTimestamp(clock.now())
                .scanId(1L)
                .indexToUse(sortedIndexId)
                .batchSize(4)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Request bounded.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(Timestamp.nextVersion().toUuid())
                .readTimestamp(clock.now())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBound(toIndexBound(1))
                .upperBound(toIndexBound(3))
                .flags(SortedIndexStorage.LESS_OR_EQUAL)
                .batchSize(5)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());

        // Empty result.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(Timestamp.nextVersion().toUuid())
                .readTimestamp(clock.now())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .lowerBound(toIndexBound(5))
                .batchSize(5)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(Timestamp.nextVersion().toUuid())
                .readTimestamp(clock.now())
                .scanId(2L)
                .indexToUse(sortedIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(5)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(2, rows.size());
    }

    @Test
    public void testReadOnlyScanRetriveBatchReplicaRequstHashIndex() throws Exception {
        UUID txId = Timestamp.nextVersion().toUuid();
        UUID hashIndexId = hashIndexStorage.id();

        IntStream.range(0, 7).forEach(i -> {
            RowId rowId = new RowId(partId);
            int indexedVal = i % 2; // Non-uniq index.
            TestValue testValue = new TestValue(indexedVal, "val" + i);

            BinaryTuple indexedValue = new BinaryTuple(sortedIndexBinarySchema,
                    new BinaryTupleBuilder(1, false).appendInt(indexedVal).build());
            BinaryRow storeRow = binaryRow(key(nextBinaryKey()), testValue);

            testMvPartitionStorage.addWrite(rowId, storeRow, txId, tblId, partId);
            hashIndexStorage.storage().put(new IndexRowImpl(indexedValue, rowId));
            testMvPartitionStorage.commitWrite(rowId, clock.now());
        });

        UUID scanTxId = Timestamp.nextVersion().toUuid();

        // Request first batch
        CompletableFuture<?> fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .readTimestamp(clock.now())
                .scanId(1L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(3)
                .build());

        List<BinaryRow> rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(3, rows.size());

        // Request second batch
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(scanTxId)
                .readTimestamp(clock.now())
                .scanId(1L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(0))
                .batchSize(1)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(1, rows.size());

        // Empty result.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(Timestamp.nextVersion().toUuid())
                .readTimestamp(clock.now())
                .scanId(2L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(5))
                .batchSize(5)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(0, rows.size());

        // Lookup.
        fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlyScanRetrieveBatchReplicaRequest()
                .groupId(grpId)
                .transactionId(Timestamp.nextVersion().toUuid())
                .readTimestamp(clock.now())
                .scanId(2L)
                .indexToUse(hashIndexId)
                .exactKey(toIndexKey(1))
                .batchSize(5)
                .build());

        rows = (List<BinaryRow>) fut.get(1, TimeUnit.SECONDS);

        assertNotNull(rows);
        assertEquals(3, rows.size());
    }

    private static BinaryTuplePrefix toIndexBound(int val) {
        ByteBuffer tuple = new BinaryTuplePrefixBuilder(1, 1).appendInt(val).build();

        return new BinaryTuplePrefix(sortedIndexBinarySchema, tuple);
    }

    private static BinaryTuple toIndexKey(int val) {
        ByteBuffer tuple = new BinaryTupleBuilder(1, true).appendInt(val).build();

        return new BinaryTuple(sortedIndexBinarySchema, tuple);
    }

    protected static BinaryRow nextBinaryKey() {
        try {
            int nextInt = (int) System.nanoTime();

            return kvMarshaller.marshal(new TestKey(nextInt, "key " + nextInt));
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    protected static BinaryRow binaryRow(TestKey key, TestValue value) {
        try {
            return kvMarshaller.marshal(key, value);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    protected static TestKey key(BinaryRow binaryRow) {
        try {
            return kvMarshaller.unmarshalKey(new Row(schemaDescriptor, binaryRow));
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    protected static TestValue value(BinaryRow binaryRow) {
        try {
            return kvMarshaller.unmarshalValue(new Row(schemaDescriptor, binaryRow));
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }


    /**
     * Test pojo key.
     */
    protected static class TestKey {
        @IgniteToStringInclude
        public int intKey;

        @IgniteToStringInclude
        public String strKey;

        public TestKey() {
        }

        public TestKey(int intKey, String strKey) {
            this.intKey = intKey;
            this.strKey = strKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestKey testKey = (TestKey) o;
            return intKey == testKey.intKey && Objects.equals(strKey, testKey.strKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intKey, strKey);
        }

        @Override
        public String toString() {
            return S.toString(TestKey.class, this);
        }
    }

    /**
     * Test pojo value.
     */
    protected static class TestValue implements Comparable<TestValue> {
        @IgniteToStringInclude
        public Integer intVal;

        @IgniteToStringInclude
        public String strVal;

        public TestValue() {
        }

        public TestValue(Integer intVal, String strVal) {
            this.intVal = intVal;
            this.strVal = strVal;
        }

        @Override
        public int compareTo(TestValue o) {
            int cmp = Integer.compare(intVal, o.intVal);

            return cmp != 0 ? cmp : strVal.compareTo(o.strVal);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestValue testValue = (TestValue) o;
            return Objects.equals(intVal, testValue.intVal) && Objects.equals(strVal, testValue.strVal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intVal, strVal);
        }

        @Override
        public String toString() {
            return S.toString(TestValue.class, this);
        }
    }
}
