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

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
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
import org.apache.ignite.internal.storage.index.impl.TestHashIndexStorage;
import org.apache.ignite.internal.table.distributed.HashIndexLocker;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
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
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
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

    private static IndexLocker pkLocker;

    /** If true the local replica is considered leader, false otherwise. */
    private static boolean localLeader;

    /** The state is used to resolve write intent. */
    private static TxState txState;

    @BeforeAll
    private static void beforeAll() {
        when(mockRaftClient.refreshAndGetLeaderWithTerm()).thenAnswer(invocationOnMock -> {
            if (!localLeader) {
                return CompletableFuture.completedFuture(new IgniteBiTuple<>(new Peer(anotherNode.address()), 1L));
            }

            return CompletableFuture.completedFuture(new IgniteBiTuple<>(new Peer(localNode.address()), 1L));
        });

        when(topologySrv.getByAddress(any())).thenAnswer(invocationOnMock -> {
            NetworkAddress addr = invocationOnMock.getArgument(0);
            if (addr.equals(anotherNode.address())) {
                return anotherNode;
            } else if (addr.equals(localNode.address())) {
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

        UUID indexId = UUID.randomUUID();

        BinaryTupleSchema pkSchema = BinaryTupleSchema.create(new Element[]{
                new Element(NativeTypes.BYTES, false)
        });

        Function<BinaryRow, BinaryTuple> row2tuple = tableRow -> new BinaryTuple(pkSchema, ((BinaryRow) tableRow).keySlice());

        pkStorage = new Lazy<>(() -> new TableSchemaAwareIndexStorage(
                indexId,
                new TestHashIndexStorage(null),
                row2tuple
        ));

        LockManager lockManager = new HeapLockManager();

        pkLocker = new HashIndexLocker(indexId, true, lockManager, row2tuple);

        partitionReplicaListener = new PartitionReplicaListener(
                testMvPartitionStorage,
                mockRaftClient,
                mock(TxManager.class),
                lockManager,
                partId,
                tblId,
                () -> Map.of(pkLocker.id(), pkLocker),
                pkStorage,
                clock,
                safeTimeClock,
                txStateStorage,
                topologySrv,
                placementDriver,
                peer -> true
        );

        marshallerFactory = new ReflectionMarshallerFactory();

        schemaDescriptor = new SchemaDescriptor(1, new Column[]{
                new Column("intKey".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strKey".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        }, new Column[]{
                new Column("intVal".toUpperCase(Locale.ROOT), NativeTypes.INT32, false),
                new Column("strVal".toUpperCase(Locale.ROOT), NativeTypes.STRING, false),
        });

        kvMarshaller = marshallerFactory.create(schemaDescriptor, TestKey.class, TestValue.class);
    }

    @BeforeEach
    private void beforeTest() {
        localLeader = true;
        txState = null;
        ((TestHashIndexStorage) pkStorage.get().storage()).destroy();
    }

    @Test
    public void testTxStateReplicaRequestEmptyState() {
        CompletableFuture fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateReplicaRequest()
                .groupId(grpId)
                .commitTimestamp(clock.now())
                .txId(Timestamp.nextVersion().toUuid())
                .build());

        IgniteBiTuple<Peer, Long> tuple = (IgniteBiTuple<Peer, Long>) fut.join();

        assertNull(tuple.get1());
        assertNull(tuple.get2());
    }

    @Test
    public void testTxStateReplicaRequestCommitState() {
        UUID txId = Timestamp.nextVersion().toUuid();

        txStateStorage.put(txId, new TxMeta(TxState.COMMITED, Collections.singletonList(grpId), clock.now()));

        HybridTimestamp readTimestamp = clock.now();

        CompletableFuture fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateReplicaRequest()
                .groupId(grpId)
                .commitTimestamp(readTimestamp)
                .txId(txId)
                .build());

        IgniteBiTuple<TxMeta, ClusterNode> tuple = (IgniteBiTuple<TxMeta, ClusterNode>) fut.join();

        assertEquals(TxState.COMMITED, tuple.get1().txState());
        assertTrue(readTimestamp.compareTo(tuple.get1().commitTimestamp()) > 0);
        assertNull(tuple.get2());
    }

    @Test
    public void testTxStateReplicaRequestMissLeaderMiss() {
        localLeader = false;

        CompletableFuture fut = partitionReplicaListener.invoke(TX_MESSAGES_FACTORY.txStateReplicaRequest()
                .groupId(grpId)
                .commitTimestamp(clock.now())
                .txId(Timestamp.nextVersion().toUuid())
                .build());

        IgniteBiTuple<Peer, Long> tuple = (IgniteBiTuple<Peer, Long>) fut.join();

        assertNull(tuple.get1());
        assertNotNull(tuple.get2());
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestEmptyResult() {
        BinaryRow testBinaryKey = nextBinaryKey();

        CompletableFuture fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowReplicaRequest()
                .groupId(grpId)
                .readTimestamp(clock.now())
                .binaryRow(testBinaryKey)
                .requestType(RequestType.RO_GET)
                .build());

        BinaryRow binaryRow = (BinaryRow) fut.join();

        assertNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestCommittedResult() {
        UUID txId = Timestamp.nextVersion().toUuid();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(partId);

        pkStorage.get().put(testBinaryKey, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, tblId, partId);
        testMvPartitionStorage.commitWrite(rowId, clock.now());

        CompletableFuture fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowReplicaRequest()
                .groupId(grpId)
                .readTimestamp(clock.now())
                .binaryRow(testBinaryKey)
                .requestType(RequestType.RO_GET)
                .build());

        BinaryRow binaryRow = (BinaryRow) fut.join();

        assertNotNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentCommitted() {
        UUID txId = Timestamp.nextVersion().toUuid();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(partId);
        txState = TxState.COMMITED;

        pkStorage.get().put(testBinaryKey, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, tblId, partId);

        CompletableFuture fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowReplicaRequest()
                .groupId(grpId)
                .readTimestamp(clock.now())
                .binaryRow(testBinaryKey)
                .requestType(RequestType.RO_GET)
                .build());

        BinaryRow binaryRow = (BinaryRow) fut.join();

        assertNotNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentPending() {
        UUID txId = Timestamp.nextVersion().toUuid();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(partId);

        pkStorage.get().put(testBinaryKey, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, tblId, partId);

        CompletableFuture fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowReplicaRequest()
                .groupId(grpId)
                .readTimestamp(clock.now())
                .binaryRow(testBinaryKey)
                .requestType(RequestType.RO_GET)
                .build());

        BinaryRow binaryRow = (BinaryRow) fut.join();

        assertNull(binaryRow);
    }

    @Test
    public void testReadOnlySingleRowReplicaRequestResolveWriteIntentAborted() {
        UUID txId = Timestamp.nextVersion().toUuid();
        BinaryRow testBinaryKey = nextBinaryKey();
        BinaryRow testBinaryRow = binaryRow(key(testBinaryKey), new TestValue(1, "v1"));
        var rowId = new RowId(partId);
        txState = TxState.ABORTED;

        pkStorage.get().put(testBinaryKey, rowId);
        testMvPartitionStorage.addWrite(rowId, testBinaryRow, txId, tblId, partId);

        CompletableFuture fut = partitionReplicaListener.invoke(TABLE_MESSAGES_FACTORY.readOnlySingleRowReplicaRequest()
                .groupId(grpId)
                .readTimestamp(clock.now())
                .binaryRow(testBinaryKey)
                .requestType(RequestType.RO_GET)
                .build());

        BinaryRow binaryRow = (BinaryRow) fut.join();

        assertNull(binaryRow);
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
