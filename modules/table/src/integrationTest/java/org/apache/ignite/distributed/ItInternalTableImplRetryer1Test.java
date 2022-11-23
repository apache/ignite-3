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

package org.apache.ignite.distributed;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.replicator.action.RequestType;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.ReadOnlyTransactionImpl;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.distributed.ItTxDistributedTestSingleNode.NODE_PORT_BASE;
import static org.apache.ignite.distributed.ItTxDistributedTestSingleNode.startNode;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for operation retrying in {@link InternalTableImpl}.
 */
public class ItInternalTableImplRetryer1Test extends IgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItInternalTableImplRetryer1Test.class);

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT64, false)},
            new Column[]{new Column("value", NativeTypes.INT64, false)}
    );

    private static final Collection<BinaryRowEx> rows = List.of(
            createKeyValueRow(1L, 1L),
            createKeyValueRow(2L, 2L)
    );

    private static final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("internal-table-scheduled-pool", LOG)
    );

    /** Replicator network message factory. */
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private final TableMessagesFactory tableMessagesFactory = new TableMessagesFactory();

    private final TestInfo testInfo;

    private ReplicaService replicaService;

    private ReplicaManagerStub replicaManagerStub;

    private ReplicaManager replicaManager;

    private InternalTable internalTable;

    private ClusterService clusterService;

    private NetworkAddress networkAddress;

    private String name = "client";

    @AfterAll
    public static void afterAll() {
        shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    @BeforeEach
    public void setup() {
        networkAddress = new NetworkAddress(getLocalAddress(), NODE_PORT_BASE + 1);
        var nodeFinder = new StaticNodeFinder(List.of(networkAddress));

        clusterService = startNode(testInfo, "client", NODE_PORT_BASE + 1, nodeFinder);

        HybridClock clock = mock(HybridClock.class);

        replicaService = new ReplicaService(clusterService.messagingService(), clock);

        TxManager txManager = mock(TxManager.class);

        doAnswer(mock -> new ReadWriteTransactionImpl(txManager, Timestamp.nextVersion().toUuid()))
                .when(txManager).begin();

        doAnswer(mock -> new ReadOnlyTransactionImpl(txManager,
                Timestamp.nextVersion().toUuid(),
                new HybridTimestamp(1, 1))
        )
                .when(txManager).begin(true);

        doAnswer(mock -> CompletableFuture.completedFuture(null))
                .when(txManager)
                .finish(any(ReplicationGroupId.class),
                        any(ClusterNode.class),
                        anyLong(),
                        anyBoolean(),
                        anyMap(),
                        any(UUID.class)
                );

        RaftGroupService raftGroupService = mock(RaftGroupService.class);

        doAnswer(mock -> CompletableFuture.completedFuture(new IgniteBiTuple<>(new Peer("client"), 1L)))
                .when(raftGroupService).refreshAndGetLeaderWithTerm();

        internalTable = new InternalTableImpl(
                "",
                UUID.randomUUID(),
                Int2ObjectMaps.singleton(0, raftGroupService),
                1,
                null,
//                consistentId -> clusterService.topologyService().localMember(),
                txManager,
                null,
                null,
                replicaService,
                clock
        );

//        replicaManagerStub = new ReplicaManagerStub();

//        replicaManagerStub.start(clusterService);


        replicaManager = new ReplicaManager(clusterService,
                new HybridClockImpl(),
                Set.of(TableMessageGroup.class, TxMessageGroup.class));

        replicaManager.start();
    }

    @AfterEach
    public void teardown() {
        clusterService.stop();
    }

    public ItInternalTableImplRetryer1Test(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

//    @Test
    public void test1() throws Exception {
        ClusterNode clusterNode = new ClusterNode(name, name, networkAddress);

        TablePartitionId tablePartitionId = new TablePartitionId(UUID.randomUUID(), 1);

        ReadWriteSingleRowReplicaRequest request = tableMessagesFactory.readWriteSingleRowReplicaRequest()
                .groupId(tablePartitionId)
                .binaryRow(createKeyValueRow(1L, 1L))
                .requestType(RequestType.RW_GET)
                .build();

        clusterService.messagingService().addMessageHandler(ReplicaMessageGroup.class,
                (message, sender, correlationId) -> {
                    try {
                        replicaManager.startReplica(tablePartitionId, request0 -> CompletableFuture.completedFuture(null));
                    } catch (NodeStoppingException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        replicaService.invoke(clusterNode, request).get(10, TimeUnit.SECONDS);
    }

//    @Test
    public void test2() {
        ClusterNode clusterNode = new ClusterNode(name, name, networkAddress);

        TablePartitionId tablePartitionId = new TablePartitionId(UUID.randomUUID(), 1);

        ReadWriteSingleRowReplicaRequest request = tableMessagesFactory.readWriteSingleRowReplicaRequest()
                .groupId(tablePartitionId)
                .binaryRow(createKeyValueRow(1L, 1L))
                .requestType(RequestType.RW_GET)
                .build();

        Exception e0 = null;
        Exception e1 = null;

        try {
            replicaService.invoke(clusterNode, request).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            e0 = e;
        }

        try {
            replicaService.invoke(clusterNode, request).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            e1 = e;
        }

        assertTrue(e0 != null);
        assertTrue(e0.getCause() instanceof ReplicationTimeoutException, e0.toString());

        assertTrue(e1 != null);
        assertTrue(e1.getCause() instanceof ReplicationTimeoutException, e1.toString());
    }

    //@Test
    public void testRwOneRowOperationRetrying() throws Exception {
        replicaManagerStub.invokesWithException(1);

        internalTable.upsert(createKeyValueRow(1L, 1L), null)
                .get(5, TimeUnit.SECONDS);
    }

    //@Test
    public void testRwOneRowOperationRetryingFail() {
        replicaManagerStub.invokesWithException(Integer.MAX_VALUE);

        Exception e = null;

        try {
            internalTable.upsert(createKeyValueRow(1L, 1L), null)
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof ReplicaUnavailableException, e.toString());
    }

    //@Test
    public void testRwCollectionRowOperationRetrying() throws Exception {
        replicaManagerStub.invokesWithException(1);

        internalTable.upsertAll(rows, null)
                .get(5, TimeUnit.SECONDS);
    }

    //@Test
    public void testRwCollectionRowOperationRetryingFail() {
        replicaManagerStub.invokesWithException(Integer.MAX_VALUE);

        Exception e = null;

        try {
            internalTable.upsertAll(rows, null)
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof ReplicaUnavailableException, e.toString());
    }

    //@Test
    public void testRwScanOperationRetrying() throws Exception {
        replicaManagerStub.invokesWithException(1);

        AtomicReference<Throwable> e = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);

        internalTable.scan(0, null).subscribe(new TestSubscriber(e, latch));

        latch.await(5, TimeUnit.SECONDS);

        assertTrue(e.get() == null, e.toString());
    }

    //@Test
    public void testRwScanOperationRetryingFail() throws Exception {
        replicaManagerStub.invokesWithException(Integer.MAX_VALUE);

        AtomicReference<Throwable> e = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);

        internalTable.scan(0, null).subscribe(new TestSubscriber(e, latch));

        latch.await(5, TimeUnit.SECONDS);

        assertTrue(e.get() != null, e.get().toString());
        assertTrue(e.get().getCause() instanceof ReplicaUnavailableException, e.get().toString());
    }

    //@Test
    public void testRoOneRowOperationRetrying() throws Exception {
        replicaManagerStub.invokesWithException(1);

        internalTable.get(createKeyValueRow(1L, 1L),
                        new HybridTimestamp(1, 1),
                        clusterService.topologyService().localMember()
                )
                .get(5, TimeUnit.SECONDS);
    }

    //@Test
    public void testRoOneRowOperationRetryingFail() {
        replicaManagerStub.invokesWithException(Integer.MAX_VALUE);

        Exception e = null;

        try {
            internalTable.get(createKeyValueRow(1L, 1L),
                            new HybridTimestamp(1, 1),
                            clusterService.topologyService().localMember()
                    )
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof ReplicaUnavailableException, e.toString());
    }

    //@Test
    public void testRoCollectionRowOperationRetrying() throws Exception {
        replicaManagerStub.invokesWithException(1);

        internalTable.getAll(rows,
                        new HybridTimestamp(1, 1),
                        clusterService.topologyService().localMember()
                )
                .get(5, TimeUnit.SECONDS);
    }

    //@Test
    public void testRoCollectionRowOperationRetryingFail() {
        replicaManagerStub.invokesWithException(Integer.MAX_VALUE);

        Exception e = null;

        try {
            internalTable.getAll(rows,
                            new HybridTimestamp(1, 1),
                            clusterService.topologyService().localMember()
                    )
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e0) {
            e = e0;
        }

        assertTrue(e != null);
        assertTrue(e.getCause() instanceof ReplicaUnavailableException, e.toString());
    }

    //@Test
    public void testRoScanOperationRetrying() throws Exception {
        replicaManagerStub.invokesWithException(1);

        AtomicReference<Throwable> e = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);

        internalTable.scan(0,
                new HybridTimestamp(1, 1),
                clusterService.topologyService().localMember()
        ).subscribe(new TestSubscriber(e, latch));

        latch.await(5, TimeUnit.SECONDS);

        assertTrue(e.get() == null, e.toString());
    }

    //@Test
    public void testRoScanOperationRetryingFail() throws Exception {
        replicaManagerStub.invokesWithException(Integer.MAX_VALUE);

        AtomicReference<Throwable> e = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);

        internalTable.scan(0, null).subscribe(new TestSubscriber(e, latch));

        latch.await(5, TimeUnit.SECONDS);

        assertTrue(e.get() != null, e.get().toString());
        assertTrue(e.get().getCause() instanceof ReplicaUnavailableException, e.get().toString());
    }

    /**
     * Replica manager which respond {@link ErrorReplicaResponse} with {@link ReplicaUnavailableException} or
     * {@link ReplicaResponse} without an exception depends on the counter.
     */
    private static class ReplicaManagerStub {
        private int invokesWithException = 0;

        private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

        public void invokesWithException(int invokesWithException) {
            this.invokesWithException = invokesWithException;
        }

        public void start(ClusterService clusterService) {
            NetworkMessageHandler handler = (message, senderAddr, correlationId) -> {
                if (!(message instanceof ReplicaRequest)) {
                    return;
                }

                ReplicaRequest request = (ReplicaRequest) message;

                if (invokesWithException > 0) {
                    invokesWithException--;

                    clusterService.messagingService().respond(
                            senderAddr,
                            REPLICA_MESSAGES_FACTORY
                                    .errorReplicaResponse()
                                    .throwable(
                                            new ReplicaUnavailableException(
                                                    request.groupId(),
                                                    clusterService.topologyService().localMember())
                                    )
                                    .build(),
                            correlationId);
                } else {
                    NetworkMessage msg = REPLICA_MESSAGES_FACTORY
                            .replicaResponse()
                            .result(null)
                            .build();

                    clusterService.messagingService().respond(senderAddr, msg, correlationId);
                }
            };

            clusterService.messagingService().addMessageHandler(ReplicaMessageGroup.class, handler);

            Set.of(TableMessageGroup.class, TxMessageGroup.class)
                    .forEach(mg -> clusterService.messagingService().addMessageHandler(mg, handler));
        }
    }

    private static class TestSubscriber implements Flow.Subscriber<BinaryRow> {
        private AtomicReference<Throwable> ex;

        private CountDownLatch latch;

        public TestSubscriber(AtomicReference<Throwable> ex, CountDownLatch latch) {
            this.ex = ex;
            this.latch = latch;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(BinaryRow item) {
        }

        @Override
        public void onError(Throwable throwable) {
            ex.set(throwable);

            latch.countDown();
        }

        @Override
        public void onComplete() {
            latch.countDown();
        }
    }

    private static Row createKeyValueRow(long id, long value) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(id);
        rowBuilder.appendLong(value);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }
}
