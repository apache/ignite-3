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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.distributed.ItTxTestCluster.NODE_PORT_BASE;
import static org.apache.ignite.internal.partition.replicator.network.replication.RequestType.RW_GET;
import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.table.TxAbstractTest.startNode;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_TIMEOUT_ERR;
import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.raft.storage.impl.LocalLogStorageFactory;
import org.apache.ignite.internal.replicator.Replica;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.exception.ReplicaStoppingException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaResponse;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.test.TestTransactionIds;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests handling requests from {@link ReplicaService} to {@link ReplicaManager} when the {@link Replica} is not started.
 */
@ExtendWith(ConfigurationExtension.class)
public class ReplicaUnavailableTest extends IgniteAbstractTest {
    private static final String NODE_NAME = "client";

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT64, false)},
            new Column[]{new Column("value", NativeTypes.INT64, false)}
    );

    @InjectConfiguration("mock.rpcTimeout= 3000")
    private ReplicationConfiguration replicationConfiguration;

    private final PartitionReplicationMessagesFactory tableMessagesFactory = new PartitionReplicationMessagesFactory();

    private final ReplicaMessagesFactory replicaMessageFactory = new ReplicaMessagesFactory();

    private final HybridClock clock = new HybridClockImpl();

    private final TestInfo testInfo;

    private ReplicaService replicaService;

    private ReplicaManager replicaManager;

    private ClusterService clusterService;

    private ExecutorService requestsExecutor;

    private Loza raftManager;

    private TopologyAwareRaftGroupService raftClient;

    private final Function<BiFunction<ReplicaRequest, String, CompletableFuture<ReplicaResult>>, ReplicaListener> replicaListenerCreator =
            (invokeImpl) -> new ReplicaListener() {
                @Override
                public CompletableFuture<ReplicaResult> invoke(ReplicaRequest request, String senderId) {
                    return invokeImpl.apply(request, senderId);
                }

                @Override
                public RaftCommandRunner raftClient() {
                    return raftClient;
                }
            };

    @BeforeEach
    public void setup() throws NodeStoppingException {
        var networkAddress = new NetworkAddress(getLocalAddress(), NODE_PORT_BASE + 1);

        var nodeFinder = new StaticNodeFinder(List.of(networkAddress));

        clusterService = startNode(testInfo, NODE_NAME, NODE_PORT_BASE + 1, nodeFinder);

        var cmgManager = mock(ClusterManagementGroupManager.class);

        // This test is run without Meta storage.
        when(cmgManager.metaStorageNodes()).thenReturn(emptySetCompletedFuture());

        raftManager = mock(Loza.class);
        raftClient = mock(TopologyAwareRaftGroupService.class);
        when(raftManager.startRaftGroupService(any(), any(), any(), any())).thenReturn(completedFuture(raftClient));

        requestsExecutor = new ThreadPoolExecutor(
                0, 5,
                0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(NODE_NAME, "partition-operations", log)
        );

        replicaService = new ReplicaService(
                clusterService.messagingService(),
                clock,
                replicationConfiguration
        );

        replicaManager = new ReplicaManager(
                NODE_NAME,
                clusterService,
                cmgManager,
                new TestClockService(clock),
                Set.of(PartitionReplicationMessageGroup.class, TxMessageGroup.class),
                new TestPlacementDriver(clusterService.topologyService().localMember()),
                requestsExecutor,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                new NoOpFailureProcessor(),
                mock(ThreadLocalPartitionCommandsMarshaller.class),
                mock(TopologyAwareRaftGroupServiceFactory.class),
                raftManager,
                view -> new LocalLogStorageFactory(),
                ForkJoinPool.commonPool()
        );

        assertThat(replicaManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    public void teardown() {
        IgniteUtils.shutdownAndAwaitTermination(requestsExecutor, 10, TimeUnit.SECONDS);

        assertThat(clusterService.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    public ReplicaUnavailableTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @Test
    public void testWithReplicaStartedAfterRequestSending() throws Exception {
        ClusterNode clusterNode = clusterService.topologyService().localMember();

        TablePartitionId tablePartitionId = new TablePartitionId(1, 1);

        ReadWriteSingleRowReplicaRequest request = getRequest(tablePartitionId);

        PeersAndLearners newConfiguration = PeersAndLearners.fromConsistentIds(Set.of(clusterNode.name()));

        clusterService.messagingService().addMessageHandler(ReplicaMessageGroup.class,
                (message, sender, correlationId) -> {
                    try {
                        log.info("Replica msg " + message.getClass().getSimpleName());

                        ReplicaListener listener = replicaListenerCreator.apply((req, senderId) -> {
                            ReplicaResponse response = replicaMessageFactory.replicaResponse()
                                    .result(5)
                                    .build();
                            return completedFuture(new ReplicaResult(response, null));
                        });

                        replicaManager.startReplica(
                                tablePartitionId,
                                newConfiguration,
                                (unused) -> listener,
                                new PendingComparableValuesTracker<>(0L),
                                completedFuture(mock(TopologyAwareRaftGroupService.class))
                        );
                    } catch (NodeStoppingException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        CompletableFuture<ReplicaResponse> respFur = replicaService.invoke(clusterNode, request);

        assertThat(respFur, willSucceedIn(10, TimeUnit.SECONDS));

        assertEquals(5, respFur.get().result());
    }

    private ReadWriteSingleRowReplicaRequest getRequest(TablePartitionId tablePartitionId) {
        BinaryRow binaryRow = createKeyValueRow(1L, 1L);

        return tableMessagesFactory.readWriteSingleRowReplicaRequest()
                .groupId(toTablePartitionIdMessage(replicaMessageFactory, tablePartitionId))
                .transactionId(TestTransactionIds.newTransactionId())
                .commitPartitionId(tablePartitionId())
                .timestamp(clock.now())
                .schemaVersion(binaryRow.schemaVersion())
                .binaryTuple(binaryRow.tupleSlice())
                .requestTypeInt(RW_GET.ordinal())
                .enlistmentConsistencyToken(1L)
                .coordinatorId(clusterService.topologyService().localMember().id())
                .build();
    }

    @Test
    public void testStopReplicaException() {
        ClusterNode clusterNode = clusterService.topologyService().localMember();

        TablePartitionId tablePartitionId = new TablePartitionId(1, 1);

        ReadWriteSingleRowReplicaRequest request = getRequest(tablePartitionId);

        clusterService.messagingService().addMessageHandler(ReplicaMessageGroup.class, (message, sender, correlationId) -> {
            runAsync(() -> {
                try {
                    log.info("Replica msg " + message.getClass().getSimpleName());

                    // If we 'stop' a replica before its future even appears, invocation will not get ReplicaStoppingException
                    // as there was no sign of the replica yet.
                    assertTrue(
                            waitForCondition(() -> replicaManager.isReplicaTouched(tablePartitionId), TimeUnit.SECONDS.toMillis(10))
                    );

                    assertThat(replicaManager.stopReplica(tablePartitionId), willSucceedFast());
                } catch (NodeStoppingException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new RuntimeException(e);
                }
            });
        });

        CompletableFuture<ReplicaResponse> respFut = replicaService.invoke(clusterNode, request);

        assertThat(respFut, willThrow(instanceOf(ReplicaStoppingException.class)));
    }

    @Test
    public void testWithNotStartedReplica() {
        ClusterNode clusterNode = clusterService.topologyService().localMember();

        TablePartitionId tablePartitionId = new TablePartitionId(1, 1);

        ReadWriteSingleRowReplicaRequest request = getRequest(tablePartitionId);

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

        assertNotNull(e0);
        assertTrue(unwrapCause(e0) instanceof ReplicationException, e0.toString());

        assertNotNull(e1);
        assertTrue(unwrapCause(e1) instanceof ReplicationException, e1.toString());
    }

    @Test
    public void testWithNotReadyReplica() {
        ClusterNode clusterNode = clusterService.topologyService().localMember();

        TablePartitionId tablePartitionId = new TablePartitionId(1, 1);

        PeersAndLearners newConfiguration = PeersAndLearners.fromConsistentIds(Set.of(clusterNode.name()));

        clusterService.messagingService().addMessageHandler(ReplicaMessageGroup.class, (message, sender, correlationId) -> {
            runAsync(() -> {
                try {
                    log.info("Replica msg " + message.getClass().getSimpleName());

                    ReplicaListener listener = replicaListenerCreator.apply((r, id) -> new CompletableFuture<>());

                    replicaManager.startReplica(
                            tablePartitionId,
                            newConfiguration,
                            (unused) -> listener,
                            new PendingComparableValuesTracker<>(0L),
                            completedFuture(mock(TopologyAwareRaftGroupService.class))
                    );
                } catch (NodeStoppingException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        ReadWriteSingleRowReplicaRequest request = getRequest(tablePartitionId);

        Exception e0 = null;

        try {
            replicaService.invoke(clusterNode, request).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            e0 = e;
        }

        assertNotNull(e0);
        assertTrue(unwrapCause(e0) instanceof ReplicationTimeoutException, e0.toString());
        assertEquals(REPLICA_TIMEOUT_ERR, ((ReplicationTimeoutException) unwrapCause(e0)).code());
    }

    private static BinaryRow createKeyValueRow(long id, long value) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, -1);

        rowBuilder.appendLong(id);
        rowBuilder.appendLong(value);

        return rowBuilder.build();
    }

    private TablePartitionIdMessage tablePartitionId() {
        return replicaMessageFactory.tablePartitionIdMessage()
                .tableId(1)
                .partitionId(1)
                .build();
    }
}
