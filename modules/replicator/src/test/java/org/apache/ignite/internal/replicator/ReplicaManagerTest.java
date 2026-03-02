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

package org.apache.ignite.internal.replicator;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.replicator.LocalReplicaEvent.AFTER_REPLICA_STARTED;
import static org.apache.ignite.internal.replicator.LocalReplicaEvent.BEFORE_REPLICA_STOPPED;
import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.emptySetCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_ABSENT_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupEventsListener;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageManagerCreator;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ErrorReplicaResponse;
import org.apache.ignite.internal.replicator.message.ReplicaMessageGroup;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.ExecutorChooser;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.option.PermissiveSafeTimeValidator;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link ReplicaManager}.
 */
@ExtendWith(MockitoExtension.class)
public class ReplicaManagerTest extends BaseIgniteAbstractTest {
    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private ExecutorService requestsExecutor;

    private ReplicaManager replicaManager;

    @Mock
    private Loza raftManager;

    private final AtomicReference<NetworkMessageHandler> msgHandlerRef = new AtomicReference<>();

    private final Map<Long, NetworkMessage> messagingResponses = new HashMap<>();

    private MessagingService messagingService = new TestMessagingService();

    @BeforeEach
    void startReplicaManager(
            TestInfo testInfo,
            @Mock ClusterService clusterService,
            @Mock ClusterManagementGroupManager cmgManager,
            @Mock PlacementDriver placementDriver,
            @Mock TopologyService topologyService,
            @Mock Marshaller marshaller,
            @Mock TopologyAwareRaftGroupServiceFactory raftGroupServiceFactory,
            @Mock VolatileLogStorageManagerCreator volatileLogStorageManagerCreator
    ) {
        messagingResponses.clear();

        String nodeName = testNodeName(testInfo, 0);

        when(clusterService.messagingService()).thenReturn(messagingService);
        when(clusterService.topologyService()).thenReturn(topologyService);

        when(topologyService.localMember()).thenReturn(new ClusterNodeImpl(randomUUID(), nodeName, new NetworkAddress("foo", 0)));

        when(cmgManager.metaStorageNodes()).thenReturn(emptySetCompletedFuture());

        var clock = new HybridClockImpl();

        requestsExecutor = Executors.newFixedThreadPool(
                5,
                IgniteThreadFactory.create(nodeName, "partition-operations", log)
        );

        RaftGroupOptionsConfigurer partitionsConfigurer = mock(RaftGroupOptionsConfigurer.class);

        replicaManager = new ReplicaManager(
                nodeName,
                clusterService,
                cmgManager,
                groupId -> completedFuture(Assignments.EMPTY),
                new TestClockService(clock),
                Set.of(),
                placementDriver,
                requestsExecutor,
                () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                new NoOpFailureManager(),
                marshaller,
                new PermissiveSafeTimeValidator(),
                raftGroupServiceFactory,
                raftManager,
                partitionsConfigurer,
                volatileLogStorageManagerCreator,
                Executors.newSingleThreadScheduledExecutor(),
                replicaGrpId -> nullCompletedFuture(),
                ForkJoinPool.commonPool()
        );

        assertThat(replicaManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void stopReplicaManager() {
        CompletableFuture<?>[] replicaStopFutures = replicaManager.startedGroups().stream()
                .map(id -> {
                    try {
                        return replicaManager.stopReplica(id);
                    } catch (NodeStoppingException e) {
                        throw new AssertionError(e);
                    }
                })
                .toArray(CompletableFuture[]::new);

        assertThat(allOf(replicaStopFutures), willCompleteSuccessfully());

        assertThat(replicaManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        IgniteUtils.shutdownAndAwaitTermination(requestsExecutor, 10, TimeUnit.SECONDS);
    }

    /**
     * Tests that Replica Manager produces events when a Replica is started or stopped.
     */
    @Test
    void testReplicaEvents(
            TestInfo testInfo,
            @Mock EventListener<LocalReplicaEventParameters> createReplicaListener,
            @Mock EventListener<LocalReplicaEventParameters> removeReplicaListener,
            @Mock RaftGroupEventsListener raftGroupEventsListener,
            @Mock RaftGroupListener raftGroupListener,
            @Mock ReplicaListener replicaListener,
            @Mock TopologyAwareRaftGroupService raftGroupService
    ) throws NodeStoppingException {
        when(raftGroupService.unsubscribeLeader()).thenReturn(nullCompletedFuture());

        when(raftManager.startRaftGroupNode(
                any(RaftNodeId.class),
                any(PeersAndLearners.class),
                any(RaftGroupListener.class),
                any(RaftGroupEventsListener.class),
                any(RaftGroupOptions.class),
                any(TopologyAwareRaftGroupServiceFactory.class))
        )
                .thenReturn(raftGroupService);

        when(createReplicaListener.notify(any())).thenReturn(falseCompletedFuture());
        when(removeReplicaListener.notify(any())).thenReturn(falseCompletedFuture());

        replicaManager.listen(AFTER_REPLICA_STARTED, createReplicaListener);
        replicaManager.listen(BEFORE_REPLICA_STOPPED, removeReplicaListener);

        var groupId = new TablePartitionId(0, 0);
        when(replicaListener.raftClient()).thenReturn(raftGroupService);

        String nodeName = testNodeName(testInfo, 0);
        PeersAndLearners newConfiguration = PeersAndLearners.fromConsistentIds(Set.of(nodeName));

        CompletableFuture<Replica> startReplicaFuture = replicaManager.startReplica(
                raftGroupEventsListener,
                raftGroupListener,
                false,
                null,
                (unused) -> replicaListener,
                new PendingComparableValuesTracker<>(0L),
                groupId,
                newConfiguration
        );

        assertThat(startReplicaFuture, willCompleteSuccessfully());

        var expectedCreateParams = new LocalReplicaEventParameters(groupId);

        verify(createReplicaListener).notify(eq(expectedCreateParams));
        verify(removeReplicaListener, never()).notify(any());

        CompletableFuture<Boolean> stopReplicaFuture = replicaManager.stopReplica(groupId);

        assertThat(stopReplicaFuture, willBe(true));

        verify(createReplicaListener).notify(eq(expectedCreateParams));
        verify(removeReplicaListener).notify(eq(expectedCreateParams));
    }

    @Test
    public void testReplicaAbsence() {
        ReplicaSafeTimeSyncRequest replicaRequest = REPLICA_MESSAGES_FACTORY.replicaSafeTimeSyncRequest()
                .groupId(toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, new ZonePartitionId(-1, -1)))
                .build();

        long correlationId = 1L;

        msgHandlerRef.get().onReceived(replicaRequest, mock(InternalClusterNode.class), correlationId);

        Awaitility.await()
                .timeout(5, TimeUnit.SECONDS)
                .until(() -> messagingResponses.get(correlationId) != null);

        NetworkMessage resp = messagingResponses.get(correlationId);

        assertNotNull(resp);
        assertInstanceOf(ErrorReplicaResponse.class, resp);

        ErrorReplicaResponse errorResp = (ErrorReplicaResponse) resp;

        assertInstanceOf(ReplicationException.class, errorResp.throwable());

        ReplicationException e = (ReplicationException) errorResp.throwable();
        assertEquals(REPLICA_ABSENT_ERR, e.code());
    }

    private class TestMessagingService implements MessagingService {
        @Override
        public void weakSend(InternalClusterNode recipient, NetworkMessage msg) {
            // No-op.
        }

        @Override
        public CompletableFuture<Void> send(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg) {
            // No-op.
            return null;
        }

        @Override
        public CompletableFuture<Void> send(String recipientConsistentId, ChannelType channelType, NetworkMessage msg) {
            // No-op.
            return null;
        }

        @Override
        public CompletableFuture<Void> send(NetworkAddress recipientNetworkAddress, ChannelType channelType, NetworkMessage msg) {
            // No-op.
            return null;
        }

        @Override
        public CompletableFuture<Void> respond(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg,
                long correlationId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> respond(String recipientConsistentId, ChannelType channelType, NetworkMessage msg,
                long correlationId) {
            messagingResponses.put(correlationId, msg);
            return completedFuture(null);
        }

        @Override
        public CompletableFuture<NetworkMessage> invoke(InternalClusterNode recipient, ChannelType channelType, NetworkMessage msg,
                long timeout) {
            // No-op.
            return null;
        }

        @Override
        public CompletableFuture<NetworkMessage> invoke(String recipientConsistentId, ChannelType channelType, NetworkMessage msg,
                long timeout) {
            // No-op.
            return null;
        }

        @Override
        public void addMessageHandler(Class<?> messageGroup, NetworkMessageHandler handler) {
            if (messageGroup.equals(ReplicaMessageGroup.class)) {
                msgHandlerRef.set(handler);
            }
        }

        @Override
        public void addMessageHandler(Class<?> messageGroup, ExecutorChooser<NetworkMessage> executorChooser,
                NetworkMessageHandler handler) {
            addMessageHandler(messageGroup, handler);
        }
    }
}
