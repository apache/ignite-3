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

package org.apache.ignite.internal.raft;

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.raft.TestThrottlingContextHolder.throttlingContextHolder;
import static org.apache.ignite.internal.raft.util.OptimizedMarshaller.NO_POOL;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.deriveUuidFrom;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.raft.TestWriteCommand.testWriteCommand;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.ConnectException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.client.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.TestWriteCommand;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemoveLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ResetLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.TransferLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.ReadActionRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexRequest;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test methods of raft group service.
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RaftGroupServiceTest extends BaseIgniteAbstractTest {
    private static final List<Peer> NODES = Stream.of(20000, 20001, 20002)
            .map(port -> new Peer("localhost-" + port))
            .collect(toUnmodifiableList());

    private static final List<Peer> NODES_FOR_LEARNERS = Stream.of(20003, 20004, 20005)
            .map(port -> new Peer("localhost-" + port))
            .collect(toUnmodifiableList());

    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    private volatile @Nullable Peer leader = NODES.get(0);

    /** Call timeout. */
    private static final long TIMEOUT = 1000;

    /** Current term. */
    private static final long CURRENT_TERM = 1;

    /** Test group id. */
    private static final TestReplicationGroupId TEST_GRP = new TestReplicationGroupId("test");

    @InjectConfiguration("mock.retryTimeoutMillis=" + TIMEOUT)
    private RaftConfiguration raftConfiguration;

    /** Mock cluster. */
    @Mock
    private ClusterService cluster;

    /** Mock messaging service. */
    @Mock
    private MessagingService messagingService;

    /** Mock topology service. */
    @Mock
    private TopologyService topologyService;

    /** Executor for raft group services. */
    private ScheduledExecutorService executor;

    @BeforeEach
    void before() {
        when(cluster.messagingService()).thenReturn(messagingService);
        when(cluster.topologyService()).thenReturn(topologyService);

        MessageSerializationRegistry serializationRegistry = ClusterServiceTestUtils.defaultSerializationRegistry();
        when(cluster.serializationRegistry()).thenReturn(serializationRegistry);

        when(topologyService.getByConsistentId(any()))
                .thenAnswer(invocation -> {
                    String consistentId = invocation.getArgument(0);

                    return new ClusterNodeImpl(deriveUuidFrom(consistentId), consistentId, new NetworkAddress("localhost", 123));
                });

        executor = new ScheduledThreadPoolExecutor(20, IgniteThreadFactory.create("common", Loza.CLIENT_POOL_NAME, logger()));
    }

    /**
     * Shutdown executor for raft group services.
     */
    @AfterEach
    void after() {
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    @Test
    public void testRefreshLeaderStable() {
        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES);

        assertNull(service.leader());

        assertThat(service.refreshLeader(), willCompleteSuccessfully());

        assertEquals(leader, service.leader());
    }

    @Test
    public void testRefreshLeaderNotElected() {
        mockLeaderRequest(false);

        // Simulate running elections.
        leader = null;

        RaftGroupService service = startRaftGroupService(NODES);

        assertNull(service.leader());

        assertThat(service.refreshLeader(), willThrow(TimeoutException.class));
    }

    @Test
    public void testRefreshLeaderElectedAfterDelay() {
        mockLeaderRequest(false);

        // Simulate running elections.
        leader = null;

        executor.schedule((Runnable) () -> leader = NODES.get(0), 500, TimeUnit.MILLISECONDS);

        RaftGroupService service = startRaftGroupService(NODES);

        assertNull(service.leader());

        assertThat(service.refreshLeader(), willCompleteSuccessfully());

        assertEquals(NODES.get(0), service.leader());
    }

    @Test
    public void testRefreshLeaderWithTimeout() {
        mockLeaderRequest(true);

        RaftGroupService service = startRaftGroupService(NODES);

        assertThat(service.refreshLeader(), willThrow(TimeoutException.class, 500, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testUserRequestLeaderElected() {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service = startRaftGroupService(NODES);

        assertThat(service.refreshLeader(), willCompleteSuccessfully());

        assertThat(service.run(testWriteCommand()), willBe(instanceOf(TestResponse.class)));
    }

    @Test
    public void testUserRequestLazyInitLeader() {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service = startRaftGroupService(NODES);

        assertNull(service.leader());

        assertThat(service.run(testWriteCommand()), willBe(instanceOf(TestResponse.class)));

        assertEquals(leader, service.leader());
    }

    @Test
    public void testUserRequestWithTimeout() {
        mockLeaderRequest(false);
        mockUserInput(true, null);

        RaftGroupService service = startRaftGroupService(NODES);

        assertThat(service.run(testWriteCommand()), willThrow(TimeoutException.class, 500, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testUserRequestLeaderNotElected() {
        mockUserInput(false, null);

        RaftGroupService service = startRaftGroupServiceWithRefreshLeader(NODES);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        assertThat(service.run(testWriteCommand()), willThrow(TimeoutException.class));
    }

    @Test
    public void testUserRequestLeaderElectedAfterDelay() {
        mockUserInput(false, null);

        RaftGroupService service = startRaftGroupServiceWithRefreshLeader(NODES);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        executor.schedule((Runnable) () -> this.leader = NODES.get(0), 500, TimeUnit.MILLISECONDS);

        assertThat(service.run(testWriteCommand()), willBe(instanceOf(TestResponse.class)));

        assertEquals(NODES.get(0), service.leader());
    }

    @Test
    public void testUserRequestLeaderElectedAfterDelayWithFailedNode() {
        mockUserInput(false, NODES.get(0));

        CompletableFuture<Void> confUpdateFuture = raftConfiguration.retryTimeoutMillis().update(TIMEOUT * 3);

        assertThat(confUpdateFuture, willCompleteSuccessfully());

        RaftGroupService service = startRaftGroupServiceWithRefreshLeader(NODES);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        executor.schedule(
                () -> {
                    logger().info("Set leader {}", NODES.get(1));

                    this.leader = NODES.get(1);
                },
                500, TimeUnit.MILLISECONDS
        );

        assertThat(service.run(testWriteCommand()), willBe(instanceOf(TestResponse.class)));

        assertEquals(NODES.get(1), service.leader());
    }

    @Test
    public void testUserRequestLeaderChanged() {
        mockUserInput(false, null);

        RaftGroupService service = startRaftGroupServiceWithRefreshLeader(NODES);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        Peer newLeader = NODES.get(1);

        this.leader = newLeader;

        assertEquals(leader, service.leader());
        assertNotEquals(leader, newLeader);

        // Runs the command on an old leader. It should respond with leader changed error, when transparently retry.
        assertThat(service.run(testWriteCommand()), willBe(instanceOf(TestResponse.class)));

        assertEquals(newLeader, service.leader());
    }

    @Test
    public void testSnapshotExecutionException() {
        mockSnapshotRequest(false);

        RaftGroupService service = startRaftGroupService(NODES);

        assertThat(service.snapshot(new Peer("localhost-8082"), false), willThrow(IgniteInternalException.class));
    }

    @Test
    public void testSnapshotRetriedOnException() {
        mockSnapshotRequest(true);

        RaftGroupService service = startRaftGroupService(NODES);

        assertThat(
                service.snapshot(new Peer("localhost-8082"), false),
                willTimeoutFast()
        );
    }

    @Test
    public void testRefreshMembers() {
        List<String> respPeers = peersToIds(NODES.subList(0, 2));
        List<String> respLearners = peersToIds(NODES.subList(2, 2));

        when(messagingService.invoke(any(InternalClusterNode.class), any(GetPeersRequest.class), anyLong()))
                .then(invocation -> completedFuture(FACTORY.getPeersResponse().peersList(respPeers).learnersList(respLearners).build()));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES);

        assertThat(service.peers(), containsInAnyOrder(NODES.toArray()));
        assertThat(service.learners(), is(empty()));

        assertThat(service.refreshMembers(false), willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 2).toArray()));
        assertThat(service.learners(), containsInAnyOrder(NODES.subList(2, 2).toArray()));
    }

    @Test
    public void testAddPeer() {
        List<String> respPeers = peersToIds(NODES);

        when(messagingService.invoke(any(InternalClusterNode.class), any(AddPeerRequest.class), anyLong()))
                .then(invocation -> completedFuture(FACTORY.addPeerResponse().newPeersList(respPeers).build()));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES.subList(0, 2));

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 2).toArray()));
        assertThat(service.learners(), is(empty()));

        assertThat(service.addPeer(NODES.get(2), Configuration.NO_SEQUENCE_TOKEN), willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.toArray()));
        assertThat(service.learners(), is(empty()));
    }

    @Test
    public void testRemovePeer() {
        List<String> respPeers = peersToIds(NODES.subList(0, 2));

        when(messagingService.invoke(any(InternalClusterNode.class), any(RemovePeerRequest.class), anyLong()))
                .then(invocation -> completedFuture(FACTORY.removePeerResponse().newPeersList(respPeers).build()));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES);

        assertThat(service.peers(), containsInAnyOrder(NODES.toArray()));
        assertThat(service.learners(), is(empty()));

        assertThat(service.removePeer(NODES.get(2), Configuration.NO_SEQUENCE_TOKEN), willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 2).toArray()));
        assertThat(service.learners(), is(empty()));
    }

    @Test
    public void testChangePeersAndLearners() throws Exception {
        List<String> shrunkPeers = peersToIds(NODES.subList(0, 1));

        List<String> extendedPeers = peersToIds(NODES);

        List<String> fullLearners = peersToIds(NODES_FOR_LEARNERS);

        when(messagingService.invoke(any(InternalClusterNode.class), any(ChangePeersAndLearnersRequest.class), anyLong()))
                .then(invocation -> completedFuture(FACTORY.changePeersAndLearnersResponse()
                        .newPeersList(shrunkPeers)
                        .newLearnersList(emptyList())
                        .build()
                ))
                .then(invocation -> completedFuture(FACTORY.changePeersAndLearnersResponse()
                        .newPeersList(extendedPeers)
                        .newLearnersList(emptyList())
                        .build()
                ))
                .then(invocation -> completedFuture(FACTORY.changePeersAndLearnersResponse()
                        .newPeersList(shrunkPeers)
                        .newLearnersList(fullLearners)
                        .build()
                ));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES.subList(0, 2));

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 2).toArray()));
        assertThat(service.learners(), is(empty()));

        CompletableFuture<LeaderWithTerm> leaderWithTermFuture = service.refreshAndGetLeaderWithTerm();
        assertThat(leaderWithTermFuture, willCompleteSuccessfully());
        LeaderWithTerm leaderWithTerm = leaderWithTermFuture.get();

        // Peers[0, 1], Learners [empty]
        PeersAndLearners configuration = PeersAndLearners.fromPeers(NODES.subList(0, 1), emptyList());
        assertThat(service.changePeersAndLearners(configuration, leaderWithTerm.term(), 5L),
                willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 1).toArray()));
        assertThat(service.learners(), is(empty()));

        // Peers[0, 1, 2], Learners [empty]
        PeersAndLearners configuration2 = PeersAndLearners.fromPeers(NODES, emptyList());
        assertThat(service.changePeersAndLearners(configuration2, leaderWithTerm.term(), 6L),
                willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.toArray()));
        assertThat(service.learners(), is(empty()));

        // Peers[0, 1], Learners [3, 4, 5]
        PeersAndLearners configuration3 = PeersAndLearners.fromPeers(NODES.subList(0, 1), NODES_FOR_LEARNERS);
        assertThat(service.changePeersAndLearners(configuration3, leaderWithTerm.term(), 7L),
                willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 1).toArray()));
        assertThat(service.learners(), containsInAnyOrder(NODES_FOR_LEARNERS.toArray()));
    }

    @Test
    public void testTransferLeadership() {
        when(messagingService.invoke(any(InternalClusterNode.class), any(TransferLeaderRequest.class), anyLong()))
                .then(invocation -> completedFuture(RaftRpcFactory.DEFAULT.newResponse(FACTORY, Status.OK())));

        RaftGroupService service = startRaftGroupServiceWithRefreshLeader(NODES);

        assertEquals(NODES.get(0), service.leader());

        assertThat(service.transferLeadership(NODES.get(1)), willCompleteSuccessfully());

        assertEquals(NODES.get(1), service.leader());
    }

    @Test
    public void testAddLearners() {
        List<String> addLearners = peersToIds(NODES.subList(1, 3));

        when(messagingService.invoke(any(InternalClusterNode.class), any(AddLearnersRequest.class), anyLong()))
                .then(invocation -> completedFuture(FACTORY.learnersOpResponse().newLearnersList(addLearners).build()));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES.subList(0, 1));

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 1).toArray()));
        assertThat(service.learners(), is(empty()));

        assertThat(service.addLearners(NODES.subList(1, 3), Configuration.NO_SEQUENCE_TOKEN), willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 1).toArray()));
        assertThat(service.learners(), containsInAnyOrder(NODES.subList(1, 3).toArray()));
    }

    @Test
    public void testResetLearners() {
        List<String> addLearners = peersToIds(NODES.subList(1, 3));

        List<String> resetLearners = peersToIds(NODES.subList(2, 3));

        when(messagingService.invoke(any(InternalClusterNode.class), any(ResetLearnersRequest.class), anyLong()))
                .then(invocation -> completedFuture(FACTORY.learnersOpResponse().newLearnersList(resetLearners).build()));

        mockAddLearners(addLearners);

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES.subList(0, 1));

        assertThat(service.addLearners(NODES.subList(1, 3), Configuration.NO_SEQUENCE_TOKEN), willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 1).toArray()));
        assertThat(service.learners(), containsInAnyOrder(NODES.subList(1, 3).toArray()));

        assertThat(service.resetLearners(NODES.subList(2, 3), Configuration.NO_SEQUENCE_TOKEN), willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 1).toArray()));
        assertThat(service.learners(), containsInAnyOrder(NODES.subList(2, 3).toArray()));
    }

    @Test
    public void testRemoveLearners() {
        List<String> addLearners = peersToIds(NODES.subList(1, 3));

        List<String> resultLearners = peersToIds(NODES.subList(1, 2));

        when(messagingService.invoke(any(InternalClusterNode.class), any(RemoveLearnersRequest.class), anyLong()))
                .then(invocation -> completedFuture(FACTORY.learnersOpResponse().newLearnersList(resultLearners).build()));

        mockAddLearners(addLearners);

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES.subList(0, 1));

        assertThat(service.addLearners(NODES.subList(1, 3), Configuration.NO_SEQUENCE_TOKEN), willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 1).toArray()));
        assertThat(service.learners(), containsInAnyOrder(NODES.subList(1, 3).toArray()));

        assertThat(service.removeLearners(NODES.subList(2, 3), Configuration.NO_SEQUENCE_TOKEN), willCompleteSuccessfully());

        assertThat(service.peers(), containsInAnyOrder(NODES.subList(0, 1).toArray()));
        assertThat(service.learners(), containsInAnyOrder(NODES.subList(1, 2).toArray()));
    }

    @Test
    public void testGetLeaderRequest() {
        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES);

        assertNull(service.leader());

        assertThat(service.refreshLeader(), willCompleteSuccessfully());

        GetLeaderRequest req = FACTORY.getLeaderRequest().groupId(TEST_GRP.toString()).build();

        CompletableFuture<GetLeaderResponse> fut = messagingService.invoke(new ClusterNodeImpl(null, null, null), req, TIMEOUT)
                        .thenApply(GetLeaderResponse.class::cast);

        assertThat(fut.thenApply(GetLeaderResponse::leaderId), willBe(equalTo(PeerId.fromPeer(leader).toString())));
        assertThat(fut.thenApply(GetLeaderResponse::currentTerm), willBe(equalTo(CURRENT_TERM)));
    }

    @Test
    public void testReadIndex() {
        RaftGroupService service = startRaftGroupService(NODES);
        mockReadIndex(false);

        CompletableFuture<Long> fut = service.readIndex();

        assertThat(fut, willSucceedFast());

        assertEquals(1L, fut.join());
    }

    @Test
    public void testReadIndexWithMessageSendTimeout() {
        RaftGroupService service = startRaftGroupService(NODES);
        mockReadIndex(true);

        CompletableFuture<Long> fut = service.readIndex();

        assertThat(fut, willThrowFast(TimeoutException.class));
    }

    @ParameterizedTest
    @EnumSource(names = {"ESHUTDOWN", "EHOSTDOWN", "ENODESHUTDOWN"})
    public void testRetryOnErrorWithUnavailablePeers(RaftError error) {
        when(messagingService.invoke(any(InternalClusterNode.class), any(ReadActionRequest.class), anyLong()))
                .thenReturn(completedFuture(FACTORY.errorResponse()
                        .errorCode(error.getNumber())
                        .build()));

        RaftGroupService service = startRaftGroupServiceWithRefreshLeader(NODES);

        CompletableFuture<Object> response = service.run(mock(ReadCommand.class));

        assertThat(response, willThrow(TimeoutException.class, "Send with retry timed out"));

        // Verify that we tried to send the message to every node.
        NODES.forEach(node -> verify(messagingService, atLeastOnce()).invoke(
                argThat((InternalClusterNode target) -> target != null && target.name().equals(node.consistentId())),
                any(ReadActionRequest.class),
                anyLong()
        ));
    }

    @ParameterizedTest
    @EnumSource(names = {"UNKNOWN", "EINTERNAL", "ENOENT"})
    public void testRetryOnErrorWithTimeout(RaftError error) {
        when(messagingService.invoke(any(InternalClusterNode.class), any(ReadActionRequest.class), anyLong()))
                .thenReturn(completedFuture(FACTORY.errorResponse()
                        .errorCode(error.getNumber())
                        .build()));

        RaftGroupService service = startRaftGroupServiceWithRefreshLeader(NODES);

        CompletableFuture<Object> response = service.run(mock(ReadCommand.class));

        assertThat(response, willThrow(TimeoutException.class, "Send with retry timed out"));
    }

    @ParameterizedTest
    @EnumSource(names = {"EPERM"})
    public void testRetryOnErrorWithUpdateLeader(RaftError error) {
        when(messagingService.invoke(
                argThat((InternalClusterNode node) -> node != null && node.name().equals(NODES.get(0).consistentId())),
                any(ReadActionRequest.class),
                anyLong())
        )
                .thenReturn(completedFuture(FACTORY.errorResponse()
                        .errorCode(error.getNumber())
                        .leaderId(NODES.get(NODES.size() - 1).consistentId())
                        .build()));

        when(messagingService.invoke(
                argThat((InternalClusterNode node) -> node != null && node.name().equals(NODES.get(NODES.size() - 1).consistentId())),
                any(ReadActionRequest.class),
                anyLong())
        )
                .thenReturn(completedFuture(FACTORY.actionResponse().result(null).build()));

        RaftGroupService service = startRaftGroupServiceWithRefreshLeader(NODES);

        assertThat(service.leader(), is(NODES.get(0)));

        CompletableFuture<Object> response = service.run(mock(ReadCommand.class));

        assertThat(response, willBe(nullValue()));

        // Check that the leader was updated as well.
        assertThat(service.leader(), is(NODES.get(NODES.size() - 1)));
    }

    @Test
    public void testRetryOnRecipientLeftException() {
        when(messagingService.invoke(any(InternalClusterNode.class), any(ReadActionRequest.class), anyLong()))
                .thenReturn(failedFuture(new RecipientLeftException()));

        RaftGroupService service = startRaftGroupServiceWithRefreshLeader(NODES);

        CompletableFuture<Object> response = service.run(mock(ReadCommand.class));

        assertThat(response, willThrow(TimeoutException.class, "Send with retry timed out"));
    }

    private RaftGroupService startRaftGroupService(List<Peer> peers) {
        PeersAndLearners memberConfiguration = PeersAndLearners.fromPeers(peers, Set.of());

        var commandsSerializer = new ThreadLocalOptimizedMarshaller(cluster.serializationRegistry());

        return RaftGroupServiceImpl.start(
                TEST_GRP, cluster,
                FACTORY,
                raftConfiguration,
                memberConfiguration,
                executor,
                commandsSerializer,
                throttlingContextHolder()
        );
    }

    private RaftGroupService startRaftGroupServiceWithRefreshLeader(List<Peer> peers) {
        RaftGroupService service = startRaftGroupService(peers);

        mockLeaderRequest(false);
        service.refreshLeader().join();

        return service;
    }

    /**
     * Mock read index request.
     */
    private void mockReadIndex(boolean timeout) {
        when(messagingService.invoke(any(InternalClusterNode.class), any(ReadIndexRequest.class), anyLong()))
                .then(invocation -> timeout
                        ? failedFuture(new TimeoutException())
                        : completedFuture(FACTORY.readIndexResponse().index(1L).build())
                );
    }

    /**
     * Mocks sending {@link ActionRequest}s.
     *
     * @param delay {@code True} to create a delay before response.
     * @param peer Fail the request targeted to given peer.
     */
    private void mockUserInput(boolean delay, @Nullable Peer peer) {
        //noinspection Convert2Lambda
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                // Must be an anonymous class, to deduce the message type from the generic superclass.
                argThat(new ArgumentMatcher<WriteActionRequest>() {
                    @Override
                    public boolean matches(WriteActionRequest arg) {
                        Object command = new OptimizedMarshaller(cluster.serializationRegistry(), NO_POOL).unmarshall(arg.command());

                        return command instanceof TestWriteCommand;
                    }
                }),
                anyLong()
        )).then(invocation -> {
            InternalClusterNode target = invocation.getArgument(0);

            if (peer != null && target.name().equals(peer.consistentId())) {
                return failedFuture(new ConnectException());
            }

            if (delay) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        fail();
                    }

                    return FACTORY.actionResponse().result(new TestResponse()).build();
                });
            }

            Object resp;

            if (leader == null) {
                resp = FACTORY.errorResponse().errorCode(RaftError.EPERM.getNumber()).build();
            } else if (!target.name().equals(leader.consistentId())) {
                resp = FACTORY.errorResponse()
                        .errorCode(RaftError.EPERM.getNumber()).leaderId(PeerId.fromPeer(leader).toString()).build();
            } else {
                resp = FACTORY.actionResponse().result(new TestResponse()).build();
            }

            return completedFuture(resp);
        });
    }

    /**
     * Mocks sending {@link GetLeaderRequest}s.
     *
     * @param delay {@code True} to delay response.
     */
    private void mockLeaderRequest(boolean delay) {
        when(messagingService.invoke(any(InternalClusterNode.class), any(GetLeaderRequest.class), anyLong()))
                .then(invocation -> {
                    if (delay) {
                        return CompletableFuture.supplyAsync(() -> {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                fail();
                            }

                            return FACTORY.errorResponse().errorCode(RaftError.EPERM.getNumber()).build();
                        });
                    }

                    PeerId leader0 = PeerId.fromPeer(leader);

                    Object resp = leader0 == null
                            ? FACTORY.errorResponse().errorCode(RaftError.EPERM.getNumber()).build()
                            : FACTORY.getLeaderResponse().leaderId(leader0.toString()).currentTerm(CURRENT_TERM).build();

                    return completedFuture(resp);
                });
    }

    private void mockSnapshotRequest(boolean returnResponseWithError) {
        when(messagingService.invoke(any(InternalClusterNode.class), any(CliRequests.SnapshotRequest.class), anyLong()))
                .then(invocation -> {
                    if (returnResponseWithError) {
                        ErrorResponse response = FACTORY.errorResponse()
                                .errorCode(RaftError.UNKNOWN.getNumber())
                                .errorMsg("Failed to create a snapshot")
                                .build();

                        return completedFuture(response);
                    } else {
                        return failedFuture(new IgniteInternalException("Very bad"));
                    }
                });
    }

    private void mockAddLearners(List<String> resultLearners) {
        when(messagingService.invoke(any(InternalClusterNode.class), any(AddLearnersRequest.class), anyLong()))
                .then(invocation -> completedFuture(FACTORY.learnersOpResponse().newLearnersList(resultLearners).build()));
    }

    /**
     * Convert list of {@link Peer} to list of string representations.
     *
     * @param peers List of {@link Peer}
     * @return List of string representations.
     */
    private static List<String> peersToIds(List<Peer> peers) {
        return peers.stream().map(p -> PeerId.fromPeer(p).toString()).collect(Collectors.toList());
    }

    private static class TestResponse {
    }
}
