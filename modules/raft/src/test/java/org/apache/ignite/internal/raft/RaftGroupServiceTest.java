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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.jraft.test.TestUtils.peersToIds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.net.ConnectException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderResponse;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.impl.RaftException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test methods of raft group service.
 */
@ExtendWith(MockitoExtension.class)
public class RaftGroupServiceTest extends BaseIgniteAbstractTest {
    private static final List<Peer> NODES = Stream.of(20000, 20001, 20002)
            .map(port -> new Peer("localhost-" + port))
            .collect(toUnmodifiableList());

    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    private volatile @Nullable Peer leader = NODES.get(0);

    /** Call timeout. */
    private static final int TIMEOUT = 1000;

    /** Retry delay. */
    private static final int DELAY = 200;

    /** Current term. */
    private static final long CURRENT_TERM = 1;

    /** Test group id. */
    private static final TestReplicationGroupId TEST_GRP = new TestReplicationGroupId("test");

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

        when(topologyService.getByConsistentId(any()))
                .thenAnswer(invocation -> {
                    String consistentId = invocation.getArgument(0);

                    return new ClusterNode(consistentId, consistentId, new NetworkAddress("localhost", 123));
                });

        executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory(Loza.CLIENT_POOL_NAME, logger()));
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

        RaftGroupService service = startRaftGroupService(NODES, false);

        assertNull(service.leader());

        assertThat(service.refreshLeader(), willCompleteSuccessfully());

        assertEquals(leader, service.leader());
    }

    @Test
    public void testRefreshLeaderNotElected() {
        mockLeaderRequest(false);

        // Simulate running elections.
        leader = null;

        RaftGroupService service = startRaftGroupService(NODES, false);

        assertNull(service.leader());

        assertThat(service.refreshLeader(), willThrow(TimeoutException.class));
    }

    @Test
    public void testRefreshLeaderElectedAfterDelay() {
        mockLeaderRequest(false);

        // Simulate running elections.
        leader = null;

        executor.schedule((Runnable) () -> leader = NODES.get(0), 500, TimeUnit.MILLISECONDS);

        RaftGroupService service = startRaftGroupService(NODES, false);

        assertNull(service.leader());

        assertThat(service.refreshLeader(), willCompleteSuccessfully());

        assertEquals(NODES.get(0), service.leader());
    }

    @Test
    public void testRefreshLeaderWithTimeout() {
        mockLeaderRequest(true);

        RaftGroupService service = startRaftGroupService(NODES, false);

        assertThat(service.refreshLeader(), willThrow(TimeoutException.class, 500, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testUserRequestLeaderElected() {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service = startRaftGroupService(NODES, false);

        assertThat(service.refreshLeader(), willCompleteSuccessfully());

        assertThat(service.run(new TestCommand()), willBe(instanceOf(TestResponse.class)));
    }

    @Test
    public void testUserRequestLazyInitLeader() {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service = startRaftGroupService(NODES, false);

        assertNull(service.leader());

        assertThat(service.run(new TestCommand()), willBe(instanceOf(TestResponse.class)));

        assertEquals(leader, service.leader());
    }

    @Test
    public void testUserRequestWithTimeout() {
        mockLeaderRequest(false);
        mockUserInput(true, null);

        RaftGroupService service = startRaftGroupService(NODES, false);

        assertThat(service.run(new TestCommand()), willThrow(TimeoutException.class, 500, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testUserRequestLeaderNotElected() {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service = startRaftGroupService(NODES, true);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        assertThat(service.run(new TestCommand()), willThrow(TimeoutException.class));
    }

    @Test
    public void testUserRequestLeaderElectedAfterDelay() {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service = startRaftGroupService(NODES, true);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        executor.schedule((Runnable) () -> this.leader = NODES.get(0), 500, TimeUnit.MILLISECONDS);

        assertThat(service.run(new TestCommand()), willBe(instanceOf(TestResponse.class)));

        assertEquals(NODES.get(0), service.leader());
    }

    @Test
    public void testUserRequestLeaderElectedAfterDelayWithFailedNode() {
        mockLeaderRequest(false);
        mockUserInput(false, NODES.get(0));

        RaftGroupService service = startRaftGroupService(NODES, true, TIMEOUT * 3);

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

        assertThat(service.run(new TestCommand()), willBe(instanceOf(TestResponse.class)));

        assertEquals(NODES.get(1), service.leader());
    }

    @Test
    public void testUserRequestLeaderChanged() {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service = startRaftGroupService(NODES, true);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        Peer newLeader = NODES.get(1);

        this.leader = newLeader;

        assertEquals(leader, service.leader());
        assertNotEquals(leader, newLeader);

        // Runs the command on an old leader. It should respond with leader changed error, when transparently retry.
        assertThat(service.run(new TestCommand()), willBe(instanceOf(TestResponse.class)));

        assertEquals(newLeader, service.leader());
    }

    @Test
    public void testSnapshotExecutionException() {
        mockSnapshotRequest(1);

        RaftGroupService service = startRaftGroupService(NODES, false);

        assertThat(service.snapshot(new Peer("localhost-8082")), willThrow(IgniteInternalException.class));
    }

    @Test
    public void testSnapshotExecutionFailedResponse() {
        mockSnapshotRequest(0);

        RaftGroupService service = startRaftGroupService(NODES, false);

        assertThat(service.snapshot(new Peer("localhost-8082")), willThrow(RaftException.class));
    }

    @Test
    public void testRefreshMembers() {
        List<String> respPeers = peersToIds(NODES.subList(0, 2));
        List<String> respLearners = peersToIds(NODES.subList(2, 2));

        when(messagingService.invoke(any(),
                eq(FACTORY.getPeersRequest().onlyAlive(false).groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.getPeersResponse().peersList(respPeers).learnersList(respLearners).build()));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES, true);

        assertEquals(NODES, service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        assertThat(service.refreshMembers(false), willCompleteSuccessfully());

        assertEquals(NODES.subList(0, 2), service.peers());
        assertEquals(NODES.subList(2, 2), service.learners());
    }

    @Test
    public void testAddPeer() {
        List<String> respPeers = peersToIds(NODES);

        when(messagingService.invoke(any(),
                eq(FACTORY.addPeerRequest()
                        .peerId(PeerId.fromPeer(NODES.get(2)).toString())
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.addPeerResponse().newPeersList(respPeers).build()));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES.subList(0, 2), true);

        assertEquals(NODES.subList(0, 2), service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        assertThat(service.addPeer(NODES.get(2)), willCompleteSuccessfully());

        assertEquals(NODES, service.peers());
        assertEquals(Collections.emptyList(), service.learners());
    }

    @Test
    public void testRemovePeer() {
        List<String> respPeers = peersToIds(NODES.subList(0, 2));

        when(messagingService.invoke(any(),
                eq(FACTORY.removePeerRequest()
                        .peerId(PeerId.fromPeer(NODES.get(2)).toString())
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.removePeerResponse().newPeersList(respPeers).build()));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES, true);

        assertEquals(NODES, service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        assertThat(service.removePeer(NODES.get(2)), willCompleteSuccessfully());

        assertEquals(NODES.subList(0, 2), service.peers());
        assertEquals(Collections.emptyList(), service.learners());
    }

    @Test
    public void testChangePeers() {
        List<String> shrunkPeers = peersToIds(NODES.subList(0, 1));

        List<String> extendedPeers = peersToIds(NODES);

        when(messagingService.invoke(any(),
                eq(FACTORY.changePeersRequest()
                        .newPeersList(shrunkPeers)
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.changePeersResponse().newPeersList(shrunkPeers).build()));

        when(messagingService.invoke(any(),
                eq(FACTORY.changePeersRequest()
                        .newPeersList(extendedPeers)
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.changePeersResponse().newPeersList(extendedPeers).build()));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES.subList(0, 2), true);

        assertEquals(NODES.subList(0, 2), service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        assertThat(service.changePeers(NODES.subList(0, 1)), willCompleteSuccessfully());

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        assertThat(service.changePeers(NODES), willCompleteSuccessfully());

        assertEquals(NODES, service.peers());
        assertEquals(Collections.emptyList(), service.learners());
    }

    @Test
    public void testTransferLeadership() {
        when(messagingService.invoke(any(),
                eq(FACTORY.transferLeaderRequest()
                        .peerId(PeerId.fromPeer(NODES.get(1)).toString())
                        .leaderId(PeerId.fromPeer(NODES.get(0)).toString())
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(RaftRpcFactory.DEFAULT.newResponse(FACTORY, Status.OK())));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES, true);

        assertEquals(NODES.get(0), service.leader());

        assertThat(service.transferLeadership(NODES.get(1)), willCompleteSuccessfully());

        assertEquals(NODES.get(1), service.leader());
    }

    @Test
    public void testAddLearners() {
        List<String> addLearners = peersToIds(NODES.subList(1, 3));

        when(messagingService.invoke(any(),
                eq(FACTORY.addLearnersRequest()
                        .learnersList(addLearners)
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.learnersOpResponse().newLearnersList(addLearners).build()));

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES.subList(0, 1), true);

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        assertThat(service.addLearners(NODES.subList(1, 3)), willCompleteSuccessfully());

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(NODES.subList(1, 3), service.learners());
    }

    @Test
    public void testResetLearners() {
        List<String> addLearners = peersToIds(NODES.subList(1, 3));

        List<String> resetLearners = peersToIds(NODES.subList(2, 3));

        when(messagingService.invoke(any(),
                eq(FACTORY.resetLearnersRequest()
                        .learnersList(resetLearners)
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.learnersOpResponse().newLearnersList(resetLearners).build()));

        mockAddLearners(TEST_GRP.toString(), addLearners, addLearners);

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES.subList(0, 1), true);

        assertThat(service.addLearners(NODES.subList(1, 3)), willCompleteSuccessfully());

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(NODES.subList(1, 3), service.learners());

        assertThat(service.resetLearners(NODES.subList(2, 3)), willCompleteSuccessfully());

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(NODES.subList(2, 3), service.learners());
    }

    @Test
    public void testRemoveLearners() {
        List<String> addLearners = peersToIds(NODES.subList(1, 3));

        List<String> removeLearners = peersToIds(NODES.subList(2, 3));

        List<String> resultLearners = peersToIds(NODES.subList(1, 2));

        when(messagingService.invoke(any(),
                eq(FACTORY.removeLearnersRequest()
                        .learnersList(removeLearners)
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.learnersOpResponse().newLearnersList(resultLearners).build()));

        mockAddLearners(TEST_GRP.toString(), addLearners, addLearners);

        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES.subList(0, 1), true);

        assertThat(service.addLearners(NODES.subList(1, 3)), willCompleteSuccessfully());

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(NODES.subList(1, 3), service.learners());

        assertThat(service.removeLearners(NODES.subList(2, 3)), willCompleteSuccessfully());

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(NODES.subList(1, 2), service.learners());
    }

    @Test
    public void testGetLeaderRequest() {
        mockLeaderRequest(false);

        RaftGroupService service = startRaftGroupService(NODES, false);

        assertNull(service.leader());

        assertThat(service.refreshLeader(), willCompleteSuccessfully());

        GetLeaderRequest req = FACTORY.getLeaderRequest().groupId(TEST_GRP.toString()).build();

        CompletableFuture<GetLeaderResponse> fut = messagingService.invoke(new ClusterNode(null, null, null), req, TIMEOUT)
                        .thenApply(GetLeaderResponse.class::cast);

        assertThat(fut.thenApply(GetLeaderResponse::leaderId), willBe(equalTo(PeerId.fromPeer(leader).toString())));
        assertThat(fut.thenApply(GetLeaderResponse::currentTerm), willBe(equalTo(CURRENT_TERM)));
    }

    private RaftGroupService startRaftGroupService(List<Peer> peers, boolean getLeader) {
        CompletableFuture<RaftGroupService> service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, peers, getLeader, DELAY, executor);

        assertThat(service, willCompleteSuccessfully());

        return service.join();
    }

    private RaftGroupService startRaftGroupService(List<Peer> peers, boolean getLeader, int timeout) {
        CompletableFuture<RaftGroupService> service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, timeout, peers, getLeader, DELAY, executor);

        assertThat(service, willCompleteSuccessfully());

        return service.join();
    }

    /**
     * Mocks sending {@link ActionRequest}s.
     *
     * @param delay {@code True} to create a delay before response.
     * @param peer Fail the request targeted to given peer.
     */
    private void mockUserInput(boolean delay, @Nullable Peer peer) {
        when(messagingService.invoke(
                any(),
                argThat(new ArgumentMatcher<ActionRequest>() {
                    @Override
                    public boolean matches(ActionRequest arg) {
                        return arg.command() instanceof TestCommand;
                    }
                }),
                anyLong()
        )).then(invocation -> {
            ClusterNode target = invocation.getArgument(0);

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
        when(messagingService.invoke(any(), any(GetLeaderRequest.class), anyLong()))
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

    private void mockSnapshotRequest(int mode) {
        when(messagingService.invoke(any(), any(CliRequests.SnapshotRequest.class), anyLong()))
                .then(invocation -> {
                    if (mode == 0) {
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

    private void mockAddLearners(String groupId, List<String> addLearners, List<String> resultLearners) {
        when(messagingService.invoke(any(),
                eq(FACTORY.addLearnersRequest()
                        .learnersList(addLearners)
                        .groupId(groupId).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.learnersOpResponse().newLearnersList(resultLearners).build()));

    }

    private static class TestCommand implements WriteCommand {
    }

    private static class TestResponse {
    }

    /**
     * Test replication group id.
     */
    private static class TestReplicationGroupId implements ReplicationGroupId {
        private final String name;

        TestReplicationGroupId(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestReplicationGroupId that = (TestReplicationGroupId) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
