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
import static org.apache.ignite.raft.jraft.test.TestUtils.peersToIds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.RaftGroupService;
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
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test methods of raft group service.
 */
@ExtendWith(MockitoExtension.class)
public class RaftGroupServiceTest {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RaftGroupServiceTest.class);

    private static final List<Peer> NODES = Stream.of(20000, 20001, 20002)
            .map(port -> new NetworkAddress("localhost", port))
            .map(Peer::new)
            .collect(Collectors.toUnmodifiableList());

    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    private volatile @Nullable Peer leader = NODES.get(0);

    /** Call timeout. */
    private static final int TIMEOUT = 1000;

    /** Retry delay. */
    private static final int DELAY = 200;

    /** Current term. */
    private static final int CURRENT_TERM = 1;

    /** Test group id. */
    private static final TestReplicationGroupId TEST_GRP = new TestReplicationGroupId("test");

    /** Mock cluster. */
    @Mock
    private ClusterService cluster;

    /** Mock messaging service. */
    @Mock
    private MessagingService messagingService;

    /** Executor for raft group services. */
    private ScheduledExecutorService executor;

    @BeforeEach
    void before(TestInfo testInfo) {
        when(cluster.messagingService()).thenReturn(messagingService);

        executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory(Loza.CLIENT_POOL_NAME, LOG));

        LOG.info(">>>> Starting test {}", testInfo.getTestMethod().orElseThrow().getName());
    }

    /**
     * Shutdown executor for raft group services.
     */
    @AfterEach
    void after() {
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    @Test
    public void testRefreshLeaderStable() throws Exception {
        mockLeaderRequest(false);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, executor).get(3, TimeUnit.SECONDS);

        assertNull(service.leader());

        service.refreshLeader().get();

        assertEquals(leader, service.leader());
    }

    @Test
    public void testRefreshLeaderNotElected() throws Exception {
        mockLeaderRequest(false);

        // Simulate running elections.
        leader = null;

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, executor).get(3, TimeUnit.SECONDS);

        assertNull(service.leader());

        try {
            service.refreshLeader().get();

            fail("Should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testRefreshLeaderElectedAfterDelay() throws Exception {
        mockLeaderRequest(false);

        // Simulate running elections.
        leader = null;

        executor.schedule((Runnable) () -> leader = NODES.get(0), 500, TimeUnit.MILLISECONDS);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, executor).get(3, TimeUnit.SECONDS);

        assertNull(service.leader());

        service.refreshLeader().get();

        assertEquals(NODES.get(0), service.leader());
    }

    @Test
    public void testRefreshLeaderWithTimeout() throws Exception {
        mockLeaderRequest(true);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, executor).get(3, TimeUnit.SECONDS);

        try {
            service.refreshLeader().get(500, TimeUnit.MILLISECONDS);

            fail();
        } catch (TimeoutException e) {
            // Expected.
        }
    }

    @Test
    public void testUserRequestLeaderElected() throws Exception {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, executor).get(3, TimeUnit.SECONDS);

        service.refreshLeader().get();

        TestResponse resp = service.<TestResponse>run(new TestCommand()).get();

        assertNotNull(resp);
    }

    @Test
    public void testUserRequestLazyInitLeader() throws Exception {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, executor).get(3, TimeUnit.SECONDS);

        assertNull(service.leader());

        TestResponse resp = service.<TestResponse>run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(leader, service.leader());
    }

    @Test
    public void testUserRequestWithTimeout() throws Exception {
        mockLeaderRequest(false);
        mockUserInput(true, null);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, executor).get(3, TimeUnit.SECONDS);

        try {
            service.run(new TestCommand()).get(500, TimeUnit.MILLISECONDS);

            fail();
        } catch (TimeoutException e) {
            // Expected.
        }
    }

    @Test
    public void testUserRequestLeaderNotElected() throws Exception {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, true, DELAY, executor).get(3, TimeUnit.SECONDS);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        try {
            service.run(new TestCommand()).get();

            fail("Expecting timeout");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testUserRequestLeaderElectedAfterDelay() throws Exception {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, true, DELAY, executor).get(3, TimeUnit.SECONDS);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        executor.schedule((Runnable) () -> this.leader = NODES.get(0), 500, TimeUnit.MILLISECONDS);

        TestResponse resp = service.<TestResponse>run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(NODES.get(0), service.leader());
    }

    @Test
    public void testUserRequestLeaderElectedAfterDelayWithFailedNode() throws Exception {
        mockLeaderRequest(false);
        mockUserInput(false, NODES.get(0));

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT * 3, NODES, true, DELAY, executor).get(3, TimeUnit.SECONDS);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        executor.schedule(
                () -> {
                    LOG.info("Set leader {}", NODES.get(1));

                    this.leader = NODES.get(1);
                },
                500, TimeUnit.MILLISECONDS
        );

        TestResponse resp = service.<TestResponse>run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(NODES.get(1), service.leader());
    }

    @Test
    public void testUserRequestLeaderChanged() throws Exception {
        mockLeaderRequest(false);
        mockUserInput(false, null);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, true, DELAY, executor).get(3, TimeUnit.SECONDS);

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        Peer newLeader = NODES.get(1);

        this.leader = newLeader;

        assertEquals(leader, service.leader());
        assertNotEquals(leader, newLeader);

        // Runs the command on an old leader. It should respond with leader changed error, when transparently retry.
        TestResponse resp = service.<TestResponse>run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(newLeader, service.leader());
    }

    @Test
    public void testSnapshotExecutionException() throws Exception {
        mockSnapshotRequest(1);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, executor).get(3, TimeUnit.SECONDS);

        var addr = new NetworkAddress("localhost", 8082);

        CompletableFuture<Void> fut = service.snapshot(new Peer(addr));

        try {
            fut.get();

            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IgniteInternalException);
        }
    }

    @Test
    public void testSnapshotExecutionFailedResponse() throws Exception {
        mockSnapshotRequest(0);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, executor).get(3, TimeUnit.SECONDS);

        var addr = new NetworkAddress("localhost", 8082);

        CompletableFuture<Void> fut = service.snapshot(new Peer(addr));

        try {
            fut.get();

            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RaftException);
        }
    }

    @Test
    public void testRefreshMembers() throws Exception {
        List<String> respPeers = peersToIds(NODES.subList(0, 2));
        List<String> respLearners = peersToIds(NODES.subList(2, 2));

        when(messagingService.invoke(any(NetworkAddress.class),
                eq(FACTORY.getPeersRequest().onlyAlive(false).groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.getPeersResponse().peersList(respPeers).learnersList(respLearners).build()));

        mockLeaderRequest(false);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, true, DELAY, executor).get(3, TimeUnit.SECONDS);

        assertEquals(NODES, service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        service.refreshMembers(false).get(3, TimeUnit.SECONDS);

        assertEquals(NODES.subList(0, 2), service.peers());
        assertEquals(NODES.subList(2, 2), service.learners());
    }

    @Test
    public void testAddPeer() throws Exception {
        List<String> respPeers = peersToIds(NODES);

        when(messagingService.invoke(any(NetworkAddress.class),
                eq(FACTORY.addPeerRequest()
                        .peerId(PeerId.parsePeer(NODES.get(2).address().host() + ":" + NODES.get(2).address().port()).toString())
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.addPeerResponse().newPeersList(respPeers).build()));

        mockLeaderRequest(false);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES.subList(0, 2), true, DELAY, executor)
                        .get(3, TimeUnit.SECONDS);

        assertEquals(NODES.subList(0, 2), service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        service.addPeer(NODES.get(2)).get();

        assertEquals(NODES, service.peers());
        assertEquals(Collections.emptyList(), service.learners());
    }

    @Test
    public void testRemovePeer() throws Exception {
        List<String> respPeers = peersToIds(NODES.subList(0, 2));

        when(messagingService.invoke(any(NetworkAddress.class),
                eq(FACTORY.removePeerRequest()
                        .peerId(PeerId.parsePeer(NODES.get(2).address().host() + ":" + NODES.get(2).address().port()).toString())
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.removePeerResponse().newPeersList(respPeers).build()));

        mockLeaderRequest(false);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, true, DELAY, executor).get(3, TimeUnit.SECONDS);

        assertEquals(NODES, service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        service.removePeer(NODES.get(2)).get();

        assertEquals(NODES.subList(0, 2), service.peers());
        assertEquals(Collections.emptyList(), service.learners());
    }

    @Test
    public void testChangePeers() throws Exception {
        List<String> shrunkPeers = peersToIds(NODES.subList(0, 1));

        List<String> extendedPeers = peersToIds(NODES);

        when(messagingService.invoke(any(NetworkAddress.class),
                eq(FACTORY.changePeersRequest()
                        .newPeersList(shrunkPeers)
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.changePeersResponse().newPeersList(shrunkPeers).build()));

        when(messagingService.invoke(any(NetworkAddress.class),
                eq(FACTORY.changePeersRequest()
                        .newPeersList(extendedPeers)
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.changePeersResponse().newPeersList(extendedPeers).build()));

        mockLeaderRequest(false);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES.subList(0, 2), true, DELAY, executor)
                        .get(3, TimeUnit.SECONDS);

        assertEquals(NODES.subList(0, 2), service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        service.changePeers(NODES.subList(0, 1)).get();

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        service.changePeers(NODES).get();

        assertEquals(NODES, service.peers());
        assertEquals(Collections.emptyList(), service.learners());
    }

    @Test
    public void testTransferLeadership() throws Exception {
        when(messagingService.invoke(any(NetworkAddress.class),
                eq(FACTORY.transferLeaderRequest()
                        .peerId(PeerId.fromPeer(NODES.get(1)).toString())
                        .leaderId(PeerId.fromPeer(NODES.get(0)).toString())
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(RaftRpcFactory.DEFAULT.newResponse(FACTORY, Status.OK())));

        mockLeaderRequest(false);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, true, DELAY, executor).get(3, TimeUnit.SECONDS);

        assertEquals(NODES.get(0), service.leader());

        service.transferLeadership(NODES.get(1)).get();

        assertEquals(NODES.get(1), service.leader());
    }

    @Test
    public void testAddLearners() throws Exception {
        List<String> addLearners = peersToIds(NODES.subList(1, 3));

        when(messagingService.invoke(any(NetworkAddress.class),
                eq(FACTORY.addLearnersRequest()
                        .learnersList(addLearners)
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.learnersOpResponse().newLearnersList(addLearners).build()));

        mockLeaderRequest(false);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES.subList(0, 1), true, DELAY, executor)
                        .get(3, TimeUnit.SECONDS);

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(Collections.emptyList(), service.learners());

        service.addLearners(NODES.subList(1, 3)).get();

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(NODES.subList(1, 3), service.learners());
    }

    @Test
    public void testResetLearners() throws Exception {
        List<String> addLearners = peersToIds(NODES.subList(1, 3));

        List<String> resetLearners = peersToIds(NODES.subList(2, 3));

        when(messagingService.invoke(any(NetworkAddress.class),
                eq(FACTORY.resetLearnersRequest()
                        .learnersList(resetLearners)
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.learnersOpResponse().newLearnersList(resetLearners).build()));

        mockAddLearners(TEST_GRP.toString(), addLearners, addLearners);

        mockLeaderRequest(false);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES.subList(0, 1), true, DELAY, executor)
                        .get(3, TimeUnit.SECONDS);

        service.addLearners(NODES.subList(1, 3)).get();

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(NODES.subList(1, 3), service.learners());

        service.resetLearners(NODES.subList(2, 3)).get();

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(NODES.subList(2, 3), service.learners());
    }

    @Test
    public void testRemoveLearners() throws Exception {
        List<String> addLearners = peersToIds(NODES.subList(1, 3));

        List<String> removeLearners = peersToIds(NODES.subList(2, 3));

        List<String> resultLearners =
                NODES.subList(1, 2).stream().map(p -> PeerId.fromPeer(p).toString()).collect(Collectors.toList());

        when(messagingService.invoke(any(NetworkAddress.class),
                eq(FACTORY.removeLearnersRequest()
                        .learnersList(removeLearners)
                        .groupId(TEST_GRP.toString()).build()), anyLong()))
                .then(invocation ->
                        completedFuture(FACTORY.learnersOpResponse().newLearnersList(resultLearners).build()));

        mockAddLearners(TEST_GRP.toString(), addLearners, addLearners);

        mockLeaderRequest(false);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES.subList(0, 1), true, DELAY, executor)
                        .get(3, TimeUnit.SECONDS);

        service.addLearners(NODES.subList(1, 3)).get();

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(NODES.subList(1, 3), service.learners());

        service.removeLearners(NODES.subList(2, 3)).get();

        assertEquals(NODES.subList(0, 1), service.peers());
        assertEquals(NODES.subList(1, 2), service.learners());
    }

    @Test
    public void testGetLeaderRequest() throws Exception {
        mockLeaderRequest(false);

        RaftGroupService service =
                RaftGroupServiceImpl.start(TEST_GRP, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, executor).get(3, TimeUnit.SECONDS);

        assertNull(service.leader());

        service.refreshLeader().get();

        GetLeaderRequest req = FACTORY.getLeaderRequest().groupId(TEST_GRP.toString()).build();

        GetLeaderResponse fut = (GetLeaderResponse) messagingService.invoke(leader.address(), req, TIMEOUT).get();

        assertEquals(PeerId.fromPeer(leader).toString(), fut.leaderId());

        assertEquals(CURRENT_TERM, fut.currentTerm());
    }

    /**
     * Mocks sending {@link ActionRequest}s.
     *
     * @param delay {@code True} to create a delay before response.
     * @param peer Fail the request targeted to given peer.
     */
    private void mockUserInput(boolean delay, @Nullable Peer peer) {
        when(messagingService.invoke(
                any(NetworkAddress.class),
                argThat(new ArgumentMatcher<ActionRequest>() {
                    @Override
                    public boolean matches(ActionRequest arg) {
                        return arg.command() instanceof TestCommand;
                    }
                }),
                anyLong()
        )).then(invocation -> {
            NetworkAddress target = invocation.getArgument(0);

            if (peer != null && target.equals(peer.address())) {
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
            } else if (!target.equals(leader.address())) {
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
        when(messagingService.invoke(any(NetworkAddress.class), any(GetLeaderRequest.class), anyLong()))
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
        when(messagingService.invoke(any(NetworkAddress.class), any(CliRequests.SnapshotRequest.class), anyLong()))
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
        when(messagingService.invoke(any(NetworkAddress.class),
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
    protected static class TestReplicationGroupId implements ReplicationGroupId {
        private final String name;

        public TestReplicationGroupId(String name) {
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
