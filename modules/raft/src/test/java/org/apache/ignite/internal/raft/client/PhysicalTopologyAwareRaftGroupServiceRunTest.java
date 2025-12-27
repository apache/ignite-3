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

package org.apache.ignite.internal.raft.client;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.raft.TestThrottlingContextHolder.throttlingContextHolder;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.deriveUuidFrom;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.TestWriteCommand.testWriteCommand;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.ReplicationGroupUnavailableException;
import org.apache.ignite.internal.raft.StoppingExceptionFactories;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.TestWriteCommand;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests for {@link PhysicalTopologyAwareRaftGroupService} run() method semantics.
 *
 * <p>Tests verify the timeout-based behavior:
 * <ul>
 *     <li>timeout = 0: Single attempt, fail immediately if group unavailable</li>
 *     <li>timeout = Long.MAX_VALUE: Wait indefinitely for leader availability</li>
 *     <li>0 &lt; timeout &lt; Long.MAX_VALUE: Bounded wait for leader availability</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@Disabled("IGNITE-27156")
public class PhysicalTopologyAwareRaftGroupServiceRunTest extends BaseIgniteAbstractTest {

    private static final List<Peer> NODES = Stream.of(20000, 20001, 20002)
            .map(port -> new Peer("localhost-" + port))
            .collect(toUnmodifiableList());

    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    private static final FailureManager NOOP_FAILURE_PROCESSOR = new FailureManager(new NoOpFailureHandler());

    /** Test group id. */
    private static final TestReplicationGroupId TEST_GRP = new TestReplicationGroupId("test");

    /** Call timeout. */
    private static final long TIMEOUT = 1000;

    /** Current term. */
    private static final long CURRENT_TERM = 1;

    @Mock
    private RaftConfiguration raftConfiguration;

    @Mock
    private ClusterService cluster;

    @Mock
    private MessagingService messagingService;

    @Mock
    private TopologyService topologyService;

    private ScheduledExecutorService executor;

    private RaftGroupEventsClientListener eventsClientListener;

    private PhysicalTopologyAwareRaftGroupService service;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void before() {
        when(cluster.messagingService()).thenReturn(messagingService);
        when(cluster.topologyService()).thenReturn(topologyService);

        MessageSerializationRegistry serializationRegistry = ClusterServiceTestUtils.defaultSerializationRegistry();
        when(cluster.serializationRegistry()).thenReturn(serializationRegistry);

        lenient().when(topologyService.getByConsistentId(any()))
                .thenAnswer(invocation -> {
                    String consistentId = invocation.getArgument(0);
                    return new ClusterNodeImpl(deriveUuidFrom(consistentId), consistentId, new NetworkAddress("localhost", 123));
                });

        // Return empty list for allMembers() - service uses this during initialization.
        lenient().when(topologyService.allMembers()).thenReturn(List.of());

        // Mock RaftConfiguration.
        ConfigurationValue<Long> retryTimeoutValue = mock(ConfigurationValue.class);
        lenient().when(retryTimeoutValue.value()).thenReturn(TIMEOUT);
        lenient().when(raftConfiguration.retryTimeoutMillis()).thenReturn(retryTimeoutValue);

        ConfigurationValue<Long> responseTimeoutValue = mock(ConfigurationValue.class);
        lenient().when(responseTimeoutValue.value()).thenReturn(3000L);
        lenient().when(raftConfiguration.responseTimeoutMillis()).thenReturn(responseTimeoutValue);

        executor = new ScheduledThreadPoolExecutor(20, IgniteThreadFactory.create("common", Loza.CLIENT_POOL_NAME, logger()));
        eventsClientListener = new RaftGroupEventsClientListener();
    }

    @AfterEach
    void after() {
        if (service != null) {
            service.shutdown();
        }
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    private PhysicalTopologyAwareRaftGroupService startService() {
        var commandsMarshaller = new ThreadLocalOptimizedMarshaller(cluster.serializationRegistry());

        PeersAndLearners peersAndLearners = PeersAndLearners.fromConsistentIds(
                NODES.stream().map(Peer::consistentId).collect(Collectors.toSet())
        );

        service = PhysicalTopologyAwareRaftGroupService.start(
                TEST_GRP,
                cluster,
                raftConfiguration,
                peersAndLearners,
                executor,
                eventsClientListener,
                commandsMarshaller,
                StoppingExceptionFactories.indicateComponentStop(),
                throttlingContextHolder(),
                NOOP_FAILURE_PROCESSOR
        );

        return service;
    }

    /**
     * Simulates leader election notification to the service (async, does not wait).
     */
    private void simulateLeaderElection(Peer leaderPeer, long term) {
        InternalClusterNode leaderNode = new ClusterNodeImpl(
                deriveUuidFrom(leaderPeer.consistentId()),
                leaderPeer.consistentId(),
                new NetworkAddress("localhost", 123)
        );
        eventsClientListener.onLeaderElected(TEST_GRP, leaderNode, term);
    }

    /**
     * Simulates leader election and waits for the notification to be processed.
     */
    private void simulateLeaderElectionAndWait(Peer leaderPeer, long term) {
        // Use a latch to wait for the leader election callback to be processed.
        var latch = new CountDownLatch(1);
        service.subscribeLeader((node, electedTerm) -> {
            if (electedTerm >= term) {
                latch.countDown();
            }
        });

        simulateLeaderElection(leaderPeer, term);

        // Wait for the leader election callback to be processed.
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS), "Leader election callback should be processed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Mocks sending GetLeaderRequest - returns the first node as leader.
     */
    private void mockLeaderRequest() {
        when(messagingService.invoke(any(InternalClusterNode.class), any(GetLeaderRequest.class), anyLong()))
                .then(invocation -> completedFuture(FACTORY.getLeaderResponse()
                        .leaderId(PeerId.fromPeer(NODES.get(0)).toString())
                        .currentTerm(CURRENT_TERM)
                        .build()));
    }

    private boolean isTestWriteCommand(WriteActionRequest arg) {
        if (arg == null) {
            return false;
        }
        Object command = new OptimizedMarshaller(cluster.serializationRegistry(),
                OptimizedMarshaller.NO_POOL).unmarshall(arg.command());
        return command instanceof TestWriteCommand;
    }

    /**
     * Mocks sending WriteActionRequest - returns success.
     */
    private void mockUserInputSuccess() {
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).then(invocation -> completedFuture(FACTORY.actionResponse().result(new TestResponse()).build()));
    }

    /**
     * Mocks sending WriteActionRequest - returns EPERM with no leader (leader absence).
     */
    private void mockUserInputNoLeader() {
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).then(invocation -> completedFuture(FACTORY.errorResponse()
                .errorCode(RaftError.EPERM.getNumber())
                .build()));
    }

    /**
     * Tests that with timeout=0, the command succeeds immediately when leader is available.
     */
    @Test
    void testZeroTimeoutSuccessWhenLeaderAvailable() {
        mockLeaderRequest();
        mockUserInputSuccess();

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Simulate leader election and wait so the service knows about the leader.
        simulateLeaderElectionAndWait(NODES.get(0), CURRENT_TERM);

        // With timeout=0, should succeed immediately since leader is available.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 0);

        assertThat(result, willBe(instanceOf(TestResponse.class)));
    }

    /**
     * Tests that with timeout=0, the command throws ReplicationGroupUnavailableException when no leader and all peers tried.
     */
    @Test
    void testZeroTimeoutFailWhenNoLeader() {
        mockUserInputNoLeader();

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // No leader election simulated - service is in WAITING_FOR_LEADER state.
        // With timeout=0, should fail immediately.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 0);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class));
    }

    /**
     * Tests that with timeout=0, all peers are tried before failing.
     */
    @Test
    void testZeroTimeoutTriesAllPeersBeforeFailing() {
        // All peers return EPERM with no leader.
        mockUserInputNoLeader();

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // With timeout=0, should try each peer once and then fail.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 0);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class));
    }

    /**
     * Tests that with Long.MAX_VALUE timeout, the command succeeds when leader is available immediately.
     */
    @Test
    void testInfiniteTimeoutSuccessWhenLeaderAvailable() {
        mockLeaderRequest();
        mockUserInputSuccess();

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Simulate leader election and wait for it to be processed.
        simulateLeaderElectionAndWait(NODES.get(0), CURRENT_TERM);

        CompletableFuture<Object> result = svc.run(testWriteCommand(), Long.MAX_VALUE);

        assertThat(result, willBe(instanceOf(TestResponse.class)));
    }

    /**
     * Tests that with Long.MAX_VALUE timeout, the command waits for leader and succeeds when leader appears.
     */
    @Test
    void testInfiniteTimeoutWaitsForLeaderAndSucceeds() {
        mockLeaderRequest();
        mockUserInputSuccess();

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Start the command - it should wait for leader.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), Long.MAX_VALUE);

        // Initially not complete (waiting for leader).
        // After a short delay, simulate leader election.
        executor.schedule(() -> simulateLeaderElection(NODES.get(0), CURRENT_TERM), 100, TimeUnit.MILLISECONDS);

        // Should eventually complete successfully.
        assertThat(result, willCompleteSuccessfully());
    }

    /**
     * Tests that negative timeout is treated as infinite wait.
     */
    @Test
    void testNegativeTimeoutTreatedAsInfinite() {
        mockLeaderRequest();
        mockUserInputSuccess();

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Simulate leader election and wait for it to be processed.
        simulateLeaderElectionAndWait(NODES.get(0), CURRENT_TERM);

        // Negative timeout should be treated as infinite.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), -1);

        assertThat(result, willBe(instanceOf(TestResponse.class)));
    }

    /**
     * Tests that with bounded timeout, the command succeeds within timeout when leader is available.
     */
    @Test
    void testBoundedTimeoutSuccessWithinTimeout() {
        mockLeaderRequest();
        mockUserInputSuccess();

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Simulate leader election and wait for it to be processed.
        simulateLeaderElectionAndWait(NODES.get(0), CURRENT_TERM);

        CompletableFuture<Object> result = svc.run(testWriteCommand(), 5000);

        assertThat(result, willBe(instanceOf(TestResponse.class)));
    }

    /**
     * Tests that with bounded timeout, ReplicationGroupUnavailableException is thrown when timeout expires waiting for leader.
     */
    @Test
    void testBoundedTimeoutExpiresWaitingForLeader() {
        // No leader will be elected.
        mockUserInputNoLeader();

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // With a short timeout and no leader, should fail with ReplicationGroupUnavailableException.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 200);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class, 1, TimeUnit.SECONDS));
    }

    /**
     * Tests that with bounded timeout, the command succeeds when leader appears before timeout.
     */
    @Test
    void testBoundedTimeoutSucceedsWhenLeaderAppearsBeforeTimeout() {
        mockLeaderRequest();
        mockUserInputSuccess();

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Start the command with reasonable timeout.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 5000);

        // Simulate leader election after short delay (before timeout).
        executor.schedule(() -> simulateLeaderElection(NODES.get(0), CURRENT_TERM), 100, TimeUnit.MILLISECONDS);

        assertThat(result, willCompleteSuccessfully());
    }

    private static class TestResponse {
    }
}
