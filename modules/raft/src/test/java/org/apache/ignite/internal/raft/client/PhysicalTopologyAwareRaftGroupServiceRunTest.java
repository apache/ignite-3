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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.lang.NodeStoppingException;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
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
                StoppingExceptionFactories.indicateNodeStop(),
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
     * Tests that with timeout=0, all peers are tried before failing with ReplicationGroupUnavailableException.
     */
    @Test
    void testZeroTimeoutTriesAllPeersBeforeFailing() {
        // All peers return EPERM with no leader.
        mockUserInputNoLeader();

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // With timeout=0, should try each peer once and then fail.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 0);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class));

        verifyExact3PeersCalled();
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
     * Tests that with Long.MAX_VALUE timeout, all peers are tried first, then waits for leader, and succeeds when leader appears.
     */
    @Test
    void testInfiniteTimeoutWaitsForLeaderAndSucceeds() throws Exception {
        // First 3 WriteActionRequest calls return EPERM (no leader), then success after leader appears.
        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch allPeersTried = new CountDownLatch(3);

        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            if (callCount.incrementAndGet() <= 3) {
                allPeersTried.countDown();
                return completedFuture(FACTORY.errorResponse()
                        .errorCode(RaftError.EPERM.getNumber())
                        .build());
            }
            return completedFuture(FACTORY.actionResponse().result(new TestResponse()).build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Start the command - it should try all peers first, then wait for leader.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), Long.MAX_VALUE);

        // Wait for all peer attempts to complete.
        assertTrue(allPeersTried.await(5, TimeUnit.SECONDS), "All peers should be tried");

        verifyExact3PeersCalled();

        // Initially not complete (waiting for leader).
        assertThat(result.isDone(), is(false));

        // Simulate leader election.
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

    /**
     * Tests that with bounded timeout, the command fails if peer responses are slow and exceed the deadline.
     */
    @Test
    void testBoundedTimeoutExpiresWhileTryingPeers() {
        // Each peer response takes 200ms and returns EPERM (no leader).
        // With 3 peers and 500ms timeout, we should exceed the deadline.
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return FACTORY.errorResponse()
                        .errorCode(RaftError.EPERM.getNumber())
                        .build();
            }, executor);
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // With 500ms timeout and 3 peers each taking 200ms, total time ~600ms > 500ms.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 500);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class, 2, TimeUnit.SECONDS));
    }

    /**
     * Tests that individual peer request timeouts are bounded by the deadline.
     *
     * <p>If the deadline is 500ms but the default response timeout is 3000ms, the first peer request
     * should timeout at ~500ms, not wait the full 3000ms.
     */
    @Test
    void testBoundedTimeoutLimitsIndividualPeerRequestTimeout() {
        // First peer never responds (simulating network issue).
        // Default response timeout is 3000ms, but our deadline is 500ms.
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenReturn(new CompletableFuture<>()); // Never completes

        PhysicalTopologyAwareRaftGroupService svc = startService();

        long startTime = System.currentTimeMillis();

        // With 500ms timeout, should fail within ~600ms (500ms + some margin), not 3000ms.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 500);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class, 1, TimeUnit.SECONDS));

        long elapsed = System.currentTimeMillis() - startTime;
        // Should complete much faster than the default response timeout (3000ms).
        assertTrue(elapsed < 1500, "Expected to fail within 1500ms but took " + elapsed + "ms");
    }

    /**
     * Tests that with timeout=0, if the client is shutting down during peer trying,
     * the future completes with NodeStoppingException.
     */
    @Test
    void testZeroTimeoutShutdownDuringPeerTrying() throws Exception {
        List<CompletableFuture<Object>> pendingInvokes = new CopyOnWriteArrayList<>();
        CountDownLatch invokeStarted = new CountDownLatch(1);

        // Return a future that doesn't complete immediately - we'll complete it after shutdown.
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            var pendingFuture = new CompletableFuture<>();
            pendingInvokes.add(pendingFuture);
            invokeStarted.countDown();
            return pendingFuture;
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Start the command with timeout=0.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 0);

        // Wait for at least one invoke to be pending.
        assertTrue(invokeStarted.await(5, TimeUnit.SECONDS), "Invoke should start");

        // Shutdown the service.
        svc.shutdown();

        // Complete the pending invoke with EPERM - the callback should see shutdown.
        for (var pending : pendingInvokes) {
            pending.complete(FACTORY.errorResponse()
                    .errorCode(RaftError.EPERM.getNumber())
                    .build());
        }

        // The result should complete with NodeStoppingException.
        assertThat(result, willThrow(NodeStoppingException.class, 5, TimeUnit.SECONDS));
    }

    /**
     * Tests that with infinite timeout, if the client is shutting down during leader waiting,
     * the future completes with NodeStoppingException.
     */
    @Test
    void testInfiniteTimeoutShutdownDuringLeaderWaiting() throws Exception {
        CountDownLatch allPeersTried = new CountDownLatch(3);

        // All peers return EPERM (no leader) to trigger leader waiting phase.
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            allPeersTried.countDown();
            return completedFuture(FACTORY.errorResponse()
                    .errorCode(RaftError.EPERM.getNumber())
                    .build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Start the command with infinite timeout.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), Long.MAX_VALUE);

        // Wait for all peers to be tried (3 peers).
        assertTrue(allPeersTried.await(5, TimeUnit.SECONDS), "All peers should be tried");

        // The result should not be complete yet (waiting for leader).
        assertThat(result.isDone(), is(false));

        // Shutdown the service.
        svc.shutdown();

        // The result should complete with NodeStoppingException.
        assertThat(result, willThrow(NodeStoppingException.class, 5, TimeUnit.SECONDS));
    }

    /**
     * Tests that with bounded timeout, if the client is shutting down during retry phase,
     * the future completes with NodeStoppingException immediately without new retry round.
     */
    @Test
    void testBoundedTimeoutShutdownDuringRetryPhase() throws Exception {
        var pendingRetryInvoke = new CompletableFuture<Void>();
        var retryPhaseStarted = new CountDownLatch(1);
        var allPeersTried = new CountDownLatch(3);

        AtomicInteger callCount = new AtomicInteger(0);
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count <= 3) {
                // First 3 calls: return EPERM to trigger retry logic.
                allPeersTried.countDown();
                return completedFuture(FACTORY.errorResponse()
                        .errorCode(RaftError.EPERM.getNumber())
                        .build());
            } else if (count == 4) {
                // Fourth call: we're in retry phase after leader was notified.
                // Signal that retry phase started and return a pending future.
                retryPhaseStarted.countDown();
                return pendingRetryInvoke.thenApply(v -> FACTORY.errorResponse()
                        .errorCode(RaftError.EBUSY.getNumber())
                        .build());
            }
            // Should not reach here.
            return completedFuture(FACTORY.actionResponse().result(new TestResponse()).build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Start the command with bounded timeout (long enough).
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 30_000);

        // Wait for all peers to be tried.
        assertTrue(allPeersTried.await(5, TimeUnit.SECONDS), "All peers should be tried");

        // Simulate leader election to trigger retry phase.
        simulateLeaderElection(NODES.get(0), CURRENT_TERM);

        // Wait for retry phase to start.
        assertTrue(retryPhaseStarted.await(5, TimeUnit.SECONDS), "Retry phase should start");

        // Shutdown the service.
        svc.shutdown();

        // Complete the pending retry invoke - the callback should see shutdown.
        pendingRetryInvoke.complete(null);

        // The result should complete with NodeStoppingException.
        assertThat(result, willThrow(NodeStoppingException.class, 5, TimeUnit.SECONDS));

        // Give some time for any additional retry attempts.
        Thread.sleep(200);

        // Verify no additional retry attempts were made after shutdown.
        // We should have exactly 4 invocations (3 initial + 1 retry attempt that was interrupted).
        assertThat(callCount.get(), is(4));
    }

    private void verifyExact3PeersCalled() {
        ArgumentCaptor<InternalClusterNode> nodeCaptor = ArgumentCaptor.forClass(InternalClusterNode.class);
        Mockito.verify(this.messagingService, Mockito.times(3)).invoke(
                nodeCaptor.capture(),
                argThat(WriteActionRequest.class::isInstance),
                anyLong()
        );

        // Verify each peer was tried exactly once.
        Set<String> triedPeers = nodeCaptor.getAllValues().stream()
                .map(InternalClusterNode::name)
                .collect(Collectors.toSet());

        Set<String> expectedPeers = NODES.stream()
                .map(Peer::consistentId)
                .collect(Collectors.toSet());

        assertThat(triedPeers, equalTo(expectedPeers));
    }

    private static class TestResponse {
    }
}
