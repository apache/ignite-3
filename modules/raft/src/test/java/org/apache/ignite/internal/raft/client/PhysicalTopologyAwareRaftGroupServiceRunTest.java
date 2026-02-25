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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.deriveUuidFrom;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.TestWriteCommand.testWriteCommand;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
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
public class PhysicalTopologyAwareRaftGroupServiceRunTest extends BaseIgniteAbstractTest {

    private static final List<Peer> NODES = Stream.of(20000, 20001, 20002)
            .map(port -> new Peer("localhost-" + port))
            .collect(toUnmodifiableList());

    private static final List<Peer> FIVE_NODES = Stream.of(20000, 20001, 20002, 20003, 20004)
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

        ConfigurationValue<Long> retryDelayValue = mock(ConfigurationValue.class);
        lenient().when(retryDelayValue.value()).thenReturn(50L);
        lenient().when(raftConfiguration.retryDelayMillis()).thenReturn(retryDelayValue);

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
        return startService(NODES);
    }

    private PhysicalTopologyAwareRaftGroupService startService(List<Peer> peers) {
        var commandsMarshaller = new ThreadLocalOptimizedMarshaller(cluster.serializationRegistry());

        PeersAndLearners peersAndLearners = PeersAndLearners.fromConsistentIds(
                peers.stream().map(Peer::consistentId).collect(Collectors.toSet())
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
     * Tests that command succeeds when leader is available, regardless of timeout value.
     */
    @ParameterizedTest
    @ValueSource(longs = {0, 5000, Long.MAX_VALUE, -1})
    void testSuccessWhenLeaderAvailable(long timeout) {
        mockLeaderRequest();
        mockUserInputSuccess();

        PhysicalTopologyAwareRaftGroupService svc = startService();
        simulateLeaderElectionAndWait(NODES.get(0), CURRENT_TERM);

        CompletableFuture<Object> result = svc.run(testWriteCommand(), timeout);

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
     * Tests that UNKNOWN/EINTERNAL/ENOENT errors retry on the same peer for ActionRequests.
     * This is because these errors are transient and the peer is likely to recover.
     * This behavior is consistent with {@link RaftGroupServiceImpl}.
     *
     * <p>The test verifies that after receiving an EINTERNAL error from a peer,
     * the next retry goes to the same peer (not a different one).
     */
    @Test
    void testTransientErrorRetriesOnSamePeer() {
        // Track the sequence of peers called.
        List<String> calledPeers = new CopyOnWriteArrayList<>();

        // Mock all WriteActionRequest calls - track which peer is called.
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong())
        ).thenAnswer(invocation -> {
            InternalClusterNode target = invocation.getArgument(0);
            calledPeers.add(target.name());

            if (calledPeers.size() == 1) {
                // First call returns EINTERNAL error.
                return completedFuture(FACTORY.errorResponse()
                        .errorCode(RaftError.EINTERNAL.getNumber())
                        .build());
            }
            // Second call succeeds.
            return completedFuture(FACTORY.actionResponse().result(new TestResponse()).build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Simulate leader election and wait for it to be processed.
        simulateLeaderElectionAndWait(NODES.get(0), CURRENT_TERM);

        CompletableFuture<Object> result = svc.run(testWriteCommand(), Long.MAX_VALUE);

        assertThat(result, willBe(instanceOf(TestResponse.class)));

        // Verify that exactly 2 calls were made.
        assertThat("Should have exactly 2 calls", calledPeers.size(), is(2));

        // Key assertion: both calls should be to the SAME peer (retry on same peer for transient errors).
        assertThat("Transient error should retry on the same peer, but first call was to "
                        + calledPeers.get(0) + " and second was to " + calledPeers.get(1),
                calledPeers.get(0), is(calledPeers.get(1)));
    }

    /**
     * Tests that with bounded timeout, a non-leader-related retriable error (like EBUSY) causes
     * retries until timeout without waiting for leader. This differs from EPERM with no leader,
     * which tries all peers once then waits for leader.
     */
    @Test
    void testBoundedTimeoutRetriesNonLeaderErrorUntilTimeout() {
        AtomicInteger callCount = new AtomicInteger(0);

        // All peers return EBUSY (a retriable error that is NOT related to missing leader).
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            callCount.incrementAndGet();
            return completedFuture(FACTORY.errorResponse()
                    .errorCode(RaftError.EBUSY.getNumber())
                    .build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // With 300ms timeout and EBUSY errors, should retry until timeout.
        // Unlike EPERM (no leader), EBUSY should cause continuous retries, not wait for leader.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 300);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class, 1, TimeUnit.SECONDS));

        // Should have made more than 3 calls (cycling through peers multiple times).
        // With retryDelayMillis=50 and 300ms timeout, we should get several retry rounds.
        assertTrue(callCount.get() > 3,
                "Expected more than 3 calls (multiple retry rounds), but got " + callCount.get());
    }

    /**
     * Tests that with infinite timeout, a non-leader-related retriable error (like EBUSY) causes
     * retries indefinitely until success. This differs from EPERM with no leader.
     */
    @Test
    void testInfiniteTimeoutRetriesNonLeaderErrorUntilSuccess() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch multipleRetriesDone = new CountDownLatch(5);

        // First 5 calls return EBUSY, then success.
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count <= 5) {
                multipleRetriesDone.countDown();
                return completedFuture(FACTORY.errorResponse()
                        .errorCode(RaftError.EBUSY.getNumber())
                        .build());
            }
            return completedFuture(FACTORY.actionResponse().result(new TestResponse()).build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // With infinite timeout and EBUSY errors, should retry until success.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), Long.MAX_VALUE);

        // Wait for multiple retry attempts.
        assertTrue(multipleRetriesDone.await(5, TimeUnit.SECONDS), "Should have multiple retries");

        // Should eventually succeed.
        assertThat(result, willCompleteSuccessfully());

        // Should have made more than 5 calls.
        assertTrue(callCount.get() > 5, "Expected more than 5 calls, but got " + callCount.get());
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
        var fifthCallAttempted = new CountDownLatch(1);

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
            // Fifth call should not happen after shutdown.
            fifthCallAttempted.countDown();
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

        // Verify no additional retry attempts were made after shutdown.
        // The fifth call latch should NOT be counted down (wait briefly and check).
        assertThat("No 5th call should be attempted after shutdown",
                fifthCallAttempted.await(100, TimeUnit.MILLISECONDS), is(false));
        assertThat(callCount.get(), is(4));
    }

    /**
     * Tests that when a leader was previously elected and then becomes unavailable (all peers return EPERM with no leader),
     * the service tries all peers exactly once and then waits for leader notification.
     *
     * <p>This test verifies that the term is correctly passed to the retry logic so that
     * {@code onGroupUnavailable(term)} can properly transition the state from LEADER_AVAILABLE to WAITING_FOR_LEADER.
     */
    @Test
    void testInfiniteTimeoutWithPreviousLeaderTriesPeersOnceBeforeWaiting() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch allPeersTried = new CountDownLatch(3);
        // This latch will be counted down if more than 3 calls are made (indicating the bug).
        CountDownLatch extraCallsMade = new CountDownLatch(1);

        // All peers return EPERM with no leader.
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count <= 3) {
                allPeersTried.countDown();
            } else {
                // More than 3 calls means the bug exists - extra retry cycle happened.
                extraCallsMade.countDown();
            }
            return completedFuture(FACTORY.errorResponse()
                    .errorCode(RaftError.EPERM.getNumber())
                    .build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Simulate leader election BEFORE calling run().
        // After this, state is LEADER_AVAILABLE with currentTerm=1.
        simulateLeaderElectionAndWait(NODES.get(0), CURRENT_TERM);

        // Start the command with infinite timeout.
        // The service should try all peers once and then wait for leader (not do an extra cycle).
        CompletableFuture<Object> result = svc.run(testWriteCommand(), Long.MAX_VALUE);

        // Wait for all 3 peer attempts to complete.
        assertTrue(allPeersTried.await(5, TimeUnit.SECONDS), "All 3 peers should be tried");

        // Give some time for potential extra calls (if bug exists).
        // If the term is incorrectly passed as -1, awaitLeader() returns immediately
        // and another retry cycle starts immediately.
        boolean extraCallsHappened = extraCallsMade.await(500, TimeUnit.MILLISECONDS);

        // The result should NOT be complete yet - it should be waiting for leader.
        assertThat("Result should be waiting for leader, not completed",
                result.isDone(), is(false));

        // Verify exactly 3 calls were made (one per peer), not 6.
        assertThat("Expected exactly 3 calls (one per peer), but got " + callCount.get()
                        + ". If 6+ calls were made, the term was incorrectly passed as -1 "
                        + "causing an extra retry cycle before proper waiting.",
                extraCallsHappened, is(false));
        assertThat(callCount.get(), is(3));
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

    // ==========================================================================================
    // Tests for refreshLeader and refreshAndGetLeaderWithTerm (RANDOM strategy)
    // ==========================================================================================

    /**
     * Tests that for GetLeaderRequest with UNKNOWN/EINTERNAL/ENOENT errors, the executor tries another peer
     * instead of retrying the same peer. This matches RaftGroupServiceImpl behavior.
     *
     * <p>The test verifies that after receiving one of these errors from a peer, the next request goes to
     * a DIFFERENT peer, not the same one.
     */
    @ParameterizedTest
    @EnumSource(names = {"UNKNOWN", "EINTERNAL", "ENOENT"})
    void testGetLeaderRequestTriesDifferentPeerOnTransientError(RaftError error) throws Exception {
        Set<String> calledPeers = ConcurrentHashMap.newKeySet();
        AtomicInteger callCount = new AtomicInteger(0);

        when(messagingService.invoke(
                any(InternalClusterNode.class),
                any(GetLeaderRequest.class),
                anyLong())
        ).thenAnswer(invocation -> {
            InternalClusterNode target = invocation.getArgument(0);
            calledPeers.add(target.name());
            int count = callCount.incrementAndGet();

            if (count == 1) {
                // First call returns transient error.
                return completedFuture(FACTORY.errorResponse()
                        .errorCode(error.getNumber())
                        .build());
            }

            // Second call succeeds.
            return completedFuture(FACTORY.getLeaderResponse()
                    .leaderId(PeerId.fromPeer(NODES.get(0)).toString())
                    .currentTerm(CURRENT_TERM)
                    .build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        CompletableFuture<LeaderWithTerm> result = svc.refreshAndGetLeaderWithTerm(TIMEOUT);

        assertThat(result, willCompleteSuccessfully());

        // Verify that at least 2 different peers were called.
        // If the same peer was retried, calledPeers would have size 1.
        assertThat("Should try different peers on " + error + " error, but called peers were: " + calledPeers,
                calledPeers.size(), greaterThan(1));
    }

    /**
     * Tests that RANDOM strategy with bounded timeout keeps cycling through peers until timeout.
     *
     * <p>When all peers return EPERM (no leader), RaftGroupServiceImpl resets unavailable peers
     * and keeps cycling until timeout. RaftCommandExecutor should do the same for RANDOM strategy.
     */
    @Test
    void testRandomStrategyWithBoundedTimeoutKeepsCyclingUntilTimeout() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch sixCallsReached = new CountDownLatch(6);

        // All peers return EPERM (no leader) every time.
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                any(GetLeaderRequest.class),
                anyLong())
        ).thenAnswer(invocation -> {
            callCount.incrementAndGet();
            sixCallsReached.countDown();
            return completedFuture(FACTORY.errorResponse()
                    .errorCode(RaftError.EPERM.getNumber())
                    .leaderId(null)
                    .build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // With bounded timeout (500ms), should keep cycling until timeout.
        // With 3 peers and 50ms retry delay, we should get more than 3 calls before timeout.
        CompletableFuture<Void> result = svc.refreshLeader(500);

        // Wait for at least 6 calls (2 complete cycles through all 3 peers).
        boolean sixCallsHappened = sixCallsReached.await(2, TimeUnit.SECONDS);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class, 2, TimeUnit.SECONDS));

        // Verify more than 3 calls were made (cycling through peers multiple times).
        assertThat("Should cycle through peers multiple times before timeout, but only " + callCount.get() + " calls were made",
                sixCallsHappened, is(true));
        assertThat(callCount.get(), greaterThan(3));
    }

    /**
     * Tests that the timeout parameter is respected for refreshLeader/refreshAndGetLeaderWithTerm.
     * With a short timeout, the request should fail fast, not wait for the default response timeout.
     */
    @Test
    void testTimeoutParameterIsRespectedForRefreshLeader() {
        // All peers never respond (simulating network issue).
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                any(GetLeaderRequest.class),
                anyLong())
        ).thenReturn(new CompletableFuture<>()); // Never completes

        PhysicalTopologyAwareRaftGroupService svc = startService();

        long startTime = System.currentTimeMillis();

        // With 200ms timeout, should fail within ~300ms, not the default 3000ms.
        CompletableFuture<Void> result = svc.refreshLeader(200);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class, 1, TimeUnit.SECONDS));

        long elapsed = System.currentTimeMillis() - startTime;
        assertThat("Expected to fail within 1000ms but took " + elapsed + "ms", elapsed < 1000, is(true));
    }

    /**
     * Tests that RANDOM strategy with timeout=0 (single attempt) tries all peers once and fails.
     *
     * <p>With timeout=0, each peer should be tried at most once, then fail with ReplicationGroupUnavailableException.
     */
    @Test
    void testRandomStrategySingleAttemptTriesAllPeersOnce() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        Set<String> calledPeers = ConcurrentHashMap.newKeySet();
        CountDownLatch constructorCallsDone = new CountDownLatch(3); // Constructor tries all 3 peers

        // All peers return EPERM (no leader).
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                any(GetLeaderRequest.class),
                anyLong())
        ).thenAnswer(invocation -> {
            InternalClusterNode target = invocation.getArgument(0);
            calledPeers.add(target.name());
            int count = callCount.incrementAndGet();
            // Signal when constructor's calls complete (first 3 calls).
            if (count <= 3) {
                constructorCallsDone.countDown();
            }
            return completedFuture(FACTORY.errorResponse()
                    .errorCode(RaftError.EPERM.getNumber())
                    .leaderId(null)
                    .build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Wait for constructor's refreshAndGetLeaderWithTerm() to try all peers.
        assertTrue(constructorCallsDone.await(5, TimeUnit.SECONDS),
                "Constructor's refreshAndGetLeaderWithTerm should try all peers");

        // Reset counters to measure only the test call.
        int callsBeforeTest = callCount.get();
        calledPeers.clear();
        callCount.set(0);

        // With timeout=0, should try each peer once and fail.
        CompletableFuture<Void> result = svc.refreshLeader(0);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class, 1, TimeUnit.SECONDS));

        // Verify each peer was tried exactly once for this call.
        assertThat("Should call exactly 3 peers, but got " + callCount.get() + " (calls before test: " + callsBeforeTest + ")",
                callCount.get(), is(3));
        assertThat("Should call all 3 unique peers", calledPeers.size(), is(3));
    }

    /**
     * Tests that RANDOM strategy succeeds when one peer returns the leader after other peers fail.
     */
    @Test
    void testRandomStrategySucceedsWhenOnePeerReturnsLeader() {
        AtomicInteger callCount = new AtomicInteger(0);

        when(messagingService.invoke(
                any(InternalClusterNode.class),
                any(GetLeaderRequest.class),
                anyLong())
        ).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();

            // First 2 calls return EPERM (no leader), third succeeds.
            if (count <= 2) {
                return completedFuture(FACTORY.errorResponse()
                        .errorCode(RaftError.EPERM.getNumber())
                        .leaderId(null)
                        .build());
            }

            return completedFuture(FACTORY.getLeaderResponse()
                    .leaderId(PeerId.fromPeer(NODES.get(0)).toString())
                    .currentTerm(CURRENT_TERM)
                    .build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        CompletableFuture<LeaderWithTerm> result = svc.refreshAndGetLeaderWithTerm(TIMEOUT);

        assertThat(result, willCompleteSuccessfully());

        LeaderWithTerm leaderWithTerm = result.join();
        assertThat(leaderWithTerm.leader().consistentId(), is(NODES.get(0).consistentId()));
        assertThat(leaderWithTerm.term(), is(CURRENT_TERM));
    }

    private static class TestResponse {
    }

    /**
     * Tests single-attempt mode (timeout=0) with 5 nodes and mixed errors.
     *
     * <p>In single-attempt mode, all errors are treated the same - each peer is tried once.
     * The request should fail after all 5 peers are tried.
     */
    @Test
    void testSingleAttemptModeWithMixedErrors() {
        AtomicInteger callCount = new AtomicInteger(0);
        Set<String> calledPeers = ConcurrentHashMap.newKeySet();

        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            InternalClusterNode target = invocation.getArgument(0);
            calledPeers.add(target.name());
            int count = callCount.incrementAndGet();

            // First 3 calls return "no leader", next 2 return EHOSTDOWN.
            if (count <= 3) {
                return completedFuture(FACTORY.errorResponse()
                        .errorCode(RaftError.EPERM.getNumber())
                        .leaderId(null)
                        .build());
            }
            return completedFuture(FACTORY.errorResponse()
                    .errorCode(RaftError.EHOSTDOWN.getNumber())
                    .build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService(FIVE_NODES);

        // With timeout=0, should try each peer once and fail.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 0);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class));

        // Verify each peer was tried exactly once.
        assertThat("Should call exactly 5 peers", callCount.get(), is(5));
        assertThat("Should call all 5 unique peers", calledPeers.size(), is(5));
    }

    /**
     * Tests leader-wait mode: 3 nodes return "no leader", then those same nodes succeed after leader election.
     *
     * <p>Key verification: "no leader" peers are NOT in unavailablePeers, so they CAN be retried
     * after leader notification arrives.
     */
    @Test
    void testLeaderWaitModeRetriesNoLeaderPeersAfterLeaderElection() throws Exception {
        List<String> calledPeers = new CopyOnWriteArrayList<>();
        AtomicInteger noLeaderResponseCount = new AtomicInteger(0);
        CountDownLatch allPeersTriedOnce = new CountDownLatch(3);

        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            InternalClusterNode target = invocation.getArgument(0);
            calledPeers.add(target.name());

            // First round: all 3 peers return "no leader".
            if (noLeaderResponseCount.get() < 3) {
                noLeaderResponseCount.incrementAndGet();
                allPeersTriedOnce.countDown();
                return completedFuture(FACTORY.errorResponse()
                        .errorCode(RaftError.EPERM.getNumber())
                        .leaderId(null)
                        .build());
            }

            // After leader election: succeed.
            return completedFuture(FACTORY.actionResponse().result(new TestResponse()).build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Start the command with infinite timeout.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), Long.MAX_VALUE);

        // Wait for all 3 peers to be tried once.
        assertTrue(allPeersTriedOnce.await(5, TimeUnit.SECONDS), "All peers should be tried once");

        // The result should NOT be complete yet - waiting for leader.
        assertThat(result.isDone(), is(false));

        // Simulate leader election.
        simulateLeaderElection(NODES.get(0), CURRENT_TERM);

        // Should eventually succeed (by retrying one of the "no leader" peers).
        assertThat(result, willCompleteSuccessfully());

        // Verify that at least one peer was called twice (first with "no leader", then success).
        long totalCalls = calledPeers.size();
        assertTrue(totalCalls > 3, "Should have more than 3 calls (some peers retried after leader election), got " + totalCalls);
    }

    /**
     * Tests that after leader election notification, the next request goes directly to the notified leader.
     *
     * <p>This verifies that the leader field is updated when leader election notification is received,
     * so subsequent requests don't waste time trying random peers.
     */
    @Test
    void testLeaderElectionNotificationUpdatesLeaderField() {
        Peer expectedLeader = NODES.get(1); // Use node 1 as the leader (not node 0)
        AtomicReference<String> firstCalledPeer = new AtomicReference<>();

        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            InternalClusterNode target = invocation.getArgument(0);
            firstCalledPeer.compareAndSet(null, target.name());
            return completedFuture(FACTORY.actionResponse().result(new TestResponse()).build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Simulate leader election notification for node 1.
        simulateLeaderElectionAndWait(expectedLeader, CURRENT_TERM);

        // Now run a command - it should go directly to the notified leader.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), Long.MAX_VALUE);
        assertThat(result, willCompleteSuccessfully());

        // Verify the first (and only) call went to the notified leader.
        assertEquals(expectedLeader.consistentId(), firstCalledPeer.get(),
                "After leader election notification, request should go directly to the notified leader");
    }

    /**
     * Tests single-attempt mode (timeout=0) with transient errors (EBUSY).
     *
     * <p>In single-attempt mode, each peer should be tried at most once, even for transient errors.
     * EBUSY should cause the executor to move to the next peer, not retry the same peer indefinitely.
     */
    @Test
    void testSingleAttemptModeWithTransientErrors() {
        AtomicInteger callCount = new AtomicInteger(0);
        Set<String> calledPeers = ConcurrentHashMap.newKeySet();

        // All peers return EBUSY (transient error).
        when(messagingService.invoke(
                any(InternalClusterNode.class),
                argThat(this::isTestWriteCommand),
                anyLong()
        )).thenAnswer(invocation -> {
            InternalClusterNode target = invocation.getArgument(0);
            calledPeers.add(target.name());
            callCount.incrementAndGet();
            return completedFuture(FACTORY.errorResponse()
                    .errorCode(RaftError.EBUSY.getNumber())
                    .build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // With timeout=0, should try each peer at most once and fail, not loop forever.
        CompletableFuture<Object> result = svc.run(testWriteCommand(), 0);

        assertThat(result, willThrow(ReplicationGroupUnavailableException.class, 2, TimeUnit.SECONDS));

        // Should have tried each peer at most once (3 peers).
        assertThat("Should call at most 3 peers, but got " + callCount.get(), callCount.get(), is(3));
        assertThat("Should call all 3 unique peers", calledPeers.size(), is(3));
    }

    /**
     * Tests that refreshAndGetLeaderWithTerm correctly tracks term progression.
     *
     * <p>Scenario:
     * <ol>
     *     <li>Leader election sets term=2, leader=node0</li>
     *     <li>refreshAndGetLeaderWithTerm returns term=5, leader=node1 → update cached leader</li>
     *     <li>refreshAndGetLeaderWithTerm returns stale term=4, leader=node2 → should NOT update</li>
     * </ol>
     *
     * <p>This tests that the fix tracks the highest term seen across all refreshAndGetLeaderWithTerm calls,
     * not just the term from the last leader election notification.
     */
    @Test
    void testRefreshAndGetLeaderWithTermTracksTermProgressionAcrossMultipleCalls() throws Exception {
        Peer initialLeader = NODES.get(0);
        Peer newerLeader = NODES.get(1);
        Peer staleLeader = NODES.get(2);

        // Track call count and signal when constructor's call completes.
        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch constructorCallDone = new CountDownLatch(1);

        when(messagingService.invoke(
                any(InternalClusterNode.class),
                any(GetLeaderRequest.class),
                anyLong())
        ).thenAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                // Constructor's call - return low term and signal completion.
                return completedFuture(FACTORY.getLeaderResponse()
                        .leaderId(PeerId.fromPeer(initialLeader).toString())
                        .currentTerm(1)
                        .build())
                        .whenComplete((r, e) -> constructorCallDone.countDown());
            } else if (count == 2) {
                // First test call - term=5 (new leader)
                return completedFuture(FACTORY.getLeaderResponse()
                        .leaderId(PeerId.fromPeer(newerLeader).toString())
                        .currentTerm(5)
                        .build());
            }
            // Subsequent calls - stale term=4 (should NOT update)
            return completedFuture(FACTORY.getLeaderResponse()
                    .leaderId(PeerId.fromPeer(staleLeader).toString())
                    .currentTerm(4)
                    .build());
        });

        PhysicalTopologyAwareRaftGroupService svc = startService();

        // Wait for constructor's call to complete.
        assertTrue(constructorCallDone.await(5, TimeUnit.SECONDS),
                "Constructor's refreshAndGetLeaderWithTerm should complete");

        // Set initial leader via leader election to establish a known baseline.
        // This sets term=2 in leaderAvailabilityState via leaderElectionListener.
        simulateLeaderElectionAndWait(initialLeader, 2);
        assertThat(svc.leader().consistentId(), is(initialLeader.consistentId()));

        // First call: term=5, leader=node1 (new leader, newer term)
        assertThat(svc.refreshAndGetLeaderWithTerm(TIMEOUT), willCompleteSuccessfully());
        assertThat("After term=5, leader should be node1",
                svc.leader().consistentId(), is(newerLeader.consistentId()));

        // Second call: stale term=4, leader=node2 (should NOT update because 4 < 5)
        assertThat(svc.refreshAndGetLeaderWithTerm(TIMEOUT), willCompleteSuccessfully());
        assertThat("After stale term=4, leader should still be node1 (not overwritten with stale info)",
                svc.leader().consistentId(), is(newerLeader.consistentId()));
    }
}
