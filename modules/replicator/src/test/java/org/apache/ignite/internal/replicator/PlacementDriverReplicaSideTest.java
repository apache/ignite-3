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

import static java.lang.System.currentTimeMillis;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverReplicaMessage;
import org.apache.ignite.internal.raft.LeaderElectionListener;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupService;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.failure.FailureManagerExtension;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test for placement driver messages processing on replica side.
 */
@ExtendWith(FailureManagerExtension.class)
public class PlacementDriverReplicaSideTest extends BaseIgniteAbstractTest {
    private static final ReplicationGroupId GRP_ID = new TestReplicationGroupId("group_1");

    private static final InternalClusterNode LOCAL_NODE = new ClusterNodeImpl(
            randomUUID(),
            "name0",
            new NetworkAddress("localhost", 1234)
    );
    private static final InternalClusterNode ANOTHER_NODE = new ClusterNodeImpl(
            randomUUID(),
            "name`",
            new NetworkAddress("localhost", 2345)
    );

    private static final PlacementDriverMessagesFactory MSG_FACTORY = new PlacementDriverMessagesFactory();

    private Replica replica;

    private final AtomicReference<LeaderElectionListener> callbackHolder = new AtomicReference<>();

    private PendingComparableValuesTracker<Long, Void> storageIndexTracker;

    private final AtomicLong indexOnLeader = new AtomicLong(0);

    private Peer currentLeader = null;

    private int countOfTimeoutExceptionsOnReadIndexToThrow = 0;

    private Runnable reservationClosure;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            IgniteThreadFactory.create("common", "replica", log)
    );

    private Replica startReplica() {
        TopologyAwareRaftGroupService raftClient = mock(TopologyAwareRaftGroupService.class);

        when(raftClient.subscribeLeader(any())).thenAnswer(invocationOnMock -> {
            LeaderElectionListener callback = invocationOnMock.getArgument(0);
            callbackHolder.set(callback);

            return nullCompletedFuture();
        });

        when(raftClient.transferLeadership(any())).thenAnswer(invocationOnMock -> {
            Peer peer = invocationOnMock.getArgument(0);
            currentLeader = peer;

            return nullCompletedFuture();
        });

        when(raftClient.readIndex()).thenAnswer(invocationOnMock -> {
            if (countOfTimeoutExceptionsOnReadIndexToThrow > 0) {
                countOfTimeoutExceptionsOnReadIndexToThrow--;
                return failedFuture(new TimeoutException());
            } else {
                return completedFuture(indexOnLeader.get());
            }
        });

        when(raftClient.run(any())).thenAnswer(invocationOnMock -> nullCompletedFuture());

        var listener = mock(ReplicaListener.class);
        when(listener.raftClient()).thenReturn(raftClient);

        var placementDriver = new TestPlacementDriver(LOCAL_NODE);

        var placementDriverMessageProcessor = new PlacementDriverMessageProcessor(
                GRP_ID,
                LOCAL_NODE,
                placementDriver,
                new TestClockService(new HybridClockImpl()),
                (unused0, unused1) -> reservationClosure.run(),
                executor,
                storageIndexTracker,
                raftClient
        );

        return new ReplicaImpl(
                GRP_ID,
                listener,
                LOCAL_NODE,
                placementDriver,
                groupId -> null,
                null,
                placementDriverMessageProcessor
        );
    }

    @BeforeEach
    public void beforeEach() {
        storageIndexTracker = new PendingComparableValuesTracker<>(0L);
        indexOnLeader.set(1L);
        currentLeader = null;
        countOfTimeoutExceptionsOnReadIndexToThrow = 0;
        reservationClosure = () -> { };
        replica = startReplica();
    }

    @AfterEach
    void tearDown() {
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    /**
     * Imitates leader election for the group.
     *
     * @param leader The leader.
     */
    private void leaderElection(InternalClusterNode leader) {
        if (callbackHolder.get() != null) {
            callbackHolder.get().onLeaderElected(leader, 1L);
        }
    }

    /**
     * Imitates sending {@link org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessage} to the replica.
     *
     * @param leaseStartTime Lease start time.
     * @param leaseExpirationTime Lease expiration time.
     * @param force Force flag.
     * @return Future that is completed when replica sends a response.
     */
    private CompletableFuture<LeaseGrantedMessageResponse> sendLeaseGranted(
            HybridTimestamp leaseStartTime,
            HybridTimestamp leaseExpirationTime,
            boolean force
    ) {
        PlacementDriverReplicaMessage msg = MSG_FACTORY.leaseGrantedMessage()
                .groupId(GRP_ID)
                .leaseStartTime(leaseStartTime)
                .leaseExpirationTime(leaseExpirationTime)
                .force(force)
                .build();

        return replica.processPlacementDriverMessage(msg).thenApply(LeaseGrantedMessageResponse.class::cast);
    }

    private HybridTimestamp hts(long physical) {
        return new HybridTimestamp(currentTimeMillis() + physical * 1000, 0);
    }

    private void updateIndex(long index) {
        storageIndexTracker.update(index, null);
    }

    @Test
    public void replicationGroupReadinessAwait() {
        updateIndex(1L);
        CompletableFuture<LeaseGrantedMessageResponse> respFut = sendLeaseGranted(hts(10), hts(20), false);
        assertFalse(respFut.isDone());
        leaderElection(LOCAL_NODE);
        assertThat(respFut, willSucceedFast());
    }

    @Test
    public void replicationGroupReadinessAwaitAnotherNodeLeader() {
        CompletableFuture<LeaseGrantedMessageResponse> respFut = sendLeaseGranted(hts(10), hts(20), false);
        assertFalse(respFut.isDone());
        leaderElection(ANOTHER_NODE);
        assertThat(respFut, willSucceedFast());
    }

    @Test
    public void testGrantLeaseToLeader() {
        leaderElection(LOCAL_NODE);
        CompletableFuture<LeaseGrantedMessageResponse> respFut = sendLeaseGranted(hts(10), hts(20), false);

        updateIndex(1L);
        assertThat(respFut, willSucceedFast());

        LeaseGrantedMessageResponse resp = respFut.join();
        assertTrue(resp.accepted());
        assertNull(resp.redirectProposal());
    }

    @Test
    public void testGrantLeaseToNonLeader() {
        leaderElection(ANOTHER_NODE);
        CompletableFuture<LeaseGrantedMessageResponse> respFut = sendLeaseGranted(hts(10), hts(20), false);

        assertThat(respFut, willSucceedFast());

        LeaseGrantedMessageResponse resp = respFut.join();
        assertFalse(resp.accepted());
        assertEquals(ANOTHER_NODE.name(), resp.redirectProposal());
    }

    @Test
    public void testGrantLeaseRepeat() {
        long leaseStartTime = 10;
        leaderElection(ANOTHER_NODE);
        // Sending message with force == true.
        CompletableFuture<LeaseGrantedMessageResponse> respFut0 = sendLeaseGranted(hts(leaseStartTime), hts(leaseStartTime + 10), true);

        updateIndex(1L);
        assertThat(respFut0, willSucceedFast());

        LeaseGrantedMessageResponse resp0 = respFut0.join();
        assertTrue(resp0.accepted());
        assertNull(resp0.redirectProposal());

        // Sending the same message once again, with force == false (placement driver actor may have changed and the new lease interval
        // intersects with previous one).
        CompletableFuture<LeaseGrantedMessageResponse> respFut1 =
                sendLeaseGranted(hts(leaseStartTime + 8), hts(leaseStartTime + 18), false);

        assertThat(respFut1, willSucceedFast());

        LeaseGrantedMessageResponse resp1 = respFut1.join();
        assertFalse(resp1.accepted());
        assertEquals(ANOTHER_NODE.name(), resp1.redirectProposal());
    }

    @Test
    public void testGrantLeaseToNodeWithExpiredLease() {
        long leaseStartTime = 10;
        updateIndex(1L);
        leaderElection(LOCAL_NODE);
        CompletableFuture<LeaseGrantedMessageResponse> respFut0 = sendLeaseGranted(hts(leaseStartTime), hts(leaseStartTime + 10), false);

        assertThat(respFut0, willSucceedFast());

        LeaseGrantedMessageResponse resp0 = respFut0.join();
        assertTrue(resp0.accepted());
        assertNull(resp0.redirectProposal());

        CompletableFuture<LeaseGrantedMessageResponse> respFut1 =
                sendLeaseGranted(hts(leaseStartTime + 11), hts(leaseStartTime + 20), false);
        assertThat(respFut1, willSucceedFast());

        LeaseGrantedMessageResponse resp1 = respFut1.join();
        assertTrue(resp1.accepted());
        assertNull(resp1.redirectProposal());
    }

    @Test
    public void testGrantLeaseToNodeWithExpiredLeaseAndAnotherLeaderElected() {
        long leaseStartTime = 10;
        updateIndex(1L);
        leaderElection(LOCAL_NODE);
        CompletableFuture<LeaseGrantedMessageResponse> respFut0 = sendLeaseGranted(hts(leaseStartTime), hts(leaseStartTime + 10), false);

        assertThat(respFut0, willSucceedFast());

        LeaseGrantedMessageResponse resp0 = respFut0.join();
        assertTrue(resp0.accepted());
        assertNull(resp0.redirectProposal());

        leaderElection(ANOTHER_NODE);

        CompletableFuture<LeaseGrantedMessageResponse> respFut1 =
                sendLeaseGranted(hts(leaseStartTime + 11), hts(leaseStartTime + 20), false);
        assertThat(respFut1, willSucceedFast());

        LeaseGrantedMessageResponse resp1 = respFut1.join();
        assertFalse(resp1.accepted());
        assertEquals(ANOTHER_NODE.name(), resp1.redirectProposal());
    }

    @Test
    public void testForce() {
        long leaseStartTime = 10;
        leaderElection(ANOTHER_NODE);
        CompletableFuture<LeaseGrantedMessageResponse> respFut = sendLeaseGranted(hts(leaseStartTime), hts(leaseStartTime + 10), true);

        assertFalse(respFut.isDone());

        updateIndex(1L);
        assertThat(respFut, willSucceedFast());

        LeaseGrantedMessageResponse resp = respFut.join();
        assertTrue(resp.accepted());
        assertNull(resp.redirectProposal());

        // Replica should initiate the leadership transfer.
        assertEquals(LOCAL_NODE.name(), currentLeader.consistentId());
    }

    @Test
    public void testForceToActualLeader() {
        long leaseStartTime = 10;

        leaderElection(ANOTHER_NODE);
        CompletableFuture<LeaseGrantedMessageResponse> respFut0 = sendLeaseGranted(hts(leaseStartTime), hts(leaseStartTime + 10), false);

        assertThat(respFut0, willSucceedFast());

        LeaseGrantedMessageResponse resp0 = respFut0.join();
        assertFalse(resp0.accepted());
        assertEquals(ANOTHER_NODE.name(), resp0.redirectProposal());

        // After declining the lease grant, local node is elected as a leader and new message with force == true is sent to this
        // node as actual leader.
        leaderElection(LOCAL_NODE);

        updateIndex(1L);
        CompletableFuture<LeaseGrantedMessageResponse> respFut1 = sendLeaseGranted(hts(leaseStartTime), hts(leaseStartTime + 10), true);
        assertThat(respFut1, willSucceedFast());

        LeaseGrantedMessageResponse resp1 = respFut1.join();

        assertTrue(resp1.accepted());
        assertNull(resp1.redirectProposal());
    }

    @Test
    public void testLongReadIndexWait() {
        countOfTimeoutExceptionsOnReadIndexToThrow = 100;
        updateIndex(1L);
        leaderElection(LOCAL_NODE);
        CompletableFuture<LeaseGrantedMessageResponse> respFut0 = sendLeaseGranted(hts(1), hts(10), false);
        // Actually, it completes faster because TimeoutException is thrown from mock instantly.
        assertThat(respFut0, willSucceedIn(5, TimeUnit.SECONDS));
        assertEquals(0, countOfTimeoutExceptionsOnReadIndexToThrow);
    }

    @Test
    public void testReservationFailed() {
        long leaseStartTime = 10;

        reservationClosure = () -> {
            throw new ReplicaReservationFailedException("Test: replica reservation failed");
        };

        leaderElection(LOCAL_NODE);

        CompletableFuture<LeaseGrantedMessageResponse> respFut = sendLeaseGranted(hts(leaseStartTime), hts(leaseStartTime + 10), false);
        assertThat(respFut, willSucceedIn(5, TimeUnit.SECONDS));
        LeaseGrantedMessageResponse resp = respFut.join();
        assertFalse(resp.accepted());

        CompletableFuture<LeaseGrantedMessageResponse> respFutForce = sendLeaseGranted(hts(leaseStartTime), hts(leaseStartTime + 10), true);
        assertThat(respFutForce, willSucceedIn(5, TimeUnit.SECONDS));
        LeaseGrantedMessageResponse respForce = respFutForce.join();
        // Force lease grant should also fail because of exception on replica side.
        assertFalse(respForce.accepted());
    }
}
