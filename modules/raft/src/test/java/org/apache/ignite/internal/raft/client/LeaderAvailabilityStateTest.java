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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.deriveUuidFrom;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.raft.client.LeaderAvailabilityState.State;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LeaderAvailabilityState} state machine.
 */
public class LeaderAvailabilityStateTest extends BaseIgniteAbstractTest {

    /** Initial state should be {@link State#WAITING_FOR_LEADER} with term -1. */
    @Test
    void testInitialState() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();

        assertEquals(State.WAITING_FOR_LEADER, state.currentState());
        assertEquals(-1, state.currentTerm());
    }

    /** {@link LeaderAvailabilityState#awaitLeader()} returns incomplete future when no leader elected. */
    @Test
    void testAwaitLeaderWhenNoLeader() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();

        CompletableFuture<Long> future = state.awaitLeader();

        assertFalse(future.isDone(), "Future should not be completed when no leader");
    }

    /**
     * {@link LeaderAvailabilityState#onLeaderElected(InternalClusterNode, long)} transitions to {@link State#LEADER_AVAILABLE} state and
     * completes waiting futures.
     * {@link LeaderAvailabilityState#awaitLeader()} returns completed future when leader is available.
     */
    @Test
    void testAwaitLeader() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leaderNode = createNode("leader-node");

        long expectedTerm = 1;

        // No leader present. The future will wait.
        CompletableFuture<Long> waiter = state.awaitLeader();
        assertFalse(waiter.isDone());

        state.onLeaderElected(leaderNode, expectedTerm);

        // Verify leader is available after onLeaderElected.
        assertEquals(State.LEADER_AVAILABLE, state.currentState());
        assertEquals(expectedTerm, state.currentTerm());

        assertTrue(waiter.isDone());
        assertEquals(expectedTerm, waiter.join());

        // Leader is present. The future is completed.
        CompletableFuture<Long> future = state.awaitLeader();

        assertTrue(future.isDone(), "Future should be completed when leader available");
        assertEquals(expectedTerm, future.join());

        // Verify the state has not changed and we see exactly the same results as previously.
        assertEquals(State.LEADER_AVAILABLE, state.currentState());
        assertEquals(expectedTerm, state.currentTerm());
    }

    /** Stale term notifications are ignored. */
    @Test
    void testStaleTermIgnored() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leaderNode1 = createNode("leader-1");
        InternalClusterNode leaderNode2 = createNode("leader-2");

        long expectedTerm = 5;

        state.onLeaderElected(leaderNode1, expectedTerm);
        assertEquals(expectedTerm, state.currentTerm());

        // Stale notification with lower term should be ignored.
        state.onLeaderElected(leaderNode2, 3);
        assertEquals(expectedTerm, state.currentTerm());
    }

    /** Equal term notifications are ignored. */
    @Test
    void testEqualTermIgnored() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leaderNode = createNode("leader");

        long expectedTerm = 5;

        state.onLeaderElected(leaderNode, expectedTerm);
        assertEquals(expectedTerm, state.currentTerm());

        // Same term should be ignored.
        state.onLeaderElected(leaderNode, expectedTerm);
        assertEquals(expectedTerm, state.currentTerm());
    }

    /** Higher term updates current term. */
    @Test
    void testHigherTermUpdates() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leaderNode1 = createNode("leader-1");
        InternalClusterNode leaderNode2 = createNode("leader-2");

        state.onLeaderElected(leaderNode1, 1);
        assertEquals(1, state.currentTerm());

        state.onLeaderElected(leaderNode2, 5);
        assertEquals(5, state.currentTerm());
        assertEquals(State.LEADER_AVAILABLE, state.currentState());
    }

    /**
     * {@link LeaderAvailabilityState#onGroupUnavailable(long)} transitions back to WAITING_FOR_LEADER.
     */
    @Test
    void testOnGroupUnavailableTransition() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leaderNode = createNode("leader-node");

        state.onLeaderElected(leaderNode, 1);
        assertEquals(State.LEADER_AVAILABLE, state.currentState());

        state.onGroupUnavailable(1);
        assertEquals(State.WAITING_FOR_LEADER, state.currentState());
    }

    /**
     * {@link LeaderAvailabilityState#onGroupUnavailable(long)} ignored if term changed.
     */
    @Test
    void testOnGroupUnavailableIgnoredIfTermChanged() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leaderNode = createNode("leader-node");

        state.onLeaderElected(leaderNode, 1);
        state.onLeaderElected(leaderNode, 2);
        assertEquals(State.LEADER_AVAILABLE, state.currentState());
        assertEquals(2, state.currentTerm());

        // Should be ignored because term changed from 1 to 2
        state.onGroupUnavailable(1);
        assertEquals(State.LEADER_AVAILABLE, state.currentState());
    }

    /**
     * {@link LeaderAvailabilityState#onGroupUnavailable(long)} has no effect when already in {@link State#WAITING_FOR_LEADER}.
     */
    @Test
    void testOnGroupUnavailableWhenAlreadyWaiting() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();

        // Already in WAITING_FOR_LEADER, should have no effect
        state.onGroupUnavailable(0);
        assertEquals(State.WAITING_FOR_LEADER, state.currentState());
    }

    /**
     * After {@link LeaderAvailabilityState#onGroupUnavailable(long)}, new waiters get fresh future.
     */
    @Test
    void testNewWaitersAfterUnavailable() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leaderNode = createNode("leader-node");

        state.onLeaderElected(leaderNode, 1);
        CompletableFuture<Long> future1 = state.awaitLeader();
        assertTrue(future1.isDone());

        state.onGroupUnavailable(1);

        CompletableFuture<Long> future2 = state.awaitLeader();
        assertFalse(future2.isDone(), "New waiter should get incomplete future after unavailable");
    }

    /** Multiple waiters are all completed on leader election. */
    @Test
    void testMultipleWaitersCompleted() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leaderNode = createNode("leader-node");

        CompletableFuture<Long> waiter1 = state.awaitLeader();
        CompletableFuture<Long> waiter2 = state.awaitLeader();
        CompletableFuture<Long> waiter3 = state.awaitLeader();

        assertFalse(waiter1.isDone());
        assertFalse(waiter2.isDone());
        assertFalse(waiter3.isDone());

        state.onLeaderElected(leaderNode, 10);

        assertTrue(waiter1.isDone());
        assertTrue(waiter2.isDone());
        assertTrue(waiter3.isDone());

        assertEquals(10L, waiter1.join());
        assertEquals(10L, waiter2.join());
        assertEquals(10L, waiter3.join());
    }

    /** Term 0 is accepted as first valid term. */
    @Test
    void testTermZeroAccepted() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leaderNode = createNode("leader");

        assertEquals(-1, state.currentTerm());

        state.onLeaderElected(leaderNode, 0);

        assertEquals(0, state.currentTerm());
        assertEquals(State.LEADER_AVAILABLE, state.currentState());
    }

    /**
     * Negative terms are rejected with {@link IllegalArgumentException}.
     */
    @Test
    void testNegativeTermRejected() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leaderNode = createNode("leader");

        assertEquals(-1, state.currentTerm());

        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> state.onLeaderElected(leaderNode, -1)
        );

        assertEquals("Term must be non-negative: -1", thrown.getMessage());
        assertEquals(State.WAITING_FOR_LEADER, state.currentState());
        assertEquals(-1, state.currentTerm());
    }

    /** Concurrent leader elections with different terms. */
    @Test
    void testConcurrentLeaderElections() throws Exception {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicLong maxTerm = new AtomicLong(-1);

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int term = i;
            threads[i] = new Thread(() -> {
                try {
                    startLatch.await();
                    InternalClusterNode leader = createNode("leader-" + term);
                    state.onLeaderElected(leader, term);
                    maxTerm.updateAndGet(current -> Math.max(current, term));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            threads[i].start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS));

        // The final term should be the maximum term that was set
        assertEquals(threadCount - 1, state.currentTerm());
    }

    /** State machine handles rapid state transitions. */
    @Test
    void testRapidStateTransitions() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leader = createNode("leader");

        for (int i = 0; i < 100; i++) {
            state.onLeaderElected(leader, i);
            assertEquals(State.LEADER_AVAILABLE, state.currentState());
            assertEquals(i, state.currentTerm());

            state.onGroupUnavailable(i);
            assertEquals(State.WAITING_FOR_LEADER, state.currentState());
        }
    }

    /**
     * {@link LeaderAvailabilityState#awaitLeader()} returns same future for multiple calls when waiting.
     */
    @Test
    void testAwaitLeaderReturnsSameFuture() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();

        CompletableFuture<Long> future1 = state.awaitLeader();
        CompletableFuture<Long> future2 = state.awaitLeader();

        // Should return the same future instance when in WAITING_FOR_LEADER state
        assertSame(future1, future2, "Should return same future instance when waiting");
    }

    /**
     * {@link LeaderAvailabilityState#awaitLeader()} returns new completed future for multiple calls when leader available.
     */
    @Test
    void testAwaitLeaderReturnsNewCompletedFuture() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leader = createNode("leader");

        state.onLeaderElected(leader, 1);

        CompletableFuture<Long> future1 = state.awaitLeader();
        CompletableFuture<Long> future2 = state.awaitLeader();

        // Both should be completed
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
    }

    /** Stop completes waiting futures exceptionally. */
    @Test
    void testStopCompletesExceptionally() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        RuntimeException testException = new RuntimeException("Test shutdown");

        CompletableFuture<Long> waiter = state.awaitLeader();
        assertFalse(waiter.isDone());

        state.stop(testException);

        assertTrue(waiter.isDone());
        assertTrue(waiter.isCompletedExceptionally());

        CompletionException thrown = assertThrows(CompletionException.class, waiter::join);
        assertSame(testException, thrown.getCause());
    }

    /** Stop completes multiple waiters exceptionally. */
    @Test
    void testStopCompletesMultipleWaiters() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        RuntimeException testException = new RuntimeException("Test shutdown");

        // All waiters share the same future
        CompletableFuture<Long> waiter1 = state.awaitLeader();
        CompletableFuture<Long> waiter2 = state.awaitLeader();
        CompletableFuture<Long> waiter3 = state.awaitLeader();

        assertFalse(waiter1.isDone());
        assertFalse(waiter2.isDone());
        assertFalse(waiter3.isDone());

        state.stop(testException);

        assertTrue(waiter1.isCompletedExceptionally());
        assertTrue(waiter2.isCompletedExceptionally());
        assertTrue(waiter3.isCompletedExceptionally());
    }

    /** Stop causes subsequent awaitLeader to fail immediately. */
    @Test
    void testStopFailsSubsequentWaiters() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        RuntimeException testException = new RuntimeException("Test shutdown");

        CompletableFuture<Long> waiter1 = state.awaitLeader();
        state.stop(testException);

        assertTrue(waiter1.isCompletedExceptionally());

        // New waiter should immediately fail with the same exception
        CompletableFuture<Long> waiter2 = state.awaitLeader();
        assertTrue(waiter2.isDone(), "New waiter should be immediately completed");
        assertTrue(waiter2.isCompletedExceptionally());

        CompletionException thrown = assertThrows(CompletionException.class, waiter2::join);
        assertSame(testException, thrown.getCause());
    }

    /** Stop when no waiters are pending still marks state as stopped. */
    @Test
    void testStopWhenNoWaiters() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        RuntimeException testException = new RuntimeException("Test shutdown");

        // Stop without any waiters - should not throw
        state.stop(testException);

        assertTrue(state.stopped());

        // Subsequent awaitLeader should fail
        CompletableFuture<Long> waiter = state.awaitLeader();
        assertTrue(waiter.isDone());
        assertTrue(waiter.isCompletedExceptionally());

        CompletionException thrown = assertThrows(CompletionException.class, waiter::join);
        assertSame(testException, thrown.getCause());
    }

    /** Stop when leader is available fails subsequent awaitLeader. */
    @Test
    void testStopWhenLeaderAvailable() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leader = createNode("leader");
        RuntimeException testException = new RuntimeException("Test shutdown");

        state.onLeaderElected(leader, 1);
        assertEquals(State.LEADER_AVAILABLE, state.currentState());

        state.stop(testException);

        assertTrue(state.stopped());

        // awaitLeader should fail after stop, even though leader was available
        CompletableFuture<Long> waiter = state.awaitLeader();
        assertTrue(waiter.isDone());
        assertTrue(waiter.isCompletedExceptionally());

        CompletionException thrown = assertThrows(CompletionException.class, waiter::join);
        assertSame(testException, thrown.getCause());
    }

    /** Multiple stop calls are idempotent. */
    @Test
    void testMultipleStopCalls() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        RuntimeException exception1 = new RuntimeException("Shutdown 1");
        RuntimeException exception2 = new RuntimeException("Shutdown 2");

        CompletableFuture<Long> waiter1 = state.awaitLeader();
        state.stop(exception1);
        assertTrue(waiter1.isCompletedExceptionally());

        // Second stop call should be ignored
        state.stop(exception2);

        // Subsequent awaitLeader should fail with the FIRST exception
        CompletableFuture<Long> waiter2 = state.awaitLeader();
        assertTrue(waiter2.isDone());

        CompletionException thrown = assertThrows(CompletionException.class, waiter2::join);
        assertSame(exception1, thrown.getCause(), "Should use exception from first stop call");
    }

    /**
     * {@link LeaderAvailabilityState#onLeaderElected(InternalClusterNode, long)} is ignored after stop.
     */
    @Test
    void testOnLeaderElectedIgnoredAfterStop() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leader = createNode("leader");
        RuntimeException testException = new RuntimeException("Test shutdown");

        CompletableFuture<Long> waiter1 = state.awaitLeader();
        state.stop(testException);
        assertTrue(waiter1.isCompletedExceptionally());

        // Leader election after stop should be ignored
        state.onLeaderElected(leader, 5);

        // State should still be stopped.
        assertTrue(state.stopped());

        // New waiter should still fail
        CompletableFuture<Long> waiter2 = state.awaitLeader();
        assertTrue(waiter2.isDone());
        assertTrue(waiter2.isCompletedExceptionally());
    }

    /**
     * {@link LeaderAvailabilityState#onGroupUnavailable(long)} is ignored after stop.
     */
    @Test
    void testOnGroupUnavailableIgnoredAfterStop() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        InternalClusterNode leader = createNode("leader");
        RuntimeException testException = new RuntimeException("Test shutdown");

        state.onLeaderElected(leader, 1);
        assertEquals(State.LEADER_AVAILABLE, state.currentState());

        state.stop(testException);

        // Group unavailable after stop should be ignored
        state.onGroupUnavailable(1);

        assertTrue(state.stopped());
    }

    /** Stop preserves exception type. */
    @Test
    void testStopPreservesExceptionType() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();
        IllegalStateException testException = new IllegalStateException("Component stopping");

        CompletableFuture<Long> waiter = state.awaitLeader();
        state.stop(testException);

        CompletionException thrown = assertThrows(CompletionException.class, waiter::join);
        assertThat(thrown.getCause(), instanceOf(IllegalStateException.class));
        assertEquals("Component stopping", thrown.getCause().getMessage());
    }

    /**
     * {@link LeaderAvailabilityState#stopped()} returns false before stop and true after.
     */
    @Test
    void testStopped() {
        LeaderAvailabilityState state = new LeaderAvailabilityState();

        assertFalse(state.stopped());

        state.stop(new RuntimeException("Test"));

        assertTrue(state.stopped());
    }

    private static InternalClusterNode createNode(String name) {
        return new ClusterNodeImpl(deriveUuidFrom(name), name, new NetworkAddress("localhost", 123));
    }
}
