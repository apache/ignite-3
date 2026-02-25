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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Peer;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * State machine for tracking leader availability.
 *
 * <p>Manages transitions between two states:
 * <ul>
 *     <li>{@link State#WAITING_FOR_LEADER} - No leader is currently known, operations must wait</li>
 *     <li>{@link State#LEADER_AVAILABLE} - A leader is known, operations can proceed</li>
 * </ul>
 *
 * <p>State transitions:
 * <pre>
 *     WAITING_FOR_LEADER --[updateKnownLeaderAndTerm(non-null peer)]--> LEADER_AVAILABLE
 *     LEADER_AVAILABLE --[onGroupUnavailable]--> WAITING_FOR_LEADER
 *     Any state --[stop]--> stopped (terminal state)
 * </pre>
 *
 * <p>This class is the single source of truth for the current leader peer and term.
 *
 * <p>Thread-safe: all public methods are synchronized or use proper synchronization.
 */
@VisibleForTesting
class LeaderAvailabilityState {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaderAvailabilityState.class);

    /** Synchronization mutex. */
    private final Object mutex = new Object();

    /** Possible states of leader availability. */
    enum State {
        /** No leader is known, waiting for leader election. */
        WAITING_FOR_LEADER,
        /** A leader is available and operations can proceed. */
        LEADER_AVAILABLE
    }

    /** Current state. Guarded by {@code mutex}. */
    private State currentState = State.WAITING_FOR_LEADER;

    /** Current leader term. Initialized to -1 to accept term 0 as the first valid term. Guarded by {@code mutex}. */
    private long currentTerm = -1;

    /** Current leader peer. Guarded by {@code mutex}. */
    private @Nullable Peer leader;

    /** Future that waiters block on. Guarded by {@code mutex}. */
    private CompletableFuture<Long> waiters = new CompletableFuture<>();

    /** Whether the state machine has been stopped. Guarded by {@code mutex}. */
    private boolean stopped = false;

    /** Exception used to fail futures after destruction. Guarded by {@code mutex}. */
    private Throwable stopException;

    /**
     * Returns a future that completes when a leader becomes available.
     *
     * <p>If a leader is already available, returns an already-completed future with the current term.
     * Otherwise, returns a future that will complete when {@link #updateKnownLeaderAndTerm} is called.
     * If the state machine has been destroyed, returns a failed future.
     *
     * @return Future that completes with the leader term when a leader is available.
     */
    CompletableFuture<Long> awaitLeader() {
        synchronized (mutex) {
            if (stopped) {
                return CompletableFuture.failedFuture(stopException);
            }
            if (currentState == State.LEADER_AVAILABLE) {
                return CompletableFuture.completedFuture(currentTerm);
            }
            return waiters;
        }
    }

    /**
     * Updates the known leader and term.
     *
     * <p>If the new term is greater than the current term:
     * <ul>
     *     <li>Updates the term and stores the peer.</li>
     *     <li>If peer is non-null: transitions from {@link State#WAITING_FOR_LEADER} to {@link State#LEADER_AVAILABLE}
     *         and completes waiters.</li>
     *     <li>If peer is null: updates the term only, does NOT transition state or wake waiters.</li>
     * </ul>
     * Stale notifications (with term &lt;= current) are ignored.
     * Has no effect if the state machine has been stopped.
     *
     * @param leader The leader peer (may be {@code null} if only the term is known).
     * @param term The term of the leader.
     * @return {@code true} if accepted (term is newer), {@code false} if ignored (stale or stopped).
     */
    boolean updateKnownLeaderAndTerm(@Nullable Peer leader, long term) {
        if (term < 0) {
            throw new IllegalArgumentException("Term must be non-negative: " + term);
        }

        CompletableFuture<Long> futureToComplete = null;

        synchronized (mutex) {
            if (stopped) {
                LOG.debug("Ignoring leader update after stop [leader={}, term={}]", leader, term);
                return false;
            }

            // Ignore stale term notifications.
            if (term <= currentTerm) {
                LOG.debug("Ignoring stale leader [newTerm={}, currentTerm={}]", term, currentTerm);
                return false;
            }

            long previousTerm = currentTerm;
            State previousState = currentState;

            currentTerm = term;
            this.leader = leader;

            if (leader != null && currentState == State.WAITING_FOR_LEADER) {
                currentState = State.LEADER_AVAILABLE;
                futureToComplete = waiters;
            }

            LOG.debug("Leader updated [leader={}, term={}, previousTerm={}, stateChange={}->{}]",
                    leader, term, previousTerm, previousState, currentState);
        }

        // Complete outside the lock to avoid potential deadlocks with future callbacks.
        if (futureToComplete != null) {
            futureToComplete.complete(term);
        }

        return true;
    }

    /**
     * Updates the cached leader without changing the term or availability state.
     *
     * <p>This is an optimistic cache update used when a response or redirect provides leader information
     * without a term change. Has no effect after stop.
     *
     * @param leader The leader peer hint (may be {@code null} to clear).
     */
    void setLeaderHint(@Nullable Peer leader) {
        synchronized (mutex) {
            if (stopped) {
                return;
            }

            this.leader = leader;
        }
    }

    /**
     * Returns the current leader peer.
     *
     * @return Current leader or {@code null} if unknown.
     */
    @Nullable Peer leader() {
        synchronized (mutex) {
            return leader;
        }
    }

    /**
     * Handles group unavailability notification.
     *
     * <p>Transitions from {@link State#LEADER_AVAILABLE} to {@link State#WAITING_FOR_LEADER}
     * only if the term hasn't changed since the unavailability was detected. This prevents
     * resetting the state if a new leader was already elected.
     *
     * <p>Note: does NOT clear the leader field — intentionally allows retry to the old leader as a first guess.
     *
     * <p>Has no effect if the state machine has been stopped.
     *
     * @param termWhenDetected The term at which unavailability was detected.
     */
    void onGroupUnavailable(long termWhenDetected) {
        synchronized (mutex) {
            if (stopped) {
                return;
            }

            // Only transition if we're still at the same term (no new leader elected in the meantime).
            if (currentTerm == termWhenDetected && currentState == State.LEADER_AVAILABLE) {
                State previousState = currentState;

                currentState = State.WAITING_FOR_LEADER;
                waiters = new CompletableFuture<>();

                LOG.debug("Group unavailable [term={}, stateChange={}->{}]",
                        termWhenDetected, previousState, currentState);
            }
        }
    }

    /**
     * Returns the current leader term.
     *
     * @return Current leader term (-1 if no leader has been elected yet).
     */
    long currentTerm() {
        synchronized (mutex) {
            return currentTerm;
        }
    }

    /**
     * Returns the current state.
     *
     * @return Current state.
     */
    State currentState() {
        synchronized (mutex) {
            return currentState;
        }
    }

    /**
     * Returns whether the state machine has been stopped.
     *
     * @return True if stopped.
     */
    boolean stopped() {
        synchronized (mutex) {
            return stopped;
        }
    }

    /**
     * Stops the state machine, cancelling all pending waiters with the given exception.
     *
     * <p>After stop, the state machine is no longer active:
     * <ul>
     *     <li>{@link #awaitLeader()} returns a failed future</li>
     *     <li>{@link #updateKnownLeaderAndTerm} is ignored</li>
     *     <li>{@link #setLeaderHint} is ignored</li>
     *     <li>{@link #onGroupUnavailable} is ignored</li>
     * </ul>
     *
     * <p>Called during shutdown to ensure waiters don't hang indefinitely.
     *
     * @param exception The exception to complete waiters with.
     */
    void stop(Throwable exception) {
        CompletableFuture<Long> futureToCancel;

        synchronized (mutex) {
            if (stopped) {
                return;
            }

            stopped = true;
            stopException = exception;
            futureToCancel = waiters;
        }

        // Complete outside the lock to avoid potential deadlocks.
        futureToCancel.completeExceptionally(exception);
    }
}
