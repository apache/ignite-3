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

package org.apache.ignite.internal.util.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TimeoutState}.
 *
 * <p>Verifies the correctness of initial state construction, atomic CAS updates,
 * stale-snapshot rejection, and the consistency between the raw packed {@code long}
 * and the individual accessor methods.
 */
public class TimeoutStateTest {
    /** Timeout value used to construct the shared {@link TimeoutState} instance. */
    private static final int TIMEOUT = 20;

    /** Attempt count used to construct the shared {@link TimeoutState} instance. */
    private static final int ATTEMPT = 10;

    /** Shared state instance recreated before each test. */
    private TimeoutState state;

    /**
     * Creates a fresh {@link TimeoutState} with {@link #TIMEOUT} and {@link #ATTEMPT}
     * before each test to ensure full isolation.
     */
    @BeforeEach
    void setUp() {
        state = new TimeoutState(TIMEOUT, ATTEMPT);
    }

    /**
     * Verifies that a newly constructed {@link TimeoutState} returns the timeout and
     * attempt values it was initialized with.
     */
    @Test
    void testInitialState() {
        assertEquals(TIMEOUT, state.getTimeout());
        assertEquals(ATTEMPT, state.getAttempt());
    }

    /**
     * Verifies that {@link TimeoutState#update(long, int, int)} succeeds when the
     * provided snapshot matches the current internal state, and that both fields
     * are updated atomically.
     */
    @Test
    void testUpdate() {
        int newTimeout = 40;
        int newAttempt = 11;

        long raw = state.getRawState();
        boolean updated = state.update(raw, newTimeout, newAttempt);

        assertTrue(updated);
        assertEquals(newTimeout, state.getTimeout());
        assertEquals(newAttempt, state.getAttempt());
    }

    /**
     * Verifies that {@link TimeoutState#update(long, int, int)} rejects an update
     * when the snapshot is stale — i.e., the internal state has been modified by
     * another call since the snapshot was taken.
     *
     * <p>After a successful update advances the state, a second update using the
     * original snapshot must return {@code false} and leave the state unchanged
     * at the first update's values.
     */
    @Test
    void testUpdateFailsOnStaleSnapshot() {
        long staleSnapshot = state.getRawState();

        // advance state so staleSnapshot is no longer current
        state.update(staleSnapshot, 40, 11);

        // update with stale snapshot must fail
        boolean updated = state.update(staleSnapshot, 80, 12);

        assertFalse(updated);
        // state must remain at the first update's values
        assertEquals(40, state.getTimeout());
        assertEquals(11, state.getAttempt());
    }

    /**
     * Verifies that {@link TimeoutState#getTimeout()} and {@link TimeoutState#getAttempt()}
     * are consistent with the raw packed value returned by {@link TimeoutState#getRawState()}.
     *
     * <p>This confirms that the pack/unpack bit manipulation is symmetric and that
     * callers who take a raw snapshot and decompose it manually get the same result
     * as callers who use the individual accessors.
     */
    @Test
    void testGetTimeoutAndGetAttemptAreConsistentWithRawState() {
        long raw = state.getRawState();

        assertEquals(state.getTimeout(), TimeoutState.timeout(raw));
        assertEquals(state.getAttempt(), TimeoutState.attempt(raw));
    }

    /**
     * Verifies that {@link TimeoutState#pack(int, int)} followed by
     * {@link TimeoutState#timeout(long)} and {@link TimeoutState#attempt(long)}
     * recovers the original values exactly.
     *
     * <p>Tests the bit-level correctness of the packing scheme independently of
     * the {@link TimeoutState} object lifecycle.
     */
    @Test
    void testPackUnpackRoundtrip() {
        long packed = TimeoutState.pack(TIMEOUT, ATTEMPT);

        assertEquals(TIMEOUT, TimeoutState.timeout(packed));
        assertEquals(ATTEMPT, TimeoutState.attempt(packed));
    }
}
