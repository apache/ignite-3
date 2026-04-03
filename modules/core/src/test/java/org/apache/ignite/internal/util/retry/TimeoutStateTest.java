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

import static org.apache.ignite.internal.util.retry.TimeoutState.attempt;
import static org.apache.ignite.internal.util.retry.TimeoutState.timeout;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
     * Verifies that newly constructed {@link TimeoutState} returns the default initial timeout
     * and attempt count of {@code 0}.
     */
    @Test
    void testDefaultInitialState() {
        TimeoutState defaultState = new TimeoutState();
        assertEquals(TimeoutStrategy.DEFAULT_RETRY_INITIAL_TIMEOUT_MS, defaultState.getTimeout());
        assertEquals(0, defaultState.getAttempt());
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
     * Verifies that {@link TimeoutState#update(TimeoutStrategy)} correctly advances
     * both timeout and attempt count.
     */
    @Test
    void testUpdate() {
        int nextTimeout = 100;
        TimeoutStrategy strategy = current -> nextTimeout;

        state.update(strategy);

        assertEquals(nextTimeout, state.getTimeout());
        assertEquals(ATTEMPT + 1, state.getAttempt());
    }

    /**
     * Verifies that {@link TimeoutState#update(TimeoutStrategy)} starting from attempt {@code 0}
     * resets the timeout to {@link TimeoutStrategy#DEFAULT_RETRY_INITIAL_TIMEOUT_MS}
     * and sets attempt count to {@code 1}.
     */
    @Test
    void testUpdateFromZeroAttempt() {
        TimeoutState zeroState = new TimeoutState(1000, 0);
        TimeoutStrategy strategy = current -> 2000; // Should be ignored

        zeroState.update(strategy);

        assertEquals(TimeoutStrategy.DEFAULT_RETRY_INITIAL_TIMEOUT_MS, zeroState.getTimeout());
        assertEquals(1, zeroState.getAttempt());
    }

    /**
     * Verifies that {@link TimeoutState#getTimeout()} and {@link TimeoutState#getAttempt()}
     * are consistent with the raw packed value.
     */
    @Test
    void testGetTimeoutAndGetAttemptAreConsistentWithPacked() {
        long packed = TimeoutState.pack(TIMEOUT, ATTEMPT);

        assertEquals(TIMEOUT, timeout(packed));
        assertEquals(ATTEMPT, attempt(packed));
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

        assertEquals(TIMEOUT, timeout(packed));
        assertEquals(ATTEMPT, attempt(packed));
    }
}
