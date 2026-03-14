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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ExponentialBackoffTimeoutStrategy}.
 *
 * <p>Verifies the correctness of exponential timeout progression, the maximum timeout
 * ceiling, and optional jitter behavior. Tests use predictable integer arithmetic to
 * make expected values easy to verify by hand.
 */
public class ExponentialBackoffTimeoutStrategyTest {
    /** Initial timeout passed to {@link TimeoutStrategy#next(int)} as the starting value. */
    private static final int INITIAL_TIMEOUT = 20;

    /** Maximum timeout the strategy is allowed to produce. */
    private static final int MAX_TIMEOUT = 100;

    /** Backoff coefficient used by the default strategy instance. Doubles each timeout step. */
    private static final double BACKOFF_COEFFICIENT = 2.0;

    /** Strategy instance under test, recreated before each test. */
    private TimeoutStrategy timeoutStrategy;

    /**
     * Creates a fresh {@link ExponentialBackoffTimeoutStrategy} with {@link #MAX_TIMEOUT}
     * and {@link #BACKOFF_COEFFICIENT}, without jitter, before each test.
     */
    @BeforeEach
    void setUp() {
        timeoutStrategy = new ExponentialBackoffTimeoutStrategy(MAX_TIMEOUT, BACKOFF_COEFFICIENT);
    }

    /**
     * Verifies that a single call to {@link TimeoutStrategy#next(int)} returns
     * {@code currentTimeout * backoffCoefficient}.
     *
     * <p>This is the core contract of the exponential strategy — each step multiplies
     * the current timeout by the configured coefficient.
     */
    @Test
    void testGettingNextTimeout() {
        assertEquals(BACKOFF_COEFFICIENT * INITIAL_TIMEOUT, timeoutStrategy.next(INITIAL_TIMEOUT));
    }

    /**
     * Verifies that the timeout progression reaches {@link #MAX_TIMEOUT} within the
     * expected number of steps and does not exceed it on subsequent calls.
     *
     * <p>The upper bound on steps is computed from the coefficient and the ratio of
     * {@code MAX_TIMEOUT} to {@code INITIAL_TIMEOUT}. If the strategy fails to reach
     * the cap within this bound, the test fails with a descriptive message. Once the
     * cap is reached, a further call to {@link TimeoutStrategy#next(int)} must return
     * exactly {@link #MAX_TIMEOUT}.
     */
    @Test
    void testMaxTimeoutNotExceeded() {
        int maxSteps = 3;
        int steps = 0;

        int timeout = INITIAL_TIMEOUT;
        do {
            timeout  = timeoutStrategy.next(timeout);

            assertTrue(++steps <= maxSteps,
                    "Strategy did not reach MAX_TIMEOUT within expected steps, last timeout: " + timeout);
        } while (timeout < MAX_TIMEOUT);

        assertEquals(MAX_TIMEOUT, timeout);
        assertEquals(MAX_TIMEOUT, timeoutStrategy.next(timeout));
    }

    /**
     * Verifies that when jitter is enabled, the returned timeout falls within the
     * expected randomized range {@code [initialTimeout, initialTimeout * coefficient^2)}.
     *
     * <p>A separate strategy instance with jitter enabled is created for this test.
     * The lower bound confirms the jitter does not produce values below the initial
     * timeout; the upper bound confirms it does not jump more than two coefficient
     * steps in a single call.
     */
    @Test
    void testJitterApplying() {
        timeoutStrategy = new ExponentialBackoffTimeoutStrategy(MAX_TIMEOUT, BACKOFF_COEFFICIENT, true);

        int timeout = timeoutStrategy.next(INITIAL_TIMEOUT);
        assertTrue(
                INITIAL_TIMEOUT <= timeout && timeout < INITIAL_TIMEOUT * BACKOFF_COEFFICIENT * BACKOFF_COEFFICIENT,
                "Timeout is out of range: " + timeout
        );
    }
}
