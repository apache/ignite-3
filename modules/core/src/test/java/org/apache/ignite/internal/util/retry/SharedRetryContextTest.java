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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link SharedRetryContext}.
 *
 * <p>Verifies lazy initialization, sequential timeout progression, reset behavior,
 * and thread safety of concurrent updates. A deterministic {@link TestProgressiveTimeoutStrategy}
 * with a fixed multiplier is used to make expected timeout values easy to compute by hand.
 */
public class SharedRetryContextTest {
    /** Message included in exceptions thrown when an expected state is absent. */
    private static final String MISSING_STATE_MESSAGE = "TimeoutState is missing!";

    /**
     * Multiplier applied by {@link TestProgressiveTimeoutStrategy} on each step.
     * Used to compute expected timeout values in assertions.
     */
    private static final int MULTIPLYING_COEFFICIENT = 4;

    /** Initial timeout passed to the {@link SharedRetryContext} under test. */
    private static final int INITIAL_TIMEOUT = 20;

    /**
     * Maximum timeout configured in {@link TestProgressiveTimeoutStrategy}.
     * The progression is capped at this value.
     */
    private static final int MAX_TIMEOUT = 1_000;

    /** Retry context under test, recreated before each test. */
    private SharedRetryContext retryContext;

    /**
     * Creates a fresh {@link SharedRetryContext} with {@link #INITIAL_TIMEOUT} and
     * a {@link TestProgressiveTimeoutStrategy} before each test.
     */
    @BeforeEach
    void setUp() {
        retryContext = new SharedRetryContext(INITIAL_TIMEOUT, new TestProgressiveTimeoutStrategy());
    }

    /**
     * Verifies lazy initialization behavior of {@link SharedRetryContext}.
     *
     * <p>Before any call to {@link SharedRetryContext#updateAndGetState()}, {@link
     * SharedRetryContext#getState()} must return an empty {@link java.util.Optional}.
     * After the first update, the state must be present with {@link #INITIAL_TIMEOUT}
     * and attempt count {@code 1}.
     */
    @Test
    void testGettingState() {
        assertFalse(retryContext.getState().isPresent());

        retryContext.updateAndGetState();

        assertTrue(retryContext.getState().isPresent());

        TimeoutState state = retryContext.getState().get();

        assertEquals(INITIAL_TIMEOUT, state.getTimeout());
        assertEquals(1, state.getAttempt());
    }

    /**
     * Verifies that {@link SharedRetryContext#updateAndGetState()} returns the same
     * object reference as {@link SharedRetryContext#getState()}, and that the timeout
     * advances correctly after multiple calls.
     *
     * <p>After three updates, the expected timeout is
     * {@code INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT^2} with attempt count {@code 3},
     * reflecting that the first update returns the initial timeout and subsequent updates
     * apply the coefficient.
     */
    @Test
    void testUpdatingAndGettingState() {
        retryContext.updateAndGetState();
        retryContext.updateAndGetState();
        TimeoutState returnedState = retryContext.updateAndGetState();
        TimeoutState observedState = retryContext.getState().orElseThrow(() -> new IllegalStateException(MISSING_STATE_MESSAGE));

        assertSame(returnedState, observedState);

        checkRetryContextState(INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT * MULTIPLYING_COEFFICIENT, 3);
    }

    /**
     * Verifies that {@link SharedRetryContext#resetState()} clears the shared state,
     * causing {@link SharedRetryContext#getState()} to return an empty
     * {@link java.util.Optional} after the reset.
     */
    @Test
    void testResettingState() {
        retryContext.updateAndGetState();
        retryContext.updateAndGetState();
        retryContext.updateAndGetState();

        checkRetryContextState(INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT * MULTIPLYING_COEFFICIENT, 3);

        retryContext.resetState();

        assertFalse(retryContext.getState().isPresent());
    }

    /**
     * Verifies that concurrent calls to {@link SharedRetryContext#updateAndGetState()}
     * from multiple threads all succeed, and that the final state reflects exactly
     * {@code attemptsNumber} advancements.
     *
     * <p>Submits {@code attemptsNumber} tasks to a 5-thread pool, waits for all to
     * complete, then asserts that the timeout has reached {@link #MAX_TIMEOUT} and
     * the attempt count equals the number of submitted tasks.
     *
     * @throws Exception if the thread pool is interrupted during shutdown.
     */
    @Test
    @Timeout(value = 5, unit = SECONDS)
    void testConcurrentStateUpdating() throws Exception {
        ExecutorService threadPool = Executors.newFixedThreadPool(5);

        int attemptsNumber = 20;

        List<Future<TimeoutState>> futures = IntStream.range(0, attemptsNumber)
                .mapToObj(i -> threadPool.submit(() -> retryContext.updateAndGetState()))
                .collect(toList());

        try {
            futures.forEach(SharedRetryContextTest::getQuietly);

            checkRetryContextState(MAX_TIMEOUT, attemptsNumber);
        } finally {
            threadPool.shutdown();
            threadPool.awaitTermination(5, SECONDS);
        }
    }

    /**
     * Asserts that the shared retry context holds the expected timeout and attempt count.
     *
     * <p>Fails with {@link #MISSING_STATE_MESSAGE} if the state is absent.
     *
     * @param expectedTimeout  expected current timeout in milliseconds.
     * @param expectedAttempts expected current attempt count.
     */
    private void checkRetryContextState(int expectedTimeout, int expectedAttempts) {
        retryContext.getState().ifPresentOrElse(state -> {
            assertEquals(expectedTimeout, state.getTimeout());
            assertEquals(expectedAttempts, state.getAttempt());
        }, () -> {
            throw new IllegalStateException(MISSING_STATE_MESSAGE);
        });
    }

    /**
     * A deterministic {@link TimeoutStrategy} that multiplies the current timeout by
     * {@link #MULTIPLYING_COEFFICIENT} on each step, capped at {@link #MAX_TIMEOUT}.
     *
     * <p>Using integer multiplication rather than a floating-point coefficient avoids
     * rounding ambiguity, making expected values in test assertions exact and easy to
     * compute by hand.
     */
    private static class TestProgressiveTimeoutStrategy implements TimeoutStrategy {
        /**
         * {@inheritDoc}
         *
         * <p>Multiplies {@code currentTimeout} by {@link #MULTIPLYING_COEFFICIENT},
         * capped at {@link #MAX_TIMEOUT}.
         */
        @Override
        public int next(int currentTimeout) {
            return Math.min(currentTimeout * MULTIPLYING_COEFFICIENT, MAX_TIMEOUT);
        }

        /**
         * {@inheritDoc}
         *
         * <p>Returns {@link #MAX_TIMEOUT}.
         */
        @Override
        public int maxTimeout() {
            return MAX_TIMEOUT;
        }
    }

    /**
     * Waits for the given {@link Future} to complete and returns its result.
     *
     * <p>Wraps checked exceptions as {@link AssertionError} so they propagate cleanly
     * through {@link java.util.function.Consumer} lambdas in test code without requiring
     * explicit try-catch blocks.
     *
     * <ul>
     *     <li>{@link ExecutionException} — wraps the cause as an {@link AssertionError},
     *         preserving the original exception for diagnosis.</li>
     *     <li>{@link InterruptedException} — restores the interrupt flag and wraps
     *         as an {@link AssertionError}.</li>
     * </ul>
     *
     * @param <T>    the future's result type.
     * @param future the future to wait for.
     * @return the future's result.
     * @throws AssertionError if the future completed exceptionally or the thread was interrupted.
     */
    private static <T> T getQuietly(Future<T> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new AssertionError("Future completed exceptionally", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for future", e);
        }
    }
}
