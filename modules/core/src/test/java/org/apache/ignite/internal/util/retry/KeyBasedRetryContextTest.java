package org.apache.ignite.internal.util.retry;


import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link KeyBasedRetryContext}.
 *
 * <p>Verifies per-key state isolation, sequential timeout progression, reset behavior,
 * registry size limit enforcement, fallback state for overflow keys, and thread safety
 * under concurrent updates to both the same and different keys.
 *
 * <p>A deterministic {@link TestProgressiveTimeoutStrategy} with a fixed multiplier is
 * used to make expected timeout values easy to compute by hand.
 */
public class KeyBasedRetryContextTest {
    /** Message included in exceptions thrown when an expected state is absent. */
    private static final String MISSING_STATE_MESSAGE = "TimeoutState is missing!";

    /**
     * Multiplier applied by {@link TestProgressiveTimeoutStrategy} on each step.
     * Used to compute expected timeout values in assertions.
     */
    private static final int MULTIPLYING_COEFFICIENT = 4;

    /** Initial timeout passed to the {@link KeyBasedRetryContext} under test. */
    private static final int INITIAL_TIMEOUT = 20;

    /**
     * Maximum timeout configured in {@link TestProgressiveTimeoutStrategy}.
     * The progression is capped at this value.
     */
    private static final int MAX_TIMEOUT = 1_000;

    /**
     * Registry size limit mirrored from {@link KeyBasedRetryContext}.
     * Used in tests that fill the registry to verify fallback behavior.
     */
    private static final int REGISTRY_SIZE_LIMIT = 1_000;

    /** Primary key used in single-key tests. */
    private static final String KEY = "key";

    /** Secondary key used in isolation and reset tests. */
    private static final String OTHER_KEY = "other-key";

    /** Retry context under test, recreated before each test. */
    private KeyBasedRetryContext retryContext;

    /**
     * Creates a fresh {@link KeyBasedRetryContext} with {@link #INITIAL_TIMEOUT} and
     * a {@link TestProgressiveTimeoutStrategy} before each test.
     */
    @BeforeEach
    void setUp() {
        retryContext = new KeyBasedRetryContext(INITIAL_TIMEOUT, new TestProgressiveTimeoutStrategy());
    }

    /**
     * Verifies that {@link KeyBasedRetryContext#getState(String)} returns an empty
     * {@link java.util.Optional} for an untracked key, and that after the first call
     * to {@link KeyBasedRetryContext#updateAndGetState(String)}, the state is present
     * with {@link #INITIAL_TIMEOUT} and attempt count {@code 1}.
     */
    @Test
    void testGettingState() {
        assertFalse(retryContext.getState(KEY).isPresent());

        retryContext.updateAndGetState(KEY);

        assertTrue(retryContext.getState(KEY).isPresent());

        TimeoutState state = retryContext.getState(KEY).get();

        assertEquals(INITIAL_TIMEOUT, state.getTimeout());
        assertEquals(1, state.getAttempt());
    }

    /**
     * Verifies that {@link KeyBasedRetryContext#updateAndGetState(String)} returns the
     * same object reference as {@link KeyBasedRetryContext#getState(String)}, and that
     * the timeout advances correctly after multiple calls for the same key.
     *
     * <p>After three updates, the expected timeout is
     * {@code INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT^2} with attempt count {@code 3}.
     */
    @Test
    void testUpdatingAndGettingState() {
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);
        TimeoutState returnedState = retryContext.updateAndGetState(KEY);
        TimeoutState observedState = retryContext.getState(KEY)
                .orElseThrow(() -> new IllegalStateException(MISSING_STATE_MESSAGE));

        assertSame(returnedState, observedState);

        checkRetryContextState(KEY, INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT * MULTIPLYING_COEFFICIENT, 3);
    }

    /**
     * Verifies that each key maintains its own independent backoff progression.
     *
     * <p>Advances {@link #KEY} three times and {@link #OTHER_KEY} once, then asserts
     * that each key holds the timeout and attempt count corresponding only to its own
     * update history, with no cross-key interference.
     */
    @Test
    void testStatesAreIsolatedPerKey() {
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);

        retryContext.updateAndGetState(OTHER_KEY);

        checkRetryContextState(KEY, INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT * MULTIPLYING_COEFFICIENT, 3);
        checkRetryContextState(OTHER_KEY, INITIAL_TIMEOUT, 1);
    }

    /**
     * Verifies that {@link KeyBasedRetryContext#resetState(String)} removes the state
     * for the given key, causing {@link KeyBasedRetryContext#getState(String)} to return
     * an empty {@link java.util.Optional} after the reset.
     */
    @Test
    void testResettingState() {
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);

        checkRetryContextState(KEY, INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT * MULTIPLYING_COEFFICIENT, 3);

        retryContext.resetState(KEY);

        assertFalse(retryContext.getState(KEY).isPresent());
    }

    /**
     * Verifies that resetting one key does not affect the state of other keys.
     *
     * <p>Advances {@link #KEY} twice and {@link #OTHER_KEY} once, resets {@link #KEY},
     * then asserts that {@link #KEY} is absent while {@link #OTHER_KEY} retains its state.
     */
    @Test
    void testResettingStateDoesNotAffectOtherKeys() {
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(OTHER_KEY);

        retryContext.resetState(KEY);

        assertFalse(retryContext.getState(KEY).isPresent());
        checkRetryContextState(OTHER_KEY, INITIAL_TIMEOUT, 1);
    }

    /**
     * Verifies that when the registry is at capacity, a new key passed to
     * {@link KeyBasedRetryContext#updateAndGetState(String)} receives the fallback
     * {@link TimeoutState} with {@link #MAX_TIMEOUT} and attempt {@code -1}.
     */
    @Test
    void testFallbackWhenRegistryIsFull() {
        // Fill registry to the limit
        IntStream.range(0, REGISTRY_SIZE_LIMIT)
                .mapToObj(i -> "key-" + i)
                .forEach(retryContext::updateAndGetState);

        // New key should get fallback
        TimeoutState state = retryContext.updateAndGetState("new-key-beyond-limit");

        assertEquals(MAX_TIMEOUT, state.getTimeout());
        assertEquals(-1, state.getAttempt());
    }

    /**
     * Verifies that a key already tracked in the registry continues to progress normally
     * even when the registry has reached its size limit.
     *
     * <p>This guards against the regression where the overflow check incorrectly
     * blocked updates for existing keys rather than only for new ones.
     */
    @Test
    void testExistingKeyStillProgressesWhenRegistryIsFull() {
        retryContext.updateAndGetState(KEY);

        // Fill registry to the limit with other keys
        IntStream.range(0, REGISTRY_SIZE_LIMIT - 1)
                .mapToObj(i -> "key-" + i)
                .forEach(retryContext::updateAndGetState);

        // Existing key should still progress normally
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);

        checkRetryContextState(KEY, INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT * MULTIPLYING_COEFFICIENT, 3);
    }

    /**
     * Verifies that {@link KeyBasedRetryContext#getState(String)} returns the fallback
     * {@link TimeoutState} (with {@link #MAX_TIMEOUT} and attempt {@code -1}) for a key
     * that is not tracked when the registry is full.
     */
    @Test
    void testGetStateReturnsFallbackWhenRegistryIsFull() {
        IntStream.range(0, REGISTRY_SIZE_LIMIT)
                .mapToObj(i -> "key-" + i)
                .forEach(retryContext::updateAndGetState);

        Optional<TimeoutState> state = retryContext.getState("new-key-beyond-limit");

        assertTrue(state.isPresent());
        assertEquals(MAX_TIMEOUT, state.get().getTimeout());
        assertEquals(-1, state.get().getAttempt());
    }

    /**
     * Verifies that concurrent calls to {@link KeyBasedRetryContext#updateAndGetState(String)}
     * for the same key from multiple threads all succeed, and that the final state reflects
     * exactly {@code attemptsNumber} advancements.
     *
     * <p>This exercises the per-key locking in {@link java.util.concurrent.ConcurrentHashMap#compute}
     * and the CAS loop inside {@link TimeoutState}.
     *
     * @throws Exception if the thread pool is interrupted during shutdown.
     */
    @Test
    @Timeout(value = 5, unit = SECONDS)
    void testConcurrentStateUpdatingSameKey() throws Exception {
        ExecutorService threadPool = Executors.newFixedThreadPool(5);

        int attemptsNumber = 20;

        List<Future<TimeoutState>> futures = IntStream.range(0, attemptsNumber)
                .mapToObj(i -> threadPool.submit(() -> retryContext.updateAndGetState(KEY)))
                .collect(toList());

        try {
            futures.forEach(fut -> {
                try {
                    fut.get();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });

            checkRetryContextState(KEY, MAX_TIMEOUT, attemptsNumber);
        } finally {
            threadPool.shutdown();
            threadPool.awaitTermination(5, SECONDS);
        }
    }

    /**
     * Verifies that concurrent calls to {@link KeyBasedRetryContext#updateAndGetState(String)}
     * for different keys do not interfere with each other.
     *
     * <p>Each key is updated exactly once from a dedicated task. After all tasks complete,
     * each key must hold {@link #INITIAL_TIMEOUT} and attempt count {@code 1}, confirming
     * no cross-key state contamination under concurrency.
     *
     * @throws Exception if the thread pool is interrupted during shutdown.
     */
    @Test
    @Timeout(value = 5, unit = SECONDS)
    void testConcurrentStateUpdatingDifferentKeys() throws Exception {
        ExecutorService threadPool = Executors.newFixedThreadPool(5);

        List<String> keys = List.of("key-1", "key-2", "key-3", "key-4", "key-5");

        List<Future<TimeoutState>> futures = keys.stream()
                .map(key -> threadPool.submit(() -> retryContext.updateAndGetState(key)))
                .collect(toList());

        try {
            futures.forEach(fut -> {
                try {
                    fut.get();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });

            keys.forEach(key -> checkRetryContextState(key, INITIAL_TIMEOUT, 1));
        } finally {
            threadPool.shutdown();
            threadPool.awaitTermination(5, SECONDS);
        }
    }

    /**
     * Asserts that the retry context holds the expected timeout and attempt count for
     * the given key.
     *
     * <p>Fails with {@link #MISSING_STATE_MESSAGE} if the state is absent.
     *
     * @param key              the key whose state to check.
     * @param expectedTimeout  expected current timeout in milliseconds.
     * @param expectedAttempts expected current attempt count.
     */
    private void checkRetryContextState(String key, int expectedTimeout, int expectedAttempts) {
        retryContext.getState(key).ifPresentOrElse(state -> {
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
}
