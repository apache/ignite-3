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

public class KeyBasedRetryContextTest {

    private static final String MISSING_STATE_MESSAGE = "TimeoutState is missing!";

    private static final int MULTIPLYING_COEFFICIENT = 4;

    private static final int INITIAL_TIMEOUT = 20;

    private static final int MAX_TIMEOUT = 1_000;

    private static final int REGISTRY_SIZE_LIMIT = 1_000;

    private static final String KEY = "key";

    private static final String OTHER_KEY = "other-key";

    private KeyBasedRetryContext retryContext;

    @BeforeEach
    void setUp() {
        retryContext = new KeyBasedRetryContext(INITIAL_TIMEOUT, new TestProgressiveTimeoutStrategy());
    }

    @Test
    void testGettingState() {
        assertFalse(retryContext.getState(KEY).isPresent());

        retryContext.updateAndGetState(KEY);

        assertTrue(retryContext.getState(KEY).isPresent());

        TimeoutState state = retryContext.getState(KEY).get();

        assertEquals(INITIAL_TIMEOUT, state.getTimeout());
        assertEquals(1, state.getAttempt());
    }

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

    @Test
    void testStatesAreIsolatedPerKey() {
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);

        retryContext.updateAndGetState(OTHER_KEY);

        checkRetryContextState(KEY, INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT * MULTIPLYING_COEFFICIENT, 3);
        checkRetryContextState(OTHER_KEY, INITIAL_TIMEOUT, 1);
    }

    @Test
    void testResettingState() {
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);

        checkRetryContextState(KEY, INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT * MULTIPLYING_COEFFICIENT, 3);

        retryContext.resetState(KEY);

        assertFalse(retryContext.getState(KEY).isPresent());
    }

    @Test
    void testResettingStateDoesNotAffectOtherKeys() {
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(KEY);
        retryContext.updateAndGetState(OTHER_KEY);

        retryContext.resetState(KEY);

        assertFalse(retryContext.getState(KEY).isPresent());
        checkRetryContextState(OTHER_KEY, INITIAL_TIMEOUT, 1);
    }

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

    private void checkRetryContextState(String key, int expectedTimeout, int expectedAttempts) {
        retryContext.getState(key).ifPresentOrElse(state -> {
            assertEquals(expectedTimeout, state.getTimeout());
            assertEquals(expectedAttempts, state.getAttempt());
        }, () -> {
            throw new IllegalStateException(MISSING_STATE_MESSAGE);
        });
    }

    private static class TestProgressiveTimeoutStrategy implements TimeoutStrategy {

        @Override
        public int next(int currentTimeout) {
            return Math.min(currentTimeout * MULTIPLYING_COEFFICIENT, MAX_TIMEOUT);
        }

        @Override
        public int maxTimeout() {
            return MAX_TIMEOUT;
        }
    }
}
