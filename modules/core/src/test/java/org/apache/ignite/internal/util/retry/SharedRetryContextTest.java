package org.apache.ignite.internal.util.retry;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class SharedRetryContextTest {
    
    private static final String MISSING_STATE_MESSAGE = "TimeoutState is missing!";

    private static final int MULTIPLYING_COEFFICIENT = 4;

    private static final int INITIAL_TIMEOUT = 20;

    private static final int MAX_TIMEOUT = 1_000;

    private SharedRetryContext retryContext;

    @BeforeEach
    void setUp() {
        retryContext = new SharedRetryContext(INITIAL_TIMEOUT, new TestProgressiveTimeoutStrategy());
    }

    @Test
    void testGettingState() {
        assertFalse(retryContext.getState().isPresent());

        retryContext.updateAndGetState();

        assertTrue(retryContext.getState().isPresent());

        TimeoutState state = retryContext.getState().get();

        assertEquals(INITIAL_TIMEOUT, state.getTimeout());
        assertEquals(1, state.getAttempt());
    }

    @Test
    void testUpdatingAndGettingState() {
        retryContext.updateAndGetState();
        retryContext.updateAndGetState();
        TimeoutState returnedState = retryContext.updateAndGetState();
        TimeoutState observedState = retryContext.getState().orElseThrow(() -> new IllegalStateException(MISSING_STATE_MESSAGE));

        assertSame(returnedState, observedState);

        checkRetryContextState(INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT * MULTIPLYING_COEFFICIENT, 3);
    }

    @Test
    void testResettingState() {
        retryContext.updateAndGetState();
        retryContext.updateAndGetState();
        retryContext.updateAndGetState();

        checkRetryContextState(INITIAL_TIMEOUT * MULTIPLYING_COEFFICIENT * MULTIPLYING_COEFFICIENT, 3);

        retryContext.resetState();

        assertFalse(retryContext.getState().isPresent());
    }

    @Test
    @Timeout(value = 5, unit = SECONDS)
    void testConcurrentStateUpdating() throws Exception {
        ExecutorService threadPool = Executors.newFixedThreadPool(5);

        int attemptsNumber = 20;

        List<Future<TimeoutState>> futures = IntStream.range(0, attemptsNumber)
                .mapToObj(i -> threadPool.submit(() -> retryContext.updateAndGetState()))
                .collect(toList());

        try {
            futures.forEach(fut -> {
                try {
                    fut.get();
                }catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });

            checkRetryContextState(MAX_TIMEOUT, attemptsNumber);
        } finally {
            threadPool.shutdown();
            threadPool.awaitTermination(5, SECONDS);
        }
    }

    private void checkRetryContextState(int expectedTimeout, int expectedAttempts) {
        retryContext.getState().ifPresentOrElse(state -> {
            assertEquals(expectedTimeout, state.getTimeout());
            assertEquals(expectedAttempts, state.getAttempt());
        }, () -> {
            throw new IllegalStateException(MISSING_STATE_MESSAGE);
        });
    }

    private static class TestProgressiveTimeoutStrategy implements TimeoutStrategy {

        @Override
        public int next(int currentTimeout) {
            return Math.min((currentTimeout * MULTIPLYING_COEFFICIENT), MAX_TIMEOUT);
        }

        @Override
        public int maxTimeout() {
            return MAX_TIMEOUT;
        }
    }
}
