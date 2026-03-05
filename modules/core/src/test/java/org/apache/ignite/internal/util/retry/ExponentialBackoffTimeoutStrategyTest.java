package org.apache.ignite.internal.util.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExponentialBackoffTimeoutStrategyTest {

    private static final int INITIAL_TIMEOUT = 20;

    private static final int MAX_TIMEOUT = 100;

    private static final double BACKOFF_COEFFICIENT = 2.0;

    private TimeoutStrategy timeoutStrategy;

    @BeforeEach
    void setUp() {
        timeoutStrategy = new ExponentialBackoffTimeoutStrategy(MAX_TIMEOUT, BACKOFF_COEFFICIENT);
    }

    @Test
    void testGettingNextTimeout() {
        assertEquals(BACKOFF_COEFFICIENT * INITIAL_TIMEOUT, timeoutStrategy.next(INITIAL_TIMEOUT));
    }

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
