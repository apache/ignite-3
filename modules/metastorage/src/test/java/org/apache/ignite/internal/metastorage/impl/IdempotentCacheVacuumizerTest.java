package org.apache.ignite.internal.metastorage.impl;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests for idempotency of {@link IdempotentCacheVacuumizer}.
 */
public class IdempotentCacheVacuumizerTest {
    private static final int TOUCH_COUNTER_CHANGE_TIMEOUT_MILLIS = 1_000;

    /**
     * Check that IdempotentCacheVacuumizer triggers vacuumization action.
     * <ol>
     *     <li>Ensure that until starting, vacuumizer will not trigger the vacuumization action.</li>
     *     <li>Start vacuumization triggering and verify that vacuumization action was called.</li>
     *     <li>Suspend vacuumization triggering and verify that vacuumization action calls were suspended.</li>
     *     <li>Start vacuumization triggering and verify that vacuumization action was called.</li>
     * </ol>
     * @throws Exception if Thread.sleep() was interrupted.
     */
    @Test
    public void testIdempotentCacheVacuumizer() throws Exception {
        AtomicInteger touchCounter =  new AtomicInteger(0);

        IdempotentCacheVacuumizer vacuumizer = new IdempotentCacheVacuumizer(
                "Node1",
                touchCounter::incrementAndGet,
                0,
                1,
                TimeUnit.MILLISECONDS
        );

        // Ensure that until starting, vacuumizer will not trigger the vacuumization action. It's a best-effort check.
        Thread.sleep(10);
        assertEquals(0, touchCounter.get());

        // Start vacuumization triggering and verify that vacuumization action was called.
        vacuumizer.startLocalVacuumizationTriggering();
        assertTrue(waitForCondition(
                () -> touchCounter.get() > 0,
                TOUCH_COUNTER_CHANGE_TIMEOUT_MILLIS)
        );

        // Suspend vacuumization triggering and verify that vacuumization action calls were suspended.
        vacuumizer.suspendLocalVacuumizationTriggering();
        int touchCounterAfterStopTriggered = touchCounter.get();
        assertTrue(waitForCondition(
                () -> touchCounter.get() == touchCounterAfterStopTriggered || touchCounter.get() == touchCounterAfterStopTriggered + 1,
                TOUCH_COUNTER_CHANGE_TIMEOUT_MILLIS)
        );

        // Start vacuumization triggering and verify that vacuumization action was called.
        vacuumizer.startLocalVacuumizationTriggering();
        assertTrue(waitForCondition(
                () -> touchCounter.get() > touchCounterAfterStopTriggered + 1,
                TOUCH_COUNTER_CHANGE_TIMEOUT_MILLIS)
        );
    }

    /**
     * Check that IdempotentCacheVacuumizer doesn't trigger vacuumization action after shutdown.
     * <ol>
     *     <li>Start vacuumization triggering and verify that vacuumization action was called.</li>
     *     <li>Shutdown the vacuumizer and check that action calls were stopped.</li>
     *     <li>Start the vacuumizer and check that it doesn't take any effect.</li>
     *     <li>Suspend vacuumization triggering and check that it doesn't take any effect.</li>
     * </ol>
     * @throws Exception if Thread.sleep() was interrupted.
     */
    @Test
    public void testIdempotentCacheVacuumizerAfterShutdown() throws Exception {
        AtomicInteger touchCounter =  new AtomicInteger(0);

        IdempotentCacheVacuumizer vacuumizer = new IdempotentCacheVacuumizer(
                "Node1",
                touchCounter::incrementAndGet,
                0,
                1,
                TimeUnit.MILLISECONDS
        );

        // Start vacuumization triggering and verify that vacuumization action was called.
        vacuumizer.startLocalVacuumizationTriggering();
        assertTrue(waitForCondition(
                () -> touchCounter.get() > 0,
                TOUCH_COUNTER_CHANGE_TIMEOUT_MILLIS)
        );

        // Shutdown the vacuumizer and check that action calls were stopped.
        vacuumizer.shutdown();
        int touchCounterAfterShutdown = touchCounter.get();
        Thread.sleep(10);
        assertTrue(touchCounter.get() == touchCounterAfterShutdown || touchCounter.get() == touchCounterAfterShutdown + 1);

        // Start the vacuumizer and check that it doesn't take any effect.
        vacuumizer.startLocalVacuumizationTriggering();
        Thread.sleep(10);
        assertTrue(touchCounter.get() == touchCounterAfterShutdown || touchCounter.get() == touchCounterAfterShutdown + 1);

        // Suspend vacuumization triggering and check that it doesn't take any effect.
        vacuumizer.suspendLocalVacuumizationTriggering();
        Thread.sleep(10);
        assertTrue(touchCounter.get() == touchCounterAfterShutdown || touchCounter.get() == touchCounterAfterShutdown + 1);
    }

    /**
     * Check that IdempotentCacheVacuumizer stops on exception in vacuumization action, but doesn't re-throw it to the outer environment.
     *
     * @throws Exception if Thread.sleep() was interrupted.
     */
    @Test
    public void testIdempotentCacheExceptionHandling() throws Exception {
        AtomicInteger touchCounter =  new AtomicInteger(0);

        Runnable vacuumizationActionStub = () -> {
            touchCounter.incrementAndGet();
            throw new NullPointerException();
        };

        IdempotentCacheVacuumizer vacuumizer = new IdempotentCacheVacuumizer(
                "Node1",
                vacuumizationActionStub,
                0,
                1,
                TimeUnit.MILLISECONDS
        );

        // Start vacuumization triggering and verify that vacuumization action was called once only.
        vacuumizer.startLocalVacuumizationTriggering();
        assertTrue(waitForCondition(
                () -> touchCounter.get() == 1,
                TOUCH_COUNTER_CHANGE_TIMEOUT_MILLIS)
        );

        Thread.sleep(10);
        assertEquals(1, touchCounter.get());
    }
}
