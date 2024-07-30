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

package org.apache.ignite.internal.metastorage.cache;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for idempotency of {@link IdempotentCacheVacuumizer}.
 */
public class IdempotentCacheVacuumizerTest extends BaseIgniteAbstractTest {
    private static final int TOUCH_COUNTER_CHANGE_TIMEOUT_MILLIS = 1_000;

    private ScheduledExecutorService scheduler;

    private ClockService clocService;

    @BeforeEach
    public void setup() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        clocService = new TestClockService(new TestHybridClock(() -> 1L));
    }

    @AfterEach
    public void tearDown() {
        scheduler.shutdown();
    }

    /**
     * Check that IdempotentCacheVacuumizer triggers vacuumization action.
     * <ol>
     *     <li>Ensure that until starting, vacuumizer will not trigger the vacuumization action.</li>
     *     <li>Start vacuumization triggering and verify that vacuumization action was called.</li>
     *     <li>Suspend vacuumization triggering and verify that vacuumization action calls were suspended.</li>
     *     <li>Start vacuumization triggering and verify that vacuumization action was called.</li>
     * </ol>
     *
     * @throws Exception if Thread.sleep() was interrupted.
     */
    @Test
    public void testIdempotentCacheVacuumizer() throws Exception {
        AtomicInteger touchCounter = new AtomicInteger(0);

        IdempotentCacheVacuumizer vacuumizer = new IdempotentCacheVacuumizer(
                "Node1",
                scheduler,
                ignored -> touchCounter.incrementAndGet(),
                0,
                clocService,
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
     *     <li>Shutdown the vacuumizer scheduler and check that action calls were stopped.</li>
     *     <li>Start the vacuumizer and check that it doesn't take any effect.</li>
     *     <li>Suspend vacuumization triggering and check that it doesn't take any effect.</li>
     * </ol>
     *
     * @throws Exception if Thread.sleep() was interrupted.
     */
    @Test
    public void testIdempotentCacheVacuumizerAfterShutdown() throws Exception {
        AtomicInteger touchCounter = new AtomicInteger(0);

        IdempotentCacheVacuumizer vacuumizer = new IdempotentCacheVacuumizer(
                "Node1",
                scheduler,
                ignored -> touchCounter.incrementAndGet(),
                0,
                clocService,
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

        // Shutdown the vacuumizer scheduler and check that action calls were stopped.
        scheduler.shutdown();
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
     * Check that IdempotentCacheVacuumizer doesn't stops on exception in vacuumization action doesn't re-throw it to the outer environment
     * but logs an exception with WARN.
     *
     * @throws Exception if Thread.sleep() was interrupted.
     */
    @Test
    public void testIdempotentCacheExceptionHandling() throws Exception {
        AtomicInteger touchCounter = new AtomicInteger(0);

        Consumer<HybridTimestamp> vacuumizationActionStub = ignored -> {
            touchCounter.incrementAndGet();
            throw new IllegalStateException();
        };

        IdempotentCacheVacuumizer vacuumizer = new IdempotentCacheVacuumizer(
                "Node1",
                scheduler,
                vacuumizationActionStub,
                0,
                clocService,
                0,
                1,
                TimeUnit.MILLISECONDS
        );

        // Start vacuumization triggering and verify that vacuumization actions were not stopped after exception.
        vacuumizer.startLocalVacuumizationTriggering();

        assertTrue(waitForCondition(
                () -> touchCounter.get() > 1,
                TOUCH_COUNTER_CHANGE_TIMEOUT_MILLIS)
        );

    }
}
