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

package org.apache.ignite.internal.distributionzones;

import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.createZoneManagerExecutor;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.lang.IgniteTriConsumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Test scenarios for the distribution zone schedulers.
 */
public class DistributionZonesSchedulersTest {
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZonesSchedulersTest.class);

    private static final int STRIPE_0 = 0;

    private static final int STRIPE_1 = 1;

    private static StripedScheduledThreadPoolExecutor executor = createZoneManagerExecutor(
            5,
            new NamedThreadFactory("test-dst-zones-scheduler", LOG)
    );

    @AfterAll
    public static void afterAll() {
        shutdownAndAwaitTermination(executor, 2, TimeUnit.SECONDS);
    }

    @Test
    void testScaleUpSchedule() throws InterruptedException {
        ZoneState state = new ZoneState(executor);

        testSchedule(state::rescheduleScaleUp);
    }

    @Test
    void testScaleDownSchedule() throws InterruptedException {
        ZoneState state = new ZoneState(executor);

        testSchedule(state::rescheduleScaleDown);
    }

    private static void testSchedule(IgniteTriConsumer<Long, Runnable, Integer> fn) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        assertEquals(1L, latch.getCount());

        fn.accept(0L, latch::countDown, STRIPE_0);

        latch.await();

        assertEquals(0L, latch.getCount());
    }

    @Test
    void testScaleUpReScheduling() throws InterruptedException {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testReScheduling(state::rescheduleScaleUp);
    }

    @Test
    void testScaleDownReScheduling() throws InterruptedException {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testReScheduling(state::rescheduleScaleDown);
    }

    /**
     * Tests that scaleUp/scaleDown tasks with a zero delay will not be canceled by other tasks.
     * Tests that scaleUp/scaleDown tasks with a delay grater then zero will be canceled by other tasks.
     */
    private static void testReScheduling(IgniteTriConsumer<Long, Runnable, Integer> fn) throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();

        CountDownLatch firstTaskLatch = new CountDownLatch(1);

        CountDownLatch lastTaskLatch = new CountDownLatch(1);

        fn.accept(
                0L,
                () -> {
                    try {
                        assertTrue(firstTaskLatch.await(3, TimeUnit.SECONDS));

                        counter.incrementAndGet();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                },
                STRIPE_0
        );

        fn.accept(1L, counter::incrementAndGet, STRIPE_0);

        fn.accept(0L, counter::incrementAndGet, STRIPE_0);

        fn.accept(0L, counter::incrementAndGet, STRIPE_0);

        fn.accept(1L, counter::incrementAndGet, STRIPE_0);

        fn.accept(1L, counter::incrementAndGet, STRIPE_0);

        fn.accept(0L, counter::incrementAndGet, STRIPE_0);

        fn.accept(
                0L,
                () -> {
                    counter.incrementAndGet();

                    lastTaskLatch.countDown();
                },
                STRIPE_0
        );

        firstTaskLatch.countDown();

        assertTrue(lastTaskLatch.await(3, TimeUnit.SECONDS));

        assertEquals(5, counter.get());
    }

    @Test
    void testScaleUpOrdering() throws InterruptedException {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testOrdering(state::rescheduleScaleUp);
    }

    @Test
    void testScaleDownOrdering() throws InterruptedException {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testOrdering(state::rescheduleScaleDown);
    }

    private static void testOrdering(IgniteTriConsumer<Long, Runnable, Integer> fn) throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();

        CountDownLatch latch = new CountDownLatch(1);

        AtomicBoolean sequentialOrder = new AtomicBoolean(true);

        fn.accept(
                0L,
                () -> {
                    try {
                        assertTrue(latch.await(3, TimeUnit.SECONDS));

                        counter.incrementAndGet();

                        if (counter.get() != 1) {
                            sequentialOrder.set(false);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                },
                STRIPE_0
        );

        for (int i = 2; i < 11; i++) {
            int j = i;

            fn.accept(
                    0L,
                    () -> {
                        counter.incrementAndGet();

                        if (counter.get() != j) {
                            sequentialOrder.set(false);
                        }
                    },
                    STRIPE_0
            );
        }

        latch.countDown();

        waitForCondition(() -> counter.get() == 10, 3000);
        assertEquals(10, counter.get(), "Not all tasks were executed.");

        assertTrue(sequentialOrder.get(), "The order of tasks execution is not sequential.");
    }

    @Test
    void testScaleUpReScheduleWhenTaskIsEnded() throws InterruptedException {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testReScheduleWhenTaskIsEnded(state::rescheduleScaleUp);
    }

    @Test
    void testScaleDownReScheduleWhenTaskIsEnded() throws InterruptedException {
        ZoneState state = new ZoneState(executor);

        testReScheduleWhenTaskIsEnded(state::rescheduleScaleUp);
    }

    private static void testReScheduleWhenTaskIsEnded(IgniteTriConsumer<Long, Runnable, Integer> fn) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        AtomicBoolean flag = new AtomicBoolean();

        assertEquals(1L, latch.getCount());

        fn.accept(0L, latch::countDown, STRIPE_0);

        latch.await(1000, TimeUnit.MILLISECONDS);

        fn.accept(0L, () -> flag.set(true), STRIPE_0);

        assertTrue(waitForCondition(() -> 0L == latch.getCount(), 1500));
        assertTrue(waitForCondition(flag::get, 1500));
    }

    @Test
    void testCancelScaleUpTaskOnStopScaleUp() {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testCancelTask(state::rescheduleScaleUp, state::stopScaleUp, () -> state.scaleUpTask().isCancelled());
    }

    @Test
    void testCancelScaleDownTaskOnStopScaleDown() {
        ZoneState state = new ZoneState(executor);

        testCancelTask(state::rescheduleScaleDown, state::stopScaleDown, () -> state.scaleDownTask().isCancelled());
    }

    @Test
    void testCancelScaleUpTasksOnStopTimers() {
        ZoneState state = new ZoneState(executor);

        testCancelTask(state::rescheduleScaleUp, state::stopTimers, () -> state.scaleUpTask().isCancelled());
    }

    @Test
    void testCancelScaleDownTasksOnStopTimers() {
        ZoneState state = new ZoneState(executor);

        testCancelTask(state::rescheduleScaleDown, state::stopTimers, () -> state.scaleDownTask().isCancelled());
    }

    /**
     * {@link ZoneState#stopScaleUp()}, {@link ZoneState#stopScaleDown()} and {@link ZoneState#stopTimers()} cancel task
     * if it is not started and has a delay greater than zero.
     */
    private static void testCancelTask(
            IgniteTriConsumer<Long, Runnable, Integer> fn,
            Runnable stopTask,
            Supplier<Boolean> isTaskCancelled
    ) {
        CountDownLatch latch = new CountDownLatch(1);

        fn.accept(
                0L,
                () -> {
                    try {
                        assertTrue(latch.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                },
                STRIPE_0
        );

        fn.accept(1L, () -> {}, STRIPE_0);

        assertFalse(isTaskCancelled.get());

        stopTask.run();

        assertTrue(isTaskCancelled.get());

        latch.countDown();
    }

    @Test
    void testNotCancelScaleUpTaskOnStopScaleUp() {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testNotCancelTask(state::rescheduleScaleUp, state::stopScaleUp, () -> state.scaleUpTask().isCancelled());
    }

    @Test
    void testNotCancelScaleDownTaskOnStopScaleDown() {
        ZoneState state = new ZoneState(executor);

        testNotCancelTask(state::rescheduleScaleDown, state::stopScaleDown, () -> state.scaleDownTask().isCancelled());

    }

    /**
     * {@link ZoneState#stopScaleUp()} and {@link ZoneState#stopScaleDown()} doesn't cancel task
     * if it is not started and has a delay equal to zero.
     */
    private static void testNotCancelTask(
            IgniteTriConsumer<Long, Runnable, Integer> fn,
            Runnable stopTask,
            Supplier<Boolean> isTaskCancelled
    ) {
        CountDownLatch latch = new CountDownLatch(1);

        fn.accept(
                0L,
                () -> {
                    try {
                        assertTrue(latch.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                },
                STRIPE_0
        );

        fn.accept(0L, () -> {}, STRIPE_0);

        assertFalse(isTaskCancelled.get());

        stopTask.run();

        assertFalse(isTaskCancelled.get());

        latch.countDown();
    }

    /**
     * {@link ZoneState#stopTimers()} cancel task if it is not started and has a delay equal to zero.
     */
    @Test
    public void testCancelTasksOnStopTimersAndImmediateTimerValues() {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        CountDownLatch scaleUpTaskLatch = new CountDownLatch(1);

        state.rescheduleScaleUp(
                0L,
                () -> {
                    try {
                        assertTrue(scaleUpTaskLatch.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                },
                STRIPE_0
        );

        state.rescheduleScaleUp(0L, () -> {}, STRIPE_0);

        assertFalse(state.scaleUpTask().isCancelled());

        CountDownLatch scaleDownTaskLatch = new CountDownLatch(1);

        state.rescheduleScaleDown(
                0L,
                () -> {
                    try {
                        assertTrue(scaleDownTaskLatch.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                },
                STRIPE_0
        );

        state.rescheduleScaleDown(0L, () -> {}, STRIPE_0);

        assertFalse(state.scaleDownTask().isCancelled());

        state.stopTimers();

        assertTrue(state.scaleUpTask().isCancelled());
        assertTrue(state.scaleDownTask().isCancelled());

        scaleUpTaskLatch.countDown();
        scaleDownTaskLatch.countDown();
    }

    /**
     * Check that two tasks for different zones can be run concurrently.
     */
    @Test
    public void testTasksForTwoZonesDoesNotBlockEachOther() throws Exception {
        ZoneState state0 = new DistributionZoneManager.ZoneState(executor);
        ZoneState state1 = new DistributionZoneManager.ZoneState(executor);

        CountDownLatch taskLatch0 = new CountDownLatch(1);
        CountDownLatch taskLatch1 = new CountDownLatch(1);

        state0.rescheduleScaleUp(
                0L,
                () -> {
                    taskLatch0.countDown();

                    try {
                        // Wait when the second task is started.
                        assertTrue(taskLatch1.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                },
                STRIPE_0
        );

        // Wait when the first task is started.
        assertTrue(taskLatch0.await(3, TimeUnit.SECONDS));

        state1.rescheduleScaleUp(0L, () -> {}, STRIPE_1);

        assertTrue(waitForCondition(() -> state1.scaleUpTask().isDone(), 3000));

        assertFalse(state0.scaleUpTask().isDone());

        taskLatch1.countDown();

        assertTrue(waitForCondition(() -> state0.scaleUpTask().isDone(), 3000));
    }
}
