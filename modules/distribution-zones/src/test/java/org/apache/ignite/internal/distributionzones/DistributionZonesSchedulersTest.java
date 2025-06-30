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
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.distributionzones.DataNodesManager.ZoneTimerSchedule;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
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
            IgniteThreadFactory.create("", "test-dst-zones-scheduler", LOG)
    );

    @AfterAll
    public static void afterAll() {
        shutdownAndAwaitTermination(executor, 2, TimeUnit.SECONDS);
    }

    @Test
    void testSchedule() throws InterruptedException {
        ZoneTimerSchedule zoneTimerSchedule = new ZoneTimerSchedule(STRIPE_0, executor);

        testSchedule(zoneTimerSchedule::reschedule);
    }

    private static void testSchedule(BiConsumer<Long, Runnable> scheduler) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        assertEquals(1L, latch.getCount());

        scheduler.accept(0L, latch::countDown);

        latch.await();

        assertEquals(0L, latch.getCount());
    }

    @Test
    void testReScheduling() throws InterruptedException {
        ZoneTimerSchedule zoneTimerSchedule = new ZoneTimerSchedule(STRIPE_0, executor);

        testReScheduling(zoneTimerSchedule::reschedule);
    }

    /**
     * Tests that tasks with a zero delay will not be canceled by other tasks.
     * Tests that tasks with a delay grater then zero will be canceled by other tasks.
     */
    private static void testReScheduling(BiConsumer<Long, Runnable> rescheduler) throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();

        CountDownLatch firstTaskLatch = new CountDownLatch(1);

        CountDownLatch lastTaskLatch = new CountDownLatch(1);

        rescheduler.accept(
                0L,
                () -> {
                    try {
                        assertTrue(firstTaskLatch.await(3, TimeUnit.SECONDS));

                        counter.incrementAndGet();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        rescheduler.accept(1L, counter::incrementAndGet);

        rescheduler.accept(0L, counter::incrementAndGet);

        rescheduler.accept(0L, counter::incrementAndGet);

        rescheduler.accept(1L, counter::incrementAndGet);

        rescheduler.accept(1L, counter::incrementAndGet);

        rescheduler.accept(0L, counter::incrementAndGet);

        rescheduler.accept(
                0L,
                () -> {
                    counter.incrementAndGet();

                    lastTaskLatch.countDown();
                }
        );

        firstTaskLatch.countDown();

        assertTrue(lastTaskLatch.await(3, TimeUnit.SECONDS));

        assertEquals(5, counter.get());
    }

    @Test
    void testOrdering() throws InterruptedException {
        ZoneTimerSchedule zoneTimerSchedule = new ZoneTimerSchedule(STRIPE_0, executor);

        testOrdering(zoneTimerSchedule::reschedule);
    }

    private static void testOrdering(BiConsumer<Long, Runnable> scheduler) throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();

        CountDownLatch latch = new CountDownLatch(1);

        AtomicBoolean sequentialOrder = new AtomicBoolean(true);

        scheduler.accept(
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
                }
        );

        for (int i = 2; i < 11; i++) {
            int j = i;

            scheduler.accept(
                    0L,
                    () -> {
                        counter.incrementAndGet();

                        if (counter.get() != j) {
                            sequentialOrder.set(false);
                        }
                    }
            );
        }

        latch.countDown();

        // No assertion here on purpose.
        waitForCondition(() -> counter.get() == 10, 3000);
        assertEquals(10, counter.get(), "Not all tasks were executed.");

        assertTrue(sequentialOrder.get(), "The order of tasks execution is not sequential.");
    }

    @Test
    void testReScheduleWhenTaskIsEnded() throws InterruptedException {
        ZoneTimerSchedule zoneTimerSchedule = new ZoneTimerSchedule(STRIPE_0, executor);

        testReScheduleWhenTaskIsEnded(zoneTimerSchedule::reschedule);
    }

    private static void testReScheduleWhenTaskIsEnded(BiConsumer<Long, Runnable> scheduler) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        AtomicBoolean flag = new AtomicBoolean();

        assertEquals(1L, latch.getCount());

        scheduler.accept(0L, latch::countDown);

        latch.await(1000, TimeUnit.MILLISECONDS);

        scheduler.accept(0L, () -> flag.set(true));

        assertTrue(waitForCondition(() -> 0L == latch.getCount(), 1500));
        assertTrue(waitForCondition(flag::get, 1500));
    }

    @Test
    void testCancelTaskOnStop() {
        ZoneTimerSchedule zoneTimerSchedule = new ZoneTimerSchedule(STRIPE_0, executor);

        testCancelTask(zoneTimerSchedule::reschedule, zoneTimerSchedule::stopScheduledTask, zoneTimerSchedule::taskIsCancelled);
    }

    private static void testCancelTask(
            BiConsumer<Long, Runnable> scheduler,
            Runnable stopTask,
            Supplier<Boolean> isTaskCancelled
    ) {
        CountDownLatch latch = new CountDownLatch(1);

        scheduler.accept(
                0L,
                () -> {
                    try {
                        assertTrue(latch.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        scheduler.accept(1L, () -> {});

        assertFalse(isTaskCancelled.get());

        stopTask.run();

        assertTrue(isTaskCancelled.get());

        latch.countDown();
    }

    @Test
    void testNotCancelTaskOnStop() {
        ZoneTimerSchedule zoneTimerSchedule = new ZoneTimerSchedule(STRIPE_0, executor);

        testRunAndCancelTask(zoneTimerSchedule::reschedule, zoneTimerSchedule::stopScheduledTask, zoneTimerSchedule::taskIsCancelled);
    }

    /**
     * {@link ZoneTimerSchedule#stopScheduledTask()} doesn't cancel task if it is not started and has a delay equal to zero.
     *
     * @param rescheduler Rescheduler function, accepting delay and task.
     * @param stopTask Stop task function.
     * @param isTaskCancelled Task cancellation check function.
     */
    private static void testRunAndCancelTask(
            BiConsumer<Long, Runnable> rescheduler,
            Runnable stopTask,
            Supplier<Boolean> isTaskCancelled
    ) {
        CountDownLatch latch = new CountDownLatch(1);

        rescheduler.accept(
                0L,
                () -> {
                    try {
                        assertTrue(latch.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        rescheduler.accept(0L, () -> {});

        assertFalse(isTaskCancelled.get());

        stopTask.run();

        assertFalse(isTaskCancelled.get());

        latch.countDown();
    }

    /**
     * {@link ZoneTimerSchedule#stopScheduledTask()} ()} cancel task if it is not started and has a delay equal to zero.
     */
    @Test
    public void testCancelTasksOnStopTimersAndImmediateTimerValues() {
        ZoneTimerSchedule zoneTimerSchedule = new ZoneTimerSchedule(STRIPE_0, executor);

        CountDownLatch latch = new CountDownLatch(1);

        zoneTimerSchedule.reschedule(
                0L,
                () -> {
                    try {
                        assertTrue(latch.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        zoneTimerSchedule.reschedule(0L, () -> {});

        assertFalse(zoneTimerSchedule.taskIsCancelled());

        zoneTimerSchedule.stopTimer();

        assertTrue(zoneTimerSchedule.taskIsCancelled());

        latch.countDown();
    }

    /**
     * Check that two tasks for different zones can be run concurrently.
     */
    @Test
    public void testTasksForTwoZonesDoesNotBlockEachOther() throws Exception {
        ZoneTimerSchedule zoneTimerSchedule0 = new ZoneTimerSchedule(STRIPE_0, executor);
        ZoneTimerSchedule zoneTimerSchedule1 = new ZoneTimerSchedule(STRIPE_1, executor);

        CountDownLatch taskLatch0 = new CountDownLatch(1);
        CountDownLatch taskLatch1 = new CountDownLatch(1);

        zoneTimerSchedule0.reschedule(
                0L,
                () -> {
                    taskLatch0.countDown();

                    try {
                        // Wait when the second task is started.
                        assertTrue(taskLatch1.await(3, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        // Wait when the first task is started.
        assertTrue(taskLatch0.await(3, TimeUnit.SECONDS));

        zoneTimerSchedule1.reschedule(0L, () -> {});

        assertTrue(waitForCondition(zoneTimerSchedule1::taskIsDone, 3000));

        assertFalse(zoneTimerSchedule0.taskIsDone());

        taskLatch1.countDown();

        assertTrue(waitForCondition(zoneTimerSchedule0::taskIsDone, 3000));
    }
}
