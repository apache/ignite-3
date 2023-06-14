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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Test scenarios for the distribution zone schedulers.
 */
public class DistributionZonesSchedulersTest {
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZonesSchedulersTest.class);

    private static final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(
            Math.min(Runtime.getRuntime().availableProcessors() * 3, 20),
            new NamedThreadFactory("test-dst-zones-scheduler", LOG),
            new ThreadPoolExecutor.DiscardPolicy()
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

    private static void testSchedule(BiConsumer<Long, Runnable> fn) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        assertEquals(1L, latch.getCount());

        fn.accept(0L, latch::countDown);

        latch.await();

        assertEquals(0L, latch.getCount());
    }

    @Test
    void testScaleUpReScheduleNotStartedTask() {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testReScheduleNotStartedTask(state::rescheduleScaleUp);
    }

    @Test
    void testScaleDownReScheduleNotStartedTask() {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testReScheduleNotStartedTask(state::rescheduleScaleDown);
    }

    private static void testReScheduleNotStartedTask(BiConsumer<Long, Runnable> fn) {
        Runnable runnable = mock(Runnable.class);

        fn.accept(1L, runnable);

        fn.accept(0L, runnable);

        verify(runnable, after(1200).times(1)).run();
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

    private static void testReScheduleWhenTaskIsEnded(BiConsumer<Long, Runnable> fn) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        AtomicBoolean flag = new AtomicBoolean();

        assertEquals(1L, latch.getCount());

        fn.accept(0L, latch::countDown);

        latch.await(1000, TimeUnit.MILLISECONDS);

        fn.accept(0L, () -> {
            flag.set(true);
        });

        assertTrue(waitForCondition(() -> 0L == latch.getCount(), 1500));
        assertTrue(waitForCondition(flag::get, 1500));
    }
}
