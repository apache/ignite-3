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
import java.util.concurrent.atomic.AtomicInteger;
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
            1,
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
    void testScaleUpReScheduling() {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testReScheduling(state::rescheduleScaleUp);
    }

    @Test
    void testScaleDownReScheduling() {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        testReScheduling(state::rescheduleScaleDown);
    }

    private static void testReScheduling(BiConsumer<Long, Runnable> fn) {
        Runnable runnable = mock(Runnable.class);

        CountDownLatch latch = new CountDownLatch(1);

        fn.accept(0L, () -> {
            try {
                assertTrue(latch.await(3, TimeUnit.SECONDS));

                runnable.run();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        fn.accept(2L, runnable);

        fn.accept(2L, runnable);

        fn.accept(0L, runnable);

        fn.accept(0L, runnable);

        fn.accept(2L, runnable);

        fn.accept(2L, runnable);

        fn.accept(0L, runnable);

        fn.accept(0L, runnable);

        latch.countDown();

        verify(runnable, after(2500).times(5)).run();
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

    private static void testOrdering(BiConsumer<Long, Runnable> fn) throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();

        CountDownLatch latch = new CountDownLatch(1);

        AtomicBoolean sequentialOrder = new AtomicBoolean(true);

        fn.accept(0L, () -> {
            try {
                assertTrue(latch.await(3, TimeUnit.SECONDS));

                counter.incrementAndGet();

                if (counter.get() != 1) {
                    sequentialOrder.set(false);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        for (int i = 2; i < 11; i++) {
            int j = i;

            fn.accept(0L, () -> {
                counter.incrementAndGet();

                if (counter.get() != j) {
                    sequentialOrder.set(false);
                }
            });
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
