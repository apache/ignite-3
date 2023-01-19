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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DistributionZonesStatesTest {
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZonesStatesTest.class);

    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(
            Math.min(Runtime.getRuntime().availableProcessors() * 3, 20),
            new NamedThreadFactory("test-dst-zones-scheduler", LOG),
            new ThreadPoolExecutor.DiscardPolicy()
    );

    @BeforeEach
    public void setUp() {
    }

    @Test
    void testScaleUpSchedule() throws InterruptedException {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        CountDownLatch latch = new CountDownLatch(1);

        assertEquals(1L, latch.getCount());

        state.rescheduleScaleUp(100L, latch::countDown);

        latch.await();

        assertEquals(0L, latch.getCount());
    }

    @Test
    void testScaleDownSchedule() throws InterruptedException {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        CountDownLatch latch = new CountDownLatch(1);

        assertEquals(1L, latch.getCount());

        state.rescheduleScaleDown(100L, latch::countDown);

        latch.await();

        assertEquals(0L, latch.getCount());
    }

    @Test
    void testScaleUpReScheduleNotStartedTask() {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        Runnable runnable = mock(Runnable.class);

        state.rescheduleScaleUp(10000L, runnable);

        verify(runnable, after(100).never()).run();

        state.rescheduleScaleUp(50L, runnable);

        verify(runnable, after(100).times(1)).run();
    }

    @Test
    void testScaleDownReScheduleNotStarted() {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        Runnable runnable = mock(Runnable.class);

        state.rescheduleScaleDown(10000L, runnable);

        verify(runnable, after(100).never()).run();

        state.rescheduleScaleDown(50L, runnable);

        verify(runnable, after(100).times(1)).run();
    }

    @Test
    void testScaleUpReScheduleWhenTaskIsStarted() throws InterruptedException {
        ZoneState state = new DistributionZoneManager.ZoneState(executor);

        CountDownLatch latch = new CountDownLatch(1);

        assertEquals(1L, latch.getCount());

        state.rescheduleScaleUp(50L, () -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                fail();
            }
        });

        Thread.sleep(70);

        state.rescheduleScaleUp(100, latch::countDown);

        assertTrue(waitForCondition(() -> 0L == latch.getCount(), 1000));
    }
}
