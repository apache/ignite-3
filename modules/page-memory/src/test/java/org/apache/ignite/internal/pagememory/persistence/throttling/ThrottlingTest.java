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

package org.apache.ignite.internal.pagememory.persistence.throttling;

import static java.lang.Thread.State.TIMED_WAITING;
import static org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteSpeedBasedThrottle.DEFAULT_MAX_DIRTY_PAGES;
import static org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteSpeedBasedThrottle.DEFAULT_MIN_RATIO_NO_THROTTLE;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PagesWriteSpeedBasedThrottle}.
 */
public class ThrottlingTest extends IgniteAbstractTest {
    /** Page memory 2 gigabytes. */
    private final PersistentPageMemory pageMemory2g = mock(PersistentPageMemory.class);

    /** State checker. */
    private final CheckpointLockStateChecker stateChecker = () -> true;

    private final PersistentPageMemoryMetricSource metricSource = new PersistentPageMemoryMetricSource("test");

    /** {@link CheckpointProgress} mock. */
    private final CheckpointProgress progress = mock(CheckpointProgress.class);

    /** {@link CheckpointProgress} provider that always provides the mock above. */
    private final Supplier<CheckpointProgress> cpProvider = () -> progress;

    @BeforeEach
    void setUp() {
        AtomicInteger evictedPagesCounter = new AtomicInteger();
        when(progress.evictedPagesCounter()).thenReturn(evictedPagesCounter);

        when(pageMemory2g.totalPages()).thenReturn((2L * Constants.GiB) / 4096);
    }

    /**
     * Tests that the speed-based throttler throttles when writing faster than target speed, AND the dirty ratio is above the target ratio.
     */
    @Test
    public void shouldThrottleWhenWritingTooFast() {
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, metricSource);

        double dirtyPagesRatio = DEFAULT_MAX_DIRTY_PAGES - 0.08;
        long parkTime = throttle.getCleanPagesProtectionParkTime(dirtyPagesRatio, (362584 + 67064) / 2, 328787, 1, 60184, 23103);

        assertTrue(parkTime > 0);
    }

    @Test
    public void shouldNotThrottleWhenWritingSlowly() {
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, metricSource);

        long parkTime = throttle.getCleanPagesProtectionParkTime(0.47, ((362584 + 67064) / 2), 328787, 1, 20103, 23103);

        assertEquals(0, parkTime);
    }

    /**
     * Tests that the speed-based throttler does NOT throttle when there are plenty clean pages, even if writing faster than the current
     * checkpoint speed.
     */
    @Test
    public void shouldNotThrottleWhenThereArePlentyCleanPages() {
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, metricSource);

        long parkTime = throttle.getCleanPagesProtectionParkTime(0.0, (362584 + 67064) / 2, 328787, 1, 60184, 23103);

        assertEquals(0, parkTime);
    }

    /**
     * Test that time to park is calculated according to both cpSpeed and mark dirty speed (in case if checkpoint buffer is not full).
     */
    @Test
    public void testCorrectTimeToPark() {
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, metricSource);

        double dirtyPagesRatio = DEFAULT_MAX_DIRTY_PAGES - 0.08;
        int completedPages = (903150 + 227217) / 2;
        int markDirtySpeed = 34422;
        int cpWriteSpeed = 19416;
        long time = throttle.getCleanPagesProtectionParkTime(dirtyPagesRatio, completedPages, 903150, 1, markDirtySpeed, cpWriteSpeed);

        assertEquals(415110, time);
    }

    @Test
    public void averageCalculation() throws InterruptedException {
        IntervalBasedMeasurement measurement = new IntervalBasedMeasurement(100, 1);

        for (int i = 0; i < 1000; i++) {
            measurement.addMeasurementForAverageCalculation(100);
        }

        assertEquals(100, measurement.getAverage());

        Thread.sleep(220);

        assertEquals(0, measurement.getAverage());

        assertEquals(0, measurement.getSpeedOpsPerSec(System.nanoTime()));
    }

    @Test
    public void speedCalculation() throws InterruptedException {
        IntervalBasedMeasurement measurement = new IntervalBasedMeasurement(100, 1);

        for (int i = 0; i < 1000; i++) {
            measurement.forceCounter(i, System.nanoTime());
        }

        long speed = measurement.getSpeedOpsPerSec(System.nanoTime());
        System.out.println("speed measured " + speed);
        assertTrue(speed > 1000);

        Thread.sleep(230);

        assertEquals(0, measurement.getSpeedOpsPerSec(System.nanoTime()));
    }

    @Test
    public void speedWithDelayCalculation() throws InterruptedException {
        IntervalBasedMeasurement measurement = new IntervalBasedMeasurement(100, 1);

        int runs = 10;
        int nanosPark = 100;
        int multiplier = 100000;
        for (int i = 0; i < runs; i++) {
            measurement.forceCounter(i * multiplier, System.nanoTime());

            LockSupport.parkNanos(nanosPark);
        }

        long speed = measurement.getSpeedOpsPerSec(System.nanoTime());

        assertTrue(speed > 0);
        long maxSpeed = (TimeUnit.SECONDS.toNanos(1) * multiplier * runs) / (runs * nanosPark);
        assertTrue(speed < maxSpeed);

        Thread.sleep(200);

        assertEquals(0, measurement.getSpeedOpsPerSec(System.nanoTime()));
    }

    @Test
    public void beginOfCp() {
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, metricSource);

        assertEquals(0, throttle.getCleanPagesProtectionParkTime(0.01, 100, 400000, 1, 20103, 23103));

        // Mark speed 22413 for mark all remaining as dirty.
        long time = throttle.getCleanPagesProtectionParkTime(DEFAULT_MIN_RATIO_NO_THROTTLE - 0.005, 100, 400000, 1, 24000, 23103);
        assertTrue(time > 0);

        assertEquals(0, throttle.getCleanPagesProtectionParkTime(0.01, 100, 400000, 1, 22412, 23103));
    }

    @Test
    public void enforceThrottleAtTheEndOfCp() {
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, metricSource);

        long time1 = throttle.getCleanPagesProtectionParkTime(DEFAULT_MAX_DIRTY_PAGES - 0.05, 300000, 400000, 1, 20200, 23000);
        long time2 = throttle.getCleanPagesProtectionParkTime(DEFAULT_MAX_DIRTY_PAGES - 0.04, 300000, 400000, 1, 20200, 23000);

        assertTrue(time2 >= time1 * 2); // Extra slowdown should be applied.

        long time3 = throttle.getCleanPagesProtectionParkTime(DEFAULT_MAX_DIRTY_PAGES - 0.02, 300000, 400000, 1, 20200, 23000);
        long time4 = throttle.getCleanPagesProtectionParkTime(DEFAULT_MAX_DIRTY_PAGES - 0.01, 300000, 400000, 1, 20200, 23000);

        assertTrue(time3 > time2);
        assertTrue(time4 > time3);
    }

    @Test
    public void doNotThrottleWhenDirtyPagesRatioIsTooHigh() {
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, null, stateChecker, metricSource);

        double dirtyPagesRatio = DEFAULT_MAX_DIRTY_PAGES;
        // 363308 350004 348976 10604
        int completedPages = (350004 + 348976) / 2;
        long time = throttle.getCleanPagesProtectionParkTime(dirtyPagesRatio, completedPages, 350004 - 10604, 4, 279, 23933);

        assertEquals(0, time);
    }

    @Test
    public void wakeupSpeedBaseThrottledThreadOnCheckpointFinish() throws Exception {
        // Given: Enabled throttling with EXPONENTIAL level.
        when(progress.writtenPages()).thenReturn(200);

        Supplier<CheckpointProgress> cpProgress = mock(Supplier.class);
        when(cpProgress.get()).thenReturn(progress);

        var plc = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProgress, stateChecker, metricSource) {
            @Override protected void doPark(long throttleParkTimeNs) {
                // Force parking to long time.
                super.doPark(TimeUnit.SECONDS.toNanos(1));
            }
        };

        simulateCheckpointBufferInDangerZoneSituation();

        AtomicBoolean stopLoad = new AtomicBoolean();
        List<Thread> loadThreads = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            loadThreads.add(new Thread(
                    () -> {
                        while (!stopLoad.get()) {
                            plc.onMarkDirty(true);
                        }
                    },
                    "load-" + i
            ));
        }

        try {
            loadThreads.forEach(Thread::start);

            // And: All load threads are parked.
            for (Thread t : loadThreads) {
                await().timeout(5, TimeUnit.SECONDS).until(t::getState, is(TIMED_WAITING));
            }

            // When: Disable throttling
            simulateCheckpointBufferInSafeZoneSituation();

            // And: Finish the checkpoint.
            plc.onFinishCheckpoint();

            stopReportingCheckpointProgress(cpProgress);

            // Then: All load threads should be unparked.
            for (Thread t : loadThreads) {
                await().timeout(5, TimeUnit.SECONDS).until(t::getState, is(not(TIMED_WAITING)));
            }

            for (Thread t : loadThreads) {
                assertNotEquals(TIMED_WAITING, t.getState(), t.getName());
            }
        } finally {
            stopLoad.set(true);

            for (Thread t : loadThreads) {
                t.join();
            }
        }
    }

    private static void stopReportingCheckpointProgress(Supplier<CheckpointProgress> cpProgress) {
        when(cpProgress.get()).thenReturn(null);
    }

    @Test
    public void speedBasedThrottleShouldThrottleWhenCheckpointBufferIsInDangerZone() {
        simulateCheckpointProgressIsStarted();
        simulateCheckpointBufferInDangerZoneSituation();

        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider, stateChecker, metricSource);

        throttle.onMarkDirty(true);

        assertThatThrottlingHappened(throttle);
    }

    private static void assertThatThrottlingHappened(PagesWriteSpeedBasedThrottle throttle) {
        assertThat(throttle.throttleParkTime(), greaterThan(0L));
    }

    private void simulateCheckpointBufferInDangerZoneSituation() {
        when(pageMemory2g.usedCheckpointBufferPages()).thenReturn(100);
        when(pageMemory2g.maxCheckpointBufferPages()).thenReturn(100);
    }

    @Test
    public void speedBasedThrottleShouldThrottleWhenCheckpointCountersAreNotReadyYetButCheckpointBufferIsInDangerZone() {
        simulateCheckpointProgressNotYetStarted();
        simulateCheckpointBufferInDangerZoneSituation();

        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider, stateChecker, metricSource);

        throttle.onMarkDirty(true);

        assertThatThrottlingHappened(throttle);
    }

    private void simulateCheckpointProgressNotYetStarted() {
        when(progress.writtenPages()).thenReturn(0);
    }

    @Test
    public void speedBasedThrottleShouldNotLeaveTracesInStatisticsWhenCpBufferIsInSafeZoneAndProgressIsNotYetStarted() {
        simulateCheckpointProgressNotYetStarted();
        simulateCheckpointBufferInSafeZoneSituation();

        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider, stateChecker, metricSource);

        throttle.onMarkDirty(true);

        assertThat(throttle.throttleParkTime(), is(0L));
    }

    private void simulateCheckpointBufferInSafeZoneSituation() {
        when(pageMemory2g.usedCheckpointBufferPages()).thenReturn(0);
        when(pageMemory2g.maxCheckpointBufferPages()).thenReturn(100);
    }

    @Test
    public void speedBasedThrottleShouldResetCpBufferProtectionParkTimeWhenItSeesThatCpBufferIsInSafeZoneAndThePageIsInCheckpoint() {
        // Preparations.
        simulateCheckpointProgressIsStarted();
        AtomicLong parkTimeNanos = new AtomicLong();
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider, stateChecker, metricSource) {
            @Override protected void doPark(long throttleParkTimeNs) {
                super.doPark(1);
                parkTimeNanos.set(throttleParkTimeNs);
            }
        };

        // Test script starts.

        // Cause a couple of CP Buffer protection parks.
        simulateCheckpointBufferInDangerZoneSituation();
        throttle.onMarkDirty(true);
        throttle.onMarkDirty(true);
        assertThat(parkTimeNanos.get(), is(4200L));

        // Cause the counter to be reset.
        simulateCheckpointBufferInSafeZoneSituation();
        throttle.onMarkDirty(true);

        // Check that next CP Buffer protection park starts from the beginning.
        simulateCheckpointBufferInDangerZoneSituation();
        throttle.onMarkDirty(true);
        assertThat(parkTimeNanos.get(), is(4000L));
    }

    private void simulateCheckpointProgressIsStarted() {
        when(progress.writtenPages()).thenReturn(1000);
    }

    @Test
    public void speedBasedThrottleShouldNotResetCpBufferProtectionParkTimeWhenItSeesThatCpBufferIsInSafeZoneAndThePageIsNotInCheckpoint() {
        // Preparations.
        simulateCheckpointProgressIsStarted();
        AtomicLong parkTimeNanos = new AtomicLong();
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider, stateChecker, metricSource) {
            @Override protected void doPark(long throttleParkTimeNs) {
                super.doPark(1);
                parkTimeNanos.set(throttleParkTimeNs);
            }
        };

        // Test script starts.

        // Cause a couple of CP Buffer protection parks.
        simulateCheckpointBufferInDangerZoneSituation();
        throttle.onMarkDirty(true);
        throttle.onMarkDirty(true);
        assertThat(parkTimeNanos.get(), is(4200L));

        // This should NOT cause the counter to be reset.
        simulateCheckpointBufferInSafeZoneSituation();
        throttle.onMarkDirty(false);

        // Check that next CP Buffer protection park is the third member of the progression.
        simulateCheckpointBufferInDangerZoneSituation();
        throttle.onMarkDirty(true);
        assertThat(parkTimeNanos.get(), is(4410L));
    }

    @Test
    public void speedBasedThrottleShouldReportCpWriteSpeedWhenThePageIsNotInCheckpointAndProgressIsReported() throws InterruptedException {
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider, stateChecker, metricSource);

        throttle.onBeginCheckpoint();

        simulateCheckpointProgressIsStarted();
        allowSomeTimeToPass();
        throttle.onMarkDirty(false);

        assertThat(throttle.getCpWriteSpeed(), is(greaterThan(0L)));
    }

    private static void allowSomeTimeToPass() throws InterruptedException {
        Thread.sleep(10);
    }

    @Test
    public void speedBasedThrottleShouldResetCpProgressToZeroOnCheckpointStart() throws InterruptedException {
        var throttle = new PagesWriteSpeedBasedThrottle(pageMemory2g, cpProvider, stateChecker, metricSource);

        simulateCheckpointProgressIsStarted();
        allowSomeTimeToPass();
        throttle.onMarkDirty(false);

        throttle.onBeginCheckpoint();

        // Verify progress speed to make a conclusion about progress itself.
        assertThat(throttle.getCpWriteSpeed(), is(0L));
    }
}
