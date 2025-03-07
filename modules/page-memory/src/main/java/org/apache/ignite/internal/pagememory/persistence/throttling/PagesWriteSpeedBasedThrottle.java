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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.jetbrains.annotations.TestOnly;

/**
 * Throttles threads that generate dirty pages during ongoing checkpoint.
 * Designed to avoid zero dropdowns that can happen if checkpoint buffer is overflowed.
 * When the page in question is not included in the current checkpoint and Checkpoint Buffer is filled over 2/3,
 * uses exponentially growing sleep time to throttle.
 * Otherwise, uses average checkpoint write speed and instant speed of marking pages as dirty.<br>
 *
 * @see <a href="https://github.com/apache/ignite/tree/master/modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/pagemem#speed-based-throttling">Speed-based throttling description</a>
 */
public class PagesWriteSpeedBasedThrottle implements PagesWriteThrottlePolicy {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PagesWriteSpeedBasedThrottle.class);

    /**
     * Throttling 'duration' used to signal that no throttling is needed, and no certain side-effects are allowed
     * (like stats collection).
     */
    static final long NO_THROTTLING_MARKER = Long.MIN_VALUE;

    /** Page memory. */
    private final PersistentPageMemory pageMemory;

    /** Checkpoint progress provider. */
    private final Supplier<CheckpointProgress> cpProgress;

    /** Threads set. Contains threads which are currently parked because of throttling. */
    private final Set<Thread> parkedThreads = ConcurrentHashMap.newKeySet();

    /**
     * Used for calculating speed of marking pages dirty.
     * Value from past 750-1000 millis only.
     * {@link IntervalBasedMeasurement#getSpeedOpsPerSec(long)} returns pages marked/second.
     * {@link IntervalBasedMeasurement#getAverage()} returns average throttle time.
     * */
    private final IntervalBasedMeasurement markSpeedAndAvgParkTime = new IntervalBasedMeasurement(250, 3);

    /** Checkpoint lock state provider. */
    private final CheckpointLockStateChecker cpLockStateChecker;

    /** Previous warning time, nanos. */
    private final AtomicLong prevWarnTime = new AtomicLong();

    /** Warning min delay nanoseconds. */
    private static final long WARN_MIN_DELAY_NS = TimeUnit.SECONDS.toNanos(10);

    /** Warning threshold: minimal level of pressure that causes warning messages to log. */
    private static final double WARN_THRESHOLD = 0.2;

    /** Checkpoint buffer protection logic. */
    private final ExponentialBackoffThrottlingStrategy cpBufferProtector = new ExponentialBackoffThrottlingStrategy();

    /** Clean pages protection logic. */
    private final SpeedBasedMemoryConsumptionThrottlingStrategy cleanPagesProtector;

    /** Checkpoint Buffer-related logic used to keep it safe. */
    private final CheckpointBufferOverflowWatchdog cpBufferWatchdog;

    /**
     * Constructor.
     *
     * @param pageMemory Page memory.
     * @param cpProgress Database manager.
     * @param stateChecker Checkpoint lock state provider.
     */
    public PagesWriteSpeedBasedThrottle(
            PersistentPageMemory pageMemory,
            Supplier<CheckpointProgress> cpProgress,
            CheckpointLockStateChecker stateChecker
    ) {
        this.pageMemory = pageMemory;
        this.cpProgress = cpProgress;
        cpLockStateChecker = stateChecker;

        cleanPagesProtector = new SpeedBasedMemoryConsumptionThrottlingStrategy(pageMemory, cpProgress,
            markSpeedAndAvgParkTime);
        cpBufferWatchdog = new CheckpointBufferOverflowWatchdog(pageMemory);
    }

    @Override public void onMarkDirty(boolean isPageInCheckpoint) {
        assert cpLockStateChecker.checkpointLockIsHeldByThread();

        long curNanoTime = System.nanoTime();
        long throttleParkTimeNs = computeThrottlingParkTime(isPageInCheckpoint, curNanoTime);

        if (throttleParkTimeNs == NO_THROTTLING_MARKER) {
            return;
        } else if (throttleParkTimeNs > 0) {
            recurrentLogIfNeeded();
            doPark(throttleParkTimeNs);
        }

        markSpeedAndAvgParkTime.addMeasurementForAverageCalculation(throttleParkTimeNs);
    }

    private long computeThrottlingParkTime(boolean isPageInCheckpoint, long curNanoTime) {
        if (isPageInCheckpoint && isCpBufferOverflowThresholdExceeded()) {
            return cpBufferProtector.protectionParkTime();
        } else {
            if (isPageInCheckpoint) {
                // The fact that we are here means that we checked whether CP Buffer is in danger zone and found that
                // it is ok, so its protector may relax, hence we reset it.
                cpBufferProtector.resetBackoff();
            }
            return cleanPagesProtector.protectionParkTime(curNanoTime);
        }
    }

    /**
     * Disables the current thread for thread scheduling purposes. May be overriden by subclasses for tests.
     *
     * @param throttleParkTimeNs The maximum number of nanoseconds to wait.
     */
    protected void doPark(long throttleParkTimeNs) {
        if (throttleParkTimeNs > LOGGING_THRESHOLD) {
            LOG.warn("Parking thread=" + Thread.currentThread().getName()
                    + " for timeout(ms)=" + (throttleParkTimeNs / 1_000_000));
        }

        parkedThreads.add(Thread.currentThread());

        try {
            LockSupport.parkNanos(throttleParkTimeNs);
        } finally {
            parkedThreads.remove(Thread.currentThread());
        }
    }

    /**
     * Returns a number of written pages.
     */
    private int cpWrittenPages() {
        CheckpointProgress cpProgress = this.cpProgress.get();

        if (cpProgress == null) {
            return 0;
        }

        return cpProgress.writtenPages();
    }

    /**
     * Prints warning to log if throttling is occurred and requires markable amount of time.
     */
    private void recurrentLogIfNeeded() {
        long prevWarningNs = prevWarnTime.get();
        long curNs = System.nanoTime();

        if (prevWarningNs != 0 && (curNs - prevWarningNs) <= WARN_MIN_DELAY_NS) {
            return;
        }

        double weight = throttleWeight();
        if (weight <= WARN_THRESHOLD) {
            return;
        }

        if (prevWarnTime.compareAndSet(prevWarningNs, curNs) && LOG.isInfoEnabled()) {
            String msg = String.format("Throttling is applied to page modifications "
                            + "[fractionOfParkTime=%.2f, markDirty=%d pages/sec, checkpointWrite=%d pages/sec, "
                            + "estIdealMarkDirty=%d pages/sec, curDirty=%.2f, maxDirty=%.2f, avgParkTime=%d ns, "
                            + "pages: (total=%d, evicted=%d, written=%d, synced=%d, cpBufUsed=%d, cpBufTotal=%d)]",
                    weight, getMarkDirtySpeed(), getCpWriteSpeed(),
                    getLastEstimatedSpeedForMarkAll(), getCurrDirtyRatio(), getTargetDirtyRatio(), throttleParkTime(),
                    cleanPagesProtector.cpTotalPages(), cleanPagesProtector.cpEvictedPages(), cpWrittenPages(),
                    cleanPagesProtector.cpSyncedPages(),
                    pageMemory.usedCheckpointBufferPages(), pageMemory.maxCheckpointBufferPages());

            LOG.info(msg);
        }
    }

    /**
     * This is only used in tests.
     *
     * @param dirtyPagesRatio Actual percent of dirty pages.
     * @param fullyCompletedPages Written & fsynced pages count.
     * @param cpTotalPages Total checkpoint scope.
     * @param threads Number of threads providing data during current checkpoint.
     * @param markDirtySpeed Registered mark dirty speed, pages/sec.
     * @param curCpWriteSpeed Average checkpoint write speed, pages/sec.
     * @return Time in nanoseconds to part or 0 if throttling is not required.
     */
    @TestOnly
    long getCleanPagesProtectionParkTime(
            double dirtyPagesRatio,
            long fullyCompletedPages,
            int cpTotalPages,
            int threads,
            long markDirtySpeed,
            long curCpWriteSpeed) {
        return cleanPagesProtector.getParkTime(dirtyPagesRatio, fullyCompletedPages, cpTotalPages, threads,
                markDirtySpeed, curCpWriteSpeed);
    }

    @Override public void onBeginCheckpoint() {
        cleanPagesProtector.reset();
    }

    @Override public void onFinishCheckpoint() {
        cpBufferProtector.resetBackoff();

        cleanPagesProtector.finish();
        markSpeedAndAvgParkTime.finishInterval();
        unparkParkedThreads();
    }

    /**
     * Unparks parked threads.
     */
    private void unparkParkedThreads() {
        parkedThreads.forEach(LockSupport::unpark);
    }

    /**
     * Returns exponential backoff counter.
     */
    public long throttleParkTime() {
        return markSpeedAndAvgParkTime.getAverage();
    }

    /**
     * Returns target (maximum) dirty pages ratio, after which throttling will start.
     */
    public double getTargetDirtyRatio() {
        return cleanPagesProtector.getTargetDirtyRatio();
    }

    /**
     * Returns current dirty pages ratio.
     */
    public double getCurrDirtyRatio() {
        return cleanPagesProtector.getCurrDirtyRatio();
    }

    /**
     * Returns speed of marking pages dirty. Value from past 750-1000 millis only. Pages/second.
     */
    public long getMarkDirtySpeed() {
        return markSpeedAndAvgParkTime.getSpeedOpsPerSec(System.nanoTime());
    }

    /**
     * Returns speed average checkpoint write speed. Current and 3 past checkpoints used. Pages/second.
     */
    public long getCpWriteSpeed() {
        return cleanPagesProtector.getCpWriteSpeed();
    }

    /**
     * Returns last estimated speed for marking all clear pages as dirty till the end of checkpoint.
     */
    public long getLastEstimatedSpeedForMarkAll() {
        return cleanPagesProtector.getLastEstimatedSpeedForMarkAll();
    }

    /**
     * Measurement shows how much throttling time is involved into average marking time.
     *
     * @return Metric started from 0.0 and showing how much throttling is involved into current marking process.
     */
    public double throttleWeight() {
        long speed = markSpeedAndAvgParkTime.getSpeedOpsPerSec(System.nanoTime());

        if (speed <= 0) {
            return 0;
        }

        long timeForOnePage = cleanPagesProtector.nsPerOperation(speed);

        if (timeForOnePage == 0) {
            return 0;
        }

        return 1.0 * throttleParkTime() / timeForOnePage;
    }

    @Override
    public void wakeupThrottledThreads() {
        if (!isCpBufferOverflowThresholdExceeded()) {
            cpBufferProtector.resetBackoff();

            unparkParkedThreads();
        }
    }

    @Override
    public boolean isCpBufferOverflowThresholdExceeded() {
        return cpBufferWatchdog.isInDangerZone();
    }
}
