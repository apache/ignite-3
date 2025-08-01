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
import org.apache.ignite.internal.metrics.DoubleGauge;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.jetbrains.annotations.TestOnly;

/**
 * Throttles threads that generate dirty pages during ongoing checkpoint.
 * Designed to avoid zero dropdowns that can happen if checkpoint buffer is overflowed.
 * When the page in question is not included in the current checkpoint and Checkpoint Buffer is filled over 2/3,
 * uses exponentially growing sleep time to throttle.
 * Otherwise, uses average checkpoint write speed and instant speed of marking pages as dirty.<br>
 */
public class PagesWriteSpeedBasedThrottle implements PagesWriteThrottlePolicy {
    /** Maximum fraction of dirty pages in a region. */
    public static final double DEFAULT_MAX_DIRTY_PAGES = 0.9;

    /** Percent of dirty pages which will not cause throttling. */
    public static final double DEFAULT_MIN_RATIO_NO_THROTTLE = 0.5;

    private static final IgniteLogger LOG = Loggers.forClass(PagesWriteSpeedBasedThrottle.class);

    /** The maximum time for a single {@link LockSupport#parkNanos(long)} call if we don't throttle the checkpoint buffer. */
    private static final int PARKING_UNIT = 10_000;

    /**
     * Throttling 'duration' used to signal that no throttling is needed, and no certain side-effects are allowed
     * (like stats collection).
     */
    static final long NO_THROTTLING_MARKER = Long.MIN_VALUE;

    private final long logThresholdNanos;

    private final PersistentPageMemory pageMemory;

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

    private final CheckpointLockStateChecker cpLockStateChecker;

    /** Previous warning time, nanos. */
    private final AtomicLong prevWarnTime = new AtomicLong();

    /** Warning min delay nanoseconds. */
    private static final long WARN_MIN_DELAY_NS = TimeUnit.SECONDS.toNanos(10);

    /** Warning threshold: minimal level of pressure that causes warning messages to log. */
    private static final double WARN_THRESHOLD = 0.2;

    private final ExponentialBackoffThrottlingStrategy cpBufferProtector = new ExponentialBackoffThrottlingStrategy();

    private final SpeedBasedMemoryConsumptionThrottlingStrategy cleanPagesProtector;

    /** Checkpoint Buffer-related logic used to keep it safe. */
    private final CheckpointBufferOverflowWatchdog cpBufferWatchdog;

    private final LongAdderMetric totalThrottlingTime = new LongAdderMetric(
            "TotalThrottlingTime",
            "Total throttling threads time in milliseconds. The Ignite throttles threads that generate "
                    + "dirty pages during the ongoing checkpoint."
    );

    /**
     * Constructor.
     *
     * @param logThresholdNanos Minimal throttling duration required for printing a warning message to the log.
     * @param pageMemory Page memory.
     * @param cpProgress Database manager.
     * @param stateChecker Checkpoint lock state provider.
     * @param metricSource Metric source.
     */
    public PagesWriteSpeedBasedThrottle(
            long logThresholdNanos,
            double minDirtyPages,
            double maxDirtyPages,
            PersistentPageMemory pageMemory,
            Supplier<CheckpointProgress> cpProgress,
            CheckpointLockStateChecker stateChecker,
            PersistentPageMemoryMetricSource metricSource
    ) {
        this.logThresholdNanos = logThresholdNanos;
        this.pageMemory = pageMemory;
        this.cpProgress = cpProgress;
        cpLockStateChecker = stateChecker;

        cleanPagesProtector = new SpeedBasedMemoryConsumptionThrottlingStrategy(
                minDirtyPages,
                maxDirtyPages,
                pageMemory,
                cpProgress,
                markSpeedAndAvgParkTime
        );
        cpBufferWatchdog = new CheckpointBufferOverflowWatchdog(pageMemory);

        initMetrics(metricSource);
    }

    /**
     * Test constructor with fewer parameters.
     */
    @TestOnly
    public PagesWriteSpeedBasedThrottle(
            PersistentPageMemory pageMemory,
            Supplier<CheckpointProgress> cpProgress,
            CheckpointLockStateChecker stateChecker,
            PersistentPageMemoryMetricSource metricSource
    ) {
        this(
                DEFAULT_LOGGING_THRESHOLD,
                DEFAULT_MIN_RATIO_NO_THROTTLE,
                DEFAULT_MAX_DIRTY_PAGES,
                pageMemory,
                cpProgress,
                stateChecker,
                metricSource
        );
    }

    private void initMetrics(PersistentPageMemoryMetricSource metricSource) {
        metricSource.addMetric(totalThrottlingTime);

        metricSource.addMetric(new DoubleGauge(
                "SpeedBasedThrottlingPercentage",
                "Measurement shows how much throttling time is involved into average marking time.",
                this::throttleWeight
        ));
        metricSource.addMetric(new LongGauge(
                "MarkDirtySpeed",
                "Speed of marking pages dirty. Value from past 750-1000 millis only. Pages/second.",
                this::getMarkDirtySpeed
        ));
        metricSource.addMetric(new LongGauge(
                "CpWriteSpeed",
                "Speed average checkpoint write speed. Current and 3 past checkpoints used. Pages/second.",
                this::getCpWriteSpeed
        ));
        metricSource.addMetric(new LongGauge(
                "LastEstimatedSpeedForMarkAll",
                "Last estimated speed for marking all clear pages as dirty till the end of checkpoint.",
                this::getLastEstimatedSpeedForMarkAll
        ));
        metricSource.addMetric(new DoubleGauge(
                "CurrDirtyRatio",
                "Current dirty pages ratio.",
                this::getCurrDirtyRatio
        ));
        metricSource.addMetric(new DoubleGauge(
                "TargetDirtyRatio",
                "Target (maximum) dirty pages ratio, after which throttling will start.",
                this::getTargetDirtyRatio
        ));
        metricSource.addMetric(new LongGauge(
                "ThrottleParkTime",
                "Exponential backoff counter.",
                this::throttleParkTime
        ));
        metricSource.addMetric(new IntGauge(
                "CpTotalPages",
                "Number of pages in current checkpoint.",
                cleanPagesProtector::cpTotalPages
        ));
        metricSource.addMetric(new IntGauge(
                "CpEvictedPages",
                "Number of evicted pages.",
                cleanPagesProtector::cpEvictedPages
        ));
        metricSource.addMetric(new IntGauge(
                "CpWrittenPages",
                "Number of written pages.",
                this::cpWrittenPages
        ));
        metricSource.addMetric(new IntGauge(
                "CpSyncedPages",
                "Counter for fsynced checkpoint pages.",
                cleanPagesProtector::cpSyncedPages
        ));
        metricSource.addMetric(new DoubleGauge(
                "WriteVsFsyncCoefficient",
                "Ratio of the write pages duration to the total 'write+fsync' duration",
                cleanPagesProtector::getWriteVsFsyncCoefficient
        ));
    }

    @Override public void onMarkDirty(boolean isPageInCheckpoint) {
        assert cpLockStateChecker.checkpointLockIsHeldByThread();

        long startTimeNs = System.nanoTime();

        long throttleParkTimeNs = parkAndReturnParkingNanos(isPageInCheckpoint);

        if (throttleParkTimeNs == NO_THROTTLING_MARKER) {
            return;
        }

        if (throttleParkTimeNs > logThresholdNanos) {
            LOG.warn("Parking thread={} for timeout(ms)={}", Thread.currentThread().getName(), throttleParkTimeNs / 1_000_000);
        }

        totalThrottlingTime.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs));

        markSpeedAndAvgParkTime.addMeasurementForAverageCalculation(throttleParkTimeNs);
    }

    private long parkAndReturnParkingNanos(boolean isPageInCheckpoint) {
        if (isPageInCheckpoint && isCpBufferOverflowThresholdExceeded()) {
            long parkTimeNanos = cpBufferProtector.protectionParkTime();

            doPark(parkTimeNanos);

            return parkTimeNanos;
        } else {
            if (isPageInCheckpoint) {
                // The fact that we are here means that we checked whether CP Buffer is in danger zone and found that
                // it is ok, so its protector may relax, hence we reset it.
                cpBufferProtector.resetBackoff();
            }

            return parkAndReturnParkingNanosWithoutCpBufferProtection();
        }
    }

    private long parkAndReturnParkingNanosWithoutCpBufferProtection() {
        long alreadyParkedNanos = 0L;

        long minimalCalculatedParkNanos = Long.MAX_VALUE;

        // Main idea behind the loop: it is possible to have a wrong estimation of parking time if there's not enough statistic or it is
        // inaccurate for some reason. Such situations might result in several seconds of parking without any ability to unpark these
        // threads. In order to deal with these imprecise calculations, we only park threads to small increments amounts of time and then
        // re-check expected parking time. If current thread has already been parked for longer than any of the proposed time periods, we
        // consider that it is safe to stop parking and modify the page.
        while (true) {
            long calculatedParkNanos = cleanPagesProtector.protectionParkTime(System.nanoTime());

            if (calculatedParkNanos == NO_THROTTLING_MARKER) {
                // Return "NO_THROTTLING_MARKER" on first iteration, or "alreadyParkedNanos" on any other iteration.
                return alreadyParkedNanos == 0L ? NO_THROTTLING_MARKER : alreadyParkedNanos;
            }

            minimalCalculatedParkNanos = Math.min(minimalCalculatedParkNanos, calculatedParkNanos);

            if (alreadyParkedNanos >= minimalCalculatedParkNanos) {
                return alreadyParkedNanos;
            }

            long realParkNanos = Math.min(calculatedParkNanos, PARKING_UNIT);

            doPark(realParkNanos);

            // Ignore spurious wake-ups, assuming they don't happen.
            alreadyParkedNanos += realParkNanos;

            if (calculatedParkNanos == realParkNanos) {
                return alreadyParkedNanos;
            }
        }
    }

    /**
     * Disables the current thread for thread scheduling purposes. May be overriden by subclasses for tests.
     *
     * @param throttleParkTimeNs The maximum number of nanoseconds to wait.
     */
    protected void doPark(long throttleParkTimeNs) {
        recurrentLogIfNeeded();

        parkedThreads.add(Thread.currentThread());

        try {
            LockSupport.parkNanos(throttleParkTimeNs);
        } finally {
            parkedThreads.remove(Thread.currentThread());
        }
    }

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
                            + "pages: (total=%d, evicted=%d, written=%d, synced=%d, cpBufUsed=%d, cpBufTotal=%d, "
                            + "writeVsFsyncCoefficient=%.2f)]",
                    weight, getMarkDirtySpeed(), getCpWriteSpeed(),
                    getLastEstimatedSpeedForMarkAll(), getCurrDirtyRatio(), getTargetDirtyRatio(), throttleParkTime(),
                    cleanPagesProtector.cpTotalPages(), cleanPagesProtector.cpEvictedPages(), cpWrittenPages(),
                    cleanPagesProtector.cpSyncedPages(), pageMemory.usedCheckpointBufferPages(), pageMemory.maxCheckpointBufferPages(),
                    cleanPagesProtector.getWriteVsFsyncCoefficient()
            );

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
