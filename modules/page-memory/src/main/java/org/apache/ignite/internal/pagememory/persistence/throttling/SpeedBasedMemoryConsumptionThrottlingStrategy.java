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

import static java.lang.Double.isNaN;
import static org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteSpeedBasedThrottle.DEFAULT_MAX_DIRTY_PAGES;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;

/**
 * Speed-based throttle used to protect clean pages from exhaustion.
 *
 * <p>The main method is {@link #protectionParkTime(long)} which is used to compute park time, if throttling is needed.
 *
 * @see #protectionParkTime(long)
 */
class SpeedBasedMemoryConsumptionThrottlingStrategy {
    private final double minDirtyPages;

    private final double maxDirtyPages;

    /** Page memory. */
    private final PersistentPageMemory pageMemory;

    /** Checkpoint progress provider. */
    private final Supplier<CheckpointProgress> cpProgress;

    /** Total pages possible to store in page memory. */
    private volatile long pageMemTotalPages;

    /** Last estimated speed for marking all clear pages as dirty till the end of checkpoint. */
    private volatile long speedForMarkAll;

    /**
     * Target (maximum) dirty pages ratio, after which throttling will start using
     * {@link #getParkTime(double, long, int, int, long, long)}.
     */
    private volatile double targetDirtyRatio;

    /** Current dirty pages ratio (percent of dirty pages in the most used segment), negative value means no cp is running. */
    private volatile double currDirtyRatio;

    /** Threads set. Contains identifiers of all threads which were marking pages for the current checkpoint. */
    private final Set<Long> threadIds = ConcurrentHashMap.newKeySet();

    /** Counter of written pages from a checkpoint. Value is saved here for detecting a checkpoint start. */
    private final AtomicInteger lastObservedWritten = new AtomicInteger(0);

    /**
     * Dirty pages ratio that was observed at a checkpoint start (here the start is a moment when the first page was actually saved to
     * store). This ratio is excluded from throttling.
     */
    private volatile double initDirtyRatioAtCpBegin;

    /**
     * An estimated proportion of "write pages" time in the total "write + fsync" time.
     */
    private volatile double writeVsFsyncCoefficient = 5.0 / 6;

    /** Average checkpoint write speed. Only the current value is used. Pages/second. */
    private final ProgressSpeedCalculation cpWriteSpeed = new ProgressSpeedCalculation();

    /**
     * Used for calculating speed of marking pages dirty. Value from past 750-1000 millis only.
     * {@link IntervalBasedMeasurement#getSpeedOpsPerSec(long)} returns pages marked/second. {@link IntervalBasedMeasurement#getAverage()}
     * returns average throttle time.
     */
    private final IntervalBasedMeasurement markSpeedAndAvgParkTime;

    SpeedBasedMemoryConsumptionThrottlingStrategy(
            double minDirtyPages,
            double maxDirtyPages,
            PersistentPageMemory pageMemory,
            Supplier<CheckpointProgress> cpProgress,
            IntervalBasedMeasurement markSpeedAndAvgParkTime
    ) {
        this.minDirtyPages = minDirtyPages;
        this.maxDirtyPages = maxDirtyPages;
        this.pageMemory = pageMemory;
        this.cpProgress = cpProgress;
        this.markSpeedAndAvgParkTime = markSpeedAndAvgParkTime;
        initDirtyRatioAtCpBegin = minDirtyPages;
    }

    /**
     * Computes next duration (in nanos) to throttle a thread. Might return {@link PagesWriteSpeedBasedThrottle#NO_THROTTLING_MARKER} as a
     * marker that no throttling should be applied.
     *
     * @return Park time in nanos or #NO_THROTTLING_MARKER if no throttling is needed.
     */
    long protectionParkTime(long curNanoTime) {
        CheckpointProgress progress = cpProgress.get();

        boolean checkpointProgressIsNotYetReported = progress == null;
        if (checkpointProgressIsNotYetReported) {
            resetStatistics();

            return PagesWriteSpeedBasedThrottle.NO_THROTTLING_MARKER;
        }

        threadIds.add(Thread.currentThread().getId());

        int writtenPages = progress.writtenPages() + progress.evictedPagesCounter().get();

        return computeParkTime(writtenPages, curNanoTime);
    }

    private void resetStatistics() {
        speedForMarkAll = 0;
        targetDirtyRatio = -1;
        currDirtyRatio = -1;
    }

    private long computeParkTime(int cpWrittenPages, long curNanoTime) {
        long donePages = cpDonePagesEstimation(cpWrittenPages);

        long instantaneousMarkDirtySpeed = markSpeedAndAvgParkTime.getSpeedOpsPerSec(curNanoTime);
        // NB: we update progress for speed calculation only in this (clean pages protection) scenario, because
        // we only use the computed speed in this same scenario and for reporting in logs (where it's not super
        // important to display an ideally accurate speed), but not in the CP Buffer protection scenario.
        // This is to simplify code.
        // The progress is set to 0 at the beginning of a checkpoint, so we can be sure that the start time remembered
        // in cpWriteSpeed is pretty accurate even without writing to cpWriteSpeed from this method.
        cpWriteSpeed.setProgress(donePages, curNanoTime);
        // TODO: IGNITE-16878 use exponential moving average so that we react to changes faster?
        long avgCpWriteSpeed = cpWriteSpeed.getOpsPerSecond(curNanoTime);

        int cpTotalPages = cpTotalPages();

        if (cpTotalPages == 0) {
            // From the current code analysis, we can only get here by accident when
            // CheckpointProgressImpl.clearCounters() is invoked at the end of a checkpoint (by falling through
            // between two volatile assignments). When we get here, we don't have any information about the total
            // number of pages in the current CP, so we calculate park time by only using information we have.
            return parkTimeToThrottleByJustCpSpeed(instantaneousMarkDirtySpeed, avgCpWriteSpeed);
        } else {
            return speedBasedParkTime(cpWrittenPages, donePages, cpTotalPages, instantaneousMarkDirtySpeed, avgCpWriteSpeed);
        }
    }

    /**
     * Returns an estimation of the progress made during the current checkpoint. It is a weighted average of written pages and fully
     * synced pages, which uses {@link #writeVsFsyncCoefficient} as a weight value.
     *
     * @param cpWrittenPages Count of pages written during current checkpoint.
     * @return Estimation of work done (in pages).
     */
    private int cpDonePagesEstimation(int cpWrittenPages) {
        double coefficient = writeVsFsyncCoefficient;

        return (int) (cpWrittenPages * coefficient + cpSyncedPages() * (1 - coefficient));
    }

    /**
     * Simplified version of park time calculation used when we don't have information about total CP size (in pages). Such a situation
     * seems to be very rare, but it can happen when finishing a CP.
     *
     * @param markDirtySpeed Speed of page dirtying.
     * @param curCpWriteSpeed Speed of CP writing pages.
     * @return Park time (nanos).
     */
    private long parkTimeToThrottleByJustCpSpeed(long markDirtySpeed, long curCpWriteSpeed) {
        boolean throttleByCpSpeed = curCpWriteSpeed > 0 && markDirtySpeed > curCpWriteSpeed;

        if (throttleByCpSpeed) {
            return nsPerOperation(curCpWriteSpeed);
        }

        return 0;
    }

    private long speedBasedParkTime(int cpWrittenPages, long donePages, int cpTotalPages,
                                    long instantaneousMarkDirtySpeed, long avgCpWriteSpeed) {
        double dirtyPagesRatio = pageMemory.dirtyPagesRatio();

        currDirtyRatio = dirtyPagesRatio;

        detectCpPagesWriteStart(cpWrittenPages, dirtyPagesRatio);

        if (dirtyPagesRatio >= maxDirtyPages) {
            return 0; // Too late to throttle, will wait on safe to update instead.
        } else {
            return getParkTime(dirtyPagesRatio,
                    donePages,
                    // TODO IGNITE-24937 Should be a "notEvictedPagesTotal(cpTotalPages)" call.
                    cpTotalPages,
                    threadIds.size(),
                    instantaneousMarkDirtySpeed,
                    avgCpWriteSpeed);
        }
    }

    // TODO IGNITE-24937 Leads to negative estimations in some cases. Should be fixed.
    @SuppressWarnings("PMD.UnusedPrivateMethod") // Will be used after IGNITE-24937 is fixed
    private int notEvictedPagesTotal(int cpTotalPages) {
        return Math.max(cpTotalPages - cpEvictedPages(), 0);
    }

    /**
     * Computes park time for throttling.
     *
     * @param dirtyPagesRatio Actual percent of dirty pages.
     * @param donePages Roughly, written & fsynced pages count.
     * @param cpTotalPages Total checkpoint scope.
     * @param threads Number of threads providing data during current checkpoint.
     * @param instantaneousMarkDirtySpeed Registered (during approx last second) mark dirty speed, pages/sec.
     * @param avgCpWriteSpeed Average checkpoint write speed, pages/sec.
     * @return Time in nanoseconds to part or 0 if throttling is not required.
     */
    long getParkTime(
            double dirtyPagesRatio,
            long donePages,
            int cpTotalPages,
            int threads,
            long instantaneousMarkDirtySpeed,
            long avgCpWriteSpeed) {

        long targetSpeedToMarkAll = calcSpeedToMarkAllSpaceTillEndOfCp(dirtyPagesRatio, donePages, avgCpWriteSpeed, cpTotalPages);
        double targetCurrentDirtyRatio = targetCurrentDirtyRatio(donePages, cpTotalPages);

        publishSpeedAndRatioForMetrics(targetSpeedToMarkAll, targetCurrentDirtyRatio);

        return delayIfMarkingFasterThanTargetSpeedAllows(instantaneousMarkDirtySpeed,
            dirtyPagesRatio, threads, targetSpeedToMarkAll, targetCurrentDirtyRatio);
    }

    private long delayIfMarkingFasterThanTargetSpeedAllows(
            long instantaneousMarkDirtySpeed, double dirtyPagesRatio,
            int threads, long targetSpeedToMarkAll, double targetCurrentDirtyRatio
    ) {
        boolean lowSpaceLeft = lowCleanSpaceLeft(dirtyPagesRatio, targetCurrentDirtyRatio);
        int slowdown = slowdownIfLowSpaceLeft(lowSpaceLeft);

        double multiplierForSpeedToMarkAll = lowSpaceLeft ? 0.8 : 1.0;
        boolean markingTooFastNow = targetSpeedToMarkAll > 0
                && instantaneousMarkDirtySpeed > multiplierForSpeedToMarkAll * targetSpeedToMarkAll;
        boolean markedTooFastSinceCpStart = dirtyPagesRatio > targetCurrentDirtyRatio;
        boolean markingTooFast = markedTooFastSinceCpStart && markingTooFastNow;

        // We must NOT subtract nsPerOperation(instantaneousMarkDirtySpeed, threads)! If we do, the actual speed
        // converges to a value that is 1-2 times higher than the target speed.
        return markingTooFast ? nsPerOperation(targetSpeedToMarkAll, threads, slowdown) : 0;
    }

    private static int slowdownIfLowSpaceLeft(boolean lowSpaceLeft) {
        return lowSpaceLeft ? 3 : 1;
    }

    /**
     * Whether too low clean space is left, so more powerful measures are needed (like heavier throttling).
     *
     * @param dirtyPagesRatio Current dirty pages ratio.
     * @param targetDirtyRatio Target dirty pages ratio.
     * @return {@code true} iff clean space left is too low.
     */
    private static boolean lowCleanSpaceLeft(double dirtyPagesRatio, double targetDirtyRatio) {
        return dirtyPagesRatio > targetDirtyRatio && (dirtyPagesRatio + 0.05 > DEFAULT_MAX_DIRTY_PAGES);
    }

    private void publishSpeedAndRatioForMetrics(long speedForMarkAll, double targetDirtyRatio) {
        this.speedForMarkAll = speedForMarkAll;
        this.targetDirtyRatio = targetDirtyRatio;
    }

    /**
     * Calculates speed needed to mark dirty all currently clean pages before the current checkpoint ends. May return 0 if the provided
     * parameters do not give enough information to calculate the speed, OR if the current dirty pages ratio is too high (higher than
     * {@link PagesWriteSpeedBasedThrottle#DEFAULT_MAX_DIRTY_PAGES}), in which case we're not going to throttle anyway.
     *
     * @param dirtyPagesRatio Current percent of dirty pages.
     * @param donePages Roughly, count of written and sync'ed pages
     * @param avgCpWriteSpeed Pages/second checkpoint write speed. 0 speed means 'no data'.
     * @param cpTotalPages Total pages in checkpoint.
     * @return Pages/second to mark to mark all clean pages as dirty till the end of checkpoint. 0 speed means 'no data', or when we are not
     *         going to throttle due to the current dirty pages ratio being too high
     */
    private long calcSpeedToMarkAllSpaceTillEndOfCp(double dirtyPagesRatio, long donePages,
                                                    long avgCpWriteSpeed, int cpTotalPages) {
        if (avgCpWriteSpeed == 0) {
            return 0;
        }

        if (cpTotalPages <= 0) {
            return 0;
        }

        if (dirtyPagesRatio >= maxDirtyPages) {
            return 0;
        }

        // IDEA: here, when calculating the count of clean pages, it includes the pages under checkpoint. It is kinda
        // legal because they can be written (using the Checkpoint Buffer to make a copy of the value to be
        // checkpointed), but the CP Buffer is usually not too big, and if it gets nearly filled, writes become
        // throttled really hard by exponential throttler. Maybe we should subtract the number of not-yet-written-by-CP
        // pages from the count of clean pages? In such a case, we would lessen the risk of CP Buffer-caused throttling.
        double remainedCleanPages = (maxDirtyPages - dirtyPagesRatio) * pageMemTotalPages();

        double secondsTillCpEnd = 1.0 * (cpTotalPages - donePages) / avgCpWriteSpeed;

        return (long) (remainedCleanPages / secondsTillCpEnd);
    }

    /** Returns total number of pages storable in page memory. */
    private long pageMemTotalPages() {
        long currentTotalPages = pageMemTotalPages;

        if (currentTotalPages == 0) {
            currentTotalPages = pageMemory.totalPages();
            pageMemTotalPages = currentTotalPages;
        }

        assert currentTotalPages > 0 : "PageMemory.totalPages() is still 0";

        return currentTotalPages;
    }

    /**
     * Returns size-based calculation of target ratio.
     *
     * @param donePages Number of completed.
     * @param cpTotalPages Total amount of pages under checkpoint.
     */
    private double targetCurrentDirtyRatio(long donePages, int cpTotalPages) {
        double cpProgress = ((double) donePages) / cpTotalPages;

        // Starting with initialDirtyRatioAtCpBegin to avoid throttle right after checkpoint start
        double constStart = initDirtyRatioAtCpBegin;

        double fractionToVaryDirtyRatio = 1.0 - constStart;

        return (cpProgress * fractionToVaryDirtyRatio + constStart) * maxDirtyPages;
    }

    /**
     * Returns target (maximum) dirty pages ratio, after which throttling will start.
     */
    double getTargetDirtyRatio() {
        return targetDirtyRatio;
    }

    /**
     * Returns current dirty pages ratio.
     */
    double getCurrDirtyRatio() {
        double ratio = currDirtyRatio;

        if (ratio >= 0) {
            return ratio;
        }

        return pageMemory.dirtyPagesRatio();
    }

    /**
     * Returns {@link #speedForMarkAll}.
     */
    long getLastEstimatedSpeedForMarkAll() {
        return speedForMarkAll;
    }

    /**
     * Returns an average checkpoint write speed. Current and 3 past checkpoints used. Pages/second.
     */
    long getCpWriteSpeed() {
        return cpWriteSpeed.getOpsPerSecondReadOnly();
    }

    /**
     * Returns counter for fsynced checkpoint pages.
     */
    int cpSyncedPages() {
        CheckpointProgress progress = cpProgress.get();

        if (progress == null) {
            return 0;
        }

        AtomicInteger syncedPagesCounter = progress.syncedPagesCounter();

        // Null-check simplifies testing, we don't have to mock this counter.
        return syncedPagesCounter == null ? 0 : syncedPagesCounter.get();
    }

    /**
     * Return a number of pages in current checkpoint.
     */
    int cpTotalPages() {
        CheckpointProgress progress = cpProgress.get();

        if (progress == null) {
            return 0;
        }

        return progress.currentCheckpointPagesCount();
    }

    /**
     * Returns a number of evicted pages.
     */
    int cpEvictedPages() {
        CheckpointProgress progress = cpProgress.get();

        if (progress == null) {
            return 0;
        }

        AtomicInteger evictedPagesCounter = progress.evictedPagesCounter();

        // Null-check simplifies testing, we don't have to mock this counter.
        return evictedPagesCounter == null ? 0 : evictedPagesCounter.get();
    }

    double getWriteVsFsyncCoefficient() {
        return writeVsFsyncCoefficient;
    }

    /**
     * Returns sleep time in nanoseconds.
     *
     * @param baseSpeed Speed to slow down.
     */
    long nsPerOperation(long baseSpeed) {
        return nsPerOperation(baseSpeed, threadIds.size());
    }

    /**
     * Returns sleep time in nanoseconds.
     *
     * @param speedPagesPerSec Speed to slow down.
     * @param threads Operating threads.
     */
    private long nsPerOperation(long speedPagesPerSec, int threads) {
        return nsPerOperation(speedPagesPerSec, threads, 1);
    }

    /**
     * Return sleep time in nanoseconds.
     *
     * @param speedPagesPerSec Speed to slow down.
     * @param threads Operating threads.
     * @param factor How much it is needed to slowdown base speed. 1 means delay to get exact base speed.
     */
    private long nsPerOperation(long speedPagesPerSec, int threads, int factor) {
        if (factor <= 0) {
            throw new IllegalStateException("Coefficient should be positive");
        }

        if (speedPagesPerSec <= 0) {
            return 0;
        }

        long updTimeNsForOnePage = TimeUnit.SECONDS.toNanos(1) * threads / (speedPagesPerSec);

        return factor * updTimeNsForOnePage;
    }

    /**
     * Detects the start of writing pages in checkpoint.
     *
     * @param cpWrittenPages Current counter of written pages.
     * @param dirtyPagesRatio Current percent of dirty pages.
     */
    private void detectCpPagesWriteStart(int cpWrittenPages, double dirtyPagesRatio) {
        if (cpWrittenPages > 0 && lastObservedWritten.get() == 0 && lastObservedWritten.compareAndSet(0, cpWrittenPages)) {
            double newMinRatio = dirtyPagesRatio;

            if (newMinRatio < minDirtyPages) {
                newMinRatio = minDirtyPages;
            }

            if (newMinRatio > 1) {
                newMinRatio = 1;
            }

            // For slow cp is completed now, drop previous dirty page percent.
            initDirtyRatioAtCpBegin = newMinRatio;
        }
    }

    /**
     * Resets the throttle to its initial state (for example, in the beginning of a checkpoint).
     */
    void reset() {
        cpWriteSpeed.forceProgress(0L, System.nanoTime());
        initDirtyRatioAtCpBegin = minDirtyPages;
        lastObservedWritten.set(0);
    }

    /**
     * Moves the throttle to its finalized state (for example, when a checkpoint ends).
     */
    void finish() {
        CheckpointProgress checkpointProgress = cpProgress.get();
        assert checkpointProgress != null : "Checkpoint progress is already null while checkpoint is finishing";

        cpWriteSpeed.forceProgress(checkpointProgress.currentCheckpointPagesCount(), System.nanoTime());
        cpWriteSpeed.closeInterval();
        threadIds.clear();

        updateWriteVsFsyncCoefficient();
    }

    private void updateWriteVsFsyncCoefficient() {
        CheckpointProgress progress = cpProgress.get();
        assert progress != null;

        if (progress.currentCheckpointPagesCount() == 0) {
            return;
        }

        long pagesWriteTimeMillis = progress.getPagesWriteTimeMillis();
        long fsyncTimeMillis = progress.getFsyncTimeMillis();

        double coefficient = ((double) pagesWriteTimeMillis) / (pagesWriteTimeMillis + fsyncTimeMillis);
        if (isNaN(coefficient)) {
            return;
        }

        // Here we use exponential smoothing with a weight of 0.85.
        double newCoefficient = writeVsFsyncCoefficient * 0.85 + coefficient * 0.15;

        // Put it within reasonable bounds just in case, so that we're not too close to 0 or 1.
        if (newCoefficient < 0.1) {
            newCoefficient = 0.1;
        } else if (newCoefficient > 0.9) {
            newCoefficient = 0.9;
        }

        writeVsFsyncCoefficient = newCoefficient;
    }
}
