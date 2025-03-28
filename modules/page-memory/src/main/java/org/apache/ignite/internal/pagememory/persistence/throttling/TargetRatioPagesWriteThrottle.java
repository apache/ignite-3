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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;

/**
 * Throttles threads that generate dirty pages during ongoing checkpoint.
 * Designed to avoid zero dropdowns that can happen if checkpoint buffer is overflowed.
 */
public class TargetRatioPagesWriteThrottle implements PagesWriteThrottlePolicy {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TargetRatioPagesWriteThrottle.class);

    private final long logThresholdNanos;

    /** Page memory. */
    private final PersistentPageMemory pageMemory;

    /** Database manager. */
    private final Supplier<CheckpointProgress> cpProgress;

    /** Checkpoint lock state checker. */
    private final CheckpointLockStateChecker stateChecker;

    /** In-checkpoint protection logic. */
    private final ExponentialBackoffThrottlingStrategy inCheckpointProtection
            = new ExponentialBackoffThrottlingStrategy();

    /** Not-in-checkpoint protection logic. */
    private final ExponentialBackoffThrottlingStrategy notInCheckpointProtection
            = new ExponentialBackoffThrottlingStrategy();

    /** Checkpoint Buffer-related logic used to keep it safe. */
    private final CheckpointBufferOverflowWatchdog cpBufferWatchdog;

    /** Threads that are throttled due to checkpoint buffer overflow. */
    private final ConcurrentHashMap<Long, Thread> cpBufThrottledThreads = new ConcurrentHashMap<>();

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
     * @param stateChecker checkpoint lock state checker.
     * @param metricSource Metric source.
     */
    public TargetRatioPagesWriteThrottle(
            long logThresholdNanos,
            PersistentPageMemory pageMemory,
            Supplier<CheckpointProgress> cpProgress,
            CheckpointLockStateChecker stateChecker,
            PersistentPageMemoryMetricSource metricSource
    ) {
        this.logThresholdNanos = logThresholdNanos;
        this.pageMemory = pageMemory;
        this.cpProgress = cpProgress;
        this.stateChecker = stateChecker;
        cpBufferWatchdog = new CheckpointBufferOverflowWatchdog(pageMemory);

        metricSource.addMetric(totalThrottlingTime);

        assert cpProgress != null
                : "cpProgress must be not null if ratio based throttling mode is used";
    }

    @Override public void onMarkDirty(boolean isPageInCheckpoint) {
        assert stateChecker.checkpointLockIsHeldByThread();

        boolean shouldThrottle = false;

        if (isPageInCheckpoint) {
            shouldThrottle = isCpBufferOverflowThresholdExceeded();
        }

        if (!shouldThrottle) {
            CheckpointProgress progress = cpProgress.get();

            if (progress == null) {
                return; // Don't throttle if checkpoint is not running.
            }

            int cpWrittenPages = progress.writtenPages();

            int cpTotalPages = progress.currentCheckpointPagesCount();

            if (cpWrittenPages == cpTotalPages) {
                // Checkpoint is already in fsync stage, increasing maximum ratio of dirty pages to 3/4.
                shouldThrottle = pageMemory.shouldThrottle(0.75);
            } else {
                double dirtyRatioThreshold = ((double) cpWrittenPages) / cpTotalPages;

                // Starting with 0.05 to avoid throttle right after checkpoint start.
                // 7/12 is maximum ratio of dirty pages.
                dirtyRatioThreshold = (dirtyRatioThreshold * 0.95 + 0.05) * 7 / 12;

                shouldThrottle = pageMemory.shouldThrottle(dirtyRatioThreshold);
            }
        }

        ExponentialBackoffThrottlingStrategy exponentialThrottle = isPageInCheckpoint
                ? inCheckpointProtection : notInCheckpointProtection;

        if (shouldThrottle) {
            long throttleParkTimeNs = exponentialThrottle.protectionParkTime();

            Thread curThread = Thread.currentThread();

            if (throttleParkTimeNs > logThresholdNanos) {
                LOG.warn("Parking thread=" + curThread.getName()
                        + " for timeout(ms)=" + TimeUnit.NANOSECONDS.toMillis(throttleParkTimeNs));
            }

            long startTimeNs = System.nanoTime();

            if (isPageInCheckpoint) {
                cpBufThrottledThreads.put(curThread.getId(), curThread);

                try {
                    LockSupport.parkNanos(throttleParkTimeNs);
                } finally {
                    cpBufThrottledThreads.remove(curThread.getId());

                    if (throttleParkTimeNs > logThresholdNanos) {
                        LOG.warn("Unparking thread=" + curThread.getName()
                                + " with park timeout(ms)=" + TimeUnit.NANOSECONDS.toMillis(throttleParkTimeNs));
                    }
                }
            } else {
                LockSupport.parkNanos(throttleParkTimeNs);
            }

            totalThrottlingTime.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs));
        } else {
            boolean backoffWasAlreadyStarted = exponentialThrottle.resetBackoff();

            if (isPageInCheckpoint && backoffWasAlreadyStarted) {
                unparkParkedThreads();
            }
        }
    }

    @Override public void wakeupThrottledThreads() {
        if (!isCpBufferOverflowThresholdExceeded()) {
            inCheckpointProtection.resetBackoff();

            unparkParkedThreads();
        }
    }

    private void unparkParkedThreads() {
        cpBufThrottledThreads.values().forEach(LockSupport::unpark);
    }

    @Override public void onBeginCheckpoint() {
    }

    @Override public void onFinishCheckpoint() {
        inCheckpointProtection.resetBackoff();
        notInCheckpointProtection.resetBackoff();
    }

    @Override public boolean isCpBufferOverflowThresholdExceeded() {
        return cpBufferWatchdog.isInDangerZone();
    }
}
