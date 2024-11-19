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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;
import org.apache.ignite.internal.util.FastTimestamps;

/**
 * Tracks various checkpoint phases and stats.
 *
 * <p>Assumed sequence of events:
 * <ol>
 *     <li>Checkpoint start</li>
 *     <li>Checkpoint write lock wait start</li>
 *     <li>Checkpoint onMarkCheckpointBegin start</li>
 *     <li>Checkpoint write Lock release</li>
 *     <li>Pages write start</li>
 *     <li>fsync start</li>
 *     <li>Checkpoint end</li>
 * </ol>
 */
public class CheckpointMetricsTracker {
    private static final AtomicIntegerFieldUpdater<CheckpointMetricsTracker> DATA_PAGES_WRITTEN_UPDATER =
            newUpdater(CheckpointMetricsTracker.class, "dataPagesWritten");

    private static final AtomicIntegerFieldUpdater<CheckpointMetricsTracker> COPY_ON_WRITE_PAGES_WRITTEN_UPDATER =
            newUpdater(CheckpointMetricsTracker.class, "copyOnWritePagesWritten");

    private volatile int dataPagesWritten;

    private volatile int copyOnWritePagesWritten;

    private final long startTimestamp = FastTimestamps.coarseCurrentTimeMillis();

    private final long startNanos = System.nanoTime();

    private long writeLockWaitStartNanos;

    private long onMarkCheckpointBeginEndNanos;

    private long onMarkCheckpointBeginStartNanos;

    private long writeLockReleaseNanos;

    private long pagesWriteStartNanos;

    private long fsyncStartNanos;

    private long syncWalStartNanos;

    private long endNanos;

    private long splitAndSortPagesStartNanos;

    private long splitAndSortPagesEndNanos;

    /**
     * Returns checkpoint start timestamp in mills.
     *
     * <p>Thread safe.
     */
    public long checkpointStartTime() {
        return startTimestamp;
    }

    /**
     * Callback on checkpoint end.
     *
     * <p>Not thread safe.
     */
    public void onCheckpointEnd() {
        endNanos = System.nanoTime();
    }

    /**
     * Increments counter if copy on write page was written.
     *
     * <p>Thread safe.
     */
    public void onCopyOnWritePageWritten() {
        COPY_ON_WRITE_PAGES_WRITTEN_UPDATER.incrementAndGet(this);
    }

    /**
     * Increments counter if data page was written.
     *
     * <p>Thread safe.
     */
    public void onDataPageWritten() {
        DATA_PAGES_WRITTEN_UPDATER.incrementAndGet(this);
    }

    /**
     * Returns written copy on write pages.
     *
     * <p>Thread safe.
     */
    public int copyOnWritePagesWritten() {
        return copyOnWritePagesWritten;
    }

    /**
     * Returns data pages written.
     *
     * <p>Thread safe.
     */
    public int dataPagesWritten() {
        return dataPagesWritten;
    }

    /**
     * Callback before acquiring checkpoint write lock.
     *
     * <p>Not thread safe.
     */
    public void onWriteLockWaitStart() {
        writeLockWaitStartNanos = System.nanoTime();
    }

    /**
     * Callback after release checkpoint write lock.
     *
     * <p>Not thread safe.
     */
    public void onWriteLockRelease() {
        writeLockReleaseNanos = System.nanoTime();
    }

    /**
     * Callback before all {@link CheckpointListener#onMarkCheckpointBegin}.
     *
     * <p>Not thread safe.
     */
    public void onMarkCheckpointBeginStart() {
        onMarkCheckpointBeginStartNanos = System.nanoTime();
    }

    /**
     * Callback after all {@link CheckpointListener#onMarkCheckpointBegin}.
     *
     * <p>Not thread safe.
     */
    public void onMarkCheckpointBeginEnd() {
        onMarkCheckpointBeginEndNanos = System.nanoTime();
    }

    /**
     * Callback on start writes pages to {@link PageStore}s.
     *
     * <p>Not thread safe.
     */
    public void onPagesWriteStart() {
        pagesWriteStartNanos = System.nanoTime();
    }

    /**
     * Callback on start fsync {@link PageStore}s.
     *
     * <p>Not thread safe.
     */
    public void onFsyncStart() {
        fsyncStartNanos = System.nanoTime();
    }

    /**
     * Callback before split and sort checkpoint pages.
     *
     * <p>Not thread safe.
     */
    public void onSplitAndSortCheckpointPagesStart() {
        splitAndSortPagesStartNanos = System.nanoTime();
    }

    /**
     * Callback after split and sort checkpoint pages.
     *
     * <p>Not thread safe.
     */
    public void onSplitAndSortCheckpointPagesEnd() {
        splitAndSortPagesEndNanos = System.nanoTime();
    }

    /**
     * Callback on start sync write-ahead-log.
     *
     * <p>Not thread safe.
     */
    public void onSyncWalStart() {
        syncWalStartNanos = System.nanoTime();
    }

    /**
     * Returns total checkpoint duration.
     *
     * <p>Not thread safe.
     */
    public long totalDuration(TimeUnit timeUnit) {
        return timeUnit.convert(endNanos - startNanos, NANOSECONDS);
    }

    /**
     * Returns checkpoint write lock wait duration in the given time unit.
     *
     * <p>Not thread safe.
     */
    public long writeLockWaitDuration(TimeUnit timeUnit) {
        return timeUnit.convert(onMarkCheckpointBeginStartNanos - writeLockWaitStartNanos, NANOSECONDS);
    }

    /**
     * Returns checkpoint action before taken write lock duration in the given time unit.
     *
     * <p>Not thread safe.
     */
    public long beforeWriteLockDuration(TimeUnit timeUnit) {
        return timeUnit.convert(writeLockWaitStartNanos - startNanos, NANOSECONDS);
    }

    /**
     * Returns execution all {@link CheckpointListener#onMarkCheckpointBegin} under write lock duration in the given time unit.
     *
     * <p>Not thread safe.
     */
    public long onMarkCheckpointBeginDuration(TimeUnit timeUnit) {
        return timeUnit.convert(onMarkCheckpointBeginEndNanos - onMarkCheckpointBeginStartNanos, NANOSECONDS);
    }

    /**
     * Returns checkpoint write lock hold duration in the given time unit.
     *
     * <p>Not thread safe.
     */
    public long writeLockHoldDuration(TimeUnit timeUnit) {
        return timeUnit.convert(writeLockReleaseNanos - onMarkCheckpointBeginStartNanos, NANOSECONDS);
    }

    /**
     * Returns pages write duration in the given time unit.
     *
     * <p>Not thread safe.
     */
    public long pagesWriteDuration(TimeUnit timeUnit) {
        return timeUnit.convert(fsyncStartNanos - pagesWriteStartNanos, NANOSECONDS);
    }

    /**
     * Returns checkpoint fsync duration in the given time unit.
     *
     * <p>Not thread safe.
     */
    public long fsyncDuration(TimeUnit timeUnit) {
        return timeUnit.convert(endNanos - fsyncStartNanos, NANOSECONDS);
    }

    /**
     * Returns duration of splitting and sorting checkpoint pages in mills.
     *
     * <p>Not thread safe.
     */
    public long splitAndSortCheckpointPagesDuration(TimeUnit timeUnit) {
        return timeUnit.convert(splitAndSortPagesEndNanos - splitAndSortPagesStartNanos, NANOSECONDS);
    }

    /**
     * Returns checkpoint sync write-ahead-log duration in the given time unit.
     *
     * <p>Not thread safe.
     */
    public long syncWalDuration(TimeUnit timeUnit) {
        return timeUnit.convert(endNanos - syncWalStartNanos, NANOSECONDS);
    }
}
