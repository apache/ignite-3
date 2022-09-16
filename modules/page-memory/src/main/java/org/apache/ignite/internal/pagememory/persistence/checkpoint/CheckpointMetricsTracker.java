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

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;

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

    private final long checkpointStartTimestamp = coarseCurrentTimeMillis();

    private long checkpointWriteLockWaitStartTimestamp;

    private long checkpointOnMarkCheckpointBeginStartTimestamp;

    private long checkpointWriteLockReleaseTimestamp;

    private long checkpointPagesWriteStartTimestamp;

    private long checkpointFsyncStartTimestamp;

    private long checkpointEndTimestamp;

    private long splitAndSortCheckpointPagesStartTimestamp;

    private long splitAndSortCheckpointPagesEndTimestamp;

    private long checkpointOnMarkCheckpointBeginEndTimestamp;

    /**
     * Returns checkpoint start timestamp in mills.
     *
     * <p>Thread safe.
     */
    public long checkpointStartTime() {
        return checkpointStartTimestamp;
    }

    /**
     * Callback on checkpoint end.
     *
     * <p>Not thread safe.
     */
    public void onCheckpointEnd() {
        checkpointEndTimestamp = coarseCurrentTimeMillis();
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
        checkpointWriteLockWaitStartTimestamp = coarseCurrentTimeMillis();
    }

    /**
     * Callback after release checkpoint write lock.
     *
     * <p>Not thread safe.
     */
    public void onWriteLockRelease() {
        checkpointWriteLockReleaseTimestamp = coarseCurrentTimeMillis();
    }

    /**
     * Callback before all {@link CheckpointListener#onMarkCheckpointBegin}.
     *
     * <p>Not thread safe.
     */
    public void onMarkCheckpointBeginStart() {
        checkpointOnMarkCheckpointBeginStartTimestamp = coarseCurrentTimeMillis();
    }

    /**
     * Callback after all {@link CheckpointListener#onMarkCheckpointBegin}.
     *
     * <p>Not thread safe.
     */
    public void onMarkCheckpointBeginEnd() {
        checkpointOnMarkCheckpointBeginEndTimestamp = coarseCurrentTimeMillis();
    }

    /**
     * Callback on start writes pages to {@link PageStore}s.
     *
     * <p>Not thread safe.
     */
    public void onPagesWriteStart() {
        checkpointPagesWriteStartTimestamp = coarseCurrentTimeMillis();
    }

    /**
     * Callback on start fsync {@link PageStore}s.
     *
     * <p>Not thread safe.
     */
    public void onFsyncStart() {
        checkpointFsyncStartTimestamp = coarseCurrentTimeMillis();
    }

    /**
     * Callback before split and sort checkpoint pages.
     *
     * <p>Not thread safe.
     */
    public void onSplitAndSortCheckpointPagesStart() {
        splitAndSortCheckpointPagesStartTimestamp = coarseCurrentTimeMillis();
    }

    /**
     * Callback after split and sort checkpoint pages.
     *
     * <p>Not thread safe.
     */
    public void onSplitAndSortCheckpointPagesEnd() {
        splitAndSortCheckpointPagesEndTimestamp = coarseCurrentTimeMillis();
    }

    /**
     * Returns total checkpoint duration.
     *
     * <p>Not thread safe.
     */
    public long totalDuration() {
        return checkpointEndTimestamp - checkpointStartTimestamp;
    }

    /**
     * Returns checkpoint write lock wait duration in mills.
     *
     * <p>Not thread safe.
     */
    public long writeLockWaitDuration() {
        return checkpointOnMarkCheckpointBeginStartTimestamp - checkpointWriteLockWaitStartTimestamp;
    }

    /**
     * Returns checkpoint action before taken write lock duration in mills.
     *
     * <p>Not thread safe.
     */
    public long beforeWriteLockDuration() {
        return checkpointWriteLockWaitStartTimestamp - checkpointStartTimestamp;
    }

    /**
     * Returns execution all {@link CheckpointListener#onMarkCheckpointBegin} under write lock duration in mills.
     *
     * <p>Not thread safe.
     */
    public long onMarkCheckpointBeginDuration() {
        return checkpointOnMarkCheckpointBeginEndTimestamp - checkpointOnMarkCheckpointBeginStartTimestamp;
    }

    /**
     * Returns checkpoint write lock hold duration in mills.
     *
     * <p>Not thread safe.
     */
    public long writeLockHoldDuration() {
        return checkpointWriteLockReleaseTimestamp - checkpointOnMarkCheckpointBeginStartTimestamp;
    }

    /**
     * Returns pages write duration in mills.
     *
     * <p>Not thread safe.
     */
    public long pagesWriteDuration() {
        return checkpointFsyncStartTimestamp - checkpointPagesWriteStartTimestamp;
    }

    /**
     * Returns checkpoint fsync duration in mills.
     *
     * <p>Not thread safe.
     */
    public long fsyncDuration() {
        return checkpointEndTimestamp - checkpointFsyncStartTimestamp;
    }

    /**
     * Returns duration of splitting and sorting checkpoint pages in mills.
     *
     * <p>Not thread safe.
     */
    public long splitAndSortCheckpointPagesDuration() {
        return splitAndSortCheckpointPagesEndTimestamp - splitAndSortCheckpointPagesStartTimestamp;
    }
}
