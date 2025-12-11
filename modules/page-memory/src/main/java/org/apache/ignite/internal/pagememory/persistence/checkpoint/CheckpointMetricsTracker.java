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
import org.apache.ignite.internal.metrics.Duration;
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

    private final Duration checkpointDuration = new Duration();

    private final Duration writeLockWaitDuration = new Duration();

    private final Duration onBeforeCheckpointBeginDuration = new Duration();

    private final Duration onMarkCheckpointBeginDuration = new Duration();

    private final Duration writeLockHoldDuration = new Duration();

    private final Duration pagesWriteDuration = new Duration();

    private final Duration fsyncDuration = new Duration();

    private final Duration replicatorLogSyncDuration = new Duration();

    private final Duration splitAndSortCheckpointPagesDuration = new Duration();

    private final Duration waitPageReplacement = new Duration();

    /**
     * Increments counter if copy on write page was written.
     *
     * <p>Thread safe.
     */
    public void onCopyOnWritePageWritten() {
        COPY_ON_WRITE_PAGES_WRITTEN_UPDATER.incrementAndGet(this);
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
     * Increments counter if data page was written.
     *
     * <p>Thread safe.
     */
    public void onDataPageWritten() {
        DATA_PAGES_WRITTEN_UPDATER.incrementAndGet(this);
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
     * Returns checkpoint start timestamp in mills.
     *
     * <p>Not thread safe.</p>
     */
    public long checkpointStartTime() {
        return startTimestamp;
    }

    /**
     * Callback on checkpoint start.
     *
     * <p>Not thread safe.</p>
     */
    public void onCheckpointStart() {
        checkpointDuration.onStart();
    }

    /**
     * Callback on checkpoint end.
     *
     * <p>Not thread safe.</p>
     */
    public void onCheckpointEnd() {
        checkpointDuration.onEnd();
    }

    /**
     * Returns total checkpoint duration.
     *
     * <p>Not thread safe.
     */
    public long checkpointDuration(TimeUnit timeUnit) {
        return checkpointDuration.duration(timeUnit);
    }

    /**
     * Callback before acquiring checkpoint write lock.
     *
     * <p>Not thread safe.</p>
     */
    public void onWriteLockWaitStart() {
        writeLockWaitDuration.onStart();
    }

    /**
     * Callback after acquiring checkpoint write lock.
     *
     * <p>Not thread safe.</p>
     */
    public void onWriteLockWaitEnd() {
        writeLockWaitDuration.onEnd();
    }

    /**
     * Returns total acquiring checkpoint write lock duration.
     *
     * <p>Not thread safe.</p>
     */
    public long writeLockWaitDuration(TimeUnit timeUnit) {
        return writeLockWaitDuration.duration(timeUnit);
    }

    /**
     * Callback before all {@link CheckpointListener#beforeCheckpointBegin}.
     *
     * <p>Not thread safe.</p>
     */
    public void onBeforeCheckpointBeginStart() {
        onBeforeCheckpointBeginDuration.onStart();
    }

    /**
     * Callback after all {@link CheckpointListener#beforeCheckpointBegin}.
     *
     * <p>Not thread safe.</p>
     */
    public void onBeforeCheckpointBeginEnd() {
        onBeforeCheckpointBeginDuration.onEnd();
    }

    /**
     * Returns execution all {@link CheckpointListener#beforeCheckpointBegin} duration in the given time unit.
     *
     * <p>Not thread safe.</p>
     */
    public long onBeforeCheckpointBeginDuration(TimeUnit timeUnit) {
        return onBeforeCheckpointBeginDuration.duration(timeUnit);
    }

    /**
     * Callback before all {@link CheckpointListener#onMarkCheckpointBegin}.
     *
     * <p>Not thread safe.</p>
     */
    public void onMarkCheckpointBeginStart() {
        onMarkCheckpointBeginDuration.onStart();
    }

    /**
     * Callback after all {@link CheckpointListener#onMarkCheckpointBegin}.
     *
     * <p>Not thread safe.</p>
     */
    public void onMarkCheckpointBeginEnd() {
        onMarkCheckpointBeginDuration.onEnd();
    }

    /**
     * Returns execution all {@link CheckpointListener#onMarkCheckpointBegin} under write lock duration in the given time unit.
     *
     * <p>Not thread safe.</p>
     */
    public long onMarkCheckpointBeginDuration(TimeUnit timeUnit) {
        return onMarkCheckpointBeginDuration.duration(timeUnit);
    }

    /**
     * Callback on start writes pages to {@link PageStore}s.
     *
     * <p>Not thread safe.</p>
     */
    public void onPagesWriteStart() {
        pagesWriteDuration.onStart();
    }

    /**
     * Callback on end writes pages to {@link PageStore}s.
     *
     * <p>Not thread safe.</p>
     */
    public void onPagesWriteEnd() {
        pagesWriteDuration.onEnd();
    }

    /**
     * Returns pages write duration in the given time unit.
     *
     * <p>Not thread safe.</p>
     */
    public long pagesWriteDuration(TimeUnit timeUnit) {
        return pagesWriteDuration.duration(timeUnit);
    }

    /**
     * Callback on start fsync {@link PageStore}s.
     *
     * <p>Not thread safe.</p>
     */
    public void onFsyncStart() {
        fsyncDuration.onStart();
    }

    /**
     * Callback on end fsync {@link PageStore}s.
     *
     * <p>Not thread safe.</p>
     */
    public void onFsyncEnd() {
        fsyncDuration.onEnd();
    }

    /**
     * Returns checkpoint fsync duration in the given time unit.
     *
     * <p>Not thread safe.</p>
     */
    public long fsyncDuration(TimeUnit timeUnit) {
        return fsyncDuration.duration(timeUnit);
    }

    /**
     * Callback before split and sort checkpoint pages.
     *
     * <p>Not thread safe.</p>
     */
    public void onSplitAndSortCheckpointPagesStart() {
        splitAndSortCheckpointPagesDuration.onStart();
    }

    /**
     * Callback after split and sort checkpoint pages.
     *
     * <p>Not thread safe.</p>
     */
    public void onSplitAndSortCheckpointPagesEnd() {
        splitAndSortCheckpointPagesDuration.onEnd();
    }

    /**
     * Returns duration of splitting and sorting checkpoint pages in mills.
     *
     * <p>Not thread safe.</p>
     */
    public long splitAndSortCheckpointPagesDuration(TimeUnit timeUnit) {
        return splitAndSortCheckpointPagesDuration.duration(timeUnit);
    }

    /**
     * Callback at the start of the replication protocol write-ahead-log sync.
     *
     * <p>Not thread safe.</p>
     */
    public void onReplicatorLogSyncStart() {
        replicatorLogSyncDuration.onStart();
    }

    /**
     * Callback at the end of the replication protocol write-ahead-log sync.
     *
     * <p>Not thread safe.</p>
     */
    public void onReplicatorLogSyncEnd() {
        replicatorLogSyncDuration.onEnd();
    }

    /**
     * Returns checkpoint replication protocol write-ahead-log sync duration in the given time unit.
     *
     * <p>Not thread safe.</p>
     */
    public long replicatorLogSyncDuration(TimeUnit timeUnit) {
        return replicatorLogSyncDuration.duration(timeUnit);
    }

    /**
     * Callback after acquire checkpoint write lock.
     *
     * <p>Not thread safe.</p>
     */
    public void onWriteLockHoldStart() {
        writeLockHoldDuration.onStart();
    }

    /**
     * Callback after release checkpoint write lock.
     *
     * <p>Not thread safe.</p>
     */
    public void onWriteLockHoldEnd() {
        writeLockHoldDuration.onEnd();
    }

    /**
     * Returns checkpoint write lock hold duration in the given time unit.
     *
     * <p>Not thread safe.</p>
     */
    public long writeLockHoldDuration(TimeUnit timeUnit) {
        return writeLockHoldDuration.duration(timeUnit);
    }

    /**
     * Returns checkpoint action before taken write lock duration in the given time unit.
     *
     * <p>Not thread safe.</p>
     */
    public long beforeWriteLockDuration(TimeUnit timeUnit) {
        return timeUnit.convert(writeLockWaitDuration.startNanos() - checkpointDuration.startNanos(), NANOSECONDS);
    }

    /**
     * Callback to start waiting for page replacement to complete for all pages.
     *
     * <p>Not thread safe.</p>
     */
    public void onWaitPageReplacementStart() {
        waitPageReplacement.onStart();
    }

    /**
     * Callback on end waiting for page replacement to complete for all pages.
     *
     * <p>Not thread safe.</p>
     */
    public void onWaitPageReplacementEnd() {
        waitPageReplacement.onEnd();
    }

    /**
     * Returns checkpoint waiting page replacement to complete duration in the given time unit.
     *
     * <p>Not thread safe.</p>
     */
    public long waitPageReplacementDuration(TimeUnit timeUnit) {
        return waitPageReplacement.duration(timeUnit);
    }
}
