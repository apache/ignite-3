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
import org.apache.ignite.internal.metrics.StopWatchTimer;
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

    private final StopWatchTimer checkpointDuration = new StopWatchTimer();

    private final StopWatchTimer writeLockWaitDuration = new StopWatchTimer();

    private final StopWatchTimer onBeforeCheckpointBeginDuration = new StopWatchTimer();

    private final StopWatchTimer onMarkCheckpointBeginDuration = new StopWatchTimer();

    private final StopWatchTimer writeLockHoldDuration = new StopWatchTimer();

    private final StopWatchTimer pagesWriteDuration = new StopWatchTimer();

    private final StopWatchTimer fsyncDuration = new StopWatchTimer();

    private final StopWatchTimer replicatorLogSyncDuration = new StopWatchTimer();

    private final StopWatchTimer splitAndSortCheckpointPagesDuration = new StopWatchTimer();

    private final StopWatchTimer waitPageReplacement = new StopWatchTimer();

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
        checkpointDuration.start();
    }

    /**
     * Callback on checkpoint end.
     *
     * <p>Not thread safe.</p>
     */
    public void onCheckpointEnd() {
        checkpointDuration.end();
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
        writeLockWaitDuration.start();
    }

    /**
     * Callback after acquiring checkpoint write lock.
     *
     * <p>Not thread safe.</p>
     */
    public void onWriteLockWaitEnd() {
        writeLockWaitDuration.end();
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
        onBeforeCheckpointBeginDuration.start();
    }

    /**
     * Callback after all {@link CheckpointListener#beforeCheckpointBegin}.
     *
     * <p>Not thread safe.</p>
     */
    public void onBeforeCheckpointBeginEnd() {
        onBeforeCheckpointBeginDuration.end();
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
        onMarkCheckpointBeginDuration.start();
    }

    /**
     * Callback after all {@link CheckpointListener#onMarkCheckpointBegin}.
     *
     * <p>Not thread safe.</p>
     */
    public void onMarkCheckpointBeginEnd() {
        onMarkCheckpointBeginDuration.end();
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
        pagesWriteDuration.start();
    }

    /**
     * Callback on end writes pages to {@link PageStore}s.
     *
     * <p>Not thread safe.</p>
     */
    public void onPagesWriteEnd() {
        pagesWriteDuration.end();
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
        fsyncDuration.start();
    }

    /**
     * Callback on end fsync {@link PageStore}s.
     *
     * <p>Not thread safe.</p>
     */
    public void onFsyncEnd() {
        fsyncDuration.end();
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
        splitAndSortCheckpointPagesDuration.start();
    }

    /**
     * Callback after split and sort checkpoint pages.
     *
     * <p>Not thread safe.</p>
     */
    public void onSplitAndSortCheckpointPagesEnd() {
        splitAndSortCheckpointPagesDuration.end();
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
        replicatorLogSyncDuration.start();
    }

    /**
     * Callback at the end of the replication protocol write-ahead-log sync.
     *
     * <p>Not thread safe.</p>
     */
    public void onReplicatorLogSyncEnd() {
        replicatorLogSyncDuration.end();
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
        writeLockHoldDuration.start();
    }

    /**
     * Callback after release checkpoint write lock.
     *
     * <p>Not thread safe.</p>
     */
    public void onWriteLockHoldEnd() {
        writeLockHoldDuration.end();
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
        waitPageReplacement.start();
    }

    /**
     * Callback on end waiting for page replacement to complete for all pages.
     *
     * <p>Not thread safe.</p>
     */
    public void onWaitPageReplacementEnd() {
        waitPageReplacement.end();
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
