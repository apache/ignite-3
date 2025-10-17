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

import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.SCHEDULED;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.jetbrains.annotations.Nullable;

/**
 * Data class representing the state of running/scheduled checkpoint.
 */
public class CheckpointProgressImpl implements CheckpointProgress {
    /** Checkpoint id. */
    private final UUID id = UUID.randomUUID();

    /** Scheduled time of checkpoint. */
    private volatile long nextCheckpointNanos;

    /** Current checkpoint state. */
    private final AtomicReference<CheckpointState> state = new AtomicReference<>(SCHEDULED);

    /** Future which would be finished when corresponds state is set. */
    private final Map<CheckpointState, CompletableFuture<Void>> stateFutures = new ConcurrentHashMap<>();

    /** Wakeup reason. */
    private volatile String reason;

    /** Number of dirty pages in current checkpoint at the beginning of checkpoint. */
    // TODO https://issues.apache.org/jira/browse/IGNITE-24878 This field is initialized twice.
    private volatile int currCheckpointPagesCnt;

    /** Cause of fail, which has happened during the checkpoint or {@code null} if checkpoint was successful. */
    @Nullable
    private volatile Throwable failCause;

    /** Counter for written checkpoint pages. */
    private final AtomicInteger writtenPagesCntr = new AtomicInteger();

    /** Counter for fsynced checkpoint pages. */
    private final AtomicInteger syncedPagesCntr = new AtomicInteger();

    /** Counter for fsynced checkpoint files. */
    private final AtomicInteger syncedFilesCntr = new AtomicInteger();

    /** Counter for evicted checkpoint pages. */
    private final AtomicInteger evictedPagesCntr = new AtomicInteger();

    /** Sorted dirty pages to be written on the checkpoint. */
    private volatile @Nullable CheckpointDirtyPages pageToWrite;

    /** Assistant for synchronizing page replacement and fsync phase. */
    private final CheckpointPageReplacement checkpointPageReplacement = new CheckpointPageReplacement();

    /** Time it took from the start of checkpoint to the moment when all pages have been written. */
    private long pagesWriteTimeMillis;

    /** Time it took to sync all updated pages. */
    private long fsyncTimeMillis;

    /**
     * Constructor.
     *
     * @param delay Delay in nanos before next checkpoint is to be executed. Value is from {@code 0} to {@code 365} days.
     */
    public CheckpointProgressImpl(long delay) {
        nextCheckpointNanos(delay);
    }

    @Override
    public UUID id() {
        return id;
    }

    @Override
    public @Nullable String reason() {
        return reason;
    }

    /**
     * Sets description of the reason of the current checkpoint.
     *
     * @param reason New wakeup reason.
     */
    public void reason(String reason) {
        this.reason = reason;
    }

    @Override
    public boolean inProgress() {
        return greaterOrEqualTo(LOCK_RELEASED) && !greaterOrEqualTo(FINISHED);
    }

    @Override
    public CompletableFuture<Void> futureFor(CheckpointState state) {
        CompletableFuture<Void> stateFut = stateFutures.computeIfAbsent(state, (k) -> new CompletableFuture<>());

        if (greaterOrEqualTo(state)) {
            completeFuture(stateFut, failCause);
        }

        return stateFut;
    }

    @Override
    public int currentCheckpointPagesCount() {
        return currCheckpointPagesCnt;
    }

    /**
     * Sets current checkpoint pages num to store.
     *
     * @param num Pages to store.
     */
    public void currentCheckpointPagesCount(int num) {
        currCheckpointPagesCnt = num;
    }

    /**
     * Returns counter for written checkpoint pages.
     */
    public AtomicInteger writtenPagesCounter() {
        return writtenPagesCntr;
    }

    @Override
    public int writtenPages() {
        return writtenPagesCntr.get();
    }

    @Override
    public AtomicInteger syncedPagesCounter() {
        return syncedPagesCntr;
    }

    /** Returns counter of fsync-ed checkpoint files. */
    public AtomicInteger syncedFilesCounter() {
        return syncedFilesCntr;
    }

    @Override
    public int syncedFiles() {
        return syncedFilesCntr.get();
    }

    @Override
    public AtomicInteger evictedPagesCounter() {
        return evictedPagesCntr;
    }

    @Override
    public long getPagesWriteTimeMillis() {
        return pagesWriteTimeMillis;
    }

    void setPagesWriteTimeMillis(long pagesWriteTimeMillis) {
        this.pagesWriteTimeMillis = pagesWriteTimeMillis;
    }

    @Override
    public long getFsyncTimeMillis() {
        return fsyncTimeMillis;
    }

    void setFsyncTimeMillis(long fsyncTimeMillis) {
        this.fsyncTimeMillis = fsyncTimeMillis;
    }

    /**
     * Returns scheduled time of checkpoint in nanos.
     */
    public long nextCheckpointNanos() {
        return nextCheckpointNanos;
    }

    /**
     * Sets new scheduled time of checkpoint in nanos.
     *
     * @param delay Delay in nanos before next checkpoint is to be executed. Value is from {@code 0} to {@code 365} days.
     */
    public void nextCheckpointNanos(long delay) {
        assert delay >= 0 : delay;
        assert delay <= TimeUnit.DAYS.toNanos(365) : delay;

        nextCheckpointNanos = System.nanoTime() + delay;
    }

    /**
     * Clear checkpoint progress counters.
     */
    public void clearCounters() {
        initCounters(0);
    }

    /**
     * Initialize all counters before checkpoint.
     *
     * @param checkpointPages Number of dirty pages in current checkpoint at the beginning of checkpoint.
     */
    public void initCounters(int checkpointPages) {
        currCheckpointPagesCnt = checkpointPages;

        writtenPagesCntr.set(0);
        syncedPagesCntr.set(0);
        evictedPagesCntr.set(0);
        syncedFilesCntr.set(0);
    }

    /**
     * Changing checkpoint state if order of state is correct.
     *
     * @param newState New checkpoint state.
     */
    public void transitTo(CheckpointState newState) {
        CheckpointState state = this.state.get();

        if (state.ordinal() < newState.ordinal()) {
            this.state.compareAndSet(state, newState);

            doFinishFuturesWhichLessOrEqualTo(newState);
        }
    }

    /**
     * Mark this checkpoint execution as failed.
     *
     * @param error Causal error of fail.
     */
    public void fail(Throwable error) {
        failCause = error;

        transitTo(FINISHED);
    }

    /**
     * Returns {@code true} if current state equal or greater to given state.
     *
     * @param expectedState Expected state.
     */
    public boolean greaterOrEqualTo(CheckpointState expectedState) {
        return state.get().ordinal() >= expectedState.ordinal();
    }

    /**
     * Returns current state.
     */
    CheckpointState state() {
        return state.get();
    }

    /**
     * Finishing futures with correct result in direct state order until lastState(included).
     *
     * @param lastState State until which futures should be done.
     */
    private void doFinishFuturesWhichLessOrEqualTo(CheckpointState lastState) {
        for (CheckpointState old : CheckpointState.values()) {
            completeFuture(stateFutures.get(old), failCause);

            if (old == lastState) {
                return;
            }
        }
    }

    private static void completeFuture(@Nullable CompletableFuture<?> future, @Nullable Throwable throwable) {
        if (future != null && !future.isDone()) {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(null);
            }
        }
    }

    @Override
    public @Nullable CheckpointDirtyPages pagesToWrite() {
        return pageToWrite;
    }

    /**
     * Sets the sorted dirty pages to be written on the checkpoint.
     *
     * @param pageToWrite Dirty pages.
     */
    void pagesToWrite(@Nullable CheckpointDirtyPages pageToWrite) {
        this.pageToWrite = pageToWrite;
    }

    /**
     * Block the start of the fsync phase at a checkpoint before replacing the page.
     *
     * <p>It is expected that the method will be invoked once and after that the {@link #unblockFsyncOnPageReplacement} will be invoked on
     * the same page.</p>
     *
     * <p>It is expected that the method will not be invoked after {@link #getUnblockFsyncOnPageReplacementFuture}, since by the start of
     * the fsync phase, write dirty pages at the checkpoint should be complete and no new page replacements should be started.</p>
     *
     * @param pageId Page ID for which page replacement is expected to begin.
     * @see #unblockFsyncOnPageReplacement
     * @see #getUnblockFsyncOnPageReplacementFuture()
     */
    void blockFsyncOnPageReplacement(DirtyFullPageId pageId) {
        checkpointPageReplacement.block(pageId);
    }

    /**
     * Unblocks the start of the fsync phase at a checkpoint after the page replacement is completed.
     *
     * <p>It is expected that the method will be invoked once and after the {@link #blockFsyncOnPageReplacement} for same page ID.</p>
     *
     * <p>The fsync phase will only be started after page replacement has been completed for all pages for which
     * {@link #blockFsyncOnPageReplacement} was invoked before {@link #getUnblockFsyncOnPageReplacementFuture} was invoked, or no page
     * replacement occurred at all.</p>
     *
     * <p>If an error occurs on any page replacement during one checkpoint, the future from {@link #getUnblockFsyncOnPageReplacementFuture}
     * will complete with the first error.</p>
     *
     * <p>The method must be invoked even if any error occurred, so as not to hang a checkpoint.</p>
     *
     * @param pageId Page ID for which the page replacement has ended.
     * @param error Error on page replacement, {@code null} if missing.
     * @see #blockFsyncOnPageReplacement
     * @see #getUnblockFsyncOnPageReplacementFuture()
     */
    void unblockFsyncOnPageReplacement(DirtyFullPageId pageId, @Nullable Throwable error) {
        checkpointPageReplacement.unblock(pageId, error);
    }

    /**
     * Return future that will be completed successfully if all {@link #blockFsyncOnPageReplacement} are completed, either if there were
     * none, or with an error from the first {@link #unblockFsyncOnPageReplacement}.
     *
     * <p>Must be invoked before the start of the fsync phase at the checkpoint and wait for the future to complete in order to safely
     * perform the phase.</p>
     *
     * @see #blockFsyncOnPageReplacement
     * @see #unblockFsyncOnPageReplacement
     */
    CompletableFuture<Void> getUnblockFsyncOnPageReplacementFuture() {
        return checkpointPageReplacement.stopBlocking();
    }
}
