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
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionProcessingCounterMap;
import org.jetbrains.annotations.Nullable;

/**
 * Data class representing the state of running/scheduled checkpoint.
 */
class CheckpointProgressImpl implements CheckpointProgress {
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
    private volatile int currCheckpointPagesCnt;

    /** Cause of fail, which has happened during the checkpoint or {@code null} if checkpoint was successful. */
    @Nullable
    private volatile Throwable failCause;

    /** Counter for written checkpoint pages. */
    private final AtomicInteger writtenPagesCntr = new AtomicInteger();

    /** Counter for fsynced checkpoint pages. */
    private final AtomicInteger syncedPagesCntr = new AtomicInteger();

    /** Counter for evicted checkpoint pages. */
    private final AtomicInteger evictedPagesCntr = new AtomicInteger();

    /** Sorted dirty pages to be written on the checkpoint. */
    private volatile @Nullable CheckpointDirtyPages pageToWrite;

    /** Partitions currently being processed, for example, writing dirty pages or doing fsync. */
    private final PartitionProcessingCounterMap processedPartitionMap = new PartitionProcessingCounterMap();

    /**
     * Constructor.
     *
     * @param delay Delay in nanos before next checkpoint is to be executed. Value is from {@code 0} to {@code 365} days.
     */
    CheckpointProgressImpl(long delay) {
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

    /**
     * Returns counter for fsynced checkpoint pages.
     */
    public AtomicInteger syncedPagesCounter() {
        return syncedPagesCntr;
    }

    /**
     * Returns Counter for evicted pages during current checkpoint.
     */
    public AtomicInteger evictedPagesCounter() {
        return evictedPagesCntr;
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
     * Callback at the beginning of checkpoint processing of a partition, for example, when writing dirty pages or executing a fsync.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     */
    public void onStartPartitionProcessing(GroupPartitionId groupPartitionId) {
        processedPartitionMap.incrementPartitionProcessingCounter(groupPartitionId);
    }

    /**
     * Callback on completion of partition processing, for example, when writing dirty pages or executing a fsync.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     */
    public void onFinishPartitionProcessing(GroupPartitionId groupPartitionId) {
        processedPartitionMap.decrementPartitionProcessingCounter(groupPartitionId);
    }

    /**
     * Returns the future if the partition according to the given parameters is currently being processed, for example, dirty pages are
     * being written or fsync is being done, {@code null} if the partition is not currently being processed.
     *
     * <p>Future will be added on {@link #onStartPartitionProcessing(GroupPartitionId)} call and completed on
     * {@link #onFinishPartitionProcessing(GroupPartitionId)} call (equal to the number of
     * {@link #onFinishPartitionProcessing(GroupPartitionId)} calls).
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     */
    @Nullable
    public CompletableFuture<Void> getProcessedPartitionFuture(GroupPartitionId groupPartitionId) {
        return processedPartitionMap.getProcessedPartitionFuture(groupPartitionId);
    }
}
