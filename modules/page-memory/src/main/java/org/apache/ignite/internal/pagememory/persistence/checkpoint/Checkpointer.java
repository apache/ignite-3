/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static java.lang.Math.max;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointReadWriteLock.CHECKPOINT_RUNNER_THREAD_PREFIX;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.IgniteUtils.safeAbs;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.lang.IgniteSystemProperties.getInteger;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.worker.IgniteWorker;
import org.apache.ignite.internal.util.worker.IgniteWorkerListener;
import org.apache.ignite.internal.util.worker.WorkProgressDispatcher;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Empty.
 */
// TODO: IGNITE-16898 Continue porting the code
//public abstract class Checkpointer extends IgniteWorker {
public abstract class Checkpointer {
    public abstract Thread runner();

    public abstract CheckpointProgress scheduleCheckpoint(long l, String s);

//    private static final String CHECKPOINT_STARTED_LOG_FORMAT = "Checkpoint started ["
//            + "checkpointId=%s, "
//            + "startPtr=%s, "
//            + "checkpointBeforeLockTime=%dms, "
//            + "checkpointLockWait=%dms, "
//            + "checkpointListenersExecuteTime=%dms, "
//            + "checkpointLockHoldTime=%dms, "
//            + "walCpRecordFsyncDuration=%dms, "
//            + "writeCheckpointEntryDuration=%dms, "
//            + "splitAndSortCpPagesDuration=%dms, "
//            + "%s"
//            + "pages=%d, "
//            + "reason='%s']";
//
//    /** Timeout between partition file destroy and checkpoint to handle it. */
//    private static final long PARTITION_DESTROY_CHECKPOINT_TIMEOUT = 30 * 1000; // 30 Seconds.
//
//    /** Skip sync. */
//    private final boolean skipSync = getBoolean(IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC);
//
//    /** Avoid the start checkpoint if checkpointer was canceled. */
//    private volatile boolean skipCheckpointOnNodeStop = getBoolean(IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, false);
//
//    /** Long JVM pause threshold. */
//    private final int longJvmPauseThreshold = getInteger(IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD, DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD);
//
//    /** Pause detector. */
//    private final LongJvmPauseDetector pauseDetector;
//
//    /** Interval in ms after which the checkpoint is triggered if there are no other events. */
//    private final long checkpointFreq;
//
//    /** Strategy of where and how to get the pages. */
//    private final CheckpointWorkflow checkpointWorkflow;
//
//    /** Factory for the creation of page-write workers. */
//    private final CheckpointPagesWriterFactory checkpointPagesWriterFactory;
//
//    /** Checkpoint frequency deviation. */
//    private final Supplier<Integer> cpFreqDeviation;
//
//    /** Checkpoint runner thread pool. If {@code null} tasks are to be run in single thread. */
//    @Nullable
//    private final ThreadPoolExecutor checkpointWritePagesPool;
//
//    /** Next scheduled checkpoint progress. */
//    private volatile CheckpointProgressImpl scheduledCheckpoint;
//
//    /** Current checkpoint. This field is updated only by checkpoint thread. */
//    @Nullable
//    private volatile CheckpointProgressImpl currentCheckpointProgress;
//
//    /** Shutdown now. */
//    private volatile boolean shutdownNow;
//
//    /** Last checkpoint timestamp. */
//    private long lastCheckpointTimestamp;
//
//    /** For testing only. */
//    @TestOnly
//    @Nullable
//    private CompletableFuture<?> enableChangeAppliedFuture;
//
//    /** For testing only. */
//    @TestOnly
//    private volatile boolean checkpointsEnabled = true;
//
//    /**
//     * Constructor.
//     *
//     * @param log Logger.
//     * @param igniteInstanceName Name of the Ignite instance.
//     * @param workerName Thread name.
//     * @param workerListener Listener for life-cycle worker events.
//     * @param detector Long JVM pause detector.
//     * @param checkpointWorkFlow Implementation of checkpoint.
//     * @param factory Page writer factory.
//     * @param checkpointFrequency Checkpoint frequency.
//     * @param checkpointWritePageThreads The number of IO-bound threads which will write pages to disk.
//     * @param cpFreqDeviation Deviation of checkpoint frequency.
//     */
//    Checkpointer(
//            IgniteLogger log,
//            String igniteInstanceName,
//            String workerName,
//            @Nullable IgniteWorkerListener workerListener,
//            LongJvmPauseDetector detector,
//            CheckpointWorkflow checkpointWorkFlow,
//            CheckpointPagesWriterFactory factory,
//            long checkpointFrequency,
//            int checkpointWritePageThreads,
//            Supplier<Integer> cpFreqDeviation
//    ) {
//        super(log, igniteInstanceName, workerName, workerListener);
//
//        this.pauseDetector = detector;
//        this.checkpointFreq = checkpointFrequency;
//        this.checkpointWorkflow = checkpointWorkFlow;
//        this.checkpointPagesWriterFactory = factory;
//        this.cpFreqDeviation = cpFreqDeviation;
//
//        scheduledCheckpoint = new CheckpointProgressImpl(nextCheckpointInterval());
//
//        if (checkpointWritePageThreads > 1) {
//            checkpointWritePagesPool = new ThreadPoolExecutor(
//                    checkpointWritePageThreads,
//                    checkpointWritePageThreads,
//                    30_000,
//                    MILLISECONDS,
//                    new LinkedBlockingQueue<>(),
//                    new NamedThreadFactory(CHECKPOINT_RUNNER_THREAD_PREFIX + "-IO")
//            );
//        } else {
//            checkpointWritePagesPool = null;
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    protected void body() {
//        Throwable err = null;
//
//        try {
//            while (!isCancelled()) {
//                waitCheckpointEvent();
//
//                if (skipCheckpointOnNodeStop && (isCancelled() || shutdownNow)) {
//                    if (log.isInfoEnabled()) {
//                        log.warn("Skipping last checkpoint because node is stopping.");
//                    }
//
//                    return;
//                }
//
//                CompletableFuture<?> enableChangeAppliedFuture = this.enableChangeAppliedFuture;
//
//                if (enableChangeAppliedFuture != null) {
//                    enableChangeAppliedFuture.complete(null);
//
//                    this.enableChangeAppliedFuture = null;
//                }
//
//                if (checkpointsEnabled) {
//                    doCheckpoint();
//                } else {
//                    synchronized (this) {
//                        scheduledCheckpoint.nextCheckpointNanos(nanoTime() + MILLISECONDS.toNanos(nextCheckpointInterval()));
//                    }
//                }
//            }
//
//            // Final run after the cancellation.
//            if (checkpointsEnabled && !shutdownNow) {
//                doCheckpoint();
//            }
//        } catch (Throwable t) {
//            err = t;
//
//            scheduledCheckpoint.fail(t);
//
//            throw t;
//        } finally {
//            if (err == null && !(isCancelled.get())) {
//                err = new IllegalStateException("Thread is terminated unexpectedly: " + name());
//            }
//
//            if (err instanceof OutOfMemoryError) {
//                failureProcessor.process(new FailureContext(CRITICAL_ERROR, err));
//            } else if (err != null) {
//                failureProcessor.process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
//            }
//
//            scheduledCheckpoint.fail(new NodeStoppingException("Node is stopping."));
//        }
//    }
//
//    /**
//     *
//     */
//    public <R> CheckpointProgress scheduleCheckpoint(
//            long delayFromNow,
//            String reason,
//            IgniteInClosure<? super IgniteInternalFuture<R>> lsnr
//    ) {
//        CheckpointProgressImpl current = currentCheckpointProgress;
//
//        //If checkpoint haven't taken the write lock yet it shouldn't trigger a new checkpoint but should return current one.
//        if (lsnr == null && current != null && !current.greaterOrEqualTo(LOCK_TAKEN)) {
//            return current;
//        }
//
//        if (lsnr != null) {
//            //To be sure lsnr always will be executed in checkpoint thread.
//            synchronized (this) {
//                current = scheduledCheckpoint;
//
//                current.futureFor(FINISHED).listen(lsnr);
//            }
//        }
//
//        current = scheduledCheckpoint;
//
//        long nextNanos = nanoTime() + MILLISECONDS.toNanos(delayFromNow);
//
//        if (current.nextCheckpointNanos() - nextNanos <= 0) {
//            return current;
//        }
//
//        synchronized (this) {
//            current = scheduledCheckpoint;
//
//            if (current.nextCheckpointNanos() - nextNanos > 0) {
//                current.reason(reason);
//
//                current.nextCheckpointNanos(nextNanos);
//            }
//
//            notifyAll();
//        }
//
//        return current;
//    }
//
//    private void doCheckpoint() {
//        Checkpoint chp = null;
//
//        try {
//            startCheckpointProgress();
//
//            try {
//                chp = checkpointWorkflow.markCheckpointBegin(lastCheckpointTimestamp, currentCheckpointProgress);
//            } catch (Exception e) {
//                if (currentCheckpointProgress != null) {
//                    currentCheckpointProgress.fail(e);
//                }
//
//                // In case of checkpoint initialization error node should be invalidated and stopped.
//                failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, e));
//
//                // Re-throw as unchecked exception to force stopping checkpoint thread.
//                throw new IgniteInternalException(e);
//            }
//
//            updateHeartbeat();
//
//            currentCheckpointProgress.initCounters(chp.pagesSize);
//
//            if (chp.hasDelta()) {
//                if (log.isInfoEnabled()) {
//                    long possibleJvmPauseDur = possibleLongJvmPauseDuration(tracker);
//
//                    if (log.isInfoEnabled()) {
//                        log.info(
//                                String.format(
//                                        CHECKPOINT_STARTED_LOG_FORMAT,
//                                        tracker.beforeLockDuration(),
//                                        tracker.lockWaitDuration(),
//                                        tracker.listenersExecuteDuration(),
//                                        tracker.lockHoldDuration(),
//                                        tracker.walCpRecordFsyncDuration(),
//                                        tracker.writeCheckpointEntryDuration(),
//                                        tracker.splitAndSortCpPagesDuration(),
//                                        possibleJvmPauseDur > 0 ? "possibleJvmPauseDuration=" + possibleJvmPauseDur + "ms, " : "",
//                                        chp.pagesSize,
//                                        chp.progress.reason()
//                                )
//                        );
//                    }
//                }
//
//                if (!writePages(chp.dirtyPages, chp.progress, this, this::isShutdownNow)) {
//                    return;
//                }
//            } else {
//                if (log.isInfoEnabled()) {
//                    log.info(String.format(
//                            "Skipping checkpoint (no pages were modified) ["
//                                    + "checkpointBeforeLockTime=%dms, checkpointLockWait=%dms, "
//                                    + "checkpointListenersExecuteTime=%dms, checkpointLockHoldTime=%dms, reason='%s']",
//                            tracker.beforeLockDuration(),
//                            tracker.lockWaitDuration(),
//                            tracker.listenersExecuteDuration(),
//                            tracker.lockHoldDuration(),
//                            chp.progress.reason())
//                    );
//                }
//
//                tracker.onPagesWriteStart();
//                tracker.onFsyncStart();
//            }
//
//            // Must mark successful checkpoint only if there are no exceptions or interrupts.
//            checkpointWorkflow.markCheckpointEnd(chp);
//
//            tracker.onEnd();
//
//            if (chp.hasDelta()) {
//                if (log.isInfoEnabled()) {
//                    log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
//                                    "walSegmentsCovered=%s, markDuration=%dms, pagesWrite=%dms, fsync=%dms, total=%dms]",
//                            chp.cpEntry != null ? chp.cpEntry.checkpointId() : "",
//                            chp.pagesSize,
//                            chp.cpEntry != null ? chp.cpEntry.checkpointMark() : "",
//                            walRangeStr(chp.walSegsCoveredRange),
//                            tracker.markDuration(),
//                            tracker.pagesWriteDuration(),
//                            tracker.fsyncDuration(),
//                            tracker.totalDuration()));
//                }
//            }
//
//            updateMetrics(chp, tracker);
//        } catch (IgniteInternalCheckedException e) {
//            chp.progress.fail(e);
//
//            failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, e));
//        }
//    }
//
//    /**
//     * @param workProgressDispatcher Work progress dispatcher.
//     * @param cpPages List of pages to write.
//     * @param curCpProgress Current checkpoint data.
//     * @param shutdownNow Checker of stop operation.
//     */
//    boolean writePages(
//            GridConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> cpPages,
//            CheckpointProgressImpl curCpProgress,
//            WorkProgressDispatcher workProgressDispatcher,
//            BooleanSupplier shutdownNow
//    ) throws IgniteInternalCheckedException {
//        ThreadPoolExecutor pageWritePool = checkpointWritePagesPool;
//
//        int checkpointWritePageThreads = pageWritePool == null ? 1 : pageWritePool.getMaximumPoolSize();
//
//        // Identity stores set.
//        ConcurrentLinkedHashMap<PageStore, LongAdder> updStores = new ConcurrentLinkedHashMap<>();
//
//        CountDownFuture doneWriteFut = new CountDownFuture(checkpointWritePageThreads);
//
//        tracker.onPagesWriteStart();
//
//        for (int i = 0; i < checkpointWritePageThreads; i++) {
//            Runnable write = checkpointPagesWriterFactory.build(
//                    tracker,
//                    cpPages,
//                    updStores,
//                    doneWriteFut,
//                    workProgressDispatcher::updateHeartbeat,
//                    curCpProgress,
//                    shutdownNow
//            );
//
//            if (pageWritePool == null) {
//                write.run();
//            } else {
//                try {
//                    pageWritePool.execute(write);
//                } catch (RejectedExecutionException ignore) {
//                    // Run the task synchronously.
//                    write.run();
//                }
//            }
//        }
//
//        workProgressDispatcher.updateHeartbeat();
//
//        // Wait and check for errors.
//        doneWriteFut.get();
//
//        // Must re-check shutdown flag here because threads may have skipped some pages.
//        // If so, we should not put finish checkpoint mark.
//        if (shutdownNow.getAsBoolean()) {
//            curCpProgress.fail(new NodeStoppingException("Node is stopping."));
//
//            return false;
//        }
//
//        tracker.onFsyncStart();
//
//        if (!skipSync) {
//            syncUpdatedStores(updStores);
//
//            if (shutdownNow.getAsBoolean()) {
//                curCpProgress.fail(new NodeStoppingException("Node is stopping."));
//
//                return false;
//            }
//        }
//
//        return true;
//    }
//
//    /**
//     * @param updStores Stores which should be synced.
//     */
//    private void syncUpdatedStores(
//            ConcurrentLinkedHashMap<PageStore, LongAdder> updStores
//    ) throws IgniteInternalCheckedException {
//        ThreadPoolExecutor pageWritePool = checkpointWritePagesPool;
//
//        if (pageWritePool == null) {
//            for (Map.Entry<PageStore, LongAdder> updStoreEntry : updStores.entrySet()) {
//                if (shutdownNow) {
//                    return;
//                }
//
//                blockingSectionBegin();
//
//                try {
//                    updStoreEntry.getKey().sync();
//                } finally {
//                    blockingSectionEnd();
//                }
//
//                currentProgress().updateSyncedPages(updStoreEntry.getValue().intValue());
//            }
//        } else {
//            int checkpointThreads = pageWritePool.getMaximumPoolSize();
//
//            CountDownFuture doneFut = new CountDownFuture(checkpointThreads);
//
//            BlockingQueue<Entry<PageStore, LongAdder>> queue = new LinkedBlockingQueue<>(updStores.entrySet());
//
//            for (int i = 0; i < checkpointThreads; i++) {
//                pageWritePool.execute(() -> {
//                    Map.Entry<PageStore, LongAdder> updStoreEntry = queue.poll();
//
//                    boolean err = false;
//
//                    try {
//                        while (updStoreEntry != null) {
//                            if (shutdownNow) {
//                                return;
//                            }
//
//                            blockingSectionBegin();
//
//                            try {
//                                updStoreEntry.getKey().sync();
//                            } finally {
//                                blockingSectionEnd();
//                            }
//
//                            currentCheckpointProgress.updateSyncedPages(updStoreEntry.getValue().intValue());
//
//                            updStoreEntry = queue.poll();
//                        }
//                    } catch (Throwable t) {
//                        err = true;
//
//                        doneFut.onDone(t);
//                    } finally {
//                        if (!err) {
//                            doneFut.onDone();
//                        }
//                    }
//                });
//            }
//
//            blockingSectionBegin();
//
//            try {
//                doneFut.get();
//            } finally {
//                blockingSectionEnd();
//            }
//        }
//    }
//
//    /**
//     * Waiting until the next checkpoint time.
//     */
//    private void waitCheckpointEvent() {
//        try {
//            synchronized (this) {
//                long remaining = NANOSECONDS.toMillis(scheduledCheckpoint.nextCheckpointNanos() - nanoTime());
//
//                while (remaining > 0 && !isCancelled()) {
//                    blockingSectionBegin();
//
//                    try {
//                        wait(remaining);
//
//                        remaining = NANOSECONDS.toMillis(scheduledCheckpoint.nextCheckpointNanos() - nanoTime());
//                    } finally {
//                        blockingSectionEnd();
//                    }
//                }
//            }
//        } catch (InterruptedException ignored) {
//            Thread.currentThread().interrupt();
//
//            isCancelled.set(true);
//        }
//    }
//
//    /**
//     * @param tracker Checkpoint metrics tracker.
//     * @return Duration of possible JVM pause, if it was detected, or {@code -1} otherwise.
//     */
//    private long possibleLongJvmPauseDuration(CheckpointMetricsTracker tracker) {
//        if (LongJVMPauseDetector.enabled()) {
//            if (tracker.lockWaitDuration() + tracker.lockHoldDuration() > longJvmPauseThreshold) {
//                long now = coarseCurrentTimeMillis();
//
//                // We must get last wake up time before search possible pause in events map.
//                long wakeUpTime = pauseDetector.getLastWakeUpTime();
//
//                IgniteBiTuple<Long, Long> lastLongPause = pauseDetector.getLastLongPause();
//
//                if (lastLongPause != null && tracker.checkpointStartTime() < lastLongPause.get1()) {
//                    return lastLongPause.get2();
//                }
//
//                if (now - wakeUpTime > longJvmPauseThreshold) {
//                    return now - wakeUpTime;
//                }
//            }
//        }
//
//        return -1L;
//    }
//
//    /**
//     * Update the current checkpoint info from the scheduled one.
//     */
//    private void startCheckpointProgress() {
//        long checkpointTimestamp = coarseCurrentTimeMillis();
//
//        // This can happen in an unlikely event of two checkpoints happening
//        // within a currentTimeMillis() granularity window.
//        if (checkpointTimestamp == lastCheckpointTimestamp) {
//            checkpointTimestamp++;
//        }
//
//        lastCheckpointTimestamp = checkpointTimestamp;
//
//        synchronized (this) {
//            CheckpointProgressImpl curr = scheduledCheckpoint;
//
//            if (curr.reason() == null) {
//                curr.reason("timeout");
//            }
//
//            // It is important that we assign a new progress object before checkpoint mark in page memory.
//            scheduledCheckpoint = new CheckpointProgressImpl(nextCheckpointInterval());
//
//            currentCheckpointProgress = curr;
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void cancel() {
//        if (log.isDebugEnabled()) {
//            log.debug("Cancelling grid runnable: " + this);
//        }
//
//        // Do not interrupt runner thread.
//        isCancelled.set(true);
//
//        synchronized (this) {
//            notifyAll();
//        }
//    }
//
//    /**
//     * Stopping all checkpoint activity immediately even if the current checkpoint is in progress.
//     */
//    public void shutdownNow() {
//        shutdownNow = true;
//
//        if (!isCancelled.get()) {
//            cancel();
//        }
//    }
//
//    /**
//     * Restart worker in IgniteThread.
//     */
//    public void start() {
//        if (runner() != null) {
//            return;
//        }
//
//        assert runner() == null : "Checkpointer is running.";
//
//        new IgniteThread(igniteInstanceName(), name(), this).start();
//    }
//
//    /**
//     * @param cancel Cancel flag.
//     */
//    @SuppressWarnings("unused")
//    public void shutdownCheckpointer(boolean cancel) {
//        if (cancel) {
//            shutdownNow();
//        } else {
//            cancel();
//        }
//
//        try {
//            U.join(this);
//        } catch (IgniteInterruptedCheckedException ignore) {
//            log.warn("Was interrupted while waiting for checkpointer shutdown, will not wait for checkpoint to finish.");
//
//            shutdownNow();
//
//            while (true) {
//                try {
//                    U.join(this);
//
//                    scheduledCheckpoint.fail(new NodeStoppingException("Checkpointer is stopped during node stop."));
//
//                    break;
//                } catch (IgniteInterruptedCheckedException ignored) {
//                    //Ignore
//                }
//            }
//
//            Thread.currentThread().interrupt();
//        }
//
//        if (checkpointWritePagesPool != null) {
//            shutdownAndAwaitTermination(checkpointWritePagesPool, 2, MINUTES);
//        }
//    }
//
//    /**
//     * Changes the information for a scheduled checkpoint if it was scheduled further than {@code delayFromNow}, or do nothing otherwise.
//     *
//     * @param delayFromNow Delay from now in milliseconds.
//     * @param reason Wakeup reason.
//     * @return Nearest scheduled checkpoint which is not started yet (dirty pages weren't collected yet).
//     */
//    public CheckpointProgress scheduleCheckpoint(long delayFromNow, String reason) {
//        return scheduleCheckpoint(delayFromNow, reason, null);
//    }
//
//    /**
//     * Returns runner thread, {@code null} if the worker has not yet started executing.
//     */
//    public abstract @Nullable Thread runner();
//
//    /**
//     * Returns Progress of current checkpoint, last finished one or {@code null}, if checkpoint has never started.
//     */
//    public @Nullable CheckpointProgress currentProgress() {
//        return currentCheckpointProgress;
//    }
//
//    /**
//     * Returns {@code true} if checkpoint should be stopped immediately.
//     */
//    private boolean isShutdownNow() {
//        return shutdownNow;
//    }
//
//    /**
//     * Skip checkpoint on node stop.
//     *
//     * @param skip If {@code true} skips checkpoint on node stop.
//     */
//    public void skipCheckpointOnNodeStop(boolean skip) {
//        skipCheckpointOnNodeStop = skip;
//    }
//
//    /**
//     * Gets a checkpoint interval with a randomized delay. It helps when the cluster makes a checkpoint in the same time in every node.
//     */
//    private long nextCheckpointInterval() {
//        Integer deviation = cpFreqDeviation.get();
//
//        if (deviation == null || deviation == 0) {
//            return checkpointFreq;
//        }
//
//        long startDelay = ThreadLocalRandom.current().nextLong(max(safeAbs(checkpointFreq * deviation) / 100, 1))
//                - max(safeAbs(checkpointFreq * deviation) / 200, 1);
//
//        return safeAbs(checkpointFreq + startDelay);
//    }
//
//    /**
//     * For test use only.
//     *
//     * @deprecated Should be rewritten to public API.
//     */
//    @TestOnly
//    public CompletableFuture<?> enableCheckpoints(boolean enable) {
//        CompletableFuture<?> future = new CompletableFuture<>();
//
//        enableChangeAppliedFuture = future;
//
//        checkpointsEnabled = enable;
//
//        return future;
//    }
//
//    /**
//     * {@link RejectedExecutionException} cannot be thrown by {@link #checkpointWritePagesPool} but this handler still exists just in case.
//     */
//    private void handleRejectiedExecutionException(RejectedExecutionException e) {
//        assert false : "Task should never be rejected by async runner";
//
//        // To protect from disabled asserts and call to failure handler.
//        throw new IgniteInternalException(e);
//    }
}
