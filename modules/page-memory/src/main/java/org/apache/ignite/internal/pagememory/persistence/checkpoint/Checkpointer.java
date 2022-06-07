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
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.IgniteUtils.safeAbs;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointView;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;
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

/**
 * Checkpointer object is used for notification on checkpoint begin, predicate is {@code nextCheckpointTimestamps - now > 0}.
 *
 * <p>Method {@link #scheduleCheckpoint} uses {@link Object#notifyAll()}, {@link #waitCheckpointEvent} uses {@link Object#wait(long)}.
 *
 * <p>Checkpointer is one threaded which means that only one checkpoint at the one moment possible.
 *
 * <p>Responsiblity:
 * <ul>
 * <li>Provide the API for schedule/trigger the checkpoint.</li>
 * <li>Schedule new checkpoint after current one according to checkpoint frequency.</li>
 * <li>Failure handling.</li>
 * <li>Managing of page write threads.</li>
 * <li>Logging and metrics of checkpoint.</li>
 * </ul>
 *
 * <p>Checkpointer steps:
 * <ul>
 * <li>Awaiting checkpoint event.</li>
 * <li>Collect all dirty pages from page memory under checkpoint write lock.</li>
 * <li>Start to write dirty pages to page store.</li>
 * <li>Finish the checkpoint.
 * </ul>
 */
public class Checkpointer extends IgniteWorker implements IgniteComponent {
    private static final String CHECKPOINT_STARTED_LOG_FORMAT = "Checkpoint started ["
            + "checkpointId=%s, "
            + "checkpointBeforeWriteLockTime=%dms, "
            + "checkpointWriteLockWait=%dms, "
            + "checkpointListenersExecuteTime=%dms, "
            + "checkpointWriteLockHoldTime=%dms, "
            + "splitAndSortPagesDuration=%dms, "
            + "%s"
            + "pages=%d, "
            + "reason='%s']";

    /**
     * Any thread with a such prefix is managed by the checkpoint.
     *
     * <p>So some conditions can rely on it(ex. we don't need a checkpoint lock there because checkpoint is already held write lock).
     */
    static final String CHECKPOINT_RUNNER_THREAD_PREFIX = "checkpoint-runner";

    /** Pause detector. */
    @Nullable
    private final LongJvmPauseDetector pauseDetector;

    /** Checkpoint config. */
    private final PageMemoryCheckpointConfiguration checkpointConfig;

    /** Strategy of where and how to get the pages. */
    private final CheckpointWorkflow checkpointWorkflow;

    /** Factory for the creation of page-write workers. */
    private final CheckpointPagesWriterFactory checkpointPagesWriterFactory;

    /** Checkpoint runner thread pool. If {@code null} tasks are to be run in single thread. */
    @Nullable
    private final ThreadPoolExecutor checkpointWritePagesPool;

    /** Next scheduled checkpoint progress. */
    private volatile CheckpointProgressImpl scheduledCheckpointProgress;

    /** Current checkpoint progress. This field is updated only by checkpoint thread. */
    @Nullable
    private volatile CheckpointProgressImpl currentCheckpointProgress;

    /** Shutdown now. */
    private volatile boolean shutdownNow;

    /** Last checkpoint timestamp, read/update only in checkpoint thread. */
    private long lastCheckpointTimestamp;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param igniteInstanceName Name of the Ignite instance.
     * @param workerListener Listener for life-cycle worker events.
     * @param detector Long JVM pause detector.
     * @param checkpointWorkFlow Implementation of checkpoint.
     * @param factory Page writer factory.
     * @param checkpointConfig Checkpoint configuration.
     */
    Checkpointer(
            IgniteLogger log,
            String igniteInstanceName,
            @Nullable IgniteWorkerListener workerListener,
            @Nullable LongJvmPauseDetector detector,
            CheckpointWorkflow checkpointWorkFlow,
            CheckpointPagesWriterFactory factory,
            PageMemoryCheckpointConfiguration checkpointConfig
    ) {
        super(log, igniteInstanceName, "checkpoint-thread", workerListener);

        this.pauseDetector = detector;
        this.checkpointConfig = checkpointConfig;
        this.checkpointWorkflow = checkpointWorkFlow;
        this.checkpointPagesWriterFactory = factory;

        scheduledCheckpointProgress = new CheckpointProgressImpl(MILLISECONDS.toNanos(nextCheckpointInterval()));

        int checkpointWritePageThreads = checkpointConfig.threads().value();

        if (checkpointWritePageThreads > 1) {
            checkpointWritePagesPool = new ThreadPoolExecutor(
                    checkpointWritePageThreads,
                    checkpointWritePageThreads,
                    30_000,
                    MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    new NamedThreadFactory(CHECKPOINT_RUNNER_THREAD_PREFIX + "-io")
            );
        } else {
            checkpointWritePagesPool = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void body() {
        try {
            while (!isCancelled()) {
                waitCheckpointEvent();

                if (isCancelled() || shutdownNow) {
                    if (log.isWarnEnabled()) {
                        log.warn("Skipping last checkpoint because node is stopping.");
                    }

                    return;
                }

                doCheckpoint();
            }

            // Final run after the cancellation.
            if (!shutdownNow) {
                doCheckpoint();
            }

            if (!isCancelled.get()) {
                throw new IllegalStateException("Thread is terminated unexpectedly: " + name());
            }

            scheduledCheckpointProgress.fail(new NodeStoppingException("Node is stopping."));
        } catch (Throwable t) {
            scheduledCheckpointProgress.fail(t);

            // TODO: IGNITE-16899 By analogy with 2.0, we need to handle the exception (err) by the FailureProcessor
            // We need to handle OutOfMemoryError and the rest in different ways

            throw new IgniteInternalException(t);
        }
    }

    /**
     * Changes the information for a scheduled checkpoint if it was scheduled further than {@code delayFromNow}, or do nothing otherwise.
     *
     * @param delayFromNow Delay from now in milliseconds.
     * @param reason Wakeup reason.
     * @return Nearest scheduled checkpoint which is not started yet (dirty pages weren't collected yet).
     */
    public CheckpointProgress scheduleCheckpoint(long delayFromNow, String reason) {
        CheckpointProgressImpl current = currentCheckpointProgress;

        // If checkpoint haven't taken write lock yet it shouldn't trigger a new checkpoint but should return current one.
        if (current != null && !current.greaterOrEqualTo(LOCK_TAKEN)) {
            return current;
        }

        current = scheduledCheckpointProgress;

        long nextNanos = nanoTime() + MILLISECONDS.toNanos(delayFromNow);

        if (current.nextCheckpointNanos() - nextNanos <= 0) {
            return current;
        }

        synchronized (this) {
            current = scheduledCheckpointProgress;

            if (current.nextCheckpointNanos() - nextNanos > 0) {
                current.reason(reason);

                current.nextCheckpointNanos(MILLISECONDS.toNanos(delayFromNow));
            }

            notifyAll();
        }

        return current;
    }

    /**
     * Executes a checkpoint.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    void doCheckpoint() throws IgniteInternalCheckedException {
        Checkpoint chp = null;

        try {
            CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

            startCheckpointProgress();

            try {
                chp = checkpointWorkflow.markCheckpointBegin(lastCheckpointTimestamp, currentCheckpointProgress, tracker);
            } catch (Exception e) {
                if (currentCheckpointProgress != null) {
                    currentCheckpointProgress.fail(e);
                }

                // TODO: IGNITE-16899 By analogy with 2.0, we need to handle the exception by the FailureProcessor
                // In case of checkpoint initialization error node should be invalidated and stopped.

                // Re-throw as unchecked exception to force stopping checkpoint thread.
                throw new IgniteInternalCheckedException(e);
            }

            updateHeartbeat();

            currentCheckpointProgress.initCounters(chp.dirtyPagesSize);

            if (chp.hasDelta()) {
                if (log.isInfoEnabled()) {
                    long possibleJvmPauseDuration = possibleLongJvmPauseDuration(tracker);

                    if (log.isInfoEnabled()) {
                        log.info(String.format(
                                CHECKPOINT_STARTED_LOG_FORMAT,
                                chp.progress.id(),
                                tracker.beforeWriteLockDuration(),
                                tracker.writeLockWaitDuration(),
                                tracker.onMarkCheckpointBeginDuration(),
                                tracker.writeLockHoldDuration(),
                                tracker.splitAndSortCheckpointPagesDuration(),
                                possibleJvmPauseDuration > 0 ? "possibleJvmPauseDuration=" + possibleJvmPauseDuration + "ms, " : "",
                                chp.dirtyPagesSize,
                                chp.progress.reason()
                        ));
                    }
                }

                if (!writePages(tracker, chp.dirtyPages, chp.progress, this, this::isShutdownNow)) {
                    return;
                }
            } else {
                if (log.isInfoEnabled()) {
                    log.info(String.format(
                            "Skipping checkpoint (no pages were modified) ["
                                    + "checkpointBeforeWriteLockTime=%dms, checkpointWriteLockWait=%dms, "
                                    + "checkpointListenersExecuteTime=%dms, checkpointWriteLockHoldTime=%dms, reason='%s']",
                            tracker.beforeWriteLockDuration(),
                            tracker.writeLockWaitDuration(),
                            tracker.onMarkCheckpointBeginDuration(),
                            tracker.writeLockHoldDuration(),
                            chp.progress.reason()
                    ));
                }

                tracker.onPagesWriteStart();
                tracker.onFsyncStart();
            }

            // Must mark successful checkpoint only if there are no exceptions or interrupts.
            checkpointWorkflow.markCheckpointEnd(chp);

            tracker.onCheckpointEnd();

            if (chp.hasDelta()) {
                if (log.isInfoEnabled()) {
                    log.info(String.format(
                            "Checkpoint finished [checkpointId=%s, pages=%d, pagesWriteTime=%dms, fsyncTime=%dms, totalTime=%dms]",
                            chp.progress.id(),
                            chp.dirtyPagesSize,
                            tracker.pagesWriteDuration(),
                            tracker.fsyncDuration(),
                            tracker.totalDuration()
                    ));
                }
            }
        } catch (IgniteInternalCheckedException e) {
            if (chp != null) {
                chp.progress.fail(e);
            }

            // TODO: IGNITE-16899 By analogy with 2.0, we need to handle the exception by the FailureProcessor

            throw e;
        }
    }

    /**
     * Writes dirty pages to the appropriate stores.
     *
     * @param tracker Checkpoint metrics tracker.
     * @param checkpointPages Checkpoint pages to write.
     * @param currentCheckpointProgress Current checkpoint progress.
     * @param workProgressDispatcher Work progress dispatcher.
     * @param shutdownNow Checker of stop operation.
     * @throws IgniteInternalCheckedException If failed.
     */
    boolean writePages(
            CheckpointMetricsTracker tracker,
            IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> checkpointPages,
            CheckpointProgressImpl currentCheckpointProgress,
            WorkProgressDispatcher workProgressDispatcher,
            BooleanSupplier shutdownNow
    ) throws IgniteInternalCheckedException {
        ThreadPoolExecutor pageWritePool = checkpointWritePagesPool;

        int checkpointWritePageThreads = pageWritePool == null ? 1 : pageWritePool.getMaximumPoolSize();

        // Identity stores set.
        ConcurrentMap<PageStore, LongAdder> updStores = new ConcurrentHashMap<>();

        CompletableFuture<?>[] futures = new CompletableFuture[checkpointWritePageThreads];

        tracker.onPagesWriteStart();

        for (int i = 0; i < checkpointWritePageThreads; i++) {
            CheckpointPagesWriter write = checkpointPagesWriterFactory.build(
                    tracker,
                    checkpointPages,
                    updStores,
                    futures[i] = new CompletableFuture<>(),
                    workProgressDispatcher::updateHeartbeat,
                    currentCheckpointProgress,
                    shutdownNow
            );

            if (pageWritePool == null) {
                write.run();
            } else {
                try {
                    pageWritePool.execute(write);
                } catch (RejectedExecutionException ignore) {
                    // Run the task synchronously.
                    write.run();
                }
            }
        }

        workProgressDispatcher.updateHeartbeat();

        // Wait and check for errors.
        CompletableFuture.allOf(futures).join();

        // Must re-check shutdown flag here because threads may have skipped some pages.
        // If so, we should not put finish checkpoint mark.
        if (shutdownNow.getAsBoolean()) {
            currentCheckpointProgress.fail(new NodeStoppingException("Node is stopping."));

            return false;
        }

        tracker.onFsyncStart();

        syncUpdatedStores(updStores);

        if (shutdownNow.getAsBoolean()) {
            currentCheckpointProgress.fail(new NodeStoppingException("Node is stopping."));

            return false;
        }

        return true;
    }

    private void syncUpdatedStores(
            ConcurrentMap<PageStore, LongAdder> updatedStoresForSync
    ) throws IgniteInternalCheckedException {
        ThreadPoolExecutor pageWritePool = checkpointWritePagesPool;

        if (pageWritePool == null) {
            for (Map.Entry<PageStore, LongAdder> updStoreEntry : updatedStoresForSync.entrySet()) {
                if (shutdownNow) {
                    return;
                }

                blockingSectionBegin();

                try {
                    updStoreEntry.getKey().sync();
                } finally {
                    blockingSectionEnd();
                }

                currentCheckpointProgress.syncedPagesCounter().addAndGet(updStoreEntry.getValue().intValue());
            }
        } else {
            int checkpointThreads = pageWritePool.getMaximumPoolSize();

            CompletableFuture<?>[] futures = new CompletableFuture[checkpointThreads];

            for (int i = 0; i < checkpointThreads; i++) {
                futures[i] = new CompletableFuture<>();
            }

            BlockingQueue<Entry<PageStore, LongAdder>> queue = new LinkedBlockingQueue<>(updatedStoresForSync.entrySet());

            for (int i = 0; i < checkpointThreads; i++) {
                int threadIdx = i;

                pageWritePool.execute(() -> {
                    Map.Entry<PageStore, LongAdder> updStoreEntry = queue.poll();

                    try {
                        while (updStoreEntry != null) {
                            if (shutdownNow) {
                                return;
                            }

                            blockingSectionBegin();

                            try {
                                updStoreEntry.getKey().sync();
                            } finally {
                                blockingSectionEnd();
                            }

                            currentCheckpointProgress.syncedPagesCounter().addAndGet(updStoreEntry.getValue().intValue());

                            updStoreEntry = queue.poll();
                        }

                        futures[threadIdx].complete(null);
                    } catch (Throwable t) {
                        futures[threadIdx].completeExceptionally(t);
                    }
                });
            }

            blockingSectionBegin();

            try {
                CompletableFuture.allOf(futures).join();
            } finally {
                blockingSectionEnd();
            }
        }
    }

    /**
     * Waiting until the next checkpoint time.
     */
    void waitCheckpointEvent() {
        try {
            synchronized (this) {
                long remaining = NANOSECONDS.toMillis(scheduledCheckpointProgress.nextCheckpointNanos() - nanoTime());

                while (remaining > 0 && !isCancelled()) {
                    blockingSectionBegin();

                    try {
                        wait(remaining);

                        remaining = NANOSECONDS.toMillis(scheduledCheckpointProgress.nextCheckpointNanos() - nanoTime());
                    } finally {
                        blockingSectionEnd();
                    }
                }
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();

            isCancelled.set(true);
        }
    }

    /**
     * Returns duration of possible JVM pause, if it was detected, or {@code -1} otherwise.
     *
     * @param tracker Checkpoint metrics tracker.
     */
    private long possibleLongJvmPauseDuration(CheckpointMetricsTracker tracker) {
        if (pauseDetector != null) {
            if (tracker.writeLockWaitDuration() + tracker.writeLockHoldDuration() > pauseDetector.longJvmPauseThreshold()) {
                long now = coarseCurrentTimeMillis();

                // We must get last wake-up time before search possible pause in events map.
                long wakeUpTime = pauseDetector.getLastWakeUpTime();

                IgniteBiTuple<Long, Long> lastLongPause = pauseDetector.getLastLongPause();

                if (lastLongPause != null && tracker.checkpointStartTime() < lastLongPause.get1()) {
                    return lastLongPause.get2();
                }

                if (now - wakeUpTime > pauseDetector.longJvmPauseThreshold()) {
                    return now - wakeUpTime;
                }
            }
        }

        return -1L;
    }

    /**
     * Update the current checkpoint info from the scheduled one.
     */
    void startCheckpointProgress() {
        long checkpointStartTimestamp = coarseCurrentTimeMillis();

        // This can happen in an unlikely event of two checkpoints happening within a currentTimeMillis() granularity window.
        if (checkpointStartTimestamp == lastCheckpointTimestamp) {
            checkpointStartTimestamp++;
        }

        lastCheckpointTimestamp = checkpointStartTimestamp;

        synchronized (this) {
            CheckpointProgressImpl curr = scheduledCheckpointProgress;

            if (curr.reason() == null) {
                curr.reason("timeout");
            }

            // It is important that we assign a new progress object before checkpoint mark in page memory.
            scheduledCheckpointProgress = new CheckpointProgressImpl(MILLISECONDS.toNanos(nextCheckpointInterval()));

            currentCheckpointProgress = curr;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void cancel() {
        if (log.isDebugEnabled()) {
            log.debug("Cancelling grid runnable: " + this);
        }

        // Do not interrupt runner thread.
        isCancelled.set(true);

        synchronized (this) {
            notifyAll();
        }
    }

    /**
     * Stopping all checkpoint activity immediately even if the current checkpoint is in progress.
     */
    public void shutdownNow() {
        shutdownNow = true;

        if (!isCancelled.get()) {
            cancel();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (runner() != null) {
            return;
        }

        assert runner() == null : "Checkpointer is running.";

        new IgniteThread(this).start();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        // Let's write the data.
        shutdownCheckpointer(true);
    }

    /**
     * Shutdown checkpointer.
     *
     * @param shutdown Shutdown flag.
     */
    public void shutdownCheckpointer(boolean shutdown) {
        if (shutdown) {
            shutdownNow();
        } else {
            cancel();
        }

        try {
            join();
        } catch (InterruptedException ignore) {
            log.warn("Was interrupted while waiting for checkpointer shutdown, will not wait for checkpoint to finish.");

            Thread.currentThread().interrupt();

            shutdownNow();

            while (true) {
                try {
                    join();

                    scheduledCheckpointProgress.fail(new NodeStoppingException("Checkpointer is stopped during node stop."));

                    break;
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }

            Thread.currentThread().interrupt();
        }

        if (checkpointWritePagesPool != null) {
            shutdownAndAwaitTermination(checkpointWritePagesPool, 2, MINUTES);
        }
    }

    /**
     * Returns progress of current checkpoint, last finished one or {@code null}, if checkpoint has never started.
     */
    public @Nullable CheckpointProgress currentProgress() {
        return currentCheckpointProgress;
    }

    /**
     * Returns progress of scheduled checkpoint.
     */
    CheckpointProgress scheduledProgress() {
        return scheduledCheckpointProgress;
    }

    /**
     * Returns {@code true} if checkpoint should be stopped immediately.
     */
    boolean isShutdownNow() {
        return shutdownNow;
    }

    /**
     * Gets a checkpoint interval with a randomized delay in mills.
     *
     * <p>It helps when the cluster makes a checkpoint in the same time in every node.
     */
    long nextCheckpointInterval() {
        PageMemoryCheckpointView checkpointConfigView = checkpointConfig.value();

        long frequency = checkpointConfigView.frequency();
        int deviation = checkpointConfigView.frequencyDeviation();

        if (deviation == 0) {
            return frequency;
        }

        long deviationMills = frequency * deviation;

        long startDelay = ThreadLocalRandom.current().nextLong(max(safeAbs(deviationMills) / 100, 1))
                - max(safeAbs(deviationMills) / 200, 1);

        return safeAbs(frequency + startDelay);
    }
}
