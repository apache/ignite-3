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

import static java.lang.Math.max;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointReadWriteLock.CHECKPOINT_RUNNER_THREAD_PREFIX;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.IgniteUtils.safeAbs;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointView;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.compaction.Compactor;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.worker.IgniteWorker;
import org.apache.ignite.internal.util.worker.IgniteWorkerListener;
import org.apache.ignite.internal.util.worker.WorkProgressDispatcher;
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
public class Checkpointer extends IgniteWorker {
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

    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(Checkpointer.class);

    /** Pause detector. */
    private final @Nullable LongJvmPauseDetector pauseDetector;

    /** Checkpoint config. */
    private final PageMemoryCheckpointConfiguration checkpointConfig;

    /** Strategy of where and how to get the pages. */
    private final CheckpointWorkflow checkpointWorkflow;

    /** Factory for the creation of page-write workers. */
    private final CheckpointPagesWriterFactory checkpointPagesWriterFactory;

    /** Checkpoint runner thread pool. If {@code null} tasks are to be run in single thread. */
    private final @Nullable ThreadPoolExecutor checkpointWritePagesPool;

    /** Next scheduled checkpoint progress. */
    private volatile CheckpointProgressImpl scheduledCheckpointProgress;

    /** Current checkpoint progress. This field is updated only by checkpoint thread. */
    private volatile @Nullable CheckpointProgressImpl currentCheckpointProgress;

    /** Checkpoint progress after releasing write lock. */
    private volatile @Nullable CheckpointProgressImpl afterReleaseWriteLockCheckpointProgress;

    /** Shutdown now. */
    private volatile boolean shutdownNow;

    /** Last checkpoint timestamp, read/update only in checkpoint thread. */
    private long lastCheckpointTimestamp;

    /** File page store manager. */
    private final FilePageStoreManager filePageStoreManager;

    /** Delta file compactor. */
    private final Compactor compactor;

    /** Failure processor. */
    private final FailureProcessor failureProcessor;

    private final LogSyncer logSyncer;

    /**
     * Constructor.
     *
     * @param igniteInstanceName Name of the Ignite instance.
     * @param workerListener Listener for life-cycle worker events.
     * @param detector Long JVM pause detector.
     * @param failureProcessor Failure processor that is used to handle critical errors.
     * @param checkpointWorkFlow Implementation of checkpoint.
     * @param factory Page writer factory.
     * @param filePageStoreManager File page store manager.
     * @param compactor Delta file compactor.
     * @param checkpointConfig Checkpoint configuration.
     * @param logSyncer Write-ahead log synchronizer.
     */
    Checkpointer(
            String igniteInstanceName,
            @Nullable IgniteWorkerListener workerListener,
            @Nullable LongJvmPauseDetector detector,
            FailureProcessor failureProcessor,
            CheckpointWorkflow checkpointWorkFlow,
            CheckpointPagesWriterFactory factory,
            FilePageStoreManager filePageStoreManager,
            Compactor compactor,
            PageMemoryCheckpointConfiguration checkpointConfig,
            LogSyncer logSyncer
    ) {
        super(LOG, igniteInstanceName, "checkpoint-thread", workerListener);

        this.pauseDetector = detector;
        this.checkpointConfig = checkpointConfig;
        this.checkpointWorkflow = checkpointWorkFlow;
        this.checkpointPagesWriterFactory = factory;
        this.filePageStoreManager = filePageStoreManager;
        this.compactor = compactor;
        this.failureProcessor = failureProcessor;
        this.logSyncer = logSyncer;

        scheduledCheckpointProgress = new CheckpointProgressImpl(MILLISECONDS.toNanos(nextCheckpointInterval()));

        int checkpointWritePageThreads = checkpointConfig.checkpointThreads().value();

        if (checkpointWritePageThreads > 1) {
            checkpointWritePagesPool = new ThreadPoolExecutor(
                    checkpointWritePageThreads,
                    checkpointWritePageThreads,
                    30_000,
                    MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    new NamedThreadFactory(CHECKPOINT_RUNNER_THREAD_PREFIX + "-io", log)
            );
        } else {
            checkpointWritePagesPool = null;
        }
    }

    @Override
    protected void body() {
        try {
            while (!isCancelled()) {
                waitCheckpointEvent();

                if (isCancelled() || shutdownNow) {
                    log.info("Skipping last checkpoint because node is stopping");

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

            // We need to handle OutOfMemoryError and the rest in different ways
            if (t instanceof OutOfMemoryError) {
                failureProcessor.process(new FailureContext(CRITICAL_ERROR, t));
            } else {
                failureProcessor.process(new FailureContext(SYSTEM_WORKER_TERMINATION, t));
            }

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
     * Marks partition as dirty, forcing partition's meta-page to be written on disk during next checkpoint.
     */
    void markPartitionAsDirty(DataRegion<?> dataRegion, int groupId, int partitionId) {
        checkpointWorkflow.markPartitionAsDirty(dataRegion, groupId, partitionId);
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
                chp = checkpointWorkflow.markCheckpointBegin(
                        lastCheckpointTimestamp,
                        currentCheckpointProgress,
                        tracker,
                        this::updateHeartbeat,
                        this::updateLastProgressAfterReleaseWriteLock
                );
            } catch (Exception e) {
                if (currentCheckpointProgress != null) {
                    currentCheckpointProgress.fail(e);
                }

                // In case of checkpoint initialization error node should be invalidated and stopped.
                failureProcessor.process(new FailureContext(CRITICAL_ERROR, e));

                // Re-throw as unchecked exception to force stopping checkpoint thread.
                throw new IgniteInternalCheckedException(e);
            }

            updateHeartbeat();

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

                try {
                    logSyncer.sync();
                } catch (Exception e) {
                    log.error("Failed to sync write-ahead log during checkpoint", e);

                    throw new IgniteInternalCheckedException(e);
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

            failureProcessor.process(new FailureContext(CRITICAL_ERROR, e));

            throw e;
        }
    }

    /**
     * Writes dirty pages to the appropriate stores.
     *
     * @param tracker Checkpoint metrics tracker.
     * @param checkpointDirtyPages Checkpoint dirty pages to write.
     * @param currentCheckpointProgress Current checkpoint progress.
     * @param workProgressDispatcher Work progress dispatcher.
     * @param shutdownNow Checker of stop operation.
     * @throws IgniteInternalCheckedException If failed.
     */
    boolean writePages(
            CheckpointMetricsTracker tracker,
            CheckpointDirtyPages checkpointDirtyPages,
            CheckpointProgressImpl currentCheckpointProgress,
            WorkProgressDispatcher workProgressDispatcher,
            BooleanSupplier shutdownNow
    ) throws IgniteInternalCheckedException {
        ThreadPoolExecutor pageWritePool = checkpointWritePagesPool;

        int checkpointWritePageThreads = pageWritePool == null ? 1 : pageWritePool.getMaximumPoolSize();

        // Updated partitions.
        ConcurrentMap<GroupPartitionId, LongAdder> updatedPartitions = new ConcurrentHashMap<>();

        CompletableFuture<?>[] futures = new CompletableFuture[checkpointWritePageThreads];

        tracker.onPagesWriteStart();

        IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> writePageIds = checkpointDirtyPages.toDirtyPageIdQueue();

        for (int i = 0; i < checkpointWritePageThreads; i++) {
            CheckpointPagesWriter write = checkpointPagesWriterFactory.build(
                    tracker,
                    writePageIds,
                    updatedPartitions,
                    futures[i] = new CompletableFuture<>(),
                    workProgressDispatcher::updateHeartbeat,
                    currentCheckpointProgress,
                    shutdownNow
            );

            if (pageWritePool == null) {
                write.run();
            } else {
                pageWritePool.execute(write);
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

        syncUpdatedPageStores(updatedPartitions, currentCheckpointProgress);

        compactor.triggerCompaction();

        if (shutdownNow.getAsBoolean()) {
            currentCheckpointProgress.fail(new NodeStoppingException("Node is stopping."));

            return false;
        }

        return true;
    }

    private void syncUpdatedPageStores(
            ConcurrentMap<GroupPartitionId, LongAdder> updatedPartitions,
            CheckpointProgressImpl currentCheckpointProgress
    ) throws IgniteInternalCheckedException {
        ThreadPoolExecutor pageWritePool = checkpointWritePagesPool;

        if (pageWritePool == null) {
            for (Map.Entry<GroupPartitionId, LongAdder> entry : updatedPartitions.entrySet()) {
                if (shutdownNow) {
                    return;
                }

                fsyncDeltaFile(currentCheckpointProgress, entry.getKey(), entry.getValue());
            }
        } else {
            int checkpointThreads = pageWritePool.getMaximumPoolSize();

            CompletableFuture<?>[] futures = new CompletableFuture[checkpointThreads];

            for (int i = 0; i < checkpointThreads; i++) {
                futures[i] = new CompletableFuture<>();
            }

            BlockingQueue<Entry<GroupPartitionId, LongAdder>> queue = new LinkedBlockingQueue<>(updatedPartitions.entrySet());

            for (int i = 0; i < checkpointThreads; i++) {
                int threadIdx = i;

                pageWritePool.execute(() -> {
                    Map.Entry<GroupPartitionId, LongAdder> entry = queue.poll();

                    try {
                        while (entry != null) {
                            if (shutdownNow) {
                                break;
                            }

                            fsyncDeltaFile(currentCheckpointProgress, entry.getKey(), entry.getValue());

                            entry = queue.poll();
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

    private void fsyncDeltaFile(
            CheckpointProgressImpl currentCheckpointProgress,
            GroupPartitionId partitionId,
            LongAdder pagesWritten
    ) throws IgniteInternalCheckedException {
        FilePageStore filePageStore = filePageStoreManager.getStore(partitionId);

        if (filePageStore == null || filePageStore.isMarkedToDestroy()) {
            return;
        }

        currentCheckpointProgress.onStartPartitionProcessing(partitionId);

        try {
            fsyncDeltaFilePageStoreOnCheckpointThread(filePageStore, pagesWritten);

            renameDeltaFileOnCheckpointThread(filePageStore, partitionId);
        } finally {
            currentCheckpointProgress.onFinishPartitionProcessing(partitionId);
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

    @Override
    @SuppressWarnings("NakedNotify")
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

    /**
     * Starts the checkpointer.
     */
    public void start() {
        if (runner() != null) {
            return;
        }

        assert runner() == null : "Checkpointer is running.";

        new IgniteThread(this).start();
    }

    /**
     * Stops the checkpointer.
     */
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
            log.info("Was interrupted while waiting for checkpointer shutdown, will not wait for checkpoint to finish");

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
     * Returns the progress of the last checkpoint, or the current checkpoint if in progress, {@code null} if no checkpoint has occurred.
     */
    public @Nullable CheckpointProgress lastCheckpointProgress() {
        // Because dirty pages may appear while holding write lock.
        return afterReleaseWriteLockCheckpointProgress;
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

        long interval = checkpointConfigView.interval();
        int deviation = checkpointConfigView.intervalDeviation();

        if (deviation == 0) {
            return interval;
        }

        long deviationMillis = interval * deviation;

        long startDelay = ThreadLocalRandom.current().nextLong(max(safeAbs(deviationMillis) / 100, 1))
                - max(safeAbs(deviationMillis) / 200, 1);

        return safeAbs(interval + startDelay);
    }

    private void fsyncDeltaFilePageStoreOnCheckpointThread(
            FilePageStore filePageStore,
            LongAdder pagesWritten
    ) throws IgniteInternalCheckedException {
        blockingSectionBegin();

        try {
            CompletableFuture<DeltaFilePageStoreIo> deltaFilePageStoreFuture = filePageStore.getNewDeltaFile();

            assert deltaFilePageStoreFuture != null;

            deltaFilePageStoreFuture.join().sync();
        } finally {
            blockingSectionEnd();
        }

        currentCheckpointProgress.syncedPagesCounter().addAndGet(pagesWritten.intValue());
    }

    private void renameDeltaFileOnCheckpointThread(
            FilePageStore filePageStore,
            GroupPartitionId partitionId
    ) throws IgniteInternalCheckedException {
        blockingSectionBegin();

        try {
            CompletableFuture<DeltaFilePageStoreIo> deltaFilePageStoreFuture = filePageStore.getNewDeltaFile();

            assert deltaFilePageStoreFuture != null;

            DeltaFilePageStoreIo deltaFilePageStoreIo = deltaFilePageStoreFuture.join();

            Path newDeltaFilePath = filePageStoreManager.deltaFilePageStorePath(
                    partitionId.getGroupId(),
                    partitionId.getPartitionId(),
                    deltaFilePageStoreIo.fileIndex()
            );

            try {
                deltaFilePageStoreIo.renameFilePath(newDeltaFilePath);
            } catch (IOException e) {
                throw new IgniteInternalCheckedException("Error when renaming delta file: " + deltaFilePageStoreIo.filePath(), e);
            }

            filePageStore.completeNewDeltaFile();
        } finally {
            blockingSectionEnd();
        }
    }

    /**
     * Updates the {@link #lastCheckpointProgress() latest progress} after write lock is released.
     */
    void updateLastProgressAfterReleaseWriteLock() {
        afterReleaseWriteLockCheckpointProgress = currentCheckpointProgress;
    }

    /**
     * Prepares the checkpointer to destroy a partition.
     *
     * <p>If the checkpoint is in progress, then wait until it finishes processing the partition that we are going to destroy, in order to
     * prevent the situation when we want to destroy the partition file along with its delta files, and at this time the checkpoint performs
     * I/O operations on them.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @return Future that will end when the checkpoint is ready to destroy the partition.
     */
    CompletableFuture<Void> prepareToDestroyPartition(GroupPartitionId groupPartitionId) {
        CheckpointProgressImpl currentCheckpointProgress = this.currentCheckpointProgress;

        // If the checkpoint starts after this line, then the data region will already know that we want to destroy the partition, and when
        // reading the page for writing to the delta file, we will receive an "outdated" page that we will not write to disk.
        if (currentCheckpointProgress == null || !currentCheckpointProgress.inProgress()) {
            return nullCompletedFuture();
        }

        CompletableFuture<Void> processedPartitionFuture = currentCheckpointProgress.getProcessedPartitionFuture(groupPartitionId);

        return processedPartitionFuture == null ? nullCompletedFuture() : processedPartitionFuture;
    }
}
