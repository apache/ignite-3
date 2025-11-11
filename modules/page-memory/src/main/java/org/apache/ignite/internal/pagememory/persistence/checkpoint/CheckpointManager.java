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

import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.MUST_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.NOT_REQUIRED;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.CheckpointConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionDestructionLockManager;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesView;
import org.apache.ignite.internal.pagememory.persistence.compaction.Compactor;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.jetbrains.annotations.Nullable;

/**
 * Main class to abstract checkpoint-related processes and actions and hide them from higher-level components.
 *
 * <p>Implements sharp checkpointing algorithm.
 *
 * <p>Represents only an intermediate step in refactoring of checkpointing component and may change in the future.
 *
 * <p>This checkpoint ensures that all pages marked as dirty under {@link #checkpointTimeoutLock} will be consistently saved to disk.
 *
 * <p>Configuration of this checkpoint allows the following:
 * <ul>
 *     <li>Collecting all pages from configured dataRegions which was marked as dirty under {@link #checkpointTimeoutLock}.</li>
 *     <li>Marking the start of checkpoint on disk.</li>
 *     <li>Notifying the subscribers of different checkpoint states through {@link CheckpointListener}.</li>
 *     <li>Synchronizing collected pages with disk using {@link FilePageStoreManager}.</li>
 * </ul>
 */
public class CheckpointManager {
    /** Checkpoint worker. */
    private final Checkpointer checkpointer;

    /** Main checkpoint steps. */
    private final CheckpointWorkflow checkpointWorkflow;

    /** Timeout checkpoint lock which should be used while write to memory happened. */
    private final CheckpointTimeoutLock checkpointTimeoutLock;

    /** Checkpoint page writer factory. */
    private final CheckpointPagesWriterFactory checkpointPagesWriterFactory;

    /** File page store manager. */
    private final FilePageStoreManager filePageStoreManager;

    /** Delta file compactor. */
    private final Compactor compactor;

    private final PartitionDestructionLockManager partitionDestructionLockManager;

    /**
     * Constructor.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param checkpointConfig Checkpoint configuration.
     * @param longJvmPauseDetector Long JVM pause detector.
     * @param failureManager Failure processor that is used to handle critical errors.
     * @param filePageStoreManager File page store manager.
     * @param partitionMetaManager Partition meta information manager.
     * @param dataRegions Data regions.
     * @param ioRegistry Page IO registry.
     * @param commonExecutorService Executor service for unspecified tasks, i.e. throttling log.
     * @param pageSize Page size in bytes.
     * @throws IgniteInternalCheckedException If failed.
     */
    public CheckpointManager(
            String igniteInstanceName,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            FailureManager failureManager,
            CheckpointConfiguration checkpointConfig,
            FilePageStoreManager filePageStoreManager,
            PartitionMetaManager partitionMetaManager,
            Collection<? extends DataRegion<PersistentPageMemory>> dataRegions,
            PageIoRegistry ioRegistry,
            LogSyncer logSyncer,
            ExecutorService commonExecutorService,
            CheckpointMetricSource checkpointMetricSource,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) throws IgniteInternalCheckedException {
        this.filePageStoreManager = filePageStoreManager;

        long logReadLockThresholdTimeout = checkpointConfig.logReadLockThresholdTimeoutMillis();

        ReentrantReadWriteLockWithTracking reentrantReadWriteLockWithTracking = logReadLockThresholdTimeout > 0
                ? new ReentrantReadWriteLockWithTracking(Loggers.forClass(CheckpointReadWriteLock.class), logReadLockThresholdTimeout)
                : new ReentrantReadWriteLockWithTracking();

        CheckpointReadWriteLock checkpointReadWriteLock = new CheckpointReadWriteLock(
                reentrantReadWriteLockWithTracking,
                commonExecutorService
        );

        checkpointWorkflow = new CheckpointWorkflow(
                igniteInstanceName,
                checkpointReadWriteLock,
                dataRegions,
                checkpointConfig.checkpointThreads()
        );

        partitionDestructionLockManager = new PartitionDestructionLockManager();

        checkpointPagesWriterFactory = new CheckpointPagesWriterFactory(
                this::writePageToFilePageStore,
                ioRegistry,
                partitionMetaManager,
                pageSize,
                partitionDestructionLockManager
        );

        compactor = new Compactor(
                Loggers.forClass(Compactor.class),
                igniteInstanceName,
                checkpointConfig.compactionThreads(),
                filePageStoreManager,
                pageSize,
                failureManager,
                partitionDestructionLockManager
        );

        checkpointer = new Checkpointer(
                igniteInstanceName,
                longJvmPauseDetector,
                failureManager,
                checkpointWorkflow,
                checkpointPagesWriterFactory,
                filePageStoreManager,
                partitionMetaManager,
                compactor,
                pageSize,
                checkpointConfig,
                logSyncer,
                partitionDestructionLockManager,
                checkpointMetricSource
        );

        checkpointTimeoutLock = new CheckpointTimeoutLock(
                checkpointReadWriteLock,
                checkpointConfig.readLockTimeoutMillis(),
                () -> checkpointUrgency(dataRegions),
                checkpointer,
                failureManager
        );
    }

    /**
     * Starts a checkpoint manger.
     */
    public void start() {
        checkpointWorkflow.start();

        checkpointer.start();

        checkpointTimeoutLock.start();

        compactor.start();
    }

    /**
     * Stops a checkpoint manger.
     */
    public void stop() throws Exception {
        closeAll(
                checkpointTimeoutLock::stop,
                checkpointer::stop,
                checkpointWorkflow::stop,
                compactor::stop
        );
    }

    /**
     * Returns checkpoint timeout lock which can be used for protection of writing to memory.
     */
    public CheckpointTimeoutLock checkpointTimeoutLock() {
        return checkpointTimeoutLock;
    }

    /**
     * Adds a listener to be called for the corresponding persistent data region.
     *
     * @param listener Listener.
     * @param dataRegion Persistent data region for which listener is corresponded to, {@code null} for all regions.
     */
    public void addCheckpointListener(CheckpointListener listener, @Nullable DataRegion<PersistentPageMemory> dataRegion) {
        checkpointWorkflow.addCheckpointListener(listener, dataRegion);
    }

    /**
     * Removes the listener.
     *
     * @param listener Listener.
     */
    public void removeCheckpointListener(CheckpointListener listener) {
        checkpointWorkflow.removeCheckpointListener(listener);
    }

    /**
     * Start the new checkpoint immediately.
     *
     * @param reason Checkpoint reason.
     * @return Triggered checkpoint progress.
     */
    public CheckpointProgress forceCheckpoint(String reason) {
        return checkpointer.scheduleCheckpoint(0, reason);
    }

    /**
     * Schedules a checkpoint in the future.
     *
     * @param delayMillis Delay in milliseconds from the curent moment.
     * @param reason Checkpoint reason.
     * @return Triggered checkpoint progress.
     */
    public CheckpointProgress scheduleCheckpoint(long delayMillis, String reason) {
        return checkpointer.scheduleCheckpoint(delayMillis, reason);
    }

    public @Nullable CheckpointProgress currentCheckpointProgress() {
        return checkpointer.currentCheckpointProgress();
    }

    public @Nullable CheckpointProgress currentCheckpointProgressForThrottling() {
        return checkpointer.currentCheckpointProgressForThrottling();
    }

    /**
     * Returns the progress of the last checkpoint, or the current checkpoint if in progress, {@code null} if no checkpoint has occurred.
     */
    public @Nullable CheckpointProgress lastCheckpointProgress() {
        return checkpointer.lastCheckpointProgress();
    }

    /**
     * Marks partition as dirty, forcing partition's meta-page to be written on disk during next checkpoint.
     */
    public void markPartitionAsDirty(DataRegion<?> dataRegion, int groupId, int partitionId, int partitionGeneration) {
        checkpointer.markPartitionAsDirty(dataRegion, groupId, partitionId, partitionGeneration);
    }

    /**
     * Returns checkpoint urgency status. {@link CheckpointUrgency#NOT_REQUIRED} if it is safe for all {@link DataRegion data regions} to
     * update their {@link PageMemory}.
     *
     * @param dataRegions Data regions.
     * @see PersistentPageMemory#checkpointUrgency()
     * @see CheckpointUrgency
     */
    static CheckpointUrgency checkpointUrgency(Collection<? extends DataRegion<PersistentPageMemory>> dataRegions) {
        CheckpointUrgency urgency = NOT_REQUIRED;
        for (DataRegion<PersistentPageMemory> dataRegion : dataRegions) {
            CheckpointUrgency regionCheckpointUrgency = dataRegion.pageMemory().checkpointUrgency();

            if (regionCheckpointUrgency.compareTo(urgency) > 0) {
                urgency = regionCheckpointUrgency;
            }

            if (urgency == MUST_TRIGGER) {
                return MUST_TRIGGER;
            }
        }

        return urgency;
    }

    /**
     * Writes a page to delta file page store.
     *
     * <p>Must be used at breakpoint and page replacement.
     *
     * @param pageMemory Page memory.
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write from.
     * @throws IgniteInternalCheckedException If page writing failed (IO error occurred).
     */
    public void writePageToFilePageStore(
            PersistentPageMemory pageMemory,
            FullPageId pageId,
            ByteBuffer pageBuf
    ) throws IgniteInternalCheckedException {
        FilePageStore filePageStore = filePageStoreManager.getStore(new GroupPartitionId(pageId.groupId(), pageId.partitionId()));

        // If the partition is deleted (or will be soon), then such writes to the disk should be skipped.
        if (filePageStore == null || filePageStore.isMarkedToDestroy()) {
            return;
        }

        if (pageId.pageIdx() >= filePageStore.checkpointedPageCount()) {
            filePageStore.write(pageId.pageId(), pageBuf);

            return;
        }

        CheckpointProgress lastCheckpointProgress = lastCheckpointProgress();

        assert lastCheckpointProgress != null : "Checkpoint has not happened yet";
        assert lastCheckpointProgress.inProgress() : "Checkpoint must be in progress";

        CheckpointDirtyPages pagesToWrite = lastCheckpointProgress.pagesToWrite();

        assert pagesToWrite != null : "Dirty pages must be sorted out";

        CompletableFuture<DeltaFilePageStoreIo> deltaFilePageStoreFuture = filePageStore.getOrCreateNewDeltaFile(
                index -> filePageStoreManager.tmpDeltaFilePageStorePath(pageId.groupId(), pageId.partitionId(), index),
                () -> {
                    CheckpointDirtyPagesView partitionView = pagesToWrite.getPartitionView(
                            pageMemory,
                            pageId.groupId(),
                            pageId.partitionId()
                    );

                    assert partitionView != null : String.format("Unable to find view for dirty pages: [partitionId=%s, pageMemory=%s]",
                            GroupPartitionId.convert(pageId), pageMemory);

                    return pageIndexesForDeltaFilePageStore(
                            partitionView,
                            pageId.groupId(),
                            pageId.partitionId(),
                            filePageStore.checkpointedPageCount()
                    );
                }
        );

        deltaFilePageStoreFuture.join().write(pageId.pageId(), pageBuf);
    }

    /**
     * Returns the indexes of the dirty pages to be written to the delta file page store.
     *
     * @param partitionDirtyPages Dirty pages of the partition.
     * @param checkpointedPages Number of pages of the partition that were stored on the disk at the beginning of the checkpoint.
     */
    static int[] pageIndexesForDeltaFilePageStore(
            CheckpointDirtyPagesView partitionDirtyPages,
            int groupId,
            int partitionId,
            int checkpointedPages
    ) {
        // TODO: IGNITE-26988 Вот тут надо починить и сделать учет поколений
        int partGen = partitionDirtyPages.pageMemory().partGeneration(groupId, partitionId);

        // If there is no partition meta page among the dirty pages, then we add an additional page to the result.
        int offset = partitionDirtyPages.containsMetaPage(partGen) ? 0 : 1;

        int[] pageIndexes = new int[partitionDirtyPages.countAlteredPages(partGen, checkpointedPages) + offset];

        int size = offset;

        for (int i = 0; i < partitionDirtyPages.size() && size < pageIndexes.length; i++) {
            DirtyFullPageId dirtyFullPageId = partitionDirtyPages.get(i);

            if (dirtyFullPageId.partitionGeneration() == partGen) {
                pageIndexes[size++] = dirtyFullPageId.pageIdx();
            }
        }

        return pageIndexes;
    }

    /**
     * Triggers compacting for new delta files.
     */
    public void triggerCompaction() {
        compactor.triggerCompaction();
    }

    /** Partition Destruction Lock Manager. */
    public PartitionDestructionLockManager partitionDestructionLockManager() {
        return partitionDestructionLockManager;
    }
}
