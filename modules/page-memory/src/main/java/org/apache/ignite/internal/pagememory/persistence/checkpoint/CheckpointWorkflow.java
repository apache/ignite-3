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

import static java.util.concurrent.ConcurrentHashMap.newKeySet;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.pagememory.persistence.PartitionMeta.partitionMetaPageId;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.DIRTY_PAGE_COMPARATOR;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.EMPTY;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointReadWriteLock.CHECKPOINT_RUNNER_THREAD_PREFIX;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SORTED;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.util.CollectionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * This class responsibility is to complement {@link Checkpointer} class with side logic of checkpointing like checkpoint listeners
 * notifications, collect dirt pages etc.
 *
 * <p>It allows {@link Checkpointer} class is to focus on its main responsibility: synchronizing memory with disk.
 *
 * <p>Additional actions needed during checkpoint are implemented in this class.
 *
 * <p>Two main blocks of logic this class is responsible for:
 *
 * <p>{@link CheckpointWorkflow#markCheckpointBegin} - Initialization of next checkpoint. It collects all required info.
 *
 * <p>{@link CheckpointWorkflow#markCheckpointEnd} - Finalization of last checkpoint.
 */
class CheckpointWorkflow {
    /**
     * Starting from this number of dirty pages in checkpoint, array will be sorted with {@link Arrays#parallelSort(Comparable[])}.
     *
     * <p>See <a href="https://www.researchgate.net/publication/331742843_Threshold_Analysis_and_Comparison_of_Sequential_and_Parallel_Divide_and_Conquer_Sorting_Algorithms">threshold
     * for parallel sort.</a>
     */
    static final int PARALLEL_SORT_THRESHOLD = 40_000;

    private static final IgniteLogger LOG = Loggers.forClass(CheckpointWorkflow.class);

    /** Checkpoint lock. */
    private final CheckpointReadWriteLock checkpointReadWriteLock;

    /** Persistent data regions for the checkpointing. */
    private final Collection<? extends DataRegion<PersistentPageMemory>> dataRegions;

    /** Collections of checkpoint listeners. */
    private final List<IgniteBiTuple<CheckpointListener, DataRegion<PersistentPageMemory>>> listeners = new CopyOnWriteArrayList<>();

    /** Thread pool for sorting dirty pages in parallel if their count is >= {@link #PARALLEL_SORT_THRESHOLD}. */
    private final ForkJoinPool parallelSortThreadPool;

    /**
     * Thread pool for {@link CheckpointListener} callbacks, when a read or write {@link #checkpointReadWriteLock lock} is taken, {@code
     * null} if it should run on a checkpoint thread.
     */
    private final @Nullable ExecutorService callbackListenerThreadPool;

    /**
     * Contains meta-page IDs for all partitions, that were explicitly marked dirty by {@link #markPartitionAsDirty(DataRegion, int, int)}.
     * Not required to be volatile, read/write is protected by a {@link #checkpointReadWriteLock}. Publication of the initial value should
     * be guaranteed by external user. {@link CheckpointManager}, in particular.
     */
    private Map<DataRegion<?>, Set<DirtyFullPageId>> dirtyPartitionsMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param checkpointReadWriteLock Checkpoint read write lock.
     * @param dataRegions Persistent data regions for the checkpointing, doesn't copy.
     * @param checkpointThreads Number of checkpoint threads.
     */
    public CheckpointWorkflow(
            String igniteInstanceName,
            CheckpointReadWriteLock checkpointReadWriteLock,
            Collection<? extends DataRegion<PersistentPageMemory>> dataRegions,
            int checkpointThreads
    ) {
        this.checkpointReadWriteLock = checkpointReadWriteLock;
        this.dataRegions = dataRegions;

        parallelSortThreadPool = new ForkJoinPool(
                Math.min(Runtime.getRuntime().availableProcessors(), 8) + 1,
                pool -> {
                    ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);

                    worker.setName(IgniteThread.threadPrefix(igniteInstanceName, "checkpoint-pages-sorter") + worker.getPoolIndex());

                    return worker;
                },
                null,
                false
        );

        if (checkpointThreads > 1) {
            callbackListenerThreadPool = Executors.newFixedThreadPool(
                    checkpointThreads,
                    IgniteCheckpointThreadFactory.create(
                            igniteInstanceName,
                            CHECKPOINT_RUNNER_THREAD_PREFIX + "-callback-listener",
                            false,
                            LOG
                    )
            );
        } else {
            callbackListenerThreadPool = null;
        }
    }

    /**
     * Starts a checkpoint workflow.
     */
    public void start() {
        // No-op.
    }

    /**
     * Stops a checkpoint workflow.
     */
    public void stop() {
        listeners.clear();

        shutdownAndAwaitTermination(parallelSortThreadPool, 10, SECONDS);

        if (callbackListenerThreadPool != null) {
            shutdownAndAwaitTermination(callbackListenerThreadPool, 2, MINUTES);
        }
    }

    /**
     * Marks partition as dirty, forcing partition's meta-page to be written on disk during next checkpoint.
     */
    public void markPartitionAsDirty(DataRegion<?> dataRegion, int groupId, int partitionId, int partitionGeneration) {
        Set<DirtyFullPageId> dirtyMetaPageIds = dirtyPartitionsMap.computeIfAbsent(dataRegion, unused -> newKeySet());

        dirtyMetaPageIds.add(new DirtyFullPageId(partitionMetaPageId(partitionId), groupId, partitionGeneration));
    }

    /**
     * First stage of checkpoint which collects demanded information (dirty pages mostly).
     *
     * @param startCheckpointTimestamp Checkpoint start timestamp.
     * @param curr Current checkpoint event info.
     * @param tracker Checkpoint metrics tracker.
     * @param updateHeartbeat Update heartbeat callback.
     * @param onBeforeWriteLockRelease Callback before write lock release.
     * @return Checkpoint collected info.
     * @throws IgniteInternalCheckedException If failed.
     */
    public Checkpoint markCheckpointBegin(
            long startCheckpointTimestamp,
            CheckpointProgressImpl curr,
            CheckpointMetricsTracker tracker,
            Runnable updateHeartbeat,
            Runnable onBeforeWriteLockRelease
    ) throws IgniteInternalCheckedException {
        List<CheckpointListener> listeners = collectCheckpointListeners(dataRegions);

        AwaitTasksCompletionExecutor executor = callbackListenerThreadPool == null
                ? null : new AwaitTasksCompletionExecutor(callbackListenerThreadPool, updateHeartbeat);

        checkpointReadWriteLock.readLock();

        try {
            updateHeartbeat.run();

            tracker.onBeforeCheckpointBeginStart();

            for (CheckpointListener listener : listeners) {
                listener.beforeCheckpointBegin(curr, executor);

                if (executor == null) {
                    updateHeartbeat.run();
                }
            }

            if (executor != null) {
                executor.awaitPendingTasksFinished();
            }

            tracker.onBeforeCheckpointBeginEnd();
        } finally {
            checkpointReadWriteLock.readUnlock();
        }

        tracker.onWriteLockWaitStart();

        checkpointReadWriteLock.writeLock();

        tracker.onWriteLockWaitEnd();

        tracker.onWriteLockHoldStart();

        DataRegionsDirtyPages dirtyPages;

        try {
            updateHeartbeat.run();

            curr.transitTo(LOCK_TAKEN);

            tracker.onMarkCheckpointBeginStart();

            for (CheckpointListener listener : listeners) {
                listener.onMarkCheckpointBegin(curr, executor);

                if (executor == null) {
                    updateHeartbeat.run();
                }
            }

            if (executor != null) {
                executor.awaitPendingTasksFinished();
            }

            tracker.onMarkCheckpointBeginEnd();

            // Page replacement is allowed only after sorting dirty pages.
            dirtyPages = beginCheckpoint(curr);

            curr.currentCheckpointPagesCount(dirtyPages.dirtyPageCount);

            curr.transitTo(PAGES_SNAPSHOT_TAKEN);
        } finally {
            // It must be called strictly before the release write lock, otherwise it can lead to a very rare race condition on updating
            // the partition meta and writing it to disk, which can cause problems when restarting the partition.
            onBeforeWriteLockRelease.run();

            checkpointReadWriteLock.writeUnlock();

            tracker.onWriteLockHoldEnd();
        }

        curr.transitTo(LOCK_RELEASED);

        for (CheckpointListener listener : listeners) {
            listener.onCheckpointBegin(curr);

            updateHeartbeat.run();
        }

        if (dirtyPages.dirtyPageCount > 0) {
            tracker.onSplitAndSortCheckpointPagesStart();

            updateHeartbeat.run();

            CheckpointDirtyPages checkpointPages = createAndSortCheckpointDirtyPages(dirtyPages);

            curr.pagesToWrite(checkpointPages);

            curr.initCounters(checkpointPages.dirtyPagesCount());

            tracker.onSplitAndSortCheckpointPagesEnd();

            curr.transitTo(PAGES_SORTED);

            return new Checkpoint(checkpointPages, curr);
        }

        return new Checkpoint(EMPTY, curr);
    }

    /**
     * Do some actions on checkpoint finish (After all pages were written to disk).
     *
     * @param chp Checkpoint snapshot.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void markCheckpointEnd(Checkpoint chp) throws IgniteInternalCheckedException {
        synchronized (this) {
            for (DataRegion<PersistentPageMemory> dataRegion : dataRegions) {
                dataRegion.pageMemory().finishCheckpoint();
            }
        }

        chp.finishCheckpoint();

        for (CheckpointListener listener : collectCheckpointListeners(dataRegions)) {
            listener.afterCheckpointEnd(chp.progress);
        }

        chp.progress.transitTo(FINISHED);
    }

    /**
     * Adds a listener to be called for the corresponding persistent data region.
     *
     * @param listener Listener.
     * @param dataRegion Persistent data region for which listener is corresponded to, {@code null} for all regions.
     */
    public void addCheckpointListener(CheckpointListener listener, @Nullable DataRegion<PersistentPageMemory> dataRegion) {
        assert dataRegion == null || dataRegions.contains(dataRegion) : dataRegion;

        listeners.add(new IgniteBiTuple<>(listener, dataRegion));
    }

    /**
     * Removes the listener.
     *
     * @param listener Listener.
     */
    public void removeCheckpointListener(CheckpointListener listener) {
        listeners.remove(new IgniteBiTuple<CheckpointListener, DataRegion<PersistentPageMemory>>() {
            /** {@inheritDoc} */
            @Override
            public boolean equals(Object o) {
                return listener == ((IgniteBiTuple<?, ?>) o).getKey();
            }

            @Override
            public int hashCode() {
                return listener.hashCode();
            }
        });
    }

    /**
     * Returns the checkpoint listeners for the data regions.
     *
     * @param dataRegions Data regions.
     */
    public List<CheckpointListener> collectCheckpointListeners(Collection<? extends DataRegion<PersistentPageMemory>> dataRegions) {
        return listeners.stream()
                .filter(tuple -> tuple.getValue() == null || dataRegions.contains(tuple.getValue()))
                .map(IgniteBiTuple::getKey)
                .collect(toUnmodifiableList());
    }

    private DataRegionsDirtyPages beginCheckpoint(CheckpointProgressImpl checkpointProgress) {
        assert checkpointReadWriteLock.isWriteLockHeldByCurrentThread();

        Map<DataRegion<?>, Set<DirtyFullPageId>> dirtyPartitionsMap = this.dirtyPartitionsMap;

        this.dirtyPartitionsMap = new ConcurrentHashMap<>();

        Collection<DataRegionDirtyPages<Collection<DirtyFullPageId>>> dataRegionsDirtyPages = new ArrayList<>(dataRegions.size());

        // First, we iterate all regions that have dirty pages.
        for (DataRegion<PersistentPageMemory> dataRegion : dataRegions) {
            Collection<DirtyFullPageId> dirtyPages = dataRegion.pageMemory().beginCheckpoint(checkpointProgress);

            Set<DirtyFullPageId> dirtyMetaPageIds = dirtyPartitionsMap.remove(dataRegion);

            if (dirtyMetaPageIds != null) {
                // Merge these two collections. There should be no intersections.
                dirtyPages = CollectionUtils.concat(dirtyMetaPageIds, dirtyPages);
            }

            dataRegionsDirtyPages.add(new DataRegionDirtyPages<>(dataRegion.pageMemory(), dirtyPages));
        }

        // Then we iterate regions that don't have dirty pages, but somehow have dirty partitions.
        for (Entry<DataRegion<?>, Set<DirtyFullPageId>> entry : dirtyPartitionsMap.entrySet()) {
            PageMemory pageMemory = entry.getKey().pageMemory();

            assert pageMemory instanceof PersistentPageMemory;

            dataRegionsDirtyPages.add(new DataRegionDirtyPages<>((PersistentPageMemory) pageMemory, entry.getValue()));
        }

        return new DataRegionsDirtyPages(dataRegionsDirtyPages);
    }

    CheckpointDirtyPages createAndSortCheckpointDirtyPages(
            DataRegionsDirtyPages dataRegionsDirtyPages
    ) throws IgniteInternalCheckedException {
        var checkpointDirtyPages = new ArrayList<DirtyPagesAndPartitions>();

        int realPagesArrSize = 0;

        // Collects dirty pages into an array (then we will sort them) and collects dirty partitions.
        for (DataRegionDirtyPages<Collection<DirtyFullPageId>> dataRegionDirtyPages : dataRegionsDirtyPages.dirtyPages) {
            var pageIds = new DirtyFullPageId[dataRegionDirtyPages.dirtyPages.size()];

            var partitionIds = new HashSet<GroupPartitionId>();

            int pagePos = 0;

            for (DirtyFullPageId dirtyPage : dataRegionDirtyPages.dirtyPages) {
                assert realPagesArrSize++ != dataRegionsDirtyPages.dirtyPageCount :
                        "Incorrect estimated dirty pages number: " + dataRegionsDirtyPages.dirtyPageCount;

                pageIds[pagePos++] = dirtyPage;
                partitionIds.add(GroupPartitionId.convert(dirtyPage));
            }

            // Some pages may have been already replaced.
            if (pagePos == 0) {
                continue;
            } else if (pagePos != pageIds.length) {
                pageIds = Arrays.copyOf(pageIds, pagePos);
            }

            checkpointDirtyPages.add(new DirtyPagesAndPartitions(dataRegionDirtyPages.pageMemory, pageIds, partitionIds));
        }

        // Add tasks to sort arrays of dirty page IDs in parallel if their number is greater than or equal to PARALLEL_SORT_THRESHOLD.
        List<ForkJoinTask<?>> parallelSortTasks = checkpointDirtyPages.stream()
                .map(dirtyPagesAndPartitions -> dirtyPagesAndPartitions.dirtyPages)
                .filter(pageIds -> pageIds.length >= PARALLEL_SORT_THRESHOLD)
                .map(pageIds -> parallelSortThreadPool.submit(() -> Arrays.parallelSort(pageIds, DIRTY_PAGE_COMPARATOR)))
                .collect(toList());

        // Sort arrays of dirty page IDs if their number is less than PARALLEL_SORT_THRESHOLD.
        for (DirtyPagesAndPartitions dirtyPagesAndPartitions : checkpointDirtyPages) {
            if (dirtyPagesAndPartitions.dirtyPages.length < PARALLEL_SORT_THRESHOLD) {
                Arrays.sort(dirtyPagesAndPartitions.dirtyPages, DIRTY_PAGE_COMPARATOR);
            }
        }

        // Waits for a parallel sort task.
        for (ForkJoinTask<?> parallelSortTask : parallelSortTasks) {
            try {
                parallelSortTask.get();
            } catch (ExecutionException | InterruptedException e) {
                throw new IgniteInternalCheckedException(
                        "Failed to perform pages array parallel sort",
                        e instanceof ExecutionException ? e.getCause() : e
                );
            }
        }

        return new CheckpointDirtyPages(checkpointDirtyPages);
    }
}
