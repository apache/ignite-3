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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.DIRTY_PAGE_COMPARATOR;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.EMPTY;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.MARKER_STORED_TO_DISK;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGE_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
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
    /** Starting from this number of dirty pages in checkpoint, array will be sorted with {@link Arrays#parallelSort(Comparable[])}. */
    static final int PARALLEL_SORT_THRESHOLD = 40_000;

    /** Checkpoint marker storage. */
    private final CheckpointMarkersStorage checkpointMarkersStorage;

    /** Checkpoint lock. */
    private final CheckpointReadWriteLock checkpointReadWriteLock;

    /** Persistent data regions for the checkpointing. */
    private final Collection<? extends DataRegion<PersistentPageMemory>> dataRegions;

    /** Collections of checkpoint listeners. */
    private final List<IgniteBiTuple<CheckpointListener, DataRegion<PersistentPageMemory>>> listeners = new CopyOnWriteArrayList<>();

    /** Thread pool for sorting dirty pages in parallel if their count is >= {@link #PARALLEL_SORT_THRESHOLD}. */
    private final ForkJoinPool parallelSortThreadPool;

    /**
     * Constructor.
     *
     * @param checkpointMarkersStorage Checkpoint marker storage.
     * @param checkpointReadWriteLock Checkpoint read write lock.
     * @param dataRegions Persistent data regions for the checkpointing, doesn't copy.
     */
    public CheckpointWorkflow(
            CheckpointMarkersStorage checkpointMarkersStorage,
            CheckpointReadWriteLock checkpointReadWriteLock,
            Collection<? extends DataRegion<PersistentPageMemory>> dataRegions
    ) {
        this.checkpointMarkersStorage = checkpointMarkersStorage;
        this.checkpointReadWriteLock = checkpointReadWriteLock;
        this.dataRegions = dataRegions;

        parallelSortThreadPool = new ForkJoinPool(
                Math.min(Runtime.getRuntime().availableProcessors(), 8) + 1,
                pool -> {
                    ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);

                    worker.setName("checkpoint-pages-sorter-" + worker.getPoolIndex());

                    return worker;
                },
                null,
                false
        );
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
    }

    /**
     * First stage of checkpoint which collects demanded information (dirty pages mostly).
     *
     * @param startCheckpointTimestamp Checkpoint start timestamp.
     * @param curr Current checkpoint event info.
     * @param tracker Checkpoint metrics tracker.
     * @return Checkpoint collected info.
     * @throws IgniteInternalCheckedException If failed.
     */
    public Checkpoint markCheckpointBegin(
            long startCheckpointTimestamp,
            CheckpointProgressImpl curr,
            CheckpointMetricsTracker tracker
    ) throws IgniteInternalCheckedException {
        List<CheckpointListener> listeners = collectCheckpointListeners(dataRegions);

        checkpointReadWriteLock.readLock();

        try {
            for (CheckpointListener listener : listeners) {
                listener.beforeCheckpointBegin(curr);
            }
        } finally {
            checkpointReadWriteLock.readUnlock();
        }

        tracker.onWriteLockWaitStart();

        checkpointReadWriteLock.writeLock();

        DataRegionsDirtyPages dirtyPages;

        try {
            curr.transitTo(LOCK_TAKEN);

            tracker.onMarkCheckpointBeginStart();

            for (CheckpointListener listener : listeners) {
                listener.onMarkCheckpointBegin(curr);
            }

            tracker.onMarkCheckpointBeginEnd();

            // There are allowable to replace pages only after checkpoint marker was stored to disk.
            dirtyPages = beginCheckpoint(dataRegions, curr.futureFor(MARKER_STORED_TO_DISK));

            curr.currentCheckpointPagesCount(dirtyPages.dirtyPageCount);

            curr.transitTo(PAGE_SNAPSHOT_TAKEN);
        } finally {
            checkpointReadWriteLock.writeUnlock();

            tracker.onWriteLockRelease();
        }

        curr.transitTo(LOCK_RELEASED);

        for (CheckpointListener listener : listeners) {
            listener.onCheckpointBegin(curr);
        }

        if (dirtyPages.dirtyPageCount > 0) {
            checkpointMarkersStorage.onCheckpointBegin(curr.id());

            curr.transitTo(MARKER_STORED_TO_DISK);

            tracker.onSplitAndSortCheckpointPagesStart();

            CheckpointDirtyPages checkpointPages = createAndSortCheckpointDirtyPages(dirtyPages);

            tracker.onSplitAndSortCheckpointPagesEnd();

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
            chp.progress.clearCounters();

            for (DataRegion<PersistentPageMemory> dataRegion : dataRegions) {
                dataRegion.pageMemory().finishCheckpoint();
            }
        }

        if (chp.hasDelta()) {
            checkpointMarkersStorage.onCheckpointEnd(chp.progress.id());
        }

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

    private DataRegionsDirtyPages beginCheckpoint(
            Collection<? extends DataRegion<PersistentPageMemory>> dataRegions,
            CompletableFuture<?> allowToReplace
    ) {
        Collection<IgniteBiTuple<PersistentPageMemory, Collection<FullPageId>>> pages = new ArrayList<>(dataRegions.size());

        for (DataRegion<PersistentPageMemory> dataRegion : dataRegions) {
            Collection<FullPageId> dirtyPages = dataRegion.pageMemory().beginCheckpoint(allowToReplace);

            pages.add(new IgniteBiTuple<>(dataRegion.pageMemory(), dirtyPages));
        }

        return new DataRegionsDirtyPages(pages);
    }

    CheckpointDirtyPages createAndSortCheckpointDirtyPages(
            DataRegionsDirtyPages dataRegionsDirtyPages
    ) throws IgniteInternalCheckedException {
        List<IgniteBiTuple<PersistentPageMemory, FullPageId[]>> checkpointPages = new ArrayList<>();

        int realPagesArrSize = 0;

        for (IgniteBiTuple<PersistentPageMemory, Collection<FullPageId>> regionDirtyPages : dataRegionsDirtyPages.dirtyPages) {
            FullPageId[] checkpointRegionDirtyPages = new FullPageId[regionDirtyPages.getValue().size()];

            int pagePos = 0;

            for (FullPageId dirtyPage : regionDirtyPages.getValue()) {
                assert realPagesArrSize++ != dataRegionsDirtyPages.dirtyPageCount :
                        "Incorrect estimated dirty pages number: " + dataRegionsDirtyPages.dirtyPageCount;

                checkpointRegionDirtyPages[pagePos++] = dirtyPage;
            }

            // Some pages may have been already replaced.
            if (pagePos == 0) {
                continue;
            } else if (pagePos != checkpointRegionDirtyPages.length) {
                checkpointPages.add(new IgniteBiTuple<>(regionDirtyPages.getKey(), Arrays.copyOf(checkpointRegionDirtyPages, pagePos)));
            } else {
                checkpointPages.add(new IgniteBiTuple<>(regionDirtyPages.getKey(), checkpointRegionDirtyPages));
            }
        }

        List<ForkJoinTask<?>> parallelSortTasks = checkpointPages.stream()
                .map(IgniteBiTuple::getValue)
                .filter(pages -> pages.length >= PARALLEL_SORT_THRESHOLD)
                .map(pages -> parallelSortThreadPool.submit(() -> Arrays.parallelSort(pages, DIRTY_PAGE_COMPARATOR)))
                .collect(toList());

        for (IgniteBiTuple<PersistentPageMemory, FullPageId[]> regionPages : checkpointPages) {
            if (regionPages.getValue().length < PARALLEL_SORT_THRESHOLD) {
                Arrays.sort(regionPages.getValue(), DIRTY_PAGE_COMPARATOR);
            }
        }

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

        return new CheckpointDirtyPages(
                checkpointPages.stream()
                        .map(tuple -> new IgniteBiTuple<>(tuple.getKey(), Arrays.asList(tuple.getValue())))
                        .collect(toList())
        );
    }
}
