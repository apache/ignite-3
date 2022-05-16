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

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.MARKER_STORED_TO_DISK;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGE_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.IgniteConcurrentMultiPairQueue.EMPTY;
import static org.apache.ignite.lang.IgniteSystemProperties.getInteger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
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
class CheckpointWorkflow implements IgniteComponent {
    /**
     * Starting from this number of dirty pages in checkpoint, array will be sorted with {@link Arrays#parallelSort(Comparable[])} in case
     * of {@link CheckpointWriteOrder#SEQUENTIAL}.
     */
    public static final String CHECKPOINT_PARALLEL_SORT_THRESHOLD = "CHECKPOINT_PARALLEL_SORT_THRESHOLD";

    /**
     * Starting from this number of dirty pages in checkpoint, array will be sorted with {@link Arrays#parallelSort(Comparable[])} in case
     * of {@link CheckpointWriteOrder#SEQUENTIAL}.
     */
    // TODO: IGNITE-16984 Move to configuration
    private final int parallelSortThreshold = getInteger(CHECKPOINT_PARALLEL_SORT_THRESHOLD, 512 * 1024);

    /** This number of threads will be created and used for parallel sorting. */
    private static final int PARALLEL_SORT_THREADS = Math.min(Runtime.getRuntime().availableProcessors(), 8);

    /** Checkpoint marker storage. */
    private final CheckpointMarkersStorage checkpointMarkersStorage;

    /** Checkpoint lock. */
    private final CheckpointReadWriteLock checkpointReadWriteLock;

    /** Persistent data regions for the checkpointing. */
    private final Collection<PageMemoryDataRegion> dataRegions;

    /** Checkpoint write order configuration. */
    private final CheckpointWriteOrder checkpointWriteOrder;

    /** Collections of checkpoint listeners. */
    private final List<IgniteBiTuple<CheckpointListener, PageMemoryDataRegion>> listeners = new CopyOnWriteArrayList<>();

    /**
     * Constructor.
     *
     * @param checkpointMarkersStorage Checkpoint marker storage.
     * @param checkpointReadWriteLock Checkpoint read write lock.
     * @param checkpointWriteOrder Checkpoint write order.
     * @param dataRegions Persistent data regions for the checkpointing, doesn't copy.
     */
    public CheckpointWorkflow(
            CheckpointMarkersStorage checkpointMarkersStorage,
            CheckpointReadWriteLock checkpointReadWriteLock,
            CheckpointWriteOrder checkpointWriteOrder,
            Collection<PageMemoryDataRegion> dataRegions
    ) {
        this.checkpointMarkersStorage = checkpointMarkersStorage;
        this.checkpointReadWriteLock = checkpointReadWriteLock;
        this.checkpointWriteOrder = checkpointWriteOrder;
        this.dataRegions = dataRegions;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        listeners.clear();
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

        CheckpointDirtyPagesInfoHolder dirtyPages;

        try {
            curr.transitTo(LOCK_TAKEN);

            tracker.onMarkCheckpointBeginStart();

            for (CheckpointListener listener : listeners) {
                listener.onMarkCheckpointBegin(curr);
            }

            tracker.onMarkCheckpointBeginEnd();

            // There are allowable to replace pages only after checkpoint entry was stored to disk.
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

            IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> dirtyPages0 = splitAndSortCheckpointPagesIfNeeded(dirtyPages);

            tracker.onSplitAndSortCheckpointPagesEnd();

            return new Checkpoint(dirtyPages0, curr);
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

            for (PageMemoryDataRegion dataRegion : dataRegions) {
                assert dataRegion.persistent() : dataRegion;

                ((PageMemoryImpl) dataRegion.pageMemory()).finishCheckpoint();
            }
        }

        checkpointMarkersStorage.onCheckpointEnd(chp.progress.id());

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
    public void addCheckpointListener(CheckpointListener listener, @Nullable PageMemoryDataRegion dataRegion) {
        assert dataRegion == null || (dataRegion.persistent() && dataRegions.contains(dataRegion)) : dataRegion;

        listeners.add(new IgniteBiTuple<>(listener, dataRegion));
    }

    /**
     * Removes the listener.
     *
     * @param listener Listener.
     */
    public void removeCheckpointListener(CheckpointListener listener) {
        listeners.remove(new IgniteBiTuple<CheckpointListener, PageMemoryDataRegion>() {
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
    public List<CheckpointListener> collectCheckpointListeners(Collection<PageMemoryDataRegion> dataRegions) {
        return listeners.stream()
                .filter(tuple -> tuple.getValue() == null || dataRegions.contains(tuple.getValue()))
                .map(IgniteBiTuple::getKey)
                .collect(toUnmodifiableList());
    }

    private CheckpointDirtyPagesInfoHolder beginCheckpoint(
            Collection<PageMemoryDataRegion> dataRegions,
            CompletableFuture<?> allowToReplace
    ) {
        Collection<IgniteBiTuple<PageMemoryImpl, Collection<FullPageId>>> pages = new ArrayList<>(dataRegions.size());

        int pageCount = 0;

        for (PageMemoryDataRegion dataRegion : dataRegions) {
            assert dataRegion.persistent() : dataRegion;

            Collection<FullPageId> dirtyPages = ((PageMemoryImpl) dataRegion.pageMemory()).beginCheckpoint(allowToReplace);

            pageCount += dirtyPages.size();

            pages.add(new IgniteBiTuple<>((PageMemoryImpl) dataRegion.pageMemory(), dirtyPages));
        }

        return new CheckpointDirtyPagesInfoHolder(pages, pageCount);
    }

    private static ForkJoinPool parallelSortInIsolatedPool(
            FullPageId[] pagesArr,
            Comparator<FullPageId> cmp,
            @Nullable ForkJoinPool pool
    ) throws IgniteInternalCheckedException {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool1 -> {
            ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool1);

            worker.setName("checkpoint-pages-sorter-" + worker.getPoolIndex());

            return worker;
        };

        ForkJoinPool execPool = pool == null ? new ForkJoinPool(PARALLEL_SORT_THREADS + 1, factory, null, false) : pool;

        Future<?> sortTask = execPool.submit(() -> Arrays.parallelSort(pagesArr, cmp));

        try {
            sortTask.get();
        } catch (ExecutionException | InterruptedException e) {
            throw new IgniteInternalCheckedException(
                    "Failed to perform pages array parallel sort",
                    e instanceof ExecutionException ? e.getCause() : e
            );
        }

        return execPool;
    }

    private IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> splitAndSortCheckpointPagesIfNeeded(
            CheckpointDirtyPagesInfoHolder dirtyPages
    ) throws IgniteInternalCheckedException {
        Set<IgniteBiTuple<PageMemoryImpl, FullPageId[]>> cpPagesPerRegion = new HashSet<>();

        int realPagesArrSize = 0;

        for (IgniteBiTuple<PageMemoryImpl, Collection<FullPageId>> regPages : dirtyPages.dirtyPages) {
            FullPageId[] pages = new FullPageId[regPages.getValue().size()];

            int pagePos = 0;

            for (FullPageId dirtyPage : regPages.getValue()) {
                assert realPagesArrSize++ != dirtyPages.dirtyPageCount :
                        "Incorrect estimated dirty pages number: " + dirtyPages.dirtyPageCount;

                pages[pagePos++] = dirtyPage;
            }

            // Some pages may have been already replaced.
            if (pagePos != pages.length) {
                cpPagesPerRegion.add(new IgniteBiTuple<>(regPages.getKey(), Arrays.copyOf(pages, pagePos)));
            } else {
                cpPagesPerRegion.add(new IgniteBiTuple<>(regPages.getKey(), pages));
            }
        }

        if (checkpointWriteOrder == CheckpointWriteOrder.SEQUENTIAL) {
            Comparator<FullPageId> cmp = Comparator.comparingInt(FullPageId::groupId).thenComparingLong(FullPageId::effectivePageId);

            ForkJoinPool pool = null;

            for (IgniteBiTuple<PageMemoryImpl, FullPageId[]> pagesPerReg : cpPagesPerRegion) {
                if (pagesPerReg.getValue().length >= parallelSortThreshold) {
                    pool = parallelSortInIsolatedPool(pagesPerReg.get2(), cmp, pool);
                } else {
                    Arrays.sort(pagesPerReg.get2(), cmp);
                }
            }

            if (pool != null) {
                pool.shutdown();
            }
        }

        return new IgniteConcurrentMultiPairQueue<>(cpPagesPerRegion);
    }
}
