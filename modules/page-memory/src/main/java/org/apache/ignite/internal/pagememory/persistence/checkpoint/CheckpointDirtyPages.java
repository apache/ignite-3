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

import static java.util.Arrays.binarySearch;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;

import java.util.Comparator;
import java.util.List;
import java.util.RandomAccess;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.jetbrains.annotations.Nullable;

/**
 * Dirty pages of data regions, with sorted page IDs by {@link #DIRTY_PAGE_COMPARATOR} and unsorted partition IDs that should be
 * checkpointed.
 */
class CheckpointDirtyPages {
    /** Dirty page ID comparator by groupId -> partitionId -> pageIdx. */
    static final Comparator<FullPageId> DIRTY_PAGE_COMPARATOR = Comparator
            .comparingInt(FullPageId::groupId)
            .thenComparingLong(FullPageId::effectivePageId);

    /** Empty checkpoint dirty pages. */
    static final CheckpointDirtyPages EMPTY = new CheckpointDirtyPages(List.of());

    /** Dirty pages of data regions, with sorted page IDs by {@link #DIRTY_PAGE_COMPARATOR} and unsorted partition IDs. */
    private final List<DataRegionDirtyPages<FullPageId[]>> dirtyPages;

    /** Total number of dirty page IDs. */
    private final int dirtyPagesCount;

    /**
     * Constructor.
     *
     * @param dirtyPages Dirty pages of data regions, with sorted page IDs by {@link #DIRTY_PAGE_COMPARATOR} and unsorted partition IDs.
     */
    public CheckpointDirtyPages(List<DataRegionDirtyPages<FullPageId[]>> dirtyPages) {
        assert dirtyPages instanceof RandomAccess : dirtyPages;

        this.dirtyPages = dirtyPages;

        dirtyPagesCount = dirtyPages.stream().mapToInt(pages -> pages.dirtyPages.length).sum();
    }

    /**
     * Returns total number of dirty page IDs.
     */
    public int dirtyPagesCount() {
        return dirtyPagesCount;
    }

    /**
     * Returns a queue of dirty page IDs to be written to a checkpoint.
     */
    public IgniteConcurrentMultiPairQueue<PersistentPageMemory, FullPageId> toDirtyPageIdQueue() {
        List<IgniteBiTuple<PersistentPageMemory, FullPageId[]>> dirtyPageIds = dirtyPages.stream()
                .map(pages -> new IgniteBiTuple<>(pages.pageMemory, pages.dirtyPages))
                .collect(toList());

        return new IgniteConcurrentMultiPairQueue<>(dirtyPageIds);
    }

    /**
     * Looks for dirty page IDs views for a specific group and partition.
     *
     * @param pageMemory Page memory.
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public @Nullable CheckpointDirtyPagesView getPartitionView(PersistentPageMemory pageMemory, int grpId, int partId) {
        for (int i = 0; i < dirtyPages.size(); i++) {
            if (dirtyPages.get(i).pageMemory == pageMemory) {
                return getPartitionView(i, grpId, partId);
            }
        }

        throw new IllegalArgumentException("Unknown PageMemory: " + pageMemory);
    }

    private @Nullable CheckpointDirtyPagesView getPartitionView(int dirtyPagesIdx, int grpId, int partId) {
        FullPageId startPageId = new FullPageId(pageId(partId, (byte) 0, 0), grpId);
        FullPageId endPageId = new FullPageId(pageId(partId + 1, (byte) 0, 0), grpId);

        FullPageId[] pageIds = dirtyPages.get(dirtyPagesIdx).dirtyPages;

        int fromIndex = binarySearch(pageIds, startPageId, DIRTY_PAGE_COMPARATOR);

        fromIndex = fromIndex >= 0 ? fromIndex : Math.min(pageIds.length - 1, -fromIndex - 1);

        if (!equalsByGroupAndPartition(startPageId, pageIds[fromIndex])) {
            return null;
        }

        int toIndex = binarySearch(pageIds, fromIndex, pageIds.length, endPageId, DIRTY_PAGE_COMPARATOR);

        toIndex = toIndex >= 0 ? toIndex : -toIndex - 1;

        return new CheckpointDirtyPagesView(dirtyPagesIdx, fromIndex, toIndex);
    }

    /**
     * Looks for the next dirty page IDs view from the current one, {@code null} if not found.
     *
     * @param currentView Current view to dirty pages, {@code null} to get first.
     */
    public @Nullable CheckpointDirtyPagesView nextPartitionView(@Nullable CheckpointDirtyPagesView currentView) {
        assert currentView == null || currentView.owner() == this : currentView;

        if (dirtyPages.isEmpty()) {
            return null;
        }

        int regionIndex;
        int fromPosition;

        if (currentView == null) {
            regionIndex = 0;
            fromPosition = 0;
        } else {
            regionIndex = currentView.needsNextRegion() ? currentView.regionIndex + 1 : currentView.regionIndex;
            fromPosition = currentView.needsNextRegion() ? 0 : currentView.toPosition;
        }

        if (regionIndex >= dirtyPages.size()) {
            return null;
        }

        FullPageId[] pageIds = dirtyPages.get(regionIndex).dirtyPages;

        FullPageId startPageId = pageIds[fromPosition];
        FullPageId endPageId = new FullPageId(pageId(partitionId(startPageId.pageId()) + 1, (byte) 0, 0), startPageId.groupId());

        int toPosition = binarySearch(pageIds, fromPosition, pageIds.length, endPageId, DIRTY_PAGE_COMPARATOR);

        toPosition = toPosition > 0 ? toPosition : -toPosition - 1;

        return new CheckpointDirtyPagesView(regionIndex, fromPosition, toPosition);
    }

    /**
     * View of {@link CheckpointDirtyPages} in which all dirty page IDs will refer to the same {@link PersistentPageMemory} and contain the
     * same groupId and partitionId and increasing pageIdx.
     *
     * <p>Thread safe.
     */
    class CheckpointDirtyPagesView {
        /** Element index in {@link CheckpointDirtyPages#dirtyPages}. */
        private final int regionIndex;

        /** Starting position (inclusive) of the dirty page within the element at {@link #regionIndex}. */
        private final int fromPosition;

        /** End position (exclusive) of the dirty page within the element at {@link #regionIndex}. */
        private final int toPosition;

        /**
         * Private constructor.
         *
         * @param regionIndex Element index in {@link CheckpointDirtyPages#dirtyPages}.
         * @param fromPosition Starting position (inclusive) of the dirty page within the element at {@link #regionIndex}.
         * @param toPosition End position (exclusive) of the dirty page within the element at {@link #regionIndex}.
         */
        private CheckpointDirtyPagesView(int regionIndex, int fromPosition, int toPosition) {
            this.regionIndex = regionIndex;
            this.fromPosition = fromPosition;
            this.toPosition = toPosition;
        }

        /**
         * Returns the dirty page by index.
         *
         * @param index Dirty page index.
         */
        public FullPageId get(int index) {
            return dirtyPages.get(this.regionIndex).dirtyPages[fromPosition + index];
        }

        /**
         * Returns the page memory for view.
         */
        public PersistentPageMemory pageMemory() {
            return dirtyPages.get(regionIndex).pageMemory;
        }

        /**
         * Returns the size of the view.
         */
        public int size() {
            return toPosition - fromPosition;
        }

        private CheckpointDirtyPages owner() {
            return CheckpointDirtyPages.this;
        }

        private boolean needsNextRegion() {
            return toPosition == dirtyPages.get(regionIndex).dirtyPages.length;
        }
    }

    private static boolean equalsByGroupAndPartition(FullPageId pageId0, FullPageId pageId1) {
        return pageId0.groupId() == pageId1.groupId() && pageId0.partitionId() == pageId1.partitionId();
    }
}
