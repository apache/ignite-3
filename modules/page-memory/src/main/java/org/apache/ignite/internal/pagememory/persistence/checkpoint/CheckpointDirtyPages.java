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

import static java.util.Collections.binarySearch;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Sorted dirty pages from data regions that should be checkpointed.
 *
 * <p>Dirty pages should be sorted by groupId -> partitionId -> pageIdx.
 */
class CheckpointDirtyPages {
    /** Dirty page comparator. */
    static final Comparator<FullPageId> DIRTY_PAGE_COMPARATOR = Comparator
            .comparingInt(FullPageId::groupId)
            .thenComparingLong(FullPageId::effectivePageId);

    /** Empty checkpoint dirty pages. */
    static final CheckpointDirtyPages EMPTY = new CheckpointDirtyPages(List.of());

    /** Sorted dirty pages from data regions by groupId -> partitionId -> pageIdx. */
    private final List<IgniteBiTuple<PersistentPageMemory, List<FullPageId>>> dirtyPages;

    /** Total number of dirty pages. */
    private final int dirtyPagesCount;

    /**
     * Constructor.
     *
     * @param dirtyPages Sorted dirty pages from data regions by groupId -> partitionId -> pageIdx.
     */
    public CheckpointDirtyPages(Map<PersistentPageMemory, List<FullPageId>> dirtyPages) {
        this(dirtyPages.isEmpty() ? List.of()
                : dirtyPages.entrySet().stream().map(e -> new IgniteBiTuple<>(e.getKey(), e.getValue())).collect(toList()));
    }

    /**
     * Constructor.
     *
     * @param dirtyPages Sorted dirty pages from data regions by groupId -> partitionId -> pageIdx.
     */
    public CheckpointDirtyPages(List<IgniteBiTuple<PersistentPageMemory, List<FullPageId>>> dirtyPages) {
        assert dirtyPages instanceof RandomAccess : dirtyPages;

        this.dirtyPages = dirtyPages;

        int count = 0;

        for (IgniteBiTuple<PersistentPageMemory, List<FullPageId>> pages : dirtyPages) {
            assert !pages.getValue().isEmpty() : pages.getKey();
            assert pages.getValue() instanceof RandomAccess : pages.getValue();

            count += pages.getValue().size();
        }

        dirtyPagesCount = count;
    }

    /**
     * Returns total number of dirty pages.
     */
    public int dirtyPagesCount() {
        return dirtyPagesCount;
    }

    /**
     * Returns a queue of dirty pages to be written to a checkpoint.
     */
    public CheckpointDirtyPagesQueue toQueue() {
        return new CheckpointDirtyPagesQueue();
    }

    /**
     * Looks for dirty page views for a specific group and partition.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public @Nullable CheckpointDirtyPagesView findView(int grpId, int partId) {
        if (dirtyPages.isEmpty()) {
            return null;
        }

        FullPageId startPageId = new FullPageId(pageId(partId, (byte) 0, 0), grpId);
        FullPageId endPageId = new FullPageId(pageId(partId + 1, (byte) 0, 0), grpId);

        for (int i = 0; i < dirtyPages.size(); i++) {
            List<FullPageId> pageIds = dirtyPages.get(i).getValue();

            int fromIndex = binarySearch(pageIds, startPageId, DIRTY_PAGE_COMPARATOR);

            fromIndex = fromIndex >= 0 ? fromIndex : Math.min(pageIds.size() - 1, -fromIndex - 1);

            if (!equalsByGroupAndPartition(startPageId, pageIds.get(fromIndex))) {
                continue;
            }

            int toIndex = binarySearch(pageIds.subList(fromIndex, pageIds.size()), endPageId, DIRTY_PAGE_COMPARATOR);

            toIndex = toIndex > 0 ? toIndex - 1 : -toIndex - 2;

            return new CheckpointDirtyPagesView(i, fromIndex, fromIndex + toIndex);
        }

        return null;
    }

    /**
     * Looks for the next dirty page view from the current one, {@code null} if not found.
     *
     * @param currentView Current view to dirty pages, {@code null} to get first.
     */
    public @Nullable CheckpointDirtyPagesView nextView(@Nullable CheckpointDirtyPagesView currentView) {
        assert currentView == null || currentView.owner() == this : currentView;

        if (dirtyPages.isEmpty()) {
            return null;
        }

        int index;
        int fromPosition;

        if (currentView == null) {
            index = 0;
            fromPosition = 0;
        } else {
            index = currentView.isToPositionLast() ? currentView.index + 1 : currentView.index;
            fromPosition = currentView.isToPositionLast() ? 0 : currentView.toPosition + 1;
        }

        if (index >= dirtyPages.size()) {
            return null;
        }

        List<FullPageId> pageIds = dirtyPages.get(index).getValue();

        if (fromPosition == pageIds.size() - 1 || !equalsByGroupAndPartition(pageIds.get(fromPosition), pageIds.get(fromPosition + 1))) {
            return new CheckpointDirtyPagesView(index, fromPosition, fromPosition);
        }

        FullPageId startPageId = pageIds.get(fromPosition);
        FullPageId endPageId = new FullPageId(pageId(partitionId(startPageId.pageId()) + 1, (byte) 0, 0), startPageId.groupId());

        int toPosition = binarySearch(pageIds.subList(fromPosition, pageIds.size()), endPageId, DIRTY_PAGE_COMPARATOR);

        toPosition = toPosition > 0 ? toPosition - 1 : -toPosition - 2;

        return new CheckpointDirtyPagesView(index, fromPosition, fromPosition + toPosition);
    }

    /**
     * Queue of dirty pages that will need to be written to a checkpoint.
     *
     * <p>Thread safe.
     */
    class CheckpointDirtyPagesQueue {
        /** Current position in the queue. */
        private final AtomicInteger position = new AtomicInteger();

        /** Sizes each element in {@link #dirtyPages} + the previous value in this array. */
        private final int[] sizes;

        /**
         * Private constructor.
         */
        private CheckpointDirtyPagesQueue() {
            int size = 0;

            int[] sizes = new int[dirtyPages.size()];

            for (int i = 0; i < dirtyPages.size(); i++) {
                sizes[i] = size += dirtyPages.get(i).getValue().size();
            }

            this.sizes = sizes;
        }

        /**
         * Returns {@link true} if the next element of the queue was obtained.
         *
         * @param result Holder is the result of getting the next dirty page.
         */
        public boolean next(QueueResult result) {
            int queuePosition = this.position.getAndIncrement();

            if (queuePosition >= dirtyPagesCount) {
                result.owner = null;

                return false;
            }

            if (result.owner != this) {
                result.owner = this;
                result.index = 0;
            }

            int index = result.index;

            if (queuePosition >= sizes[index]) {
                if (queuePosition == sizes[index]) {
                    index++;
                } else {
                    index = findDirtyPagesIndex(index, queuePosition);
                }
            }

            result.index = index;
            result.position = index > 0 ? queuePosition - sizes[index - 1] : queuePosition;

            return true;
        }

        /**
         * Returns {@link true} if the queue is empty.
         */
        public boolean isEmpty() {
            return position.get() >= dirtyPagesCount;
        }

        /**
         * Returns the size of the queue.
         */
        public int size() {
            return dirtyPagesCount - Math.min(dirtyPagesCount, position.get());
        }

        private int findDirtyPagesIndex(int index, int position) {
            return Math.abs(Arrays.binarySearch(sizes, index, sizes.length, position) + 1);
        }

        private CheckpointDirtyPages owner() {
            return CheckpointDirtyPages.this;
        }
    }

    /**
     * View of {@link CheckpointDirtyPages} in which all dirty pages will refer to the same {@link PersistentPageMemory} and contain the
     * same groupId and partitionId and increasing pageIdx.
     *
     * <p>Thread safe.
     */
    class CheckpointDirtyPagesView {
        /** Element index in {@link CheckpointDirtyPages#dirtyPages}. */
        private final int index;

        /** Starting position (inclusive) of the dirty page within the element at {@link #index}. */
        private final int fromPosition;

        /** End position (inclusive) of the dirty page within the element at {@link #index}. */
        private final int toPosition;

        /**
         * Private constructor.
         *
         * @param index Element index in {@link CheckpointDirtyPages#dirtyPages}.
         * @param fromPosition Starting position (inclusive) of the dirty page within the element at {@link #index}.
         * @param toPosition End position (inclusive) of the dirty page within the element at {@link #index}.
         */
        private CheckpointDirtyPagesView(int index, int fromPosition, int toPosition) {
            this.index = index;
            this.fromPosition = fromPosition;
            this.toPosition = toPosition;
        }

        /**
         * Returns the dirty page by index.
         *
         * @param index Dirty page index.
         */
        public FullPageId get(int index) {
            return dirtyPages.get(this.index).getValue().get(fromPosition + index);
        }

        /**
         * Returns the page memory for view.
         */
        public PersistentPageMemory pageMemory() {
            return dirtyPages.get(index).getKey();
        }

        /**
         * Returns the size of the view.
         */
        public int size() {
            return toPosition - fromPosition + 1;
        }

        private CheckpointDirtyPages owner() {
            return CheckpointDirtyPages.this;
        }

        private boolean isToPositionLast() {
            return toPosition == dirtyPages.get(index).getValue().size() - 1;
        }
    }

    /**
     * Holder is the result of getting the next dirty page in {@link CheckpointDirtyPagesQueue#next(QueueResult)}.
     *
     * <p>Not thread safe.
     */
    static class QueueResult {
        private @Nullable CheckpointDirtyPagesQueue owner;

        /** Element index in {@link CheckpointDirtyPages#dirtyPages}. */
        private int index;

        /** Position of the dirty page within the element at {@link #index}. */
        private int position;

        /**
         * Returns the page memory for the associated dirty page.
         */
        public @Nullable PersistentPageMemory pageMemory() {
            return owner == null ? null : owner.owner().dirtyPages.get(index).getKey();
        }

        /**
         * Returns dirty page.
         */
        public @Nullable FullPageId dirtyPage() {
            return owner == null ? null : owner.owner().dirtyPages.get(index).getValue().get(position);
        }
    }

    private static boolean equalsByGroupAndPartition(FullPageId pageId0, FullPageId pageId1) {
        return pageId0.groupId() == pageId1.groupId() && partitionId(pageId0.pageId()) == partitionId(pageId1.pageId());
    }
}
