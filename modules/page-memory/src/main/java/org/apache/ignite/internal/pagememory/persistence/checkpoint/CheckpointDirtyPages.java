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

import java.util.Comparator;
import java.util.List;
import java.util.RandomAccess;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.jetbrains.annotations.Nullable;

/**
 * Pages and partition IDs that should be checkpointed, with modified pages sorted page IDs by {@link #DIRTY_PAGE_COMPARATOR} and new pages
 * grouped by partition id.
 */
public class CheckpointDirtyPages {
    /** Dirty page ID comparator by groupId -> partitionId -> pageIdx. */
    static final Comparator<FullPageId> DIRTY_PAGE_COMPARATOR = Comparator
            .comparingInt(FullPageId::groupId)
            .thenComparingLong(FullPageId::effectivePageId);

    /** Empty checkpoint dirty pages. */
    static final CheckpointDirtyPages EMPTY = new CheckpointDirtyPages(List.of());

    /** Dirty pages and partitions of data regions, with modified pages sorted by {@link #DIRTY_PAGE_COMPARATOR}. */
    private final List<DirtyPagesAndPartitions> dirtyPagesAndPartitions;

    /** Total number of modified page IDs. */
    private final int modifiedPagesCount;

    /** Total number of new page IDs. */
    private final int newPagesCount;

    /**
     * Constructor.
     *
     * @param dirtyPagesAndPartitions Dirty pages and partitions of data regions, with modified pages sorted by
     *     {@link #DIRTY_PAGE_COMPARATOR}. Expected list with {@link RandomAccess}.
     */
    CheckpointDirtyPages(List<DirtyPagesAndPartitions> dirtyPagesAndPartitions) {
        assert dirtyPagesAndPartitions instanceof RandomAccess : dirtyPagesAndPartitions;

        this.dirtyPagesAndPartitions = dirtyPagesAndPartitions;

        modifiedPagesCount = dirtyPagesAndPartitions.stream().mapToInt(pages -> pages.modifiedPages.length).sum();
        newPagesCount = dirtyPagesAndPartitions.stream()
                .mapToInt(DirtyPagesAndPartitions::newPagesCount)
                .sum();
    }

    /** Returns total number of modified pages. */
    public int modifiedPagesCount() {
        return modifiedPagesCount;
    }

    /** Return total number of newly allocated pages. */
    public int newPagesCount() {
        return newPagesCount;
    }

    /** Creates a concurrent queue of dirty partitions to be written to at checkpoint. */
    public IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> toDirtyPartitionQueue() {
        List<IgniteBiTuple<PersistentPageMemory, GroupPartitionId[]>> dirtyPartitions = dirtyPagesAndPartitions.stream()
                .map(dirtyPagesAndPartitions -> new IgniteBiTuple<>(
                        dirtyPagesAndPartitions.pageMemory,
                        dirtyPagesAndPartitions.dirtyPartitions.toArray(GroupPartitionId[]::new))
                )
                .collect(toList());

        return new IgniteConcurrentMultiPairQueue<>(dirtyPartitions);
    }

    /**
     * Looks for dirty page IDs views for a specific group and partition.
     *
     * @param pageMemory Page memory.
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public @Nullable CheckpointDirtyPages.CheckpointDirtyPagesView getPartitionView(PersistentPageMemory pageMemory, int grpId, int partId) {
        for (int i = 0; i < dirtyPagesAndPartitions.size(); i++) {
            if (dirtyPagesAndPartitions.get(i).pageMemory == pageMemory) {
                return getPartitionView(i, grpId, partId);
            }
        }

        throw new IllegalArgumentException("Unknown PageMemory: " + pageMemory);
    }

    private @Nullable CheckpointDirtyPages.CheckpointDirtyPagesView getPartitionView(int dirtyPagesIdx, int grpId, int partId) {
        FullPageId startPageId = new FullPageId(pageId(partId, (byte) 0, 0), grpId);
        FullPageId endPageId = new FullPageId(pageId(partId + 1, (byte) 0, 0), grpId);

        FullPageId[] modifiedPageIds = dirtyPagesAndPartitions.get(dirtyPagesIdx).modifiedPages;

        if (modifiedPageIds.length == 0) {
            return new CheckpointDirtyPagesView(dirtyPagesIdx, 0, 0, new GroupPartitionId(grpId, partId));
        }

        int fromIndex = binarySearch(modifiedPageIds, startPageId, DIRTY_PAGE_COMPARATOR);

        fromIndex = fromIndex >= 0 ? fromIndex : Math.min(modifiedPageIds.length - 1, -fromIndex - 1);

        if (!equalsByGroupAndPartition(startPageId, modifiedPageIds[fromIndex])) {
            return null;
        }

        int toIndex = binarySearch(modifiedPageIds, fromIndex, modifiedPageIds.length, endPageId, DIRTY_PAGE_COMPARATOR);

        toIndex = toIndex >= 0 ? toIndex : -toIndex - 1;

        return new CheckpointDirtyPagesView(dirtyPagesIdx, fromIndex, toIndex, new GroupPartitionId(grpId, partId));
    }

    /**
     * Returns a full list of {@link PersistentPageMemory} instances, for which there exist at least a single dirty partition in current
     * checkpoint.
     */
    List<PersistentPageMemory> dirtyPageMemoryInstances() {
        return this.dirtyPagesAndPartitions.stream().map(p -> p.pageMemory).collect(toList());
    }

    public boolean hasDelta() {
        return modifiedPagesCount > 0 || newPagesCount > 0;
    }

    public int dirtyPagesCount() {
        return modifiedPagesCount + newPagesCount;
    }

    /**
     * View of {@link CheckpointDirtyPages} in which all page IDs will refer to the same {@link PersistentPageMemory} and contain the
     * same groupId and partitionId and increasing pageIdx.
     *
     * <p>Thread safe.
     */
    class CheckpointDirtyPagesView {
        /** Element index in {@link CheckpointDirtyPages#dirtyPagesAndPartitions}. */
        private final int regionIndex;

        /** Starting position (inclusive) of the dirty page within the element at {@link #regionIndex}. */
        private final int fromPosition;

        /** End position (exclusive) of the dirty page within the element at {@link #regionIndex}. */
        private final int toPosition;

        private final GroupPartitionId partitionId;

        /**
         * Private constructor.
         *
         * @param regionIndex Element index in {@link CheckpointDirtyPages#dirtyPagesAndPartitions}.
         * @param fromPosition Starting position (inclusive) of the dirty page within the element at {@link #regionIndex}.
         * @param toPosition End position (exclusive) of the dirty page within the element at {@link #regionIndex}.
         */
        private CheckpointDirtyPagesView(int regionIndex, int fromPosition, int toPosition, GroupPartitionId partitionId) {
            this.regionIndex = regionIndex;
            this.fromPosition = fromPosition;
            this.toPosition = toPosition;
            this.partitionId = partitionId;
        }

        /**
         * Returns the modified page by index.
         *
         * @param index Modified page index.
         */
        public FullPageId getModifiedPage(int index) {
            return dirtyPagesAndPartitions.get(this.regionIndex).modifiedPages[fromPosition + index];
        }

        /**
         * Returns the newly allocated page by index.
         *
         * @param index Newly allocated page index.
         */
        public FullPageId getNewPage(int index) {
            return dirtyPagesAndPartitions.get(this.regionIndex).newPagesByGroupPartitionId.get(partitionId)[index];
        }

        /**
         * Returns the page memory for view.
         */
        public PersistentPageMemory pageMemory() {
            return dirtyPagesAndPartitions.get(regionIndex).pageMemory;
        }

        /**
         * Returns the size of the view.
         */
        public int modifiedPagesSize() {
            return toPosition - fromPosition;
        }

        public int newPagesSize() {
            FullPageId[] partitionNewPages = dirtyPagesAndPartitions.get(regionIndex).newPagesByGroupPartitionId.get(partitionId);

            return partitionNewPages == null ? 0 : partitionNewPages.length;
        }
    }

    private static boolean equalsByGroupAndPartition(FullPageId pageId0, FullPageId pageId1) {
        return pageId0.groupId() == pageId1.groupId() && pageId0.partitionId() == pageId1.partitionId();
    }
}
