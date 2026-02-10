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
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.jetbrains.annotations.Nullable;

/** Dirty pages of data regions, with sorted page IDs by {@link #DIRTY_PAGE_COMPARATOR} and partition IDs that should be checkpointed. */
public class CheckpointDirtyPages {
    /** Dirty page ID comparator by groupId -> partitionId -> pageIdx -> partitionGeneration. */
    static final Comparator<DirtyFullPageId> DIRTY_PAGE_COMPARATOR = Comparator
            .comparingInt(DirtyFullPageId::groupId)
            .thenComparingLong(DirtyFullPageId::effectivePageId)
            .thenComparingInt(DirtyFullPageId::partitionGeneration);

    /** Empty checkpoint dirty pages. */
    static final CheckpointDirtyPages EMPTY = new CheckpointDirtyPages(List.of());

    /** Dirty pages and partitions of data regions, with sorted dirty page IDs by {@link #DIRTY_PAGE_COMPARATOR}. */
    private final List<DirtyPagesAndPartitions> dirtyPagesAndPartitions;

    /** Total number of dirty page IDs. */
    private final int dirtyPagesCount;

    /**
     * Constructor.
     *
     * @param dirtyPagesAndPartitions Dirty pages and partitions of data regions, with sorted dirty page IDs by
     *     {@link #DIRTY_PAGE_COMPARATOR}. Expected list with {@link RandomAccess}.
     */
    CheckpointDirtyPages(List<DirtyPagesAndPartitions> dirtyPagesAndPartitions) {
        assert dirtyPagesAndPartitions instanceof RandomAccess : dirtyPagesAndPartitions;

        this.dirtyPagesAndPartitions = dirtyPagesAndPartitions;

        dirtyPagesCount = dirtyPagesAndPartitions.stream().mapToInt(pages -> pages.dirtyPages.length).sum();
    }

    /**
     * Returns total number of dirty page IDs.
     */
    public int dirtyPagesCount() {
        return dirtyPagesCount;
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
    public @Nullable CheckpointDirtyPagesView getPartitionView(PersistentPageMemory pageMemory, int grpId, int partId) {
        for (int i = 0; i < dirtyPagesAndPartitions.size(); i++) {
            if (dirtyPagesAndPartitions.get(i).pageMemory == pageMemory) {
                return getPartitionView(i, grpId, partId);
            }
        }

        throw new IllegalArgumentException("Unknown PageMemory: " + pageMemory);
    }

    private @Nullable CheckpointDirtyPagesView getPartitionView(int dirtyPagesIdx, int grpId, int partId) {
        var startPageId = new DirtyFullPageId(pageId(partId, (byte) 0, 0), grpId, 0);
        var endPageId = new DirtyFullPageId(pageId(partId + 1, (byte) 0, 0), grpId, 0);

        DirtyFullPageId[] pageIds = dirtyPagesAndPartitions.get(dirtyPagesIdx).dirtyPages;

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
     * Returns a full list of {@link PersistentPageMemory} instances, for which there exist at least a single dirty partition in current
     * checkpoint.
     */
    List<PersistentPageMemory> dirtyPageMemoryInstances() {
        return this.dirtyPagesAndPartitions.stream().map(p -> p.pageMemory).collect(toList());
    }

    /**
     * View of {@link CheckpointDirtyPages} in which all dirty page IDs will refer to the same {@link PersistentPageMemory} and contain the
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

        /**
         * Private constructor.
         *
         * @param regionIndex Element index in {@link CheckpointDirtyPages#dirtyPagesAndPartitions}.
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
        public DirtyFullPageId get(int index) {
            return dirtyPagesAndPartitions.get(this.regionIndex).dirtyPages[fromPosition + index];
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
        public int size() {
            return toPosition - fromPosition;
        }

        /**
         * Checks whether this page (with index 0) is contained in the corresponding partition generation.
         *
         * <p>This implementation relies on the current implementation of the {@link #DIRTY_PAGE_COMPARATOR}
         * (groupId -> partitionId -> pageIdx -> partitionGeneration).</p>
         *
         * @param partitionGeneration Partition generation.
         */
        boolean containsMetaPage(int partitionGeneration) {
            DirtyFullPageId[] dirtyPages = dirtyPagesAndPartitions.get(this.regionIndex).dirtyPages;

            for (int i = fromPosition; i < toPosition; i++) {
                DirtyFullPageId dirtyPage = dirtyPages[i];

                if (dirtyPage.pageIdx() > 0) {
                    break;
                } else if (dirtyPage.partitionGeneration() == partitionGeneration) {
                    return true;
                }
            }

            return false;
        }

        /**
         * Counts the number of updated pages (not newly created ones) since the last checkpoint, i.e. those whose index is strictly less
         * than {@code checkpointedPages} for the corresponding partition generation.
         *
         * <p>This implementation relies on the current implementation of the {@link #DIRTY_PAGE_COMPARATOR}
         * (groupId -> partitionId -> pageIdx -> partitionGeneration).</p>
         *
         * @param partitionGeneration Partition generation.
         * @param checkpointedPages Number of pages of the partition that were stored on the disk at the beginning of the checkpoint.
         */
        int countAlteredPages(int partitionGeneration, int checkpointedPages) {
            DirtyFullPageId[] dirtyPages = dirtyPagesAndPartitions.get(this.regionIndex).dirtyPages;

            int count = 0;

            for (int i = fromPosition; i < toPosition; i++) {
                DirtyFullPageId dirtyPage = dirtyPages[i];

                if (dirtyPage.pageIdx() >= checkpointedPages) {
                    break;
                } else if (dirtyPage.partitionGeneration() == partitionGeneration) {
                    count++;
                }
            }

            return count;
        }
    }

    private static boolean equalsByGroupAndPartition(DirtyFullPageId pageId0, DirtyFullPageId pageId1) {
        return pageId0.groupId() == pageId1.groupId() && pageId0.partitionId() == pageId1.partitionId();
    }
}
