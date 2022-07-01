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

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;

import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;
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
    /** Empty checkpoint dirty pages. */
    static final CheckpointDirtyPages EMPTY = new CheckpointDirtyPages();

    private final List<IgniteBiTuple<PersistentPageMemory, FullPageId[]>> dirtyPages;

    /**
     * Constructor.
     *
     * @param dirtyPages Sorted dirty pages from data regions by groupId -> partitionId -> pageIdx.
     */
    @SafeVarargs
    CheckpointDirtyPages(@Nullable IgniteBiTuple<PersistentPageMemory, FullPageId[]>... dirtyPages) {
        this(dirtyPages == null ? List.of() : List.of(dirtyPages));
    }

    /**
     * Constructor.
     *
     * @param dirtyPages Sorted dirty pages from data regions by groupId -> partitionId -> pageIdx.
     */
    CheckpointDirtyPages(List<IgniteBiTuple<PersistentPageMemory, FullPageId[]>> dirtyPages) {
        this.dirtyPages = dirtyPages;
    }

    /**
     * Returns the total number of dirty pages.
     */
    int dirtyPagesCount() {
        int count = 0;

        for (IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages : this.dirtyPages) {
            count += dirtyPages.getValue().length;
        }

        return count;
    }

    /**
     * Returns a view of the dirty pages to be written to the checkpoint, all dirty pages will have the same groupId and partitionId, and
     * the pageIdx will increase.
     *
     * @param currentView Current view of the dirty pages, {@code null} to get the first view.
     * @return Next view of the dirty pages, {@code null} if there are no more dirty pages or {@code currentView} was taken from another
     *      {@link CheckpointDirtyPages}.
     */
    @Nullable CheckpointDirtyPagesView nextView(@Nullable CheckpointDirtyPagesView currentView) {
        if (dirtyPages.isEmpty() || (currentView != null && currentView.outer() != this)) {
            return null;
        }

        int currentDirtyPagesIndex = currentView == null ? 0 : currentView.dirtyPagesIndex;

        int fromIndex = currentView == null ? 0 : currentView.toIndex + 1;

        for (int i = currentDirtyPagesIndex; i < dirtyPages.size(); i++) {
            int toIndex = findToIndex(i, fromIndex);

            if (toIndex >= 0) {
                return new CheckpointDirtyPagesView(i, fromIndex, toIndex);
            }

            fromIndex = 0;
        }

        return null;
    }

    private int findToIndex(int dirtyPageIndex, int fromIndex) {
        FullPageId[] dirtyPagesIds = dirtyPages.get(dirtyPageIndex).getValue();

        if (fromIndex >= dirtyPagesIds.length) {
            return -1;
        }

        FullPageId firstPageId = dirtyPagesIds[fromIndex];

        int toIndex = fromIndex;

        for (int i = toIndex + 1; i < dirtyPagesIds.length; i++, toIndex++) {
            FullPageId nextPageId = dirtyPagesIds[i];

            if (nextPageId.groupId() != firstPageId.groupId() || partitionId(nextPageId.pageId()) != partitionId(firstPageId.pageId())) {
                break;
            }
        }

        return toIndex;
    }

    /**
     * Unchanged view of dirty pages for the group partition.
     */
    class CheckpointDirtyPagesView extends AbstractList<FullPageId> implements RandomAccess {
        private final int dirtyPagesIndex;

        private final int fromIndex;

        private final int toIndex;

        /**
         * Constructor.
         *
         * @param dirtyPagesIndex Sorted dirty pages by groupId -> partitionId -> pageIdx.
         * @param fromIndex Starting index (inclusive) at {@code dirtyPages}.
         * @param toIndex End index (inclusive) at {@code dirtyPages}.
         */
        private CheckpointDirtyPagesView(
                int dirtyPagesIndex,
                int fromIndex,
                int toIndex
        ) {
            this.dirtyPagesIndex = dirtyPagesIndex;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
        }

        /** {@inheritDoc} */
        @Override
        public FullPageId get(int index) {
            if (index < 0 || index >= size()) {
                throw new IndexOutOfBoundsException(index);
            }

            return dirtyPages.get(dirtyPagesIndex).getValue()[fromIndex + index];
        }

        /** {@inheritDoc} */
        @Override
        public int size() {
            return toIndex - fromIndex + 1;
        }

        /**
         * Returns page memory.
         */
        public PersistentPageMemory pageMemory() {
            return dirtyPages.get(dirtyPagesIndex).getKey();
        }

        private CheckpointDirtyPages outer() {
            return CheckpointDirtyPages.this;
        }
    }
}
