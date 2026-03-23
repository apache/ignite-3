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

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.DIRTY_PAGE_COMPARATOR;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;

import java.util.Arrays;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;

/** Helper class for checkpoint testing that may contain useful methods and constants. */
class TestCheckpointUtils {
    /** Sorts dirty pages and creates a new instance {@link DirtyPagesAndPartitions}. */
    static DirtyPagesAndPartitions createDirtyPagesAndPartitions(PersistentPageMemory pageMemory, DirtyFullPageId... dirtyPages) {
        Arrays.sort(dirtyPages, DIRTY_PAGE_COMPARATOR);

        return new DirtyPagesAndPartitions(
                pageMemory,
                dirtyPages,
                Arrays.stream(dirtyPages).map(GroupPartitionId::convert).collect(toSet())
        );
    }

    /**
     * Creates new dirty full page ID.
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     */
    static DirtyFullPageId dirtyFullPageId(int groupId, int partitionId) {
        return new DirtyFullPageId(pageId(partitionId, FLAG_DATA, 0), groupId, 1);
    }

    /** Creates an array of dirty pages. */
    static DirtyFullPageId[] dirtyFullPageIds(int groupId, int partitionId, PageIndexesWithPartitionGeneration... pageIndexes) {
        return Arrays.stream(pageIndexes)
                .flatMap(indexes -> Arrays.stream(indexes.pageIndexes)
                        .mapToLong(index -> pageId(partitionId, flag(index), index))
                        .mapToObj(pageId -> new DirtyFullPageId(pageId, groupId, indexes.partGen))
                ).toArray(DirtyFullPageId[]::new);
    }

    /** Unions arrays of dirty pages. */
    static DirtyFullPageId[] union(DirtyFullPageId[]... dirtyFullPageIds) {
        return Arrays.stream(dirtyFullPageIds)
                .flatMap(Arrays::stream)
                .toArray(DirtyFullPageId[]::new);
    }

    private static byte flag(int pageIndex) {
        return pageIndex % 2 == 0 ? FLAG_DATA : FLAG_AUX;
    }

    /** Page indexes with partition generation. */
    static class PageIndexesWithPartitionGeneration {
        private final int partGen;

        private final int[] pageIndexes;

        private PageIndexesWithPartitionGeneration(int partGen, int[] pageIndexes) {
            this.partGen = partGen;
            this.pageIndexes = pageIndexes;
        }

        /** Creates page indexes with partition generation. */
        static PageIndexesWithPartitionGeneration pageIndexesWithPartGen(int partGen, int... pageIndexes) {
            return new PageIndexesWithPartitionGeneration(partGen, pageIndexes);
        }
    }
}
