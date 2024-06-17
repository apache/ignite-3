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

package org.apache.ignite.internal.pagememory;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.flag;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.toDetailString;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.reuse.ReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Class responsible for allocating/freeing page IDs.
 */
public interface PageIdAllocator {
    /**
     * Flag for a Data page. Also used by partition meta and tracking pages. This type doesn't use the Page ID rotation mechanism.
     */
    byte FLAG_DATA = 1;

    /**
     * Flag for an internal structure page. This type uses the Page ID rotation mechanism.
     */
    byte FLAG_AUX = 2;

    /**
     * Max partition ID that can be used by affinity.
     */
    // TODO IGNITE-16350 Use constant from the table configuration.
    int MAX_PARTITION_ID = 65500;

    /**
     * Allocates a page from the space for the given partition ID and the given flags.
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     * @return Allocated page ID.
     */
    long allocatePage(int groupId, int partitionId, byte flags) throws IgniteInternalCheckedException;

    /**
     * Allocates a page from the space for the given partition ID and the given flags. If reuse list is provided, tries to recycle page.
     *
     * @param reuseList Reuse list to recycle pages from.
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     * @return Allocated page ID.
     */
    default long allocatePage(@Nullable ReuseList reuseList, int groupId, int partitionId, byte flags)
            throws IgniteInternalCheckedException {
        return allocatePage(reuseList, null, true, groupId, partitionId, flags);
    }

    /**
     * Allocates a page from the space for the given partition ID and the given flags.
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     * @return Allocated page ID.
     */
    default long allocatePage(
            @Nullable ReuseList reuseList,
            @Nullable ReuseBag bag,
            boolean useRecycled,
            int groupId,
            int partitionId,
            byte flags
    ) throws IgniteInternalCheckedException {
        long pageId = 0;

        if (reuseList != null) {
            pageId = bag != null ? bag.pollFreePage() : 0;

            if (pageId == 0) {
                pageId = reuseList.takeRecycledPage();
            }

            // Recycled. "pollFreePage" result should be reinitialized to move rotatedId to itemId.
            if (pageId != 0) {
                // Replace the partition ID, because the reused page might have come from a different data structure if the reuse
                // list is shared between them.
                pageId = replacePartitionId(pageId, partitionId);

                pageId = reuseList.initRecycledPage(pageId, flags, null);
            }
        }

        if (pageId == 0) {
            pageId = allocatePage(groupId, partitionId, flags);
        }

        assert pageId != 0;

        assert partitionId(pageId) >= 0 && partitionId(pageId) <= MAX_PARTITION_ID : toDetailString(pageId);

        assert flag(pageId) != FLAG_DATA || itemId(pageId) == 0 : toDetailString(pageId);

        return pageId;
    }

    private static long replacePartitionId(long pageId, int partId) {
        long partitionIdZeroMask = ~(PageIdUtils.PART_ID_MASK << PageIdUtils.PAGE_IDX_SIZE);

        long partitionIdMask = ((long) partId) << PageIdUtils.PAGE_IDX_SIZE;

        return pageId & partitionIdZeroMask | partitionIdMask;
    }

    /**
     * Frees the given page.
     *
     * @param groupId Group ID.
     * @param pageId Page ID.
     */
    boolean freePage(int groupId, long pageId);
}
