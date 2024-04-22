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

package org.apache.ignite.internal.pagememory.freelist;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.datastructure.DataStructure;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.reuse.ReuseBag;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;

final class RemoveRowHandler implements PageHandler<ReuseBag, Long> {
    private final FreeListImpl freeList;

    /** Indicates whether partition ID should be masked from page ID. */
    private final boolean maskPartId;

    /**
     * Constructor.
     *
     * @param maskPartId Indicates whether partition ID should be masked from page ID.
     */
    RemoveRowHandler(FreeListImpl freeList, boolean maskPartId) {
        this.freeList = freeList;
        this.maskPartId = maskPartId;
    }

    /** {@inheritDoc} */
    @Override
    public Long run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIo iox,
            ReuseBag reuseBag,
            int itemId,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        DataPageIo io = (DataPageIo) iox;

        int oldFreeSpace = io.getFreeSpace(pageAddr);

        assert oldFreeSpace >= 0 : oldFreeSpace;

        long nextLink = io.removeRow(pageAddr, itemId, freeList.pageSize());

        int newFreeSpace = io.getFreeSpace(pageAddr);

        if (newFreeSpace > FreeListImpl.MIN_PAGE_FREE_SPACE) {
            int newBucket = freeList.bucket(newFreeSpace, false);

            boolean putIsNeeded = oldFreeSpace <= FreeListImpl.MIN_PAGE_FREE_SPACE;

            if (!putIsNeeded) {
                int oldBucket = freeList.bucket(oldFreeSpace, false);

                if (oldBucket != newBucket) {
                    // It is possible that page was concurrently taken for put, in this case put will handle bucket change.
                    pageId = maskPartId ? PageIdUtils.maskPartitionId(pageId) : pageId;

                    putIsNeeded = freeList.removeDataPage(pageId, pageAddr, io, oldBucket, statHolder);
                }
            }

            if (io.isEmpty(pageAddr)) {
                freeList.evictionTracker.forgetPage(pageId);

                if (putIsNeeded) {
                    reuseBag.addFreePage(DataStructure.recyclePage(pageId, pageAddr));
                }
            } else if (putIsNeeded) {
                freeList.put(null, pageId, pageAddr, newBucket, statHolder);
            }
        }

        // For common case boxed 0L will be cached inside of Long, so no garbage will be produced.
        return nextLink;
    }
}
