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

package org.apache.ignite.internal.pagememory.datapage;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.jetbrains.annotations.Nullable;

/**
 * Contains logic for reading values (that cannot be fragmented, that it, occupy more than one page) from page memory data pages.
 */
public abstract class NonFragmentableDataPageReader<T> {
    private final PageMemory pageMemory;
    private final int groupId;
    private final IoStatisticsHolder statisticsHolder;

    /**
     * Constructs a new instance.
     *
     * @param pageMemory       page memory that will be used to lock and access memory
     * @param groupId          ID of the cache group with which the reader works (all pages must belong to this group)
     * @param statisticsHolder used to track statistics about operations
     */
    public NonFragmentableDataPageReader(PageMemory pageMemory, int groupId, IoStatisticsHolder statisticsHolder) {
        this.pageMemory = pageMemory;
        this.groupId = groupId;
        this.statisticsHolder = statisticsHolder;
    }

    /**
     * Returns a row by link. If it turns out the value was fragmented, an exception is thrown, as this implementation
     * cannot handle fragmented values.
     *
     * @param link Row link
     * @return row object assembled from the row bytes
     * @throws IgniteInternalCheckedException If failed
     * @see org.apache.ignite.internal.pagememory.util.PageIdUtils#link(long, int)
     */
    @Nullable
    public T getRowByLink(final long link) throws IgniteInternalCheckedException {
        assert link != 0;

        int pageSize = pageMemory.realPageSize(groupId);

        final long pageId = pageId(link);

        final long page = pageMemory.acquirePage(groupId, pageId, statisticsHolder);

        try {
            long pageAddr = pageMemory.readLock(groupId, pageId, page);

            assert pageAddr != 0L : link;

            try {
                DataPageIo dataIo = pageMemory.ioRegistry().resolve(pageAddr);

                int itemId = itemId(link);

                if (handleNonExistentItemsGracefully() && !dataIo.itemExists(pageAddr, itemId, pageSize)) {
                    return rowForNonExistingItem(pageId, itemId);
                }

                DataPagePayload data = dataIo.readPayload(pageAddr, itemId, pageSize);

                if (data.hasMoreFragments()) {
                    throw new IllegalStateException("Value for link " + link + " is fragmented, which is not supported");
                }

                return readRowFromAddress(link, pageAddr + data.offset());

            } finally {
                pageMemory.readUnlock(groupId, pageId, page);
            }
        } finally {
            pageMemory.releasePage(groupId, pageId, page);
        }
    }

    /**
     * Reads row object from a contiguous region of memory represented by its starting address. The memory region contains exactly
     * the bytes representing the row.
     *
     * @param link            row link
     * @param pageAddr        address of memory containing all row bytes
     * @return row object
     * @see org.apache.ignite.internal.pagememory.util.PageIdUtils#link(long, int)
     */
    protected abstract T readRowFromAddress(long link, long pageAddr);

    /**
     * Returns {@code true} if graceful handling of non-existent items should be enabled.
     * If it is enabled and a link with a non-existent item ID is provided, {@link #getRowByLink(long)} will return
     * the result of {@link #rowForNonExistingItem(long, long)} invocation.
     * If it is disabled, then an assertion will fail.
     *
     * @return {@code true} if graceful handling of non-existent items should be enabled
     */
    protected boolean handleNonExistentItemsGracefully() {
        return false;
    }

    /**
     * Returns a special row value that can be used as a marker to specify that the given itemId does not actually exist.
     *
     * @param pageId ID of the page
     * @param itemId ID of the item
     * @return special row value
     */
    @Nullable
    protected T rowForNonExistingItem(long pageId, long itemId) {
        throw new IllegalStateException("Item is invalid for pageId=" + pageId + ", itemId=" + itemId);
    }
}
