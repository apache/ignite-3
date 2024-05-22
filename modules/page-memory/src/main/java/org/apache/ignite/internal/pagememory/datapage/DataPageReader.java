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
 * Contains logic for reading values from page memory data pages (this includes handling multy-page values).
 */
public class DataPageReader {
    private final PageMemory pageMemory;
    private final int groupId;
    private final IoStatisticsHolder statisticsHolder;

    /**
     * Constructs a new instance.
     *
     * @param pageMemory       Page memory that will be used to lock and access memory.
     * @param groupId          ID of the cache group with which the reader works (all pages must belong to this group)
     * @param statisticsHolder used to track statistics about operations
     */
    public DataPageReader(PageMemory pageMemory, int groupId, IoStatisticsHolder statisticsHolder) {
        this.pageMemory = pageMemory;
        this.groupId = groupId;
        this.statisticsHolder = statisticsHolder;
    }

    /**
     * Traverses page memory starting at the given link. At each step, reads the current data payload and feeds it to the given
     * {@link PageMemoryTraversal} object which updates itself (usually) and returns next link to continue traversal
     * (or {@link PageMemoryTraversal#STOP_TRAVERSAL} to stop).
     *
     * @param link Row link
     * @param traversal Object consuming payloads and controlling the traversal.
     * @param argument Argument that is passed to the traversal.
     * @throws IgniteInternalCheckedException If failed
     * @see org.apache.ignite.internal.pagememory.util.PageIdUtils#link(long, int)
     * @see PageMemoryTraversal
     */
    public <T> void traverse(final long link, PageMemoryTraversal<T> traversal, @Nullable T argument)
            throws IgniteInternalCheckedException {
        assert link != 0;

        int pageSize = pageMemory.realPageSize(groupId);

        long currentLink = link;

        do {
            final long pageId = pageId(currentLink);
            final long page = pageMemory.acquirePage(groupId, pageId, statisticsHolder);

            try {
                long pageAddr = pageMemory.readLock(groupId, pageId, page);
                assert pageAddr != 0L : currentLink;

                try {
                    DataPageIo dataIo = pageMemory.ioRegistry().resolve(pageAddr);

                    int itemId = itemId(currentLink);

                    DataPagePayload data = dataIo.readPayload(pageAddr, itemId, pageSize);

                    currentLink = traversal.consumePagePayload(currentLink, pageAddr, data, argument);
                } finally {
                    pageMemory.readUnlock(groupId, pageId, page);
                }
            } finally {
                pageMemory.releasePage(groupId, pageId, page);
            }
        } while (currentLink != PageMemoryTraversal.STOP_TRAVERSAL);

        traversal.finish();
    }
}
