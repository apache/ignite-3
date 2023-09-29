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

package org.apache.ignite.internal.storage.pagememory.mv;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.freelist.AbstractFreeList;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.storage.pagememory.mv.io.RowVersionDataIo;
import org.jetbrains.annotations.Nullable;

/**
 * {@link AbstractFreeList} for {@link RowVersion} instances.
 */
public class RowVersionFreeList extends AbstractFreeList<RowVersion> {
    private static final IgniteLogger LOG = Loggers.forClass(RowVersionFreeList.class);

    private final IoStatisticsHolder statHolder;

    private final UpdateTimestampHandler updateTimestampHandler = new UpdateTimestampHandler();

    private final UpdateNextLinkHandler updateNextLinkHandler = new UpdateNextLinkHandler();

    /**
     * Constructor.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param reuseList Reuse list to track pages that can be reused after they get completely empty (if {@code null}, the free list itself
     *      will be used as a ReuseList.
     * @param lockLsnr Page lock listener.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @param pageListCacheLimit Page list cache limit.
     * @param evictionTracker Page eviction tracker.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     */
    public RowVersionFreeList(
            int grpId,
            int partId,
            PageMemory pageMem,
            @Nullable ReuseList reuseList,
            PageLockListener lockLsnr,
            long metaPageId,
            boolean initNew,
            @Nullable AtomicLong pageListCacheLimit,
            PageEvictionTracker evictionTracker,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        super(
                grpId,
                partId,
                "RowVersionFreeList_" + grpId,
                pageMem,
                reuseList,
                lockLsnr,
                LOG,
                metaPageId,
                initNew,
                pageListCacheLimit,
                evictionTracker
        );

        this.statHolder = statHolder;
    }

    /**
     * Inserts a row.
     *
     * @param row Row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void insertDataRow(RowVersion row) throws IgniteInternalCheckedException {
        super.insertDataRow(row, statHolder);
    }

    /**
     * Updates row version's timestamp.
     *
     * @param link link to the slot containing row version
     * @param newTimestamp timestamp to set
     * @throws IgniteInternalCheckedException if something fails
     */
    public void updateTimestamp(long link, HybridTimestamp newTimestamp) throws IgniteInternalCheckedException {
        updateDataRow(link, updateTimestampHandler, newTimestamp, statHolder);
    }

    /**
     * Updates row version's next link.
     *
     * @param link Row version link.
     * @param nextLink Next row version link to set.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void updateNextLink(long link, long nextLink) throws IgniteInternalCheckedException {
        updateDataRow(link, updateNextLinkHandler, nextLink, statHolder);
    }

    /**
     * Removes a row by link.
     *
     * @param link Row link.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void removeDataRowByLink(long link) throws IgniteInternalCheckedException {
        super.removeDataRowByLink(link, statHolder);
    }

    private class UpdateTimestampHandler implements PageHandler<HybridTimestamp, Object> {
        @Override
        public Object run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo io,
                HybridTimestamp arg,
                int itemId,
                IoStatisticsHolder statHolder
        ) throws IgniteInternalCheckedException {
            RowVersionDataIo dataIo = (RowVersionDataIo) io;

            dataIo.updateTimestamp(pageAddr, itemId, pageSize(), arg);

            evictionTracker.touchPage(pageId);

            return true;
        }
    }

    private class UpdateNextLinkHandler implements PageHandler<Long, Object> {
        @Override
        public Object run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo io,
                Long nextLink,
                int itemId,
                IoStatisticsHolder statHolder
        ) throws IgniteInternalCheckedException {
            RowVersionDataIo dataIo = (RowVersionDataIo) io;

            dataIo.updateNextLink(pageAddr, itemId, pageSize(), nextLink);

            evictionTracker.touchPage(pageId);

            return true;
        }
    }

    /**
     * Shortcut method for {@link #saveMetadata(IoStatisticsHolder)} with statistics holder.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    public void saveMetadata() throws IgniteInternalCheckedException {
        super.saveMetadata(statHolder);
    }
}
