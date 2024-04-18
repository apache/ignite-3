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

package org.apache.ignite.internal.storage.pagememory.index.freelist;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.freelist.AbstractFreeList;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.jetbrains.annotations.Nullable;

/**
 * Free list implementation to store {@link IndexColumns} values.
 */
public class IndexColumnsFreeList extends AbstractFreeList<IndexColumns>  {
    private static final IgniteLogger LOG = Loggers.forClass(IndexColumnsFreeList.class);

    private final IoStatisticsHolder statHolder;

    /**
     * Constructor.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param lockLsnr Page lock listener.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @param pageListCacheLimit Page list cache limit.
     * @param evictionTracker Page eviction tracker.
     * @throws IgniteInternalCheckedException If failed.
     */
    public IndexColumnsFreeList(
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
                "IndexColumnsFreeList_" + grpId,
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
     * Shortcut method for {@link #saveMetadata(IoStatisticsHolder)} with statistics holder.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    public void saveMetadata() throws IgniteInternalCheckedException {
        saveMetadata(statHolder);
    }

    /**
     * Shortcut method for {@link #insertDataRow(IndexColumns, IoStatisticsHolder)} with statistics holder.
     */
    public void insertDataRow(IndexColumns row) throws IgniteInternalCheckedException {
        super.insertDataRow(row, statHolder);
    }

    /**
     * Shortcut method for {@link #removeDataRowByLink(long, IoStatisticsHolder)} with statistics holder.
     */
    public void removeDataRowByLink(long link) throws IgniteInternalCheckedException {
        super.removeDataRowByLink(link, statHolder);
    }
}
