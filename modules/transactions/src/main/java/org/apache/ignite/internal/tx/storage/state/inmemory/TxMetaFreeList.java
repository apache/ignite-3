/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.tx.storage.state.inmemory;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.freelist.AbstractFreeList;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

public class TxMetaFreeList extends AbstractFreeList<TxMetaRowWrapper> {
    private final IoStatisticsHolder statHolder;

    /**
     * Constructor.
     *
     * @param grpId              Group ID.
     * @param partId             Partition ID.
     * @param name               Structure name (for debug purpose).
     * @param pageMem            Page memory.
     * @param reuseList          Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param lockLsnr           Page lock listener.
     * @param log                Logger.
     * @param metaPageId         Metadata page ID.
     * @param initNew            {@code True} if new metadata should be initialized.
     * @param pageListCacheLimit Page list cache limit.
     * @param evictionTracker    Page eviction tracker.
     * @throws IgniteInternalCheckedException If failed.
     */
    public TxMetaFreeList(
        int grpId,
        int partId,
        String name,
        PageMemory pageMem,
        @Nullable ReuseList reuseList,
        PageLockListener lockLsnr,
        IgniteLogger log,
        long metaPageId,
        boolean initNew,
        @Nullable AtomicLong pageListCacheLimit,
        PageEvictionTracker evictionTracker,
        IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        super(grpId, partId, name, pageMem, reuseList, lockLsnr, log, metaPageId, initNew, pageListCacheLimit, evictionTracker);

        this.statHolder = statHolder;
    }

    public void insertDataRow(TxMetaRowWrapper row) throws IgniteInternalCheckedException {
        super.insertDataRow(row, statHolder);
    }

    public void removeDataRowByLink(long link) throws IgniteInternalCheckedException {
        super.removeDataRowByLink(link, statHolder);
    }
}
