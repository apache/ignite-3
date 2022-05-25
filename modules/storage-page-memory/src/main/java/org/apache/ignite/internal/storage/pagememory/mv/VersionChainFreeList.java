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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.INDEX_PARTITION;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.freelist.AbstractFreeList;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainDataIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * {@link AbstractFreeList} for {@link VersionChain} instances.
 */
public class VersionChainFreeList extends AbstractFreeList<VersionChain> {
    private static final IgniteLogger LOG = IgniteLogger.forClass(VersionChainFreeList.class);

    private final PageEvictionTracker evictionTracker;
    private final IoStatisticsHolder statHolder;

    private final UpdateTransactionIdHandler updateTransactionHandler = new UpdateTransactionIdHandler();

    /**
     * Constructor.
     *
     * @param grpId              Group ID.
     * @param pageMem            Page memory.
     * @param reuseList          Reuse list to use.
     * @param lockLsnr           Page lock listener.
     * @param metaPageId         Metadata page ID.
     * @param initNew            {@code True} if new metadata should be initialized.
     * @param pageListCacheLimit Page list cache limit.
     * @param evictionTracker    Page eviction tracker.
     * @param statHolder         Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     */
    public VersionChainFreeList(
            int grpId,
            PageMemory pageMem,
            ReuseList reuseList,
            PageLockListener lockLsnr,
            long metaPageId,
            boolean initNew,
            @Nullable AtomicLong pageListCacheLimit,
            PageEvictionTracker evictionTracker,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        super(
                grpId,
                "VersionChainFreeList_" + grpId,
                pageMem,
                reuseList,
                lockLsnr,
                FLAG_AUX,
                LOG,
                metaPageId,
                initNew,
                pageListCacheLimit,
                evictionTracker
        );

        this.evictionTracker = evictionTracker;
        this.statHolder = statHolder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long allocatePageNoReuse() throws IgniteInternalCheckedException {
        return pageMem.allocatePage(grpId, INDEX_PARTITION, defaultPageFlag);
    }

    /**
     * Inserts a row.
     *
     * @param row Row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void insertDataRow(VersionChain row) throws IgniteInternalCheckedException {
        super.insertDataRow(row, statHolder);
    }

    /**
     * Updates a row by link.
     *
     * @param link Row link.
     * @param row New row data.
     * @return {@code True} if was able to update row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public boolean updateDataRow(long link, VersionChain row) throws IgniteInternalCheckedException {
        return super.updateDataRow(link, row, statHolder);
    }

    /**
     * Updates version chain's transactionId.
     *
     * @param link         link to the slot containing row version
     * @param transactionId transaction ID to set
     * @throws IgniteInternalCheckedException if something fails
     */
    public void updateTransactionId(long link, @Nullable UUID transactionId) throws IgniteInternalCheckedException {
        updateDataRow(link, updateTransactionHandler, transactionId, statHolder);
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

    private class UpdateTransactionIdHandler implements PageHandler<UUID, Object> {
        @Override
        public Object run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo io,
                UUID arg,
                int itemId,
                IoStatisticsHolder statHolder
        ) throws IgniteInternalCheckedException {
            VersionChainDataIo dataIo = (VersionChainDataIo) io;

            dataIo.updateTransactionId(pageAddr, itemId, pageSize(), arg);

            evictionTracker.touchPage(pageId);

            return true;
        }
    }
}
