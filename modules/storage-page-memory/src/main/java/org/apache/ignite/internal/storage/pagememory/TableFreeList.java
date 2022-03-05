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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.freelist.AbstractFreeList;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * {@link FreeList} implementation for storage-page-memory module.
 */
public abstract class TableFreeList extends AbstractFreeList<TableDataRow> {
    private static final IgniteLogger LOG = IgniteLogger.forClass(TableFreeList.class);

    /**
     * Constructor.
     *
     * @param grpId Group ID.
     * @param pageMem Page memory.
     * @param lockLsnr Page lock listener.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @param pageListCacheLimit Page list cache limit.
     * @param evictionTracker Page eviction tracker.
     * @throws IgniteInternalCheckedException If failed.
     */
    public TableFreeList(
            int grpId,
            PageMemory pageMem,
            PageLockListener lockLsnr,
            long metaPageId,
            boolean initNew,
            @Nullable AtomicLong pageListCacheLimit,
            PageEvictionTracker evictionTracker
    ) throws IgniteInternalCheckedException {
        super(
                grpId,
                "TableFreeList_" + grpId,
                pageMem,
                null,
                lockLsnr,
                FLAG_AUX,
                LOG,
                metaPageId,
                initNew,
                pageListCacheLimit,
                evictionTracker
        );
    }
}
