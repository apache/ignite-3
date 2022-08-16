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
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTrackerNoOp;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.pagememory.mv.RowVersionFreeList;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link DataRegion} for in-memory case.
 */
public class VolatilePageMemoryDataRegion implements DataRegion<VolatilePageMemory> {
    private static final int FREE_LIST_GROUP_ID = 0;

    private static final int FREE_LIST_PARTITION_ID = 0;

    private final VolatilePageMemoryDataRegionConfiguration cfg;

    private final PageIoRegistry ioRegistry;

    private final int pageSize;

    private volatile VolatilePageMemory pageMemory;

    private volatile RowVersionFreeList rowVersionFreeList;

    /**
     * Constructor.
     *
     * @param cfg Data region configuration.
     * @param ioRegistry IO registry.
     * @param pageSize Page size in bytes.
     */
    public VolatilePageMemoryDataRegion(
            VolatilePageMemoryDataRegionConfiguration cfg,
            PageIoRegistry ioRegistry,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) {
        this.cfg = cfg;
        this.ioRegistry = ioRegistry;
        this.pageSize = pageSize;
    }

    /**
     * Starts the in-memory data region.
     */
    public void start() {
        VolatilePageMemory pageMemory = new VolatilePageMemory(cfg, ioRegistry, pageSize);

        pageMemory.start();

        try {
            rowVersionFreeList = createRowVersionFreeList(pageMemory, null);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error creating a RowVersionFreeList", e);
        }

        this.pageMemory = pageMemory;
    }

    private static RowVersionFreeList createRowVersionFreeList(
            PageMemory pageMemory,
            @Nullable ReuseList reuseList
    ) throws IgniteInternalCheckedException {
        long metaPageId = pageMemory.allocatePage(FREE_LIST_GROUP_ID, FREE_LIST_PARTITION_ID, FLAG_AUX);

        return new RowVersionFreeList(
                FREE_LIST_GROUP_ID,
                FREE_LIST_PARTITION_ID,
                pageMemory,
                reuseList,
                PageLockListenerNoOp.INSTANCE,
                metaPageId,
                true,
                // Because in memory.
                null,
                PageEvictionTrackerNoOp.INSTANCE,
                IoStatisticsHolderNoOp.INSTANCE
        );
    }

    /**
     * Starts the in-memory data region.
     */
    public void stop() throws Exception {
        closeAll(
                pageMemory != null ? () -> pageMemory.stop(true) : null,
                rowVersionFreeList != null ? rowVersionFreeList::close : null
        );
    }

    /** {@inheritDoc} */
    @Override
    public VolatilePageMemory pageMemory() {
        checkDataRegionStarted();

        return pageMemory;
    }

    /**
     * Returns version chain free list.
     *
     * @throws StorageException If the data region did not start.
     */
    public RowVersionFreeList rowVersionFreeList() {
        checkDataRegionStarted();

        return rowVersionFreeList;
    }

    /**
     * Checks that the data region has started.
     *
     * @throws StorageException If the data region did not start.
     */
    private void checkDataRegionStarted() {
        if (pageMemory == null) {
            throw new StorageException("Data region not started");
        }
    }
}
