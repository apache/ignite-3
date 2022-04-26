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
import static org.apache.ignite.internal.pagememory.PageIdAllocator.INDEX_PARTITION;

import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfiguration;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTrackerNoOp;
import org.apache.ignite.internal.pagememory.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of {@link AbstractPageMemoryDataRegion} for in-memory case.
 */
class VolatilePageMemoryDataRegion extends AbstractPageMemoryDataRegion {
    private TableFreeList freeList;

    /**
     * Constructor.
     *
     * @param cfg Data region configuration.
     * @param ioRegistry IO registry.
     */
    public VolatilePageMemoryDataRegion(PageMemoryDataRegionConfiguration cfg, PageIoRegistry ioRegistry) {
        super(cfg, ioRegistry);
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        assert !persistent() : cfg.value().name();

        assert cfg.memoryAllocator() instanceof UnsafeMemoryAllocatorConfiguration : cfg.memoryAllocator();

        PageMemory pageMemory = new PageMemoryNoStoreImpl(new UnsafeMemoryProvider(null), cfg, ioRegistry);

        pageMemory.start();

        this.pageMemory = pageMemory;

        try {
            int grpId = 0;

            long metaPageId = pageMemory.allocatePage(grpId, INDEX_PARTITION, FLAG_AUX);

            this.freeList = new TableFreeList(
                    grpId,
                    pageMemory,
                    PageLockListenerNoOp.INSTANCE,
                    metaPageId,
                    true,
                    null,
                    PageEvictionTrackerNoOp.INSTANCE,
                    IoStatisticsHolderNoOp.INSTANCE
            );
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error creating a freeList", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        super.stop();

        if (freeList != null) {
            freeList.close();
        }
    }

    /**
     * Returns free list.
     *
     * <p>NOTE: Free list must be one for the in-memory data region.
     */
    public TableFreeList freeList() {
        return freeList;
    }
}
