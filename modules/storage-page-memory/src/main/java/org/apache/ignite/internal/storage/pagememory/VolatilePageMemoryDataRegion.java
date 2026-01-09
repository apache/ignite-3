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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema.UNSPECIFIED_SIZE;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.VolatileDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryProfileConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryProfileView;
import org.apache.ignite.internal.util.OffheapReadWriteLock;

/**
 * Implementation of {@link DataRegion} for in-memory case.
 */
public class VolatilePageMemoryDataRegion implements DataRegion<VolatilePageMemory> {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(VolatilePageMemoryDataRegion.class);

    /** Ignite page memory concurrency level. */
    private static final String IGNITE_OFFHEAP_LOCK_CONCURRENCY_LEVEL = "IGNITE_OFFHEAP_LOCK_CONCURRENCY_LEVEL";

    private static final int FREE_LIST_GROUP_ID = 0;

    private static final int FREE_LIST_PARTITION_ID = 0;

    private final VolatilePageMemoryProfileConfiguration cfg;

    private volatile long regionSize;

    private final PageIoRegistry ioRegistry;

    private final int pageSize;

    private volatile VolatilePageMemory pageMemory;

    private volatile FreeListImpl freeList;

    /**
     * Constructor.
     *
     * @param cfg Data region configuration.
     * @param ioRegistry IO registry.
     * @param pageSize Page size in bytes.
     */
    VolatilePageMemoryDataRegion(
            VolatilePageMemoryProfileConfiguration cfg,
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
        int lockConcLvl = IgniteSystemProperties.getInteger(
                IGNITE_OFFHEAP_LOCK_CONCURRENCY_LEVEL,
                Integer.highestOneBit(Runtime.getRuntime().availableProcessors() * 4)
        );

        VolatileDataRegionConfiguration regionConfiguration = regionConfiguration(this.cfg, pageSize);

        this.regionSize = regionConfiguration.maxSizeBytes();

        var pageMemory = new VolatilePageMemory(regionConfiguration, ioRegistry, new OffheapReadWriteLock(lockConcLvl));

        pageMemory.start();

        try {
            this.freeList = createFreeList(pageMemory);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error creating free list", e);
        }

        this.pageMemory = pageMemory;
    }

    private static VolatileDataRegionConfiguration regionConfiguration(VolatilePageMemoryProfileConfiguration cfg, int pageSize) {
        var cfgView = (VolatilePageMemoryProfileView) cfg.value();

        long initSize = cfgView.initSizeBytes();
        long maxSize = cfgView.maxSizeBytes();

        if (maxSize == UNSPECIFIED_SIZE) {
            maxSize = StorageEngine.defaultDataRegionSize();

            LOG.info(
                    "{}.{} property is not specified, setting its value to {}",
                    cfg.name().value(), cfg.maxSizeBytes().key(), maxSize
            );
        }

        if (initSize == UNSPECIFIED_SIZE) {
            initSize = maxSize;

            LOG.info(
                    "{}.{} property is not specified, setting its value to {}",
                    cfg.name().value(), cfg.initSizeBytes().key(), maxSize
            );
        }

        return VolatileDataRegionConfiguration.builder()
                .name(cfgView.name())
                .pageSize(pageSize)
                .initSize(initSize)
                .maxSize(maxSize)
                .build();
    }

    private static FreeListImpl createFreeList(PageMemory pageMemory) throws IgniteInternalCheckedException {
        long metaPageId = pageMemory.allocatePageNoReuse(FREE_LIST_GROUP_ID, FREE_LIST_PARTITION_ID, FLAG_AUX);

        return new FreeListImpl(
                "VolatileFreeList",
                FREE_LIST_GROUP_ID,
                FREE_LIST_PARTITION_ID,
                pageMemory,
                metaPageId,
                true,
                // Because in memory.
                null
        );
    }

    /**
     * Starts the in-memory data region.
     */
    public void stop() throws Exception {
        closeAllManually(
                freeList,
                pageMemory != null ? () -> pageMemory.stop(true) : null
        );
    }

    @Override
    public VolatilePageMemory pageMemory() {
        checkDataRegionStarted();

        return pageMemory;
    }

    public ReuseList reuseList() {
        return freeList;
    }

    /**
     * Returns version chain free list.
     *
     * @throws StorageException If the data region did not start.
     */
    public FreeListImpl freeList() {
        checkDataRegionStarted();

        return freeList;
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

    @Override
    public long regionSize() {
        return regionSize;
    }
}
