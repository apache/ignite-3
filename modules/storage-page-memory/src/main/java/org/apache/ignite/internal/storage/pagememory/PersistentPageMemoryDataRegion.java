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

import static org.apache.ignite.internal.util.Constants.GiB;
import static org.apache.ignite.internal.util.Constants.MiB;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.storage.StorageException;

/**
 * Implementation of {@link DataRegion} for persistent case.
 */
class PersistentPageMemoryDataRegion implements DataRegion<PersistentPageMemory> {
    /**
     * Threshold to calculate limit for pages list on-heap caches.
     *
     * <p>In general, this is a reservation of pages in PageMemory for converting page list caches from on-heap to off-heap.
     *
     * <p>Note: When a checkpoint is triggered, we need some amount of page memory to store pages list on-heap cache.
     * If a checkpoint is triggered by "too many dirty pages" reason and pages list cache is rather big, we can get {@code
     * IgniteOutOfMemoryException}. To prevent this, we can limit the total amount of cached page list buckets, assuming that checkpoint
     * will be triggered if no more then 3/4 of pages will be marked as dirty (there will be at least 1/4 of clean pages) and each cached
     * page list bucket can be stored to up to 2 pages (this value is not static, but depends on PagesCache.MAX_SIZE, so if
     * PagesCache.MAX_SIZE > PagesListNodeIo#getCapacity it can take more than 2 pages). Also some amount of page memory is needed to store
     * page list metadata.
     */
    private static final double PAGE_LIST_CACHE_LIMIT_THRESHOLD = 0.1;

    private final PersistentPageMemoryProfileConfiguration cfg;

    private final PageIoRegistry ioRegistry;

    private final int pageSize;

    private final FilePageStoreManager filePageStoreManager;

    private final PartitionMetaManager<StoragePartitionMeta> partitionMetaManager;

    private final CheckpointManager checkpointManager;

    private volatile PersistentPageMemory pageMemory;

    private volatile AtomicLong pageListCacheLimit;

    /**
     * Constructor.
     *
     * @param cfg Data region configuration.
     * @param ioRegistry IO registry.
     * @param filePageStoreManager File page store manager.
     * @param partitionMetaManager Partition meta information manager.
     * @param checkpointManager Checkpoint manager.
     * @param pageSize Page size in bytes.
     */
    public PersistentPageMemoryDataRegion(
            PersistentPageMemoryProfileConfiguration cfg,
            PageIoRegistry ioRegistry,
            FilePageStoreManager filePageStoreManager,
            PartitionMetaManager partitionMetaManager,
            CheckpointManager checkpointManager,
            int pageSize
    ) {
        this.cfg = cfg;
        this.ioRegistry = ioRegistry;
        this.pageSize = pageSize;

        this.filePageStoreManager = filePageStoreManager;
        this.partitionMetaManager = partitionMetaManager;
        this.checkpointManager = checkpointManager;
    }

    /**
     * Starts a persistent data region.
     */
    public void start() {
        PersistentPageMemoryProfileView dataRegionConfigView = (PersistentPageMemoryProfileView) cfg.value();

        PersistentPageMemory pageMemory = new PersistentPageMemory(
                cfg,
                ioRegistry,
                calculateSegmentSizes(dataRegionConfigView.size(), Runtime.getRuntime().availableProcessors()),
                calculateCheckpointBufferSize(dataRegionConfigView.size()),
                filePageStoreManager,
                null,
                (pageMemory0, fullPageId, buf) -> checkpointManager.writePageToDeltaFilePageStore(pageMemory0, fullPageId, buf, true),
                checkpointManager.checkpointTimeoutLock(),
                pageSize
        );

        pageMemory.start();

        pageListCacheLimit = new AtomicLong((long) (pageMemory.totalPages() * PAGE_LIST_CACHE_LIMIT_THRESHOLD));

        this.pageMemory = pageMemory;
    }

    /**
     * Stops a persistent data region.
     */
    public void stop() throws Exception {
        if (pageMemory != null) {
            pageMemory.stop(true);
        }
    }

    /** {@inheritDoc} */
    @Override
    public PersistentPageMemory pageMemory() {
        checkDataRegionStarted();

        return pageMemory;
    }

    /**
     * Returns file page store manager.
     */
    public FilePageStoreManager filePageStoreManager() {
        return filePageStoreManager;
    }

    /**
     * Returns partition meta information manager.
     */
    public PartitionMetaManager<StoragePartitionMeta> partitionMetaManager() {
        return partitionMetaManager;
    }

    /**
     * Returns checkpoint manager.
     */
    public CheckpointManager checkpointManager() {
        return checkpointManager;
    }

    /**
     * Returns page list cache limit.
     */
    public AtomicLong pageListCacheLimit() {
        checkDataRegionStarted();

        return pageListCacheLimit;
    }

    /**
     * Calculates the size of segments in bytes.
     *
     * @param size Data region size.
     * @param concurrencyLevel Number of concurrent segments in Ignite internal page mapping tables, must be greater than 0.
     */
    // TODO: IGNITE-16350 Add more and more detailed description
    static long[] calculateSegmentSizes(long size, int concurrencyLevel) {
        assert concurrencyLevel > 0 : concurrencyLevel;

        long maxSize = size;

        long fragmentSize = Math.max(maxSize / concurrencyLevel, MiB);

        long[] sizes = new long[concurrencyLevel];

        Arrays.fill(sizes, fragmentSize);

        return sizes;
    }

    /**
     * Calculates the size of the checkpoint buffer in bytes.
     *
     * @param size Data region size.
     */
    // TODO: IGNITE-16350 Add more and more detailed description
    static long calculateCheckpointBufferSize(long size) {
        long maxSize = size;

        if (maxSize < GiB) {
            return Math.min(GiB / 4L, maxSize);
        }

        if (maxSize < 8L * GiB) {
            return maxSize / 4L;
        }

        return 2L * GiB;
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
