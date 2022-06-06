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

import static org.apache.ignite.internal.util.Constants.GiB;
import static org.apache.ignite.internal.util.Constants.MiB;

import java.util.Arrays;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;

/**
 * Implementation of {@link AbstractPageMemoryDataRegion} for persistent case.
 */
class PersistentPageMemoryDataRegion extends AbstractPageMemoryDataRegion {
    private final FilePageStoreManager filePageStoreManager;

    private final CheckpointManager checkpointManager;

    /**
     * Constructor.
     *
     * @param cfg Data region configuration.
     * @param ioRegistry IO registry.
     * @param filePageStoreManager File page store manager.
     * @param checkpointManager Checkpoint manager.
     * @param pageSize Page size in bytes.
     */
    public PersistentPageMemoryDataRegion(
            PageMemoryDataRegionConfiguration cfg,
            PageIoRegistry ioRegistry,
            FilePageStoreManager filePageStoreManager,
            CheckpointManager checkpointManager,
            int pageSize
    ) {
        super(cfg, ioRegistry, pageSize);

        this.filePageStoreManager = filePageStoreManager;
        this.checkpointManager = checkpointManager;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        PageMemoryDataRegionView dataRegionConfigView = cfg.value();

        assert persistent() : dataRegionConfigView.name();

        PageMemoryImpl pageMemoryImpl = new PageMemoryImpl(
                cfg,
                ioRegistry,
                calculateSegmentSizes(dataRegionConfigView, Runtime.getRuntime().availableProcessors()),
                calculateCheckpointBufferSize(dataRegionConfigView),
                filePageStoreManager,
                null,
                (fullPageId, buf, tag) -> {
                    // Write page to disk.
                    filePageStoreManager.write(fullPageId.groupId(), fullPageId.pageId(), buf, tag, true);
                },
                checkpointManager.checkpointTimeoutLock(),
                pageSize
        );

        pageMemoryImpl.start();

        pageMemory = pageMemoryImpl;
    }

    /**
     * Returns file page store manager.
     */
    public FilePageStoreManager filePageStoreManager() {
        return filePageStoreManager;
    }

    /**
     * Returns checkpoint manager.
     */
    public CheckpointManager checkpointManager() {
        return checkpointManager;
    }

    /**
     * Calculates the size of segments in bytes.
     *
     * @param dataRegionConfigView Data region configuration.
     * @param concurrencyLevel Number of concurrent segments in Ignite internal page mapping tables, must be greater than 0.
     */
    // TODO: IGNITE-16350 Add more and more detailed description
    static long[] calculateSegmentSizes(PageMemoryDataRegionView dataRegionConfigView, int concurrencyLevel) {
        assert concurrencyLevel > 0 : concurrencyLevel;

        long maxSize = dataRegionConfigView.maxSize();

        long fragmentSize = Math.max(maxSize / concurrencyLevel, MiB);

        long[] sizes = new long[concurrencyLevel];

        Arrays.fill(sizes, fragmentSize);

        return sizes;
    }

    /**
     * Calculates the size of the checkpoint buffer in bytes.
     *
     * @param dataRegionConfigView Data region configuration.
     */
    // TODO: IGNITE-16350 Add more and more detailed description
    static long calculateCheckpointBufferSize(PageMemoryDataRegionView dataRegionConfigView) {
        long maxSize = dataRegionConfigView.maxSize();

        if (maxSize < GiB) {
            return Math.min(GiB / 4L, maxSize);
        }

        if (maxSize < 8L * GiB) {
            return maxSize / 4L;
        }

        return 2L * GiB;
    }
}
