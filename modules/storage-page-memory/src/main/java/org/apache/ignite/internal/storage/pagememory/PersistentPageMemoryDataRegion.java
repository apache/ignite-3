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

/**
 * Implementation of {@link AbstractPageMemoryDataRegion} for persistent case.
 */
public class PersistentPageMemoryDataRegion extends AbstractPageMemoryDataRegion {
    /**
     * Constructor.
     *
     * @param cfg Data region configuration.
     * @param ioRegistry IO registry.
     * @param pageSize Page size in bytes.
     */
    public PersistentPageMemoryDataRegion(
            PageMemoryDataRegionConfiguration cfg,
            PageIoRegistry ioRegistry,
            int pageSize
    ) {
        super(cfg, ioRegistry, pageSize);
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        PageMemoryDataRegionView dataRegionConfigView = cfg.value();

        assert persistent() : dataRegionConfigView.name();

        // TODO: IGNITE-16641 continue.

        PageMemoryImpl pageMemoryImpl = new PageMemoryImpl(
                cfg,
                ioRegistry,
                calculateSegmentSizes(dataRegionConfigView, Runtime.getRuntime().availableProcessors()),
                calculateCheckpointBufferSize(dataRegionConfigView),
                null,
                null,
                null,
                null,
                pageSize
        );
    }

    /**
     * Calculates the size of segments in bytes.
     *
     * @param dataRegionConfigView Data region configuration.
     * @param concurrencyLevel Number of concurrent segments in Ignite internal page mapping tables, must be greater than 0.
     */
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
