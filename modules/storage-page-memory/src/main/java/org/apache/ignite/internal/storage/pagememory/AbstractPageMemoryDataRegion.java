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

import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.configuration.schema.BasePageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.StorageException;

/**
 * Abstract data region for {@link PageMemoryStorageEngine}. Based on a {@link PageMemory}.
 */
abstract class AbstractPageMemoryDataRegion implements PageMemoryDataRegion, IgniteComponent {
    protected final BasePageMemoryDataRegionConfiguration<?, ?> cfg;

    protected final PageIoRegistry ioRegistry;

    protected final int pageSize;

    protected volatile PageMemory pageMemory;

    /**
     * Constructor.
     *
     * @param cfg Data region configuration.
     * @param ioRegistry IO registry.
     * @param pageSize Page size in bytes.
     */
    public AbstractPageMemoryDataRegion(BasePageMemoryDataRegionConfiguration<?, ?> cfg, PageIoRegistry ioRegistry, int pageSize) {
        this.cfg = cfg;
        this.ioRegistry = ioRegistry;
        this.pageSize = pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public PageMemory pageMemory() {
        checkDataRegionStarted();

        return pageMemory;
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (pageMemory != null) {
            pageMemory.stop(true);
        }
    }

    /**
     * Checks that the data region has started.
     *
     * @throws StorageException If the data region did not start.
     */
    protected void checkDataRegionStarted() {
        if (pageMemory == null) {
            throw new StorageException("Data region not started");
        }
    }
}
