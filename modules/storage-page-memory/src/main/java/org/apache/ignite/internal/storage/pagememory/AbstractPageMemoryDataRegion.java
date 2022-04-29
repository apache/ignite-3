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
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract data region for {@link PageMemoryStorageEngine}. Based on a {@link PageMemory}.
 */
// TODO: IGNITE-16641 Add support for persistent case.
abstract class AbstractPageMemoryDataRegion implements PageMemoryDataRegion, IgniteComponent {
    protected final PageMemoryDataRegionConfiguration cfg;

    protected final PageIoRegistry ioRegistry;

    protected PageMemory pageMemory;

    /**
     * Constructor.
     *
     * @param cfg Data region configuration.
     * @param ioRegistry IO registry.
     */
    public AbstractPageMemoryDataRegion(PageMemoryDataRegionConfiguration cfg, PageIoRegistry ioRegistry) {
        this.cfg = cfg;
        this.ioRegistry = ioRegistry;
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        if (pageMemory != null) {
            pageMemory.stop(true);
        }
    }

    /**
     * Returns {@link true} if the date region is persistent.
     */
    public boolean persistent() {
        return cfg.value().persistent();
    }

    /**
     * Returns page memory, {@code null} if not {@link #start started}.
     */
    public @Nullable PageMemory pageMemory() {
        return pageMemory;
    }
}
