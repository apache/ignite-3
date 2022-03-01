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

import java.nio.file.Path;
import org.apache.ignite.configuration.schemas.store.DataRegionConfiguration;
import org.apache.ignite.configuration.schemas.store.PageMemoryDataRegionConfiguration;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;

/**
 * Storage engine implementation based on {@link PageMemory}.
 */
public class PageMemoryStorageEngine implements StorageEngine {
    private final PageIoRegistry ioRegistry;

    /**
     * Default constructor.
     */
    public PageMemoryStorageEngine() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {

    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {

    }

    /** {@inheritDoc} */
    @Override
    public DataRegion createDataRegion(DataRegionConfiguration regionCfg) {
        return new PageMemoryDataRegion((PageMemoryDataRegionConfiguration) regionCfg, ioRegistry);
    }

    /** {@inheritDoc} */
    @Override
    public TableStorage createTable(Path tablePath, TableConfiguration tableCfg, DataRegion dataRegion) {
        return null;
    }
}
