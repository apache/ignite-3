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

import static org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryDataStorageView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Storage engine implementation based on {@link PageMemory}.
 */
public class PageMemoryStorageEngine implements StorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "pagememory";

    private final PageMemoryStorageEngineConfiguration engineConfig;

    private final PageIoRegistry ioRegistry;

    private final Map<String, VolatilePageMemoryDataRegion> regions = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param engineConfig PageMemory storage engine configuration.
     * @param ioRegistry IO registry.
     */
    public PageMemoryStorageEngine(
            PageMemoryStorageEngineConfiguration engineConfig,
            PageIoRegistry ioRegistry
    ) {
        this.engineConfig = engineConfig;
        this.ioRegistry = ioRegistry;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        VolatilePageMemoryDataRegion defaultRegion = new VolatilePageMemoryDataRegion(engineConfig.defaultRegion(), ioRegistry);

        defaultRegion.start();

        regions.put(DEFAULT_DATA_REGION_NAME, defaultRegion);

        for (String regionName : engineConfig.regions().value().namedListKeys()) {
            VolatilePageMemoryDataRegion region = new VolatilePageMemoryDataRegion(engineConfig.regions().get(regionName), ioRegistry);

            region.start();

            regions.put(regionName, region);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        try {
            IgniteUtils.closeAll(regions.values().stream().map(region -> region::stop));
        } catch (Exception e) {
            throw new StorageException("Error when stopping regions", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public TableStorage createTable(TableConfiguration tableCfg) {
        TableView tableView = tableCfg.value();

        assert tableView.dataStorage().name().equals(ENGINE_NAME) : tableView.dataStorage().name();

        PageMemoryDataStorageView dataStorageView = (PageMemoryDataStorageView) tableView.dataStorage();

        VolatilePageMemoryDataRegion dataRegion = regions.get(dataStorageView.dataRegion());

        return new VolatilePageMemoryTableStorage(tableCfg, dataRegion);
    }
}
