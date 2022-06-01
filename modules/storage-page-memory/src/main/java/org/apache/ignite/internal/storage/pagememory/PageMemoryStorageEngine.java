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

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
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
        addDataRegion(engineConfig.defaultRegion());

        // TODO: IGNITE-17066 Add handling deleting/updating data regions configuration
        engineConfig.regions().listenElements(new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<PageMemoryDataRegionView> ctx) {
                addDataRegion(ctx.config(PageMemoryDataRegionConfiguration.class));

                return completedFuture(null);
            }
        });
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
    public PageMemoryTableStorage createTable(TableConfiguration tableCfg) {
        TableView tableView = tableCfg.value();

        assert tableView.dataStorage().name().equals(ENGINE_NAME) : tableView.dataStorage().name();

        PageMemoryDataStorageView dataStorageView = (PageMemoryDataStorageView) tableView.dataStorage();

        VolatilePageMemoryDataRegion dataRegion = regions.get(dataStorageView.dataRegion());

        return new VolatilePageMemoryTableStorage(tableCfg, dataRegion);
    }

    /**
     * Creates, starts and adds a new data region to the engine.
     *
     * @param dataRegionConfig Data region configuration.
     */
    private void addDataRegion(PageMemoryDataRegionConfiguration dataRegionConfig) {
        int pageSize = engineConfig.pageSize().value();

        String name = dataRegionConfig.name().value();

        VolatilePageMemoryDataRegion dataRegion = new VolatilePageMemoryDataRegion(dataRegionConfig, ioRegistry, pageSize);

        dataRegion.start();

        regions.put(name, dataRegion);
    }
}
