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
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryDataRegionView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;

/**
 * Storage engine implementation based on {@link PageMemory} for in-memory case.
 */
public class VolatilePageMemoryStorageEngine implements StorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "aimem";

    private final VolatilePageMemoryStorageEngineConfiguration engineConfig;

    private final PageIoRegistry ioRegistry;

    private final Map<String, VolatilePageMemoryDataRegion> regions = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param engineConfig PageMemory storage engine configuration.
     * @param ioRegistry IO registry.
     */
    public VolatilePageMemoryStorageEngine(
            VolatilePageMemoryStorageEngineConfiguration engineConfig,
            PageIoRegistry ioRegistry
    ) {
        this.engineConfig = engineConfig;
        this.ioRegistry = ioRegistry;
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        addDataRegion(engineConfig.defaultRegion());

        // TODO: IGNITE-17066 Add handling deleting/updating data regions configuration
        engineConfig.regions().listenElements(new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<VolatilePageMemoryDataRegionView> ctx) {
                addDataRegion(ctx.config(VolatilePageMemoryDataRegionConfiguration.class));

                return completedFuture(null);
            }
        });
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        try {
            closeAll(regions.values().stream().map(region -> region::stop));
        } catch (Exception e) {
            throw new StorageException("Error when stopping components", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public VolatilePageMemoryTableStorage createMvTable(TableConfiguration tableCfg) throws StorageException {
        TableView tableView = tableCfg.value();

        assert tableView.dataStorage().name().equals(ENGINE_NAME) : tableView.dataStorage().name();

        VolatilePageMemoryDataStorageView dataStorageView = (VolatilePageMemoryDataStorageView) tableView.dataStorage();

        return new VolatilePageMemoryTableStorage(tableCfg, regions.get(dataStorageView.dataRegion()));
    }

    /**
     * Creates, starts and adds a new data region to the engine.
     *
     * @param dataRegionConfig Data region configuration.
     */
    private void addDataRegion(VolatilePageMemoryDataRegionConfiguration dataRegionConfig) {
        int pageSize = engineConfig.pageSize().value();

        String name = dataRegionConfig.name().value();

        VolatilePageMemoryDataRegion dataRegion = new VolatilePageMemoryDataRegion(dataRegionConfig, ioRegistry, pageSize);

        dataRegion.start();

        regions.put(name, dataRegion);
    }
}
