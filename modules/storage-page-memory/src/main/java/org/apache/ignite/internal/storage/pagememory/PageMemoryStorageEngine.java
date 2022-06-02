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

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.fileio.AsyncFileIoFactory;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryDataRegionView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryDataStorageView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryStorageEngineConfiguration;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Storage engine implementation based on {@link PageMemory}.
 */
public class PageMemoryStorageEngine implements StorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "pagememory";

    private final String igniteInstanceName;

    private final PageMemoryStorageEngineConfiguration engineConfig;

    private final PageIoRegistry ioRegistry;

    private final Path storagePath;

    @Nullable
    private final LongJvmPauseDetector longJvmPauseDetector;

    private final Map<String, AbstractPageMemoryDataRegion> regions = new ConcurrentHashMap<>();

    @Nullable
    private volatile FilePageStoreManager filePageStoreManager;

    @Nullable
    private volatile CheckpointManager checkpointManager;

    /**
     * Constructor.
     *
     * @param igniteInstanceName String igniteInstanceName
     * @param engineConfig PageMemory storage engine configuration.
     * @param ioRegistry IO registry.
     * @param storagePath Storage path.
     * @param longJvmPauseDetector Long JVM pause detector.
     */
    public PageMemoryStorageEngine(
            String igniteInstanceName,
            PageMemoryStorageEngineConfiguration engineConfig,
            PageIoRegistry ioRegistry,
            Path storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector
    ) {
        this.igniteInstanceName = igniteInstanceName;
        this.engineConfig = engineConfig;
        this.ioRegistry = ioRegistry;
        this.storagePath = storagePath;
        this.longJvmPauseDetector = longJvmPauseDetector;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        int pageSize = engineConfig.pageSize().value();

        try {
            FileIoFactory fileIoFactory = engineConfig.checkpoint().useAsyncFileIoFactory().value()
                    ? new AsyncFileIoFactory()
                    : new RandomAccessFileIoFactory();

            filePageStoreManager = new FilePageStoreManager(
                    IgniteLogger.forClass(FilePageStoreManager.class),
                    igniteInstanceName,
                    storagePath,
                    fileIoFactory,
                    pageSize
            );

            filePageStoreManager.start();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error starting file page store manager", e);
        }

        try {
            checkpointManager = new CheckpointManager(
                    igniteInstanceName,
                    null,
                    longJvmPauseDetector,
                    engineConfig.checkpoint(),
                    filePageStoreManager,
                    regions.values(),
                    storagePath,
                    pageSize
            );

            checkpointManager.start();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error starting checkpoint manager", e);
        }

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
            Stream<AutoCloseable> closeRegions = regions.values().stream().map(region -> region::stop);

            Stream<AutoCloseable> closeManagers = Stream.of(
                    checkpointManager == null ? null : (AutoCloseable) checkpointManager::stop,
                    filePageStoreManager == null ? null : (AutoCloseable) filePageStoreManager::stop
            );

            closeAll(Stream.concat(closeRegions, closeManagers));
        } catch (Exception e) {
            throw new StorageException("Error when stopping components", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public PageMemoryTableStorage createTable(TableConfiguration tableCfg) {
        TableView tableView = tableCfg.value();

        assert tableView.dataStorage().name().equals(ENGINE_NAME) : tableView.dataStorage().name();

        PageMemoryDataStorageView dataStorageView = (PageMemoryDataStorageView) tableView.dataStorage();

        PageMemoryDataRegion dataRegion = regions.get(dataStorageView.dataRegion());

        if (dataRegion.persistent()) {
            throw new UnsupportedOperationException("Persistent data region not supported yet");
        }

        return new VolatilePageMemoryTableStorage(tableCfg, (VolatilePageMemoryDataRegion) dataRegion);
    }

    /**
     * Creates, starts and adds a new data region to the engine.
     *
     * @param dataRegionConfig Data region configuration.
     */
    private void addDataRegion(PageMemoryDataRegionConfiguration dataRegionConfig) {
        int pageSize = engineConfig.pageSize().value();

        String name = dataRegionConfig.name().value();

        AbstractPageMemoryDataRegion dataRegion;

        if (dataRegionConfig.persistent().value()) {
            dataRegion = new PersistentPageMemoryDataRegion(
                    dataRegionConfig,
                    ioRegistry,
                    filePageStoreManager,
                    checkpointManager,
                    pageSize
            );
        } else {
            dataRegion = new VolatilePageMemoryDataRegion(dataRegionConfig, ioRegistry, pageSize);
        }

        dataRegion.start();

        regions.put(name, dataRegion);
    }
}
