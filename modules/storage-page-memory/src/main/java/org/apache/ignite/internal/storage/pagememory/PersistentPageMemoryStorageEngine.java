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

import static org.apache.ignite.internal.storage.pagememory.configuration.schema.BasePageMemoryStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.fileio.AsyncFileIoFactory;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryDataRegionView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Storage engine implementation based on {@link PageMemory} for persistent case.
 */
public class PersistentPageMemoryStorageEngine implements StorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "aipersist";

    private static final IgniteLogger LOG = Loggers.forClass(PersistentPageMemoryStorageEngine.class);

    private final String igniteInstanceName;

    private final PersistentPageMemoryStorageEngineConfiguration engineConfig;

    private final PageIoRegistry ioRegistry;

    private final Path storagePath;

    @Nullable
    private final LongJvmPauseDetector longJvmPauseDetector;

    private final Map<String, PersistentPageMemoryDataRegion> regions = new ConcurrentHashMap<>();

    @Nullable
    private volatile FilePageStoreManager filePageStoreManager;

    @Nullable
    private volatile PartitionMetaManager partitionMetaManager;

    @Nullable
    private volatile CheckpointManager checkpointManager;

    private volatile ExecutorService destructionExecutor;

    private final FailureProcessor failureProcessor;

    /**
     * Constructor.
     *
     * @param igniteInstanceName String igniteInstanceName
     * @param engineConfig PageMemory storage engine configuration.
     * @param ioRegistry IO registry.
     * @param storagePath Storage path.
     * @param failureProcessor Failure processor that is used to handle critical errors.
     * @param longJvmPauseDetector Long JVM pause detector.
     */
    public PersistentPageMemoryStorageEngine(
            String igniteInstanceName,
            PersistentPageMemoryStorageEngineConfiguration engineConfig,
            PageIoRegistry ioRegistry,
            Path storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            FailureProcessor failureProcessor
    ) {
        this.igniteInstanceName = igniteInstanceName;
        this.engineConfig = engineConfig;
        this.ioRegistry = ioRegistry;
        this.storagePath = storagePath;
        this.longJvmPauseDetector = longJvmPauseDetector;
        this.failureProcessor = failureProcessor;
    }

    /**
     * Returns a storage engine configuration.
     */
    public PersistentPageMemoryStorageEngineConfiguration configuration() {
        return engineConfig;
    }

    @Override
    public String name() {
        return ENGINE_NAME;
    }

    @Override
    public void start() throws StorageException {
        int pageSize = engineConfig.pageSize().value();

        try {
            FileIoFactory fileIoFactory = engineConfig.checkpoint().useAsyncFileIoFactory().value()
                    ? new AsyncFileIoFactory()
                    : new RandomAccessFileIoFactory();

            filePageStoreManager = new FilePageStoreManager(
                    igniteInstanceName,
                    storagePath,
                    fileIoFactory,
                    pageSize,
                    failureProcessor
            );

            filePageStoreManager.start();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error starting file page store manager", e);
        }

        partitionMetaManager = new PartitionMetaManager(ioRegistry, pageSize);

        try {
            checkpointManager = new CheckpointManager(
                    igniteInstanceName,
                    null,
                    longJvmPauseDetector,
                    failureProcessor,
                    engineConfig.checkpoint(),
                    filePageStoreManager,
                    partitionMetaManager,
                    regions.values(),
                    ioRegistry,
                    pageSize
            );

            checkpointManager.start();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error starting checkpoint manager", e);
        }

        addDataRegion(DEFAULT_DATA_REGION_NAME);

        // TODO: IGNITE-17066 Add handling deleting/updating data regions configuration
        engineConfig.regions().listenElements(new ConfigurationNamedListListener<>() {
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<PersistentPageMemoryDataRegionView> ctx) {
                addDataRegion(ctx.newName(PersistentPageMemoryDataRegionView.class));

                return nullCompletedFuture();
            }
        });

        destructionExecutor = new ThreadPoolExecutor(
                0,
                Runtime.getRuntime().availableProcessors(),
                100,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(igniteInstanceName, "persistent-mv-partition-destruction", LOG)
        );
    }

    @Override
    public void stop() throws StorageException {
        try {
            Stream<AutoCloseable> closeRegions = regions.values().stream().map(region -> region::stop);

            ExecutorService destructionExecutor = this.destructionExecutor;
            CheckpointManager checkpointManager = this.checkpointManager;
            FilePageStoreManager filePageStoreManager = this.filePageStoreManager;

            Stream<AutoCloseable> resources = Stream.of(
                    destructionExecutor == null
                            ? null
                            : (AutoCloseable) () -> shutdownAndAwaitTermination(destructionExecutor, 30, TimeUnit.SECONDS),
                    checkpointManager == null ? null : (AutoCloseable) checkpointManager::stop,
                    filePageStoreManager == null ? null : (AutoCloseable) filePageStoreManager::stop
            );

            closeAll(Stream.concat(resources, closeRegions));
        } catch (Exception e) {
            throw new StorageException("Error when stopping components", e);
        }
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    @Override
    public PersistentPageMemoryTableStorage createMvTable(
            StorageTableDescriptor tableDescriptor,
            StorageIndexDescriptorSupplier indexDescriptorSupplier
    ) throws StorageException {
        PersistentPageMemoryDataRegion dataRegion = regions.get(tableDescriptor.getDataRegion());

        assert dataRegion != null : "tableId=" + tableDescriptor.getId() + ", dataRegion=" + tableDescriptor.getDataRegion();

        return new PersistentPageMemoryTableStorage(tableDescriptor, indexDescriptorSupplier, this, dataRegion, destructionExecutor);
    }

    /**
     * Returns checkpoint manager, {@code null} if engine not started.
     */
    public @Nullable CheckpointManager checkpointManager() {
        return checkpointManager;
    }

    /**
     * Creates, starts and adds a new data region to the engine.
     *
     * @param name Data region name.
     */
    private void addDataRegion(String name) {
        PersistentPageMemoryDataRegionConfiguration dataRegionConfig = DEFAULT_DATA_REGION_NAME.equals(name)
                ? engineConfig.defaultRegion()
                : engineConfig.regions().get(name);

        int pageSize = engineConfig.pageSize().value();

        PersistentPageMemoryDataRegion dataRegion = new PersistentPageMemoryDataRegion(
                dataRegionConfig,
                ioRegistry,
                filePageStoreManager,
                partitionMetaManager,
                checkpointManager,
                pageSize
        );

        dataRegion.start();

        regions.put(name, dataRegion);
    }
}
