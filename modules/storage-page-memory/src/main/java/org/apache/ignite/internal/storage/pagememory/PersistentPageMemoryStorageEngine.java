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

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.fileio.AsyncFileIoFactory;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
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

    /**
     * Maximum "work units" that are allowed to be used during {@link BplusTree} destruction.
     *
     * @see BplusTree#startGradualDestruction
     */
    public static final int MAX_DESTRUCTION_WORK_UNITS = 1_000;

    private static final IgniteLogger LOG = Loggers.forClass(PersistentPageMemoryStorageEngine.class);

    private final String igniteInstanceName;

    private final PersistentPageMemoryStorageEngineConfiguration engineConfig;

    private final StorageConfiguration storageConfiguration;

    private final PageIoRegistry ioRegistry;

    private final Path storagePath;

    @Nullable
    private final LongJvmPauseDetector longJvmPauseDetector;

    private final Map<String, PersistentPageMemoryDataRegion> regions = new ConcurrentHashMap<>();

    @Nullable
    private volatile FilePageStoreManager filePageStoreManager;

    @Nullable
    private volatile PartitionMetaManager<StoragePartitionMeta, StoragePartitionMetaIo> partitionMetaManager;

    @Nullable
    private volatile CheckpointManager checkpointManager;

    private volatile ExecutorService destructionExecutor;

    private final FailureProcessor failureProcessor;

    private final LogSyncer logSyncer;

    /**
     * Constructor.
     *
     * @param igniteInstanceName String igniteInstanceName
     * @param engineConfig PageMemory storage engine configuration.
     * @param ioRegistry IO registry.
     * @param storagePath Storage path.
     * @param failureProcessor Failure processor that is used to handle critical errors.
     * @param longJvmPauseDetector Long JVM pause detector.
     * @param logSyncer Write-ahead log synchronizer.
     */
    public PersistentPageMemoryStorageEngine(
            String igniteInstanceName,
            PersistentPageMemoryStorageEngineConfiguration engineConfig,
            StorageConfiguration storageConfiguration,
            PageIoRegistry ioRegistry,
            Path storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            FailureProcessor failureProcessor,
            LogSyncer logSyncer
    ) {
        this.igniteInstanceName = igniteInstanceName;
        this.engineConfig = engineConfig;
        this.storageConfiguration = storageConfiguration;
        this.ioRegistry = ioRegistry;
        this.storagePath = storagePath;
        this.longJvmPauseDetector = longJvmPauseDetector;
        this.failureProcessor = failureProcessor;
        this.logSyncer = logSyncer;
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

            filePageStoreManager = createFilePageStoreManager(igniteInstanceName, storagePath, fileIoFactory, pageSize, failureProcessor);

            filePageStoreManager.start();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error starting file page store manager", e);
        }

        partitionMetaManager = new PartitionMetaManager<>(ioRegistry, pageSize,
                StoragePartitionMeta.FACTORY, StoragePartitionMetaIo.VERSIONS);

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
                    logSyncer,
                    pageSize
            );

            checkpointManager.start();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error starting checkpoint manager", e);
        }

        // TODO: IGNITE-17066 Add handling deleting/updating data regions configuration
        storageConfiguration.profiles().value().stream().forEach(p -> {
            if (p instanceof PersistentPageMemoryProfileView) {
                addDataRegion(p.name());
            }
        });

        // TODO: remove this executor, see https://issues.apache.org/jira/browse/IGNITE-21683
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
    public MvTableStorage createMvTable(
            StorageTableDescriptor tableDescriptor,
            StorageIndexDescriptorSupplier indexDescriptorSupplier
    ) throws StorageException {
        PersistentPageMemoryDataRegion dataRegion = regions.get(tableDescriptor.getStorageProfile());

        assert dataRegion != null : "tableId=" + tableDescriptor.getId() + ", dataRegion=" + tableDescriptor.getStorageProfile();

        return new PersistentPageMemoryTableStorage(tableDescriptor, indexDescriptorSupplier, this, dataRegion, destructionExecutor);
    }

    @Override
    public void dropMvTable(int tableId) {
        FilePageStoreManager filePageStoreManager = this.filePageStoreManager;

        assert filePageStoreManager != null : "Component has not started";

        try {
            filePageStoreManager.destroyGroupIfExists(tableId);
        } catch (IOException e) {
            throw new StorageException("Failed to destroy table directory: {}", e, tableId);
        }
    }

    /**
     * Returns checkpoint manager, {@code null} if engine not started.
     */
    public @Nullable CheckpointManager checkpointManager() {
        return checkpointManager;
    }

    /**
     * Creates partition file page store manager.
     *
     * @param igniteInstanceName String igniteInstanceName
     * @param storagePath Storage path.
     * @param fileIoFactory File IO factory.
     * @param pageSize Page size in bytes.
     * @param failureProcessor Failure processor that is used to handle critical errors.
     * @return Partition file page store manager.
     */
    protected FilePageStoreManager createFilePageStoreManager(String igniteInstanceName, Path storagePath, FileIoFactory fileIoFactory,
            int pageSize, FailureProcessor failureProcessor) throws IgniteInternalCheckedException {
        return new FilePageStoreManager(
                igniteInstanceName,
                storagePath,
                fileIoFactory,
                pageSize,
                failureProcessor
        );
    }

    /**
     * Creates, starts and adds a new data region to the engine.
     *
     * @param name Data region name.
     */
    private void addDataRegion(String name) {
        PersistentPageMemoryProfileConfiguration storageProfileConfiguration =
                (PersistentPageMemoryProfileConfiguration) storageConfiguration.profiles().get(name);

        int pageSize = engineConfig.pageSize().value();

        PersistentPageMemoryDataRegion dataRegion = new PersistentPageMemoryDataRegion(
                storageProfileConfiguration,
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
