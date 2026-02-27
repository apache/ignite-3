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

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.fileio.MeteredFileIoFactory;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.configuration.CheckpointConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.metrics.CollectionMetricSource;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryIoMetrics;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryCheckpointConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineExtensionConfiguration;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.jetbrains.annotations.Nullable;

/** Storage engine implementation based on {@link PersistentPageMemory}. */
public class PersistentPageMemoryStorageEngine extends AbstractPageMemoryStorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "aipersist";

    /**
     * Maximum "work units" that are allowed to be used during {@link BplusTree} destruction.
     *
     * @see BplusTree#startGradualDestruction
     */
    public static final int MAX_DESTRUCTION_WORK_UNITS = 1_000;

    public static final String THROTTLING_TYPE_SYSTEM_PROPERTY = "aipersistThrottling";

    public static final String THROTTLING_LOG_THRESHOLD_SYSTEM_PROPERTY = "aipersistThrottlingLogThresholdMillis";

    public static final String THROTTLING_MAX_DIRTY_PAGES_SYSTEM_PROPERTY = "aipersistThrottlingMaxDirtyPages";

    public static final String THROTTLING_MIN_DIRTY_PAGES_SYSTEM_PROPERTY = "aipersistThrottlingMinDirtyPages";

    private static final IgniteLogger LOG = Loggers.forClass(PersistentPageMemoryStorageEngine.class);

    private final String igniteInstanceName;

    private final MetricManager metricManager;

    private final PersistentPageMemoryStorageEngineConfiguration engineConfig;

    private CollectionMetricSource ioMetricSource;

    private CollectionMetricSource checkpointMetricSource;

    private PersistentPageMemoryStorageMetricSource storageMetricSource;

    private final StorageConfiguration storageConfig;

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

    private final FailureManager failureManager;

    private final LogSyncer logSyncer;

    /** For unspecified tasks, i.e. throttling log. */
    private final ExecutorService commonExecutorService;

    /**
     * Constructor.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param storageConfig Storage engine and storage profiles configurations.
     * @param systemLocalConfig Local system configuration.
     * @param ioRegistry IO registry.
     * @param storagePath Storage path.
     * @param longJvmPauseDetector Long JVM pause detector.
     * @param failureManager Failure processor that is used to handle critical errors.
     * @param logSyncer Write-ahead log synchronizer.
     * @param commonExecutorService Executor service.
     * @param clock Hybrid Logical Clock.
     */
    public PersistentPageMemoryStorageEngine(
            String igniteInstanceName,
            MetricManager metricManager,
            StorageConfiguration storageConfig,
            SystemLocalConfiguration systemLocalConfig,
            PageIoRegistry ioRegistry,
            Path storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            FailureManager failureManager,
            LogSyncer logSyncer,
            ExecutorService commonExecutorService,
            HybridClock clock
    ) {
        super(systemLocalConfig, clock);

        this.igniteInstanceName = igniteInstanceName;
        this.metricManager = metricManager;
        this.storageConfig = storageConfig;
        this.engineConfig = ((PersistentPageMemoryStorageEngineExtensionConfiguration) storageConfig.engines()).aipersist();
        this.ioRegistry = ioRegistry;
        this.storagePath = storagePath;
        this.longJvmPauseDetector = longJvmPauseDetector;
        this.failureManager = failureManager;
        this.logSyncer = logSyncer;
        this.commonExecutorService = commonExecutorService;
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
        super.start();

        int pageSize = engineConfig.pageSizeBytes().value();

        ioMetricSource = new CollectionMetricSource("storage." + ENGINE_NAME + ".io", "storage", "Page memory I/O metrics");
        PageMemoryIoMetrics ioMetrics = new PageMemoryIoMetrics(ioMetricSource);

        try {
            var fileIoFactory = new MeteredFileIoFactory(new RandomAccessFileIoFactory(), ioMetrics);

            filePageStoreManager = createFilePageStoreManager(igniteInstanceName, storagePath, fileIoFactory, pageSize, failureManager);

            filePageStoreManager.start();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error starting file page store manager", e);
        }

        partitionMetaManager = new PartitionMetaManager(ioRegistry, pageSize, StoragePartitionMeta.FACTORY);

        checkpointMetricSource = new CollectionMetricSource("storage." + ENGINE_NAME + ".checkpoint", "storage", null);

        try {
            checkpointManager = new CheckpointManager(
                    igniteInstanceName,
                    longJvmPauseDetector,
                    failureManager,
                    checkpointConfiguration(engineConfig.checkpoint()),
                    filePageStoreManager,
                    partitionMetaManager,
                    regions.values(),
                    ioRegistry,
                    logSyncer,
                    commonExecutorService,
                    checkpointMetricSource,
                    pageSize
            );

            checkpointManager.start();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error starting checkpoint manager", e);
        }

        // TODO: IGNITE-17066 Add handling deleting/updating data regions configuration
        for (StorageProfileView storageProfileView : storageConfig.profiles().value()) {
            if (storageProfileView instanceof PersistentPageMemoryProfileView) {
                String profileName = storageProfileView.name();

                var storageProfileConfiguration = (PersistentPageMemoryProfileConfiguration) storageConfig.profiles().get(profileName);

                assert storageProfileConfiguration != null : profileName;

                addDataRegion(storageProfileConfiguration);
            }
        }

        // TODO: remove this executor, see https://issues.apache.org/jira/browse/IGNITE-21683
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                100,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(igniteInstanceName, "persistent-mv-partition-destruction", LOG)
        );
        executor.allowCoreThreadTimeOut(true);

        destructionExecutor = executor;

        storageMetricSource = new PersistentPageMemoryStorageMetricSource("storage." + ENGINE_NAME);

        PersistentPageMemoryStorageMetrics.initMetrics(storageMetricSource, filePageStoreManager);

        metricManager.registerSource(checkpointMetricSource);
        metricManager.registerSource(storageMetricSource);
        metricManager.registerSource(ioMetricSource);

        metricManager.enable(checkpointMetricSource);
        metricManager.enable(storageMetricSource);
        metricManager.enable(ioMetricSource);
    }

    /** Creates a checkpoint configuration based on the provided {@link PageMemoryCheckpointConfiguration}. */
    public static CheckpointConfiguration checkpointConfiguration(PageMemoryCheckpointConfiguration checkpointCfg) {
        return CheckpointConfiguration.builder()
                .checkpointThreads(checkpointCfg.value().checkpointThreads())
                .compactionThreads(checkpointCfg.value().compactionThreads())
                .intervalMillis(checkpointCfg.intervalMillis()::value)
                .intervalDeviationPercent(checkpointCfg.intervalDeviationPercent()::value)
                .readLockTimeoutMillis(checkpointCfg.readLockTimeoutMillis()::value)
                .logReadLockThresholdTimeoutMillis(checkpointCfg.logReadLockThresholdTimeoutMillis()::value)
                .build();
    }

    @Override
    public void stop() throws StorageException {
        try {
            // Disable and unregister metric sources to prevent leaks
            if (ioMetricSource != null) {
                metricManager.unregisterSource(ioMetricSource);
            }
            if (checkpointMetricSource != null) {
                metricManager.unregisterSource(checkpointMetricSource);
            }
            if (storageMetricSource != null) {
                metricManager.unregisterSource(storageMetricSource);
            }

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

        var tableStorage = new PersistentPageMemoryTableStorage(
                tableDescriptor,
                indexDescriptorSupplier,
                this,
                dataRegion,
                destructionExecutor,
                failureManager
        );

        dataRegion.addTableStorage(tableStorage);

        return tableStorage;
    }

    @Override
    public void destroyMvTable(int tableId) {
        FilePageStoreManager filePageStoreManager = this.filePageStoreManager;

        assert filePageStoreManager != null : "Component has not started";

        try {
            filePageStoreManager.destroyGroupIfExists(tableId);
        } catch (IOException e) {
            throw new StorageException("Failed to destroy table directory: {}", e, tableId);
        }
    }

    @Override
    public void addTableMetrics(StorageTableDescriptor tableDescriptor, StorageEngineTablesMetricSource metricSource) {
        PersistentPageMemoryDataRegion region = regions.get(tableDescriptor.getStorageProfile());

        assert region != null : "Adding metrics to the table with non-existent data region. [tableDescriptor=" + tableDescriptor + ']';

        region.addTableMetrics(tableDescriptor, metricSource);
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
     * @param failureManager Failure processor that is used to handle critical errors.
     * @return Partition file page store manager.
     */
    protected FilePageStoreManager createFilePageStoreManager(String igniteInstanceName, Path storagePath, FileIoFactory fileIoFactory,
            int pageSize, FailureManager failureManager) throws IgniteInternalCheckedException {
        return new FilePageStoreManager(
                igniteInstanceName,
                storagePath,
                fileIoFactory,
                pageSize,
                failureManager
        );
    }

    /**
     * Creates, starts and adds a new data region to the engine.
     */
    private void addDataRegion(PersistentPageMemoryProfileConfiguration storageProfileConfiguration) {
        int pageSize = engineConfig.pageSizeBytes().value();

        PersistentPageMemoryDataRegion dataRegion = new PersistentPageMemoryDataRegion(
                metricManager,
                storageProfileConfiguration,
                systemLocalConfig,
                ioRegistry,
                filePageStoreManager,
                partitionMetaManager,
                checkpointManager,
                pageSize
        );

        dataRegion.start();

        regions.put(storageProfileConfiguration.name().value(), dataRegion);
    }

    @Override
    public long requiredOffHeapMemorySize() {
        return regions.values().stream()
                .mapToLong(PersistentPageMemoryDataRegion::regionSize)
                .sum();
    }

    @Override
    public Set<Integer> tableIdsOnDisk() {
        return requireNonNull(filePageStoreManager, "Not started").allGroupIdsOnFs();
    }
}
