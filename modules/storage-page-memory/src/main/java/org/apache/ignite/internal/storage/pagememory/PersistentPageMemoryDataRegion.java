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

import static org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema.UNSPECIFIED_SIZE;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.ENGINE_NAME;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.THROTTLING_LOG_THRESHOLD_SYSTEM_PROPERTY;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.THROTTLING_MAX_DIRTY_PAGES_SYSTEM_PROPERTY;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.THROTTLING_MIN_DIRTY_PAGES_SYSTEM_PROPERTY;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.THROTTLING_TYPE_SYSTEM_PROPERTY;
import static org.apache.ignite.internal.util.Constants.GiB;
import static org.apache.ignite.internal.util.Constants.MiB;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.ReplacementMode;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteSpeedBasedThrottle;
import org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteThrottlePolicy;
import org.apache.ignite.internal.pagememory.persistence.throttling.TargetRatioPagesWriteThrottle;
import org.apache.ignite.internal.pagememory.persistence.throttling.ThrottlingType;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.storage.pagememory.mv.PersistentPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Implementation of {@link DataRegion} for persistent case.
 */
public class PersistentPageMemoryDataRegion implements DataRegion<PersistentPageMemory> {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(PersistentPageMemoryDataRegion.class);

    /**
     * Threshold to calculate limit for pages list on-heap caches.
     *
     * <p>In general, this is a reservation of pages in PageMemory for converting page list caches from on-heap to off-heap.
     *
     * <p>Note: When a checkpoint is triggered, we need some amount of page memory to store pages list on-heap cache.
     * If a checkpoint is triggered by "too many dirty pages" reason and pages list cache is rather big, we can get {@code
     * IgniteOutOfMemoryException}. To prevent this, we can limit the total amount of cached page list buckets, assuming that checkpoint
     * will be triggered if no more then 3/4 of pages will be marked as dirty (there will be at least 1/4 of clean pages) and each cached
     * page list bucket can be stored to up to 2 pages (this value is not static, but depends on PagesCache.MAX_SIZE, so if
     * PagesCache.MAX_SIZE > PagesListNodeIo#getCapacity it can take more than 2 pages). Also some amount of page memory is needed to store
     * page list metadata.
     */
    private static final double PAGE_LIST_CACHE_LIMIT_THRESHOLD = 0.1;

    private final MetricManager metricManager;

    private final PersistentPageMemoryProfileConfiguration cfg;

    /** Can only be null in tests. Saves us from a bunch of mocking. */
    private final @Nullable SystemLocalConfiguration systemLocalConfig;

    private final PageIoRegistry ioRegistry;

    private final int pageSize;

    private volatile long regionSize;

    private final FilePageStoreManager filePageStoreManager;

    private final PartitionMetaManager partitionMetaManager;

    private final CheckpointManager checkpointManager;

    private volatile PersistentPageMemory pageMemory;

    private volatile AtomicLong pageListCacheLimit;

    private final PersistentPageMemoryMetricSource metricSource;

    private final ConcurrentMap<Integer, PersistentPageMemoryTableStorage> tableStorages = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param metricManager Metric manager.
     * @param cfg Data region configuration.
     * @param systemLocalConfig Local system configuration.
     * @param ioRegistry IO registry.
     * @param filePageStoreManager File page store manager.
     * @param partitionMetaManager Partition meta information manager.
     * @param checkpointManager Checkpoint manager.
     * @param pageSize Page size in bytes.
     */
    public PersistentPageMemoryDataRegion(
            MetricManager metricManager,
            PersistentPageMemoryProfileConfiguration cfg,
            @Nullable SystemLocalConfiguration systemLocalConfig,
            PageIoRegistry ioRegistry,
            FilePageStoreManager filePageStoreManager,
            PartitionMetaManager partitionMetaManager,
            CheckpointManager checkpointManager,
            int pageSize
    ) {
        this.metricManager = metricManager;
        this.cfg = cfg;
        this.systemLocalConfig = systemLocalConfig;
        this.ioRegistry = ioRegistry;
        this.pageSize = pageSize;

        this.filePageStoreManager = filePageStoreManager;
        this.partitionMetaManager = partitionMetaManager;
        this.checkpointManager = checkpointManager;

        metricSource = new PersistentPageMemoryMetricSource("storage." + ENGINE_NAME + "." + cfg.value().name());
    }

    /**
     * Starts a persistent data region.
     */
    public void start() {
        var dataRegionConfigView = (PersistentPageMemoryProfileView) cfg.value();

        long sizeBytes = dataRegionConfigView.sizeBytes();
        if (sizeBytes == UNSPECIFIED_SIZE) {
            sizeBytes = StorageEngine.defaultDataRegionSize();

            LOG.info(
                    "{}.{} property is not specified, setting its value to {}",
                    cfg.name().value(), cfg.sizeBytes().key(), sizeBytes
            );
        }

        this.regionSize = sizeBytes;

        PersistentPageMemory pageMemory = new PersistentPageMemory(
                regionConfiguration(dataRegionConfigView, sizeBytes, pageSize),
                metricSource,
                ioRegistry,
                calculateSegmentSizes(sizeBytes, Runtime.getRuntime().availableProcessors()),
                calculateCheckpointBufferSize(sizeBytes),
                filePageStoreManager,
                this::flushDirtyPageOnReplacement,
                checkpointManager.checkpointTimeoutLock(),
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL),
                checkpointManager.partitionDestructionLockManager()
        );

        initThrottling(pageMemory);

        pageMemory.start();

        initMetrics();

        metricManager.registerSource(metricSource);
        metricManager.enable(metricSource);

        pageListCacheLimit = new AtomicLong((long) (pageMemory.totalPages() * PAGE_LIST_CACHE_LIMIT_THRESHOLD));

        this.pageMemory = pageMemory;
    }

    private static PersistentDataRegionConfiguration regionConfiguration(
            PersistentPageMemoryProfileView cfg,
            long sizeBytes,
            int pageSize
    ) {
        return PersistentDataRegionConfiguration.builder()
                .name(cfg.name())
                .pageSize(pageSize)
                .size(sizeBytes)
                .replacementMode(ReplacementMode.valueOf(cfg.replacementMode()))
                .build();
    }

    // TODO IGNITE-24933 refactor.
    private void initThrottling(PersistentPageMemory pageMemory) {
        ThrottlingType throttlingType = getThrottlingType();

        switch (throttlingType) {
            case DISABLED:
                break;

            case TARGET_RATIO:
                pageMemory.initThrottling(new TargetRatioPagesWriteThrottle(
                        getLoggingThreshold(),
                        pageMemory,
                        checkpointManager::currentCheckpointProgressForThrottling,
                        checkpointManager.checkpointTimeoutLock()::checkpointLockIsHeldByThread,
                        metricSource
                ));
                break;

            case SPEED_BASED:
                pageMemory.initThrottling(new PagesWriteSpeedBasedThrottle(
                        getLoggingThreshold(),
                        getMinDirtyPages(),
                        getMaxDirtyPages(),
                        pageMemory,
                        checkpointManager::currentCheckpointProgressForThrottling,
                        checkpointManager.checkpointTimeoutLock()::checkpointLockIsHeldByThread,
                        metricSource
                ));
                break;

            default:
                assert false : "Impossible throttling type: " + throttlingType;
        }
    }

    private ThrottlingType getThrottlingType() {
        return getSystemConfig(THROTTLING_TYPE_SYSTEM_PROPERTY,
                ThrottlingType.SPEED_BASED,
                value -> ThrottlingType.valueOf(value.toUpperCase()),
                "Valid values are (case-insensitive): " + Arrays.toString(ThrottlingType.values()) + "."
        );
    }

    private long getLoggingThreshold() {
        return TimeUnit.MILLISECONDS.toNanos(getSystemConfig(
                THROTTLING_LOG_THRESHOLD_SYSTEM_PROPERTY,
                TimeUnit.NANOSECONDS.toMillis(PagesWriteThrottlePolicy.DEFAULT_LOGGING_THRESHOLD),
                value -> {
                    long logThresholdMillis = Long.parseLong(value);

                    if (logThresholdMillis <= 0) {
                        throw new IllegalArgumentException();
                    }

                    return logThresholdMillis;
                },
                "Positive integer is expected."
        ));
    }

    private double getMinDirtyPages() {
        return getSystemConfig(
                THROTTLING_MIN_DIRTY_PAGES_SYSTEM_PROPERTY,
                PagesWriteSpeedBasedThrottle.DEFAULT_MIN_RATIO_NO_THROTTLE,
                value -> {
                    double maxDirtyPages = Double.parseDouble(value);

                    if (maxDirtyPages <= 0.01 || maxDirtyPages > 0.75) {
                        throw new IllegalArgumentException();
                    }

                    return maxDirtyPages;
                },
                "Floating point value in a range (0.01, 0.75] is expected."
        );
    }

    private double getMaxDirtyPages() {
        return getSystemConfig(
                THROTTLING_MAX_DIRTY_PAGES_SYSTEM_PROPERTY,
                PagesWriteSpeedBasedThrottle.DEFAULT_MAX_DIRTY_PAGES,
                value -> {
                    double maxDirtyPages = Double.parseDouble(value);

                    if (maxDirtyPages <= 0.5 || maxDirtyPages > 0.99999) {
                        throw new IllegalArgumentException();
                    }

                    return maxDirtyPages;
                },
                "Floating point value in a range (0.5, 0.99999] is expected."
        );
    }

    private <T> T getSystemConfig(String name, T defaultValue, Function<String, T> parseFunction, String extraErrorMessage) {
        SystemPropertyView property = systemLocalConfig == null
                ? null
                : systemLocalConfig.value().properties().get(name);

        T value = defaultValue;

        try {
            if (property != null) {
                value = parseFunction.apply(property.propertyValue());
            }
        } catch (Exception e) {
            LOG.warn(
                    "Invalid throttling configuration {}={}, using default value {}. " + extraErrorMessage,
                    name,
                    property.propertyValue(),
                    defaultValue
            );
        }

        return value;
    }

    /**
     * Stops a persistent data region.
     */
    public void stop() throws Exception {
        if (pageMemory != null) {
            pageMemory.stop(true);

            metricManager.unregisterSource(metricSource);
        }
    }

    private void flushDirtyPageOnReplacement(
            PersistentPageMemory pageMemory, FullPageId fullPageId, ByteBuffer byteBuffer
    ) throws IgniteInternalCheckedException {
        checkpointManager.writePageToFilePageStore(pageMemory, fullPageId, byteBuffer);

        CheckpointProgress checkpointProgress = checkpointManager.currentCheckpointProgress();

        if (checkpointProgress != null) {
            checkpointProgress.evictedPagesCounter().incrementAndGet();
        }
    }

    /** {@inheritDoc} */
    @Override
    public PersistentPageMemory pageMemory() {
        checkDataRegionStarted();

        return pageMemory;
    }

    /**
     * Returns file page store manager.
     */
    public FilePageStoreManager filePageStoreManager() {
        return filePageStoreManager;
    }

    /**
     * Returns partition meta information manager.
     */
    public PartitionMetaManager partitionMetaManager() {
        return partitionMetaManager;
    }

    /**
     * Returns checkpoint manager.
     */
    public CheckpointManager checkpointManager() {
        return checkpointManager;
    }

    /**
     * Returns page list cache limit.
     */
    public AtomicLong pageListCacheLimit() {
        checkDataRegionStarted();

        return pageListCacheLimit;
    }

    /**
     * Calculates the size of segments in bytes.
     *
     * @param size Data region size.
     * @param concurrencyLevel Number of concurrent segments in Ignite internal page mapping tables, must be greater than 0.
     */
    // TODO: IGNITE-16350 Add more and more detailed description
    static long[] calculateSegmentSizes(long size, int concurrencyLevel) {
        assert concurrencyLevel > 0 : concurrencyLevel;

        long maxSize = size;

        long fragmentSize = Math.max(maxSize / concurrencyLevel, MiB);

        long[] sizes = new long[concurrencyLevel];

        Arrays.fill(sizes, fragmentSize);

        return sizes;
    }

    /**
     * Calculates the size of the checkpoint buffer in bytes.
     *
     * @param size Data region size.
     */
    // TODO: IGNITE-16350 Add more and more detailed description
    static long calculateCheckpointBufferSize(long size) {
        long maxSize = size;

        if (maxSize < GiB) {
            return Math.min(GiB / 4L, maxSize);
        }

        if (maxSize < 8L * GiB) {
            return maxSize / 4L;
        }

        return 2L * GiB;
    }

    /**
     * Checks that the data region has started.
     *
     * @throws StorageException If the data region did not start.
     */
    private void checkDataRegionStarted() {
        if (pageMemory == null) {
            throw new StorageException("Data region not started");
        }
    }

    /** Adds a table storage to the data region. */
    @VisibleForTesting
    public void addTableStorage(PersistentPageMemoryTableStorage tableStorage) {
        PersistentPageMemoryTableStorage old = tableStorages.put(tableStorage.getTableId(), tableStorage);

        assert old == null : "Table storage for tableId=" + tableStorage.getTableId() + " already exists";
    }

    void removeTableStorage(PersistentPageMemoryTableStorage tableStorage) {
        tableStorages.remove(tableStorage.getTableId());
    }

    private void initMetrics() {
        metricSource.addMetric(new LongGauge(
                "TotalAllocatedSize",
                "Total size of allocated pages on disk in bytes.",
                this::totalAllocatedPagesSizeOnDiskInBytes
        ));
        metricSource.addMetric(new LongGauge(
                "TotalUsedSize",
                "Total size of non-empty allocated pages on disk in bytes.",
                this::totalNonEmptyAllocatedPagesSizeOnDiskInBytes
        ));
    }

    /**
     * Registers region-specific metrics for the given table.
     *
     * @param tableDescriptor Table descriptor.
     * @param metricSource Metric source for registering metrics.
     */
    void addTableMetrics(StorageTableDescriptor tableDescriptor, StorageEngineTablesMetricSource metricSource) {
        PersistentPageMemoryTableStorage tableStorage = tableStorages.get(tableDescriptor.getId());

        assert tableStorage != null : "Adding metrics for a non-existent table: " + tableDescriptor;

        metricSource.addMetric(new LongGauge(
                "TotalAllocatedSize",
                "Total size of all pages allocated by '" + ENGINE_NAME + "' storage engine for a given table, in bytes.",
                () -> {
                    long totalPages = tableStorage.mvPartitionStorages.stream()
                            .mapToLong(PersistentPageMemoryMvPartitionStorage::pageCount)
                            .sum();

                    return pageSize * totalPages;
                }
        ));
    }

    private long totalAllocatedPagesSizeOnDiskInBytes() {
        long pageCount = 0;

        for (PersistentPageMemoryTableStorage tableStorage : tableStorages.values()) {
            for (PersistentPageMemoryMvPartitionStorage partitionStorage : tableStorage.mvPartitionStorages.getAll()) {
                pageCount += allocatedPageCountOnDisk(tableStorage.getTableId(), partitionStorage.partitionId());
            }
        }

        return pageCount * pageSize;
    }

    private long totalNonEmptyAllocatedPagesSizeOnDiskInBytes() {
        long pageCount = 0;

        for (PersistentPageMemoryTableStorage tableStorage : tableStorages.values()) {
            for (PersistentPageMemoryMvPartitionStorage partitionStorage : tableStorage.mvPartitionStorages.getAll()) {
                pageCount += allocatedPageCountOnDisk(tableStorage.getTableId(), partitionStorage.partitionId());

                pageCount -= partitionStorage.emptyDataPageCountInFreeList();
            }
        }

        return pageCount * pageSize;
    }

    private long allocatedPageCountOnDisk(int tableId, int partitionId) {
        FilePageStore store = filePageStoreManager.getStore(new GroupPartitionId(tableId, partitionId));

        return store == null ? 0 : store.pages();
    }

    @Override
    public long regionSize() {
        return regionSize;
    }
}
