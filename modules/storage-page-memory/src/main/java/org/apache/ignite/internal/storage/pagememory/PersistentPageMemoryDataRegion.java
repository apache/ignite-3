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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.storage.configurations.StorageProfileConfigurationSchema.UNSPECIFIED_SIZE;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.ENGINE_NAME;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.THROTTLING_LOG_THRESHOLD_SYSTEM_PROPERTY;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.THROTTLING_MAX_DIRTY_PAGES_SYSTEM_PROPERTY;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.THROTTLING_MIN_DIRTY_PAGES_SYSTEM_PROPERTY;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.THROTTLING_TYPE_SYSTEM_PROPERTY;
import static org.apache.ignite.internal.util.Constants.GiB;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.apache.ignite.internal.util.OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.DoubleGauge;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.ReplacementMode;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.PageWriteTarget;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteSpeedBasedThrottle;
import org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteThrottlePolicy;
import org.apache.ignite.internal.pagememory.persistence.throttling.TargetRatioPagesWriteThrottle;
import org.apache.ignite.internal.pagememory.persistence.throttling.ThrottlingPolicyFactory;
import org.apache.ignite.internal.pagememory.persistence.throttling.ThrottlingType;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.metrics.StorageEngineTablesMetricSource;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.storage.pagememory.mv.PersistentPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
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

    private final SystemLocalConfiguration systemLocalConfig;

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
            SystemLocalConfiguration systemLocalConfig,
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

        long checkpointReadLockTimeout = checkpointManager.checkpointTimeoutLock().checkpointReadLockTimeout();
        OffheapReadWriteLock offheapReadWriteLock = checkpointReadLockTimeout == 0L
                ? new OffheapReadWriteLock(DEFAULT_CONCURRENCY_LEVEL)
                : new OffheapReadWriteLock(DEFAULT_CONCURRENCY_LEVEL, checkpointReadLockTimeout, MILLISECONDS);

        PersistentPageMemory pageMemory = new PersistentPageMemory(
                regionConfiguration(dataRegionConfigView, sizeBytes, pageSize),
                metricSource,
                ioRegistry,
                calculateSegmentSizes(sizeBytes, Runtime.getRuntime().availableProcessors()),
                calculateCheckpointBufferSize(sizeBytes),
                filePageStoreManager,
                this::flushDirtyPageOnReplacement,
                checkpointManager.checkpointTimeoutLock(),
                offheapReadWriteLock,
                checkpointManager.partitionDestructionLockManager()
        );

        initMetrics();

        metricManager.registerSource(metricSource);
        metricManager.enable(metricSource);

        pageListCacheLimit = new AtomicLong((long) (pageMemory.totalPages() * PAGE_LIST_CACHE_LIMIT_THRESHOLD));

        this.pageMemory = pageMemory;
    }

    private PersistentDataRegionConfiguration regionConfiguration(
            PersistentPageMemoryProfileView cfg,
            long sizeBytes,
            int pageSize
    ) {
        return PersistentDataRegionConfiguration.builder()
                .name(cfg.name())
                .pageSize(pageSize)
                .size(sizeBytes)
                .replacementMode(ReplacementMode.valueOf(cfg.replacementMode()))
                .throttlingPolicyFactory(throttlingPolicyFactory())
                .build();
    }

    private ThrottlingPolicyFactory throttlingPolicyFactory() {
        ThrottlingType throttlingType = getThrottlingType();

        switch (throttlingType) {
            case DISABLED:
                return pageMemory -> null;

            case TARGET_RATIO:
                return pageMemory -> new TargetRatioPagesWriteThrottle(
                        getLoggingThreshold(),
                        pageMemory,
                        checkpointManager::currentCheckpointProgressForThrottling,
                        checkpointManager.checkpointTimeoutLock()::checkpointLockIsHeldByThread,
                        metricSource
                );

            case SPEED_BASED:
                return pageMemory -> new PagesWriteSpeedBasedThrottle(
                        getLoggingThreshold(),
                        getMinDirtyPages(),
                        getMaxDirtyPages(),
                        pageMemory,
                        checkpointManager::currentCheckpointProgressForThrottling,
                        checkpointManager.checkpointTimeoutLock()::checkpointLockIsHeldByThread,
                        metricSource
                );

            default:
                throw new IllegalArgumentException("Impossible throttling type: " + throttlingType);
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
        return MILLISECONDS.toNanos(getSystemConfig(
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

    private PageWriteTarget flushDirtyPageOnReplacement(
            PersistentPageMemory pageMemory, FullPageId fullPageId, ByteBuffer byteBuffer
    ) throws IgniteInternalCheckedException {
        PageWriteTarget target = checkpointManager.writePageToFilePageStore(pageMemory, fullPageId, byteBuffer);

        CheckpointProgress checkpointProgress = checkpointManager.currentCheckpointProgress();

        if (checkpointProgress != null) {
            checkpointProgress.evictedPagesCounter().incrementAndGet();
        }

        return target;
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
                String.format("Total size of all pages allocated by \"%s\" storage engine, in bytes.", ENGINE_NAME),
                () -> totalAllocatedPagesCount() * pageSize
        ));

        metricSource.addMetric(new LongGauge(
                "TotalUsedSize",
                String.format("Total size of all non-empty pages allocated by \"%s\" storage engine, in bytes.", ENGINE_NAME),
                () -> totalNonEmptyAllocatedPagesCount() * pageSize
        ));

        metricSource.addMetric(new LongGauge(
                "TotalEmptySize",
                String.format("Total size of all empty pages allocated by \"%s\" storage engine, in bytes.", ENGINE_NAME),
                () -> emptyPagesCount() * pageSize
        ));

        metricSource.addMetric(new LongGauge(
                "TotalDataSize",
                String.format("Total space occupied by data contained in pages allocated by \"%s\" storage engine, in bytes.", ENGINE_NAME),
                this::nonEmptySpaceBytes
        ));

        metricSource.addMetric(new DoubleGauge(
                "PagesFillFactor",
                "Ratio of number of bytes occupied by data to the total number of bytes occupied by pages that contain this data.",
                this::pagesFillFactor
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
                String.format("Total size of all pages allocated by \"%s\" storage engine for a given table, in bytes.", ENGINE_NAME),
                () -> tableAllocatedPagesCount(tableStorage) * pageSize
        ));
    }

    private long totalAllocatedPagesCount() {
        return tableStorages.values().stream()
                .mapToLong(PersistentPageMemoryDataRegion::tableAllocatedPagesCount)
                .sum();
    }

    private static long tableAllocatedPagesCount(PersistentPageMemoryTableStorage tableStorage) {
        return tableStorage.mvPartitionStorages.getAll().stream()
                .mapToLong(PersistentPageMemoryMvPartitionStorage::pageCount)
                .sum();
    }

    private long totalNonEmptyAllocatedPagesCount() {
        return allPartitions()
                .mapToLong(partitionStorage -> partitionStorage.pageCount() - partitionStorage.emptyDataPageCountInFreeList())
                .sum();
    }

    private long emptyPagesCount() {
        return allPartitions()
                .mapToLong(PersistentPageMemoryMvPartitionStorage::emptyDataPageCountInFreeList)
                .sum();
    }

    private long nonEmptySpaceBytes() {
        return allPartitions()
                .mapToLong(partitionStorage -> {
                    int pagesCount = partitionStorage.pageCount();

                    int emptyPagesCount = partitionStorage.emptyDataPageCountInFreeList();

                    long pagesWithData = (long) pagesCount - emptyPagesCount;

                    return pagesWithData * pageSize - partitionStorage.freeSpaceInFreeList();
                })
                .sum();
    }

    /**
     * Returns the ratio of space occupied by user and system data to the size of all pages that contain this data.
     *
     * <p>This metric can help to determine how much space of a data page is occupied on average. Low fill factor can
     * indicate that data pages are very fragmented (i.e. there is a lot of empty space across all data pages).
     */
    private double pagesFillFactor() {
        // Number of bytes used by pages that contain at least some data.
        long totalUsedSpaceBytes = totalNonEmptyAllocatedPagesCount() * pageSize;

        if (totalUsedSpaceBytes == 0) {
            return 0;
        }

        // Amount of free space in these pages.
        long freeSpaceBytes = allPartitions()
                .mapToLong(PersistentPageMemoryMvPartitionStorage::freeSpaceInFreeList)
                .sum();

        // Number of bytes that contain useful data.
        long nonEmptySpaceBytes = totalUsedSpaceBytes - freeSpaceBytes;

        return (double) nonEmptySpaceBytes / totalUsedSpaceBytes;
    }

    private Stream<PersistentPageMemoryMvPartitionStorage> allPartitions() {
        return tableStorages.values().stream().flatMap(tableStorage -> tableStorage.mvPartitionStorages.getAll().stream());
    }

    @Override
    public long regionSize() {
        return regionSize;
    }
}
