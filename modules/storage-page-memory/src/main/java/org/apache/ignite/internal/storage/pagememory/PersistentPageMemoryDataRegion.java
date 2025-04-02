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

import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.ENGINE_NAME;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.THROTTLING_LOG_THRESHOLD_SYSTEM_PROPERTY;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.THROTTLING_TYPE_SYSTEM_PROPERTY;
import static org.apache.ignite.internal.util.Constants.GiB;
import static org.apache.ignite.internal.util.Constants.MiB;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteSpeedBasedThrottle;
import org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteThrottlePolicy;
import org.apache.ignite.internal.pagememory.persistence.throttling.TargetRatioPagesWriteThrottle;
import org.apache.ignite.internal.pagememory.persistence.throttling.ThrottlingType;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link DataRegion} for persistent case.
 */
class PersistentPageMemoryDataRegion implements DataRegion<PersistentPageMemory> {
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

    private final FilePageStoreManager filePageStoreManager;

    private final PartitionMetaManager partitionMetaManager;

    private final CheckpointManager checkpointManager;

    private volatile PersistentPageMemory pageMemory;

    private volatile AtomicLong pageListCacheLimit;

    private PersistentPageMemoryMetricSource metricSource;

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
    PersistentPageMemoryDataRegion(
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
        PersistentPageMemoryProfileView dataRegionConfigView = (PersistentPageMemoryProfileView) cfg.value();

        PersistentPageMemory pageMemory = new PersistentPageMemory(
                cfg,
                metricSource,
                ioRegistry,
                calculateSegmentSizes(dataRegionConfigView.size(), Runtime.getRuntime().availableProcessors()),
                calculateCheckpointBufferSize(dataRegionConfigView.size()),
                filePageStoreManager,
                this::flushDirtyPageOnReplacement,
                checkpointManager.checkpointTimeoutLock(),
                pageSize,
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL)
        );

        initThrottling(pageMemory);

        pageMemory.start();

        metricManager.registerSource(metricSource);
        metricManager.enable(metricSource);

        pageListCacheLimit = new AtomicLong((long) (pageMemory.totalPages() * PAGE_LIST_CACHE_LIMIT_THRESHOLD));

        this.pageMemory = pageMemory;
    }

    // TODO IGNITE-24933 refactor.
    private void initThrottling(PersistentPageMemory pageMemory) {
        ThrottlingType throttlingType = getThrottlingType();

        long logThresholdNanos = getLoggingThreshold();

        switch (throttlingType) {
            case DISABLED:
                break;

            case TARGET_RATIO:
                pageMemory.initThrottling(new TargetRatioPagesWriteThrottle(
                        logThresholdNanos,
                        pageMemory,
                        checkpointManager::currentCheckpointProgressForThrottling,
                        checkpointManager.checkpointTimeoutLock()::checkpointLockIsHeldByThread,
                        metricSource
                ));
                break;

            case SPEED_BASED:
                pageMemory.initThrottling(new PagesWriteSpeedBasedThrottle(
                        logThresholdNanos,
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
        SystemPropertyView throttlingTypeCfg = systemLocalConfig == null
                ? null
                : systemLocalConfig.value().properties().get(THROTTLING_TYPE_SYSTEM_PROPERTY);

        ThrottlingType throttlingType = ThrottlingType.SPEED_BASED;

        if (throttlingTypeCfg != null) {
            try {
                throttlingType = ThrottlingType.valueOf(throttlingTypeCfg.propertyValue().toUpperCase());
            } catch (IllegalArgumentException e) {
                LOG.warn(
                        "Invalid throttling configuration {}={}, using default value {}",
                        THROTTLING_TYPE_SYSTEM_PROPERTY,
                        throttlingTypeCfg.propertyValue(),
                        throttlingType
                );
            }
        }
        return throttlingType;
    }

    private long getLoggingThreshold() {
        SystemPropertyView logThresholdCfg = systemLocalConfig == null
                ? null
                : systemLocalConfig.value().properties().get(THROTTLING_LOG_THRESHOLD_SYSTEM_PROPERTY);

        long logThresholdNanos = PagesWriteThrottlePolicy.DEFAULT_LOGGING_THRESHOLD;
        try {
            if (logThresholdCfg != null) {
                long logThresholdMillis = Long.parseLong(logThresholdCfg.propertyValue());

                if (logThresholdMillis <= 0) {
                    throw new IllegalArgumentException();
                }

                logThresholdNanos = TimeUnit.MILLISECONDS.toNanos(logThresholdMillis);
            }
        } catch (Exception e) {
            LOG.warn(
                    "Invalid throttling configuration {}={}, using default value {}",
                    THROTTLING_LOG_THRESHOLD_SYSTEM_PROPERTY,
                    logThresholdCfg.propertyValue(),
                    TimeUnit.NANOSECONDS.toMillis(logThresholdNanos)
            );
        }
        return logThresholdNanos;
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
        checkpointManager.writePageToDeltaFilePageStore(pageMemory, fullPageId, byteBuffer);

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
}
