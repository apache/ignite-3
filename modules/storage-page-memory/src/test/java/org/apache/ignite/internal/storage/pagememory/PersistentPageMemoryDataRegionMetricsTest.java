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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.ENGINE_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.components.NoOpLogSyncer;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Constants;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
class PersistentPageMemoryDataRegionMetricsTest extends BaseMvStoragesTest {
    private static final int TABLE_ID = 123;
    private static final int PARTITION_ID = 12;

    @InjectConfiguration("mock.profiles.default {engine = aipersist, sizeBytes = " + 256 * Constants.MiB + "}")
    private StorageConfiguration storageConfig;

    private PersistentPageMemoryStorageEngine engine;

    private final TestMetricManager metricManager = new TestMetricManager();

    @BeforeEach
    void setUp(
            @InjectExecutorService ExecutorService executorService,
            @InjectConfiguration SystemLocalConfiguration systemConfig,
            @WorkDirectory Path workDir
    ) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new PersistentPageMemoryStorageEngine(
                "test",
                metricManager,
                storageConfig,
                systemConfig,
                ioRegistry,
                workDir,
                null,
                new NoOpFailureManager(),
                new NoOpLogSyncer(),
                executorService,
                clock
        );

        assertThat(metricManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        engine.start();
    }

    @AfterEach
    protected void tearDown() throws Exception {
        closeAllManually(
                engine == null ? null : engine::stop,
                () -> assertThat(stopAsync(new ComponentContext(), metricManager), willCompleteSuccessfully())
        );
    }

    private MvTableStorage createMvTableStorage() {
        return engine.createMvTable(
                new StorageTableDescriptor(TABLE_ID, DEFAULT_PARTITION_COUNT, DEFAULT_STORAGE_PROFILE),
                indexId -> null
        );
    }

    @Test
    void testMaxSizeMetric() {
        LongMetric metric = metricManager.metric(defaultProfileMetricSourceName(), "MaxSize");

        assertThat(metric.value(), is(defaultProfileConfig().sizeBytes().value()));
    }

    @Test
    void testMaxSizeMetricAfterChangeConfig() {
        PersistentPageMemoryProfileConfiguration defaultProfileConfig = defaultProfileConfig();

        long sizeBytesBeforeChange = defaultProfileConfig.sizeBytes().value();

        assertThat(defaultProfileConfig.sizeBytes().update(2 * sizeBytesBeforeChange), willCompleteSuccessfully());

        LongMetric metric = metricManager.metric(defaultProfileMetricSourceName(), "MaxSize");

        assertThat(metric.value(), is(sizeBytesBeforeChange));
    }

    @Test
    void testRegionMetrics() {
        LongMetric totalAllocatedSize = metricManager.metric(defaultProfileMetricSourceName(), "TotalAllocatedSize");
        LongMetric totalUsedSize = metricManager.metric(defaultProfileMetricSourceName(), "TotalUsedSize");
        LongMetric totalEmptySize = metricManager.metric(defaultProfileMetricSourceName(), "TotalEmptySize");
        LongMetric totalDataSize = metricManager.metric(defaultProfileMetricSourceName(), "TotalDataSize");
        DoubleMetric pagesFillFactor = metricManager.metric(defaultProfileMetricSourceName(), "PagesFillFactor");

        assertThat(totalAllocatedSize.value(), is(0L));
        assertThat(totalUsedSize.value(), is(0L));
        assertThat(totalEmptySize.value(), is(0L));
        assertThat(totalDataSize.value(), is(0L));
        assertThat(pagesFillFactor.value(), is(0.0));

        MvTableStorage tableStorage = createMvTableStorage();

        MvPartitionStorage partitionStorage = getOrCreateMvPartition(tableStorage, PARTITION_ID);

        // After partition creation, only system pages exist (meta, free list header, etc.).
        // No data pages are in the free list yet, so freeSpace = 0 and emptyDataPages = 0.
        long allocatedSizeEmptyPartition = totalAllocatedSize.value();

        assertThat(allocatedSizeEmptyPartition, greaterThan(0L));

        assertThat(totalUsedSize.value(), is(allocatedSizeEmptyPartition));
        assertThat(totalEmptySize.value(), is(0L));
        assertThat(totalDataSize.value(), is(allocatedSizeEmptyPartition));
        assertThat(pagesFillFactor.value(), is(1.0));

        // Write a large row that forces fragmentation (payload exceeds single data page capacity).
        // After GC fully filled pages will be added to the reuse bucket, so we can guarantee that totalEmptySize > 0 after vacuum.
        String largeStr = "a".repeat((int) pageSize());

        var rowId = new RowId(PARTITION_ID);

        BinaryRow row = binaryRow(new TestKey(0, largeStr), new TestValue(1, largeStr));

        addWriteCommitted(partitionStorage, rowId, row);

        assertThat(totalAllocatedSize.value(), is(totalUsedSize.value() + totalEmptySize.value()));
        assertThat(totalUsedSize.value(), greaterThan(allocatedSizeEmptyPartition));
        assertThat(totalEmptySize.value(), is(0L));
        assertThat(totalDataSize.value(), allOf(greaterThan(0L), lessThan(totalUsedSize.value())));
        assertThat(pagesFillFactor.value(), allOf(greaterThan(0.0), lessThan(1.0)));

        // Write a tombstone for the same row to create a GC entry, then vacuum it.
        addWriteCommitted(partitionStorage, rowId, null);

        vacuum(partitionStorage);

        assertThat(totalAllocatedSize.value(), equalTo(totalUsedSize.value() + totalEmptySize.value()));
        assertThat(totalUsedSize.value(), greaterThan(0L));
        assertThat(totalEmptySize.value(), greaterThan(0L));
        assertThat(totalDataSize.value(), allOf(greaterThan(0L), lessThan(totalUsedSize.value())));
        assertThat(pagesFillFactor.value(), allOf(greaterThan(0.0), lessThan(1.0)));
    }

    private PersistentPageMemoryProfileConfiguration defaultProfileConfig() {
        StorageProfileConfiguration config = storageConfig.profiles().get(DEFAULT_STORAGE_PROFILE);

        assertNotNull(config);
        assertInstanceOf(PersistentPageMemoryProfileConfiguration.class, config);

        return (PersistentPageMemoryProfileConfiguration) config;
    }

    private String defaultProfileMetricSourceName() {
        return "storage." + ENGINE_NAME + "." + defaultProfileConfig().name().value();
    }

    private long pageSize() {
        return engine.configuration().pageSizeBytes().value();
    }

    private void addWriteCommitted(MvPartitionStorage storage, RowId rowId, @Nullable BinaryRow row) {
        storage.runConsistently(locker -> {
            locker.lock(rowId);

            storage.addWriteCommitted(rowId, row, clock.now());

            return null;
        });
    }

    private static void vacuum(MvPartitionStorage storage) {
        storage.runConsistently(locker -> {
            List<GcEntry> entries = storage.peek(HybridTimestamp.MAX_VALUE, 1);

            assertThat(entries, hasSize(1));

            GcEntry gcEntry = entries.get(0);

            locker.lock(gcEntry.getRowId());

            storage.vacuum(gcEntry);

            return null;
        });
    }
}
