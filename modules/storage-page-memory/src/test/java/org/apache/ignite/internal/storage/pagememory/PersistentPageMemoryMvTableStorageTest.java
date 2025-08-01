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
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.ENGINE_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.apache.ignite.internal.storage.engine.MvPartitionMeta;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.storage.pagememory.mv.PersistentPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link PersistentPageMemoryTableStorage} class.
 */
@ExtendWith({WorkDirectoryExtension.class, ExecutorServiceExtension.class})
public class PersistentPageMemoryMvTableStorageTest extends AbstractMvTableStorageTest {
    @InjectConfiguration("mock.profiles.default.engine = aipersist")
    private StorageConfiguration storageConfig;

    private PersistentPageMemoryStorageEngine engine;

    @InjectExecutorService
    private ExecutorService executorService;

    private final TestMetricManager metricManager = new TestMetricManager();

    @BeforeEach
    void setUp(@WorkDirectory Path workDir) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new PersistentPageMemoryStorageEngine(
                "test",
                metricManager,
                storageConfig,
                null,
                ioRegistry,
                workDir,
                null,
                mock(FailureManager.class),
                mock(LogSyncer.class),
                executorService,
                clock
        );

        engine.start();

        assertThat(metricManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        initialize();
    }

    @Override
    @AfterEach
    protected void tearDown() throws Exception {
        super.tearDown();

        IgniteUtils.closeAllManually(
                () -> assertThat(metricManager.stopAsync(new ComponentContext()), willCompleteSuccessfully()),
                engine == null ? null : engine::stop
        );
    }

    @Override
    protected MvTableStorage createMvTableStorage() {
        return engine.createMvTable(
                new StorageTableDescriptor(TABLE_ID, DEFAULT_PARTITION_COUNT, DEFAULT_STORAGE_PROFILE),
                indexDescriptorSupplier
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Override
    public void testDestroyPartition(boolean waitForDestroyFuture) {
        super.testDestroyPartition(waitForDestroyFuture);

        // Let's make sure that the checkpoint doesn't fail.
        assertThat(
                engine.checkpointManager().forceCheckpoint("after-test-destroy-partition").futureFor(FINISHED),
                willCompleteSuccessfully()
        );
    }

    @Test
    void testParallelDestroyPartitionAndCheckpoint() {
        for (int partitionId = 0; partitionId < 100; partitionId++) {
            int finalPartitionId = partitionId % DEFAULT_PARTITION_COUNT;

            MvPartitionStorage partition = getOrCreateMvPartition(finalPartitionId);

            RowId rowId = new RowId(finalPartitionId);
            BinaryRow binaryRow = binaryRow(new TestKey(1, "1"), new TestValue(2, "2"));

            partition.runConsistently(locker -> {
                locker.lock(rowId);

                partition.addWriteCommitted(rowId, binaryRow, clock.now());

                return null;
            });

            runRace(
                    () -> assertThat(tableStorage.destroyPartition(finalPartitionId), willCompleteSuccessfully()),
                    () -> assertThat(engine.checkpointManager().forceCheckpoint("test").futureFor(FINISHED), willCompleteSuccessfully())
            );
        }
    }

    @Test
    void testMaxSizeMetric() {
        LongMetric metric = (LongMetric) metricManager.metric(defaultProfileMetricSourceName(), "MaxSize");

        assertNotNull(metric);
        assertEquals(defaultProfileConfig().sizeBytes().value(), metric.value());
    }

    @Test
    void testMaxSizeMetricAfterChangeConfig() {
        PersistentPageMemoryProfileConfiguration defaultProfileConfig = defaultProfileConfig();

        Long sizeBytesBeforeChange = defaultProfileConfig.sizeBytes().value();
        assertThat(defaultProfileConfig.sizeBytes().update(2 * sizeBytesBeforeChange), willCompleteSuccessfully());

        LongMetric metric = (LongMetric) metricManager.metric(defaultProfileMetricSourceName(), "MaxSize");

        assertNotNull(metric);
        assertEquals(sizeBytesBeforeChange, metric.value());
    }

    @Test
    void testTotalAllocatedSize() {
        LongMetric metric = (LongMetric) metricManager.metric(defaultProfileMetricSourceName(), "TotalAllocatedSize");

        assertNotNull(metric);
        assertEquals(0L, metric.value());

        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        assertThat(metric.value(), allOf(greaterThan(0L), equalTo(totalAllocatedSizeInBytes(PARTITION_ID))));

        addWriteCommitted(mvPartitionStorage);
        assertThat(metric.value(), allOf(greaterThan(0L), equalTo(totalAllocatedSizeInBytes(PARTITION_ID))));
    }

    @Test
    void testTotalUsedSize() {
        LongMetric metric = (LongMetric) metricManager.metric(defaultProfileMetricSourceName(), "TotalUsedSize");

        assertNotNull(metric);
        assertEquals(0L, metric.value());

        MvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        assertThat(metric.value(), allOf(greaterThan(0L), equalTo(totalUsedSizeInBytes(PARTITION_ID))));

        addWriteCommitted(mvPartitionStorage);
        assertThat(metric.value(), allOf(greaterThan(0L), equalTo(totalUsedSizeInBytes(PARTITION_ID))));
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

    private long filePageStorePageCount(int partitionId) {
        PersistentPageMemoryTableStorage tableStorage = (PersistentPageMemoryTableStorage) this.tableStorage;

        FilePageStore store = tableStorage.dataRegion().filePageStoreManager().getStore(new GroupPartitionId(TABLE_ID, partitionId));

        return store == null ? 0 : store.pages();
    }

    private long freeListEmptyPageCount(int partitionId) {
        PersistentPageMemoryTableStorage tableStorage = (PersistentPageMemoryTableStorage) this.tableStorage;

        PersistentPageMemoryMvPartitionStorage storage = (PersistentPageMemoryMvPartitionStorage) tableStorage.getMvPartition(partitionId);

        return storage == null ? 0L : storage.emptyDataPageCountInFreeList();
    }

    private long totalAllocatedSizeInBytes(int partitionId) {
        return pageSize() * filePageStorePageCount(partitionId);
    }

    private long totalUsedSizeInBytes(int partitionId) {
        return pageSize() * (filePageStorePageCount(partitionId) - freeListEmptyPageCount(partitionId));
    }

    private void addWriteCommitted(MvPartitionStorage storage) {
        var rowId = new RowId(PARTITION_ID);

        BinaryRow binaryRow = binaryRow(new TestKey(0, "0"), new TestValue(1, "1"));

        storage.runConsistently(locker -> {
            locker.lock(rowId);

            storage.addWriteCommitted(rowId, binaryRow, clock.now());

            return null;
        });
    }

    @Test
    void createMvPartitionStorageAndDoCheckpointInParallel() throws Exception {
        stopCompactor();

        for (int i = 0; i < 10; i++) {
            runRace(
                    () -> getOrCreateMvPartition(PARTITION_ID),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );

            assertThat(tableStorage.destroyPartition(PARTITION_ID), willCompleteSuccessfully());
        }
    }

    @Test
    void clearMvPartitionStorageAndDoCheckpointInParallel() throws Exception {
        stopCompactor();

        for (int i = 0; i < 10; i++) {
            getOrCreateMvPartition(PARTITION_ID);

            runRace(
                    () -> assertThat(tableStorage.clearPartition(PARTITION_ID), willCompleteSuccessfully()),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );

            assertThat(tableStorage.destroyPartition(PARTITION_ID), willCompleteSuccessfully());
        }
    }

    @Test
    void destroyMvPartitionStorageAndDoCheckpointInParallel() throws Exception {
        stopCompactor();

        for (int i = 0; i < 10; i++) {
            getOrCreateMvPartition(PARTITION_ID);

            runRace(
                    () -> assertThat(tableStorage.destroyPartition(PARTITION_ID), willCompleteSuccessfully()),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );
        }
    }

    @Test
    void startRebalancePartitionAndDoCheckpointInParallel() throws Exception {
        stopCompactor();

        getOrCreateMvPartition(PARTITION_ID);

        for (int i = 0; i < 10; i++) {
            runRace(
                    () -> assertThat(tableStorage.startRebalancePartition(PARTITION_ID), willCompleteSuccessfully()),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );

            assertThat(tableStorage.abortRebalancePartition(PARTITION_ID), willCompleteSuccessfully());
        }
    }

    @Test
    void abortRebalancePartitionAndDoCheckpointInParallel() throws Exception {
        stopCompactor();

        getOrCreateMvPartition(PARTITION_ID);

        for (int i = 0; i < 10; i++) {
            assertThat(tableStorage.startRebalancePartition(PARTITION_ID), willCompleteSuccessfully());

            runRace(
                    () -> assertThat(tableStorage.abortRebalancePartition(PARTITION_ID), willCompleteSuccessfully()),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );
        }
    }

    @Test
    void finishRebalancePartitionAndDoCheckpointInParallel() throws Exception {
        stopCompactor();

        getOrCreateMvPartition(PARTITION_ID);

        for (int i = 0; i < 10; i++) {
            assertThat(tableStorage.startRebalancePartition(PARTITION_ID), willCompleteSuccessfully());

            var meta = new MvPartitionMeta(1, 1, BYTE_EMPTY_ARRAY, null, BYTE_EMPTY_ARRAY);

            runRace(
                    () -> assertThat(tableStorage.finishRebalancePartition(PARTITION_ID, meta), willCompleteSuccessfully()),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );
        }
    }

    private CompletableFuture<Void> forceCheckpointAsync() {
        return engine.checkpointManager().forceCheckpoint("test").futureFor(FINISHED);
    }

    // TODO: IGNITE-25861 Get rid of it
    private void stopCompactor() throws Exception {
        engine.checkpointManager().compactor().stop();
    }
}
