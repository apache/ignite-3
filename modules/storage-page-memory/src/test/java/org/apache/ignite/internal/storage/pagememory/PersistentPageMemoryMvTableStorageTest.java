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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SORTED;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.ENGINE_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTimeoutLock;
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
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.storage.pagememory.mv.PersistentPageMemoryMvPartitionStorage;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.Constants;
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
    @InjectConfiguration("mock.profiles.default {engine = aipersist, sizeBytes = " + Constants.GiB + "}")
    private StorageConfiguration storageConfig;

    @InjectConfiguration
    private SystemLocalConfiguration systemConfig;

    private PersistentPageMemoryStorageEngine engine;

    @InjectExecutorService
    private ExecutorService executorService;

    private TestMetricManager metricManager;

    @WorkDirectory
    private Path workDir;

    @BeforeEach
    void setUp() {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        metricManager = new TestMetricManager();

        engine = new PersistentPageMemoryStorageEngine(
                "test",
                metricManager,
                storageConfig,
                systemConfig,
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

        PersistentPageMemoryMvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
        assertThat(metric.value(), allOf(greaterThan(0L), equalTo(totalAllocatedSizeInBytes(PARTITION_ID))));

        addWriteCommitted(mvPartitionStorage);
        assertThat(metric.value(), allOf(greaterThan(0L), equalTo(totalAllocatedSizeInBytes(PARTITION_ID))));
    }

    @Test
    void testTotalUsedSize() {
        LongMetric metric = (LongMetric) metricManager.metric(defaultProfileMetricSourceName(), "TotalUsedSize");

        assertNotNull(metric);
        assertEquals(0L, metric.value());

        PersistentPageMemoryMvPartitionStorage mvPartitionStorage = getOrCreateMvPartition(PARTITION_ID);
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

    private void addWriteCommitted(PersistentPageMemoryMvPartitionStorage... storages) {
        assertThat(storages, not(emptyArray()));

        for (PersistentPageMemoryMvPartitionStorage storage : storages) {
            var rowId = new RowId(storage.partitionId());

            BinaryRow binaryRow = binaryRow(new TestKey(0, "0"), new TestValue(1, "1"));

            storage.runConsistently(locker -> {
                locker.lock(rowId);

                storage.addWriteCommitted(rowId, binaryRow, clock.now());

                return null;
            });
        }
    }

    private void addWriteCommitted(MvPartitionStorage storage, List<RowId> rowIds, List<BinaryRow> binaryRows) {
        assertEquals(rowIds.size(), binaryRows.size());

        storage.runConsistently(locker -> {
            HybridTimestamp now = clock.now();

            for (int i = 0; i < rowIds.size(); i++) {
                RowId rowId = rowIds.get(i);
                BinaryRow binaryRow = binaryRows.get(i);

                locker.lock(rowId);

                storage.addWriteCommitted(rowId, binaryRow, now);
            }

            return null;
        });
    }

    @Test
    void createMvPartitionStorageAndDoCheckpointInParallel() {
        for (int i = 0; i < 10; i++) {
            runRace(
                    () -> getOrCreateMvPartition(PARTITION_ID),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );

            assertThat(tableStorage.destroyPartition(PARTITION_ID), willCompleteSuccessfully());
        }
    }

    @Test
    void clearMvPartitionStorageAndDoCheckpointInParallel() {
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
    void destroyMvPartitionStorageAndDoCheckpointInParallel() {
        for (int i = 0; i < 10; i++) {
            getOrCreateMvPartition(PARTITION_ID);

            runRace(
                    () -> assertThat(tableStorage.destroyPartition(PARTITION_ID), willCompleteSuccessfully()),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );
        }
    }

    @Test
    void startRebalancePartitionAndDoCheckpointInParallel() {
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
    void abortRebalancePartitionAndDoCheckpointInParallel() {
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
    void finishRebalancePartitionAndDoCheckpointInParallel() {
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

    @Test
    void testSyncFreeListOnCheckpointAfterStartRebalance() {
        PersistentPageMemoryMvPartitionStorage storage = getOrCreateMvPartition(PARTITION_ID);

        var meta = new MvPartitionMeta(1, 1, BYTE_EMPTY_ARRAY, null, BYTE_EMPTY_ARRAY);

        for (int i = 0; i < 10; i++) {
            IntStream.rangeClosed(0, 10).forEach(n -> addWriteCommitted(storage));

            runRace(
                    () -> assertThat(tableStorage.startRebalancePartition(PARTITION_ID), willCompleteSuccessfully()),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );

            assertThat(tableStorage.finishRebalancePartition(PARTITION_ID, meta), willCompleteSuccessfully());
        }
    }

    @Test
    void testCheckpointWithTwoPartitionGeneration() throws Throwable {
        MvPartitionStorage storage = getOrCreateMvPartition(PARTITION_ID);

        List<RowId> rowIds = generateRowIds(PARTITION_ID, 10);

        int halfPageSize = (int) pageSize() / 2;

        addWriteCommitted(storage, rowIds, generateBinaryRows(generatePrefix('a', halfPageSize), 10));

        assertThat(forceCheckpointAsync(), willCompleteSuccessfully());

        inCheckpointReadLock(() -> {
            // Let's update old pages.
            addWriteCommitted(storage, rowIds, generateBinaryRows(generatePrefix('b', halfPageSize), 10));
            // Let's add new pages.
            addWriteCommitted(storage, generateRowIds(PARTITION_ID, 10), generateBinaryRows(generatePrefix('b', halfPageSize), 10));

            // Let's update the generation of the partition.
            int oldPartitionGeneration = partitionGeneration(PARTITION_ID);
            assertThat(tableStorage.clearPartition(PARTITION_ID), willCompleteSuccessfully());
            assertThat(partitionGeneration(PARTITION_ID), greaterThan(oldPartitionGeneration));

            // Let's add new pages
            addWriteCommitted(storage, generateRowIds(PARTITION_ID, 10), generateBinaryRows(generatePrefix('c', halfPageSize), 10));
        });

        assertThat(forceCheckpointAsync(), willCompleteSuccessfully());
    }

    @Test
    void testIncreasePartitionGenerationAfterSortPages() {
        MvPartitionStorage storage = getOrCreateMvPartition(PARTITION_ID);

        List<BinaryRow> binaryRows = generateBinaryRows(generatePrefix('_', (int) pageSize() / 2), 10);

        for (int i = 0; i < 10; i++) {
            CompletableFuture<Void> future = inCheckpointReadLock(() -> {
                addWriteCommitted(storage, generateRowIds(PARTITION_ID, 10), binaryRows);

                CheckpointProgress forceCheckpoint = engine.checkpointManager().forceCheckpoint("test");

                CompletableFuture<Void> clearPartitionFuture = forceCheckpoint.futureFor(PAGES_SORTED)
                        .thenCompose(unused -> tableStorage.clearPartition(PARTITION_ID));

                return CompletableFuture.allOf(forceCheckpoint.futureFor(FINISHED), clearPartitionFuture);
            });

            assertThat(future, willCompleteSuccessfully());
        }
    }

    /**
     * Checks for a very rare race condition where, after releasing the checkpoint write lock, the partition meta is updated with an
     * incorrect empty last empty checkpoint ({@code lastCheckpointId == null}), which can lead to error
     * "IGN-CMN-65535 Unknown page IO type: 0" on node restart.
     */
    @Test
    void testSuccessfulPartitionRestartAfterParallelUpdateLeaseAndCheckpoint() throws Exception {
        for (int i = 0; i < 100; i++) {
            PersistentPageMemoryMvPartitionStorage mvPartition = getOrCreateMvPartition(PARTITION_ID);

            addWriteCommitted(mvPartition);

            CountDownLatch readyToUpdateLeaseLatch = new CountDownLatch(1);
            CountDownLatch updateLeaseLatch = new CountDownLatch(1);

            CompletableFuture<Void> updateLeaseFuture = runAsync(() -> {
                readyToUpdateLeaseLatch.countDown();

                assertTrue(updateLeaseLatch.await(10, TimeUnit.SECONDS));

                mvPartition.runConsistently(locker -> {
                    mvPartition.updateLease(new LeaseInfo(100, UUID.randomUUID(), "node"));

                    return null;
                });
            });

            assertTrue(readyToUpdateLeaseLatch.await(10, TimeUnit.SECONDS));

            CheckpointProgress checkpointProgress = engine.checkpointManager().forceCheckpoint("test");

            CompletableFuture<Void> updateLeaseLatchFuture = checkpointProgress
                    .futureFor(CheckpointState.LOCK_TAKEN)
                    .thenAccept(unused -> updateLeaseLatch.countDown());

            assertThat(
                    CompletableFuture.allOf(updateLeaseLatchFuture, updateLeaseFuture, checkpointProgress.futureFor(FINISHED)),
                    willCompleteSuccessfully()
            );

            tearDown();
            setUp();

            assertDoesNotThrow(() -> getOrCreateMvPartition(PARTITION_ID));
        }
    }

    /**
     * Checks for a rare case where a partition meta update can occur before a checkpoint, which could result in the partition meta not
     * being included in the dirty pages list and, as a consequence, an "java.lang.IllegalArgumentException: Negative position" when
     * attempting to write this meta to the delta file.
     */
    @Test
    void testUpdatePartitionMetaAfterStartRebalance() {
        int[] partitionIds = IntStream.range(0, 5)
                .map(i -> PARTITION_ID + i)
                .toArray();

        PersistentPageMemoryMvPartitionStorage[] partitions = getOrCreateMvPartitions(partitionIds);

        for (int i = 0; i < 10; i++) {
            addWriteCommitted(partitions);

            runRace(
                    () -> startRebalance(partitionIds),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );

            abortRebalance(partitionIds);
        }
    }

    @Test
    void testSyncFreeListMetadataOnCheckpointAfterAbortRebalance() {
        int[] partitionIds = IntStream.range(0, 5)
                .map(i -> PARTITION_ID + i)
                .toArray();

        PersistentPageMemoryMvPartitionStorage[] partitions = getOrCreateMvPartitions(partitionIds);

        for (int i = 0; i < 10; i++) {
            addWriteCommitted(partitions);

            startRebalance(partitionIds);

            addWriteCommitted(partitions);

            runRace(
                    () -> abortRebalance(partitionIds),
                    () -> assertThat(forceCheckpointAsync(), willCompleteSuccessfully())
            );
        }
    }

    @Test
    void testRebalanceWithLotsOfWriteIntents() {
        MvPartitionStorage partitionStorage = getOrCreateMvPartition(PARTITION_ID);

        for (int i = 0; i < 50; i++) {
            addWriteUncommitted(partitionStorage);
        }

        assertThat(tableStorage.startRebalancePartition(PARTITION_ID), willCompleteSuccessfully());

        try {
            addWriteUncommitted(partitionStorage);
        } finally {
            assertThat(tableStorage.abortRebalancePartition(PARTITION_ID), willCompleteSuccessfully());
        }
    }

    private void addWriteUncommitted(MvPartitionStorage partitionStorage0) {
        String randomString = IgniteTestUtils.randomString(ThreadLocalRandom.current(), 256);
        BinaryRow binaryRow = binaryRow(new TestKey(0, randomString), new TestValue(0, randomString));

        partitionStorage0.runConsistently(locker -> {
            RowId rowId = new RowId(PARTITION_ID);

            locker.lock(rowId);

            return partitionStorage0.addWrite(rowId, binaryRow, newTransactionId(), 1, 1);
        });
    }

    private CompletableFuture<Void> forceCheckpointAsync() {
        return engine.checkpointManager().forceCheckpoint("test").futureFor(FINISHED);
    }

    private void inCheckpointReadLock(RunnableX r) throws Throwable {
        CheckpointTimeoutLock lock = engine.checkpointManager().checkpointTimeoutLock();

        lock.checkpointReadLock();

        try {
            r.run();
        } finally {
            lock.checkpointReadUnlock();
        }
    }

    private <T> T inCheckpointReadLock(Supplier<T> supplier) {
        CheckpointTimeoutLock lock = engine.checkpointManager().checkpointTimeoutLock();

        lock.checkpointReadLock();

        try {
            return supplier.get();
        } finally {
            lock.checkpointReadUnlock();
        }
    }

    private static List<RowId> generateRowIds(int partId, int count) {
        return IntStream.range(0, count).mapToObj(i -> new RowId(partId)).collect(toList());
    }

    private static List<BinaryRow> generateBinaryRows(String prefix, int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> binaryRow(new TestKey(0, prefix + "k" + i), new TestValue(1, prefix + "v" + i)))
                .collect(toList());
    }

    private static String generatePrefix(char c, int count) {
        var sb = new StringBuilder(count);

        IntStream.range(0, count).forEach(i -> sb.append(c));

        return sb.toString();
    }

    private int partitionGeneration(int partId) {
        return ((PersistentPageMemoryTableStorage) tableStorage).dataRegion().pageMemory().partGeneration(TABLE_ID, partId);
    }

    @Override
    protected PersistentPageMemoryMvPartitionStorage getOrCreateMvPartition(int partitionId) {
        return (PersistentPageMemoryMvPartitionStorage) super.getOrCreateMvPartition(partitionId);
    }

    private PersistentPageMemoryMvPartitionStorage[] getOrCreateMvPartitions(int... partitionIds) {
        return IntStream.of(partitionIds)
                .mapToObj(this::getOrCreateMvPartition)
                .toArray(PersistentPageMemoryMvPartitionStorage[]::new);
    }

    private void startRebalance(int... partitionIds) {
        List<CompletableFuture<Void>> startRebalanceFutures = IntStream.of(partitionIds)
                .mapToObj(tableStorage::startRebalancePartition)
                .collect(toList());

        assertThat(CompletableFutures.allOf(startRebalanceFutures), willCompleteSuccessfully());
    }

    private void abortRebalance(int... partitionIds) {
        List<CompletableFuture<Void>> abortRebalanceFutures = IntStream.of(partitionIds)
                .mapToObj(tableStorage::abortRebalancePartition)
                .collect(toList());

        assertThat(CompletableFutures.allOf(abortRebalanceFutures), willCompleteSuccessfully());
    }
}
