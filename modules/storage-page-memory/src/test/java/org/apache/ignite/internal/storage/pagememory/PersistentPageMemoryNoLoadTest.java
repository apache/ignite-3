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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.MUST_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.NOT_REQUIRED;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.SHOULD_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.PAGE_OVERHEAD;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.partitionGeneration;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SORTED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.mockCheckpointTimeoutLock;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.AbstractPageMemoryNoLoadSelfTest;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestDataRegion;
import org.apache.ignite.internal.pagememory.configuration.CheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionDestructionLockManager;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta.PartitionMetaSnapshot;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.TestPageReadWriteManager;
import org.apache.ignite.internal.pagememory.persistence.WriteDirtyPage;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointMetricsTracker;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgressImpl;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@link PersistentPageMemory}.
 */
@ExtendWith({WorkDirectoryExtension.class, ConfigurationExtension.class, ExecutorServiceExtension.class})
public class PersistentPageMemoryNoLoadTest extends AbstractPageMemoryNoLoadSelfTest {
    private static PageIoRegistry ioRegistry;

    @InjectExecutorService
    private ExecutorService executorService;

    private int dataRegionSize;

    @BeforeAll
    static void beforeAll() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    @BeforeEach
    void setUp() throws Exception {
        dataRegionSize = MAX_MEMORY_SIZE;
    }

    @AfterAll
    static void afterAll() {
        ioRegistry = null;
    }

    /** {@inheritDoc} */
    @Override
    protected PageMemory memory() {
        return createPageMemory(
                defaultSegmentSizes(),
                defaultCheckpointBufferSize(),
                null,
                null,
                shouldNotHappenFlushDirtyPageForReplacement()
        );
    }

    /** {@inheritDoc} */
    @Test
    @Override
    public void testPageHandleDeallocation() {
        // No-op.
    }

    @Test
    void testDirtyPages(@WorkDirectory Path workDir) throws Exception {
        FilePageStoreManager filePageStoreManager = createFilePageStoreManager(workDir);

        PartitionMetaManager partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, StoragePartitionMeta.FACTORY);

        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(
                CheckpointConfiguration.builder().build(),
                filePageStoreManager,
                partitionMetaManager,
                dataRegions
        );

        PersistentPageMemory pageMemory = createPageMemory(
                defaultSegmentSizes(),
                defaultCheckpointBufferSize(),
                filePageStoreManager,
                checkpointManager,
                shouldNotHappenFlushDirtyPageForReplacement()
        );

        dataRegions.add(new TestDataRegion<>(pageMemory));

        filePageStoreManager.start();

        checkpointManager.start();

        try {
            initGroupFilePageStores(filePageStoreManager, partitionMetaManager, checkpointManager, pageMemory);

            checkpointManager.checkpointTimeoutLock().checkpointReadLock();

            try {
                Set<DirtyFullPageId> dirtyPages = Set.of(createDirtyPage(pageMemory), createDirtyPage(pageMemory));
                assertThat(pageMemory.dirtyPages(), equalTo(dirtyPages));

                assertEquals(2, pageMemory.invalidate(GRP_ID, PARTITION_ID));

                Set<DirtyFullPageId> dirtyPagesAfterInvalidation = Set.of(createDirtyPage(pageMemory), createDirtyPage(pageMemory));
                assertThat(pageMemory.dirtyPages(), equalTo(union(dirtyPages, dirtyPagesAfterInvalidation)));
            } finally {
                checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
            }

            checkpointManager
                    .forceCheckpoint("for_test_flash_dirty_pages")
                    .futureFor(FINISHED)
                    .get(1, SECONDS);

            assertThat(pageMemory.dirtyPages(), empty());
        } finally {
            closeAll(
                    () -> pageMemory.stop(true),
                    checkpointManager::stop,
                    filePageStoreManager::stop
            );
        }
    }

    @Test
    void testCheckpointUrgency(@WorkDirectory Path workDir) throws Exception {
        FilePageStoreManager filePageStoreManager = createFilePageStoreManager(workDir);

        PartitionMetaManager partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, StoragePartitionMeta.FACTORY);

        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(
                CheckpointConfiguration.builder().build(),
                filePageStoreManager,
                partitionMetaManager,
                dataRegions
        );

        int systemPageSize = PAGE_SIZE + PAGE_OVERHEAD;

        dataRegionSize = 100 * systemPageSize;

        PersistentPageMemory pageMemory = createPageMemory(
                new long[]{dataRegionSize},
                28 * systemPageSize,
                filePageStoreManager,
                checkpointManager,
                shouldNotHappenFlushDirtyPageForReplacement()
        );

        dataRegions.add(new TestDataRegion<>(pageMemory));

        filePageStoreManager.start();

        checkpointManager.start();

        try {
            initGroupFilePageStores(filePageStoreManager, partitionMetaManager, checkpointManager, pageMemory);

            long maxPages = pageMemory.totalPages();

            long dirtyPagesSoftThreshold = (maxPages * 3 / 4);
            long dirtyPagesHardThreshold = (maxPages * 9 / 10);

            assertThat(dirtyPagesSoftThreshold, greaterThanOrEqualTo(70L));
            assertThat(dirtyPagesHardThreshold, greaterThanOrEqualTo(80L));

            checkpointManager.checkpointTimeoutLock().checkpointReadLock();

            try {
                int i = 0;

                for (; i < dirtyPagesSoftThreshold - 1; i++) {
                    createDirtyPage(pageMemory);

                    assertEquals(NOT_REQUIRED, pageMemory.checkpointUrgency(), "i=" + i);
                }

                for (; i < dirtyPagesHardThreshold - 1; i++) {
                    createDirtyPage(pageMemory);

                    assertEquals(SHOULD_TRIGGER, pageMemory.checkpointUrgency(), "i=" + i);
                }

                for (; i < maxPages; i++) {
                    createDirtyPage(pageMemory);

                    assertEquals(MUST_TRIGGER, pageMemory.checkpointUrgency(), "i=" + i);
                }
            } finally {
                checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
            }

            checkpointManager
                    .forceCheckpoint("for_test_safe_to_update")
                    .futureFor(FINISHED)
                    .get(1, SECONDS);

            assertEquals(NOT_REQUIRED, pageMemory.checkpointUrgency());
        } finally {
            closeAll(
                    () -> pageMemory.stop(true),
                    checkpointManager::stop,
                    filePageStoreManager::stop
            );
        }
    }

    @Test
    void testDeltaFilePageStore(@WorkDirectory Path workDir) throws Exception {
        FilePageStoreManager filePageStoreManager = spy(createFilePageStoreManager(workDir));

        PartitionMetaManager partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, StoragePartitionMeta.FACTORY);

        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(
                CheckpointConfiguration.builder().build(),
                filePageStoreManager,
                partitionMetaManager,
                dataRegions
        );

        PersistentPageMemory pageMemory = createPageMemory(
                defaultSegmentSizes(),
                defaultCheckpointBufferSize(),
                filePageStoreManager,
                checkpointManager,
                shouldNotHappenFlushDirtyPageForReplacement()
        );

        dataRegions.add(new TestDataRegion<>(pageMemory));

        filePageStoreManager.start();

        checkpointManager.start();

        try {
            initGroupFilePageStores(filePageStoreManager, partitionMetaManager, checkpointManager, pageMemory);

            checkpointManager.checkpointTimeoutLock().checkpointReadLock();

            try {
                createDirtyPage(pageMemory);
                createDirtyPage(pageMemory);
                createDirtyPage(pageMemory);
            } finally {
                checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
            }

            checkpointManager
                    .forceCheckpoint("for_test_delta_file_page_store")
                    .futureFor(FINISHED)
                    .get(1, SECONDS);

            verify(filePageStoreManager, times(1)).tmpDeltaFilePageStorePath(eq(GRP_ID), eq(PARTITION_ID), eq(0));

            verify(filePageStoreManager, times(1)).deltaFilePageStorePath(eq(GRP_ID), eq(PARTITION_ID), eq(0));
        } finally {
            closeAll(
                    () -> pageMemory.stop(true),
                    checkpointManager::stop,
                    filePageStoreManager::stop
            );
        }
    }

    @Test
    void testPageReplacement(@WorkDirectory Path workDir) throws Exception {
        FilePageStoreManager filePageStoreManager = createFilePageStoreManager(workDir);

        PartitionMetaManager partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, StoragePartitionMeta.FACTORY);
        partitionMetaManager = spy(partitionMetaManager);

        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(
                CheckpointConfiguration.builder().checkpointThreads(1).build(),
                filePageStoreManager,
                partitionMetaManager,
                dataRegions
        );

        CompletableFuture<?> flushDirtyPageForReplacementFuture = new CompletableFuture<>();

        PersistentPageMemory pageMemory = createPageMemory(
                defaultSegmentSizes(),
                defaultCheckpointBufferSize(),
                filePageStoreManager,
                checkpointManager,
                (pageMemory0, fullPageId, buffer) -> flushDirtyPageForReplacementFuture.complete(null)
        );

        dataRegions.add(new TestDataRegion<>(pageMemory));

        filePageStoreManager.start();

        checkpointManager.start();

        CompletableFuture<?> startWriteMetaToBufferFuture = new CompletableFuture<>();
        CompletableFuture<?> finishWaitWriteMetaToBufferFuture = new CompletableFuture<>();

        // Mock to pause writing to the disk (complete the checkpoint) and the replacement could happen.
        doAnswer(answer -> {
            startWriteMetaToBufferFuture.complete(null);

            assertThat(finishWaitWriteMetaToBufferFuture, willCompleteSuccessfully());

            return answer.callRealMethod();
        })
                .when(partitionMetaManager)
                .writeMetaToBuffer(any(GroupPartitionId.class), any(PartitionMetaSnapshot.class), any(ByteBuffer.class));

        try {
            initGroupFilePageStores(filePageStoreManager, partitionMetaManager, checkpointManager, pageMemory);

            checkpointManager.checkpointTimeoutLock().checkpointReadLock();

            try {
                for (int i = 0; i < 1_000; i++) {
                    createDirtyPage(pageMemory);
                }
            } finally {
                checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
            }

            CheckpointProgress checkpointProgress = checkpointManager.forceCheckpoint("for_test_page_replacement");

            // Replacement will not happen until the pages are sorted.
            checkpointProgress.futureFor(PAGES_SORTED).get(1, SECONDS);

            checkpointManager.checkpointTimeoutLock().checkpointReadLock();

            try {
                // We are waiting for the start of writing dirty pages to disk.
                startWriteMetaToBufferFuture.get(1, SECONDS);

                do {
                    // We create new dirty pages so that we get to the end of the data region and start page replacing.
                    createDirtyPage(pageMemory);
                } while (!flushDirtyPageForReplacementFuture.isDone());

                // Let's write the dirty pages to disk and complete the checkpoint.
                finishWaitWriteMetaToBufferFuture.complete(null);
            } finally {
                checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
            }

            checkpointProgress.futureFor(FINISHED).get(1, SECONDS);
        } finally {
            finishWaitWriteMetaToBufferFuture.complete(null);

            closeAll(
                    () -> pageMemory.stop(true),
                    checkpointManager::stop,
                    filePageStoreManager::stop
            );
        }
    }

    /**
     * Tests that {@link PersistentPageMemory#acquirePage(int, long)} works correctly when multiple threads try to acquire the same page
     * using different {@code pageId} values, assuming that one of the threads simply has an invalid identifier from some obsolete source.
     */
    @Test
    void testAcquireRace(@WorkDirectory Path workDir) throws Exception {
        int pages = 100;

        // Step 1. Start the region, allocate a number of pages, checkpoint them to the storage, stop the region.
        FilePageStoreManager filePageStoreManager = createFilePageStoreManager(workDir);
        PartitionMetaManager partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, StoragePartitionMeta.FACTORY);
        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(
                CheckpointConfiguration.builder().build(),
                filePageStoreManager,
                partitionMetaManager,
                dataRegions
        );

        int systemPageSize = PAGE_SIZE + PAGE_OVERHEAD;

        dataRegionSize = 1024 * systemPageSize;

        PersistentPageMemory pageMemory = createPageMemory(
                new long[]{dataRegionSize},
                128 * systemPageSize,
                filePageStoreManager,
                checkpointManager,
                shouldNotHappenFlushDirtyPageForReplacement()
        );

        dataRegions.add(new TestDataRegion<>(pageMemory));

        filePageStoreManager.start();
        checkpointManager.start();

        try {
            initGroupFilePageStores(filePageStoreManager, partitionMetaManager, checkpointManager, pageMemory);

            checkpointManager.checkpointTimeoutLock().checkpointReadLock();

            try {
                for (int i = 0; i < pages; i++) {
                    createDirtyPage(pageMemory);
                }
            } finally {
                checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
            }

            CompletableFuture<Void> checkpointFuture = checkpointManager
                    .forceCheckpoint("before-stopping-in-test")
                    .futureFor(FINISHED);

            assertThat(checkpointFuture, willCompleteSuccessfully());
        } finally {
            closeAll(
                    () -> pageMemory.stop(true),
                    checkpointManager::stop,
                    filePageStoreManager::stop
            );
        }

        // Step 2. Start a new region over the same persistence. The goal here is to test "acquirePage" that will actually read data from
        // the storage instead of hitting the cached page.
        PersistentPageMemory pageMemory2 = createPageMemory(
                new long[]{dataRegionSize},
                128 * systemPageSize,
                filePageStoreManager,
                checkpointManager,
                shouldNotHappenFlushDirtyPageForReplacement()
        );

        filePageStoreManager.start();
        checkpointManager.start();

        try {
            initGroupFilePageStores(filePageStoreManager, partitionMetaManager, checkpointManager, pageMemory2);

            for (int i = 0; i < pages; i++) {
                // We skip meta page, that's why we add 1 to i.
                long fakePageId = PageIdUtils.pageId(PARTITION_ID, PageIdAllocator.FLAG_AUX, i + 1);
                long realPageId = PageIdUtils.pageId(PARTITION_ID, PageIdAllocator.FLAG_DATA, i + 1);

                // Step 3. Run the race for all pages in the partition.
                // It's fine to not release/unlock these pages, we stop the region immediately after.
                IgniteTestUtils.runRace(
                        () -> pageMemory2.acquirePage(GRP_ID, fakePageId),
                        () -> {
                            long page = pageMemory2.acquirePage(GRP_ID, realPageId);

                            assertNotEquals(0L, pageMemory2.readLock(GRP_ID, realPageId, page));
                        }
                );
            }
        } finally {
            closeAll(
                    () -> pageMemory2.stop(true),
                    checkpointManager::stop,
                    filePageStoreManager::stop
            );
        }
    }

    private PersistentPageMemory createPageMemory(
            long[] segmentSizes,
            long checkpointBufferSize,
            @Nullable FilePageStoreManager filePageStoreManager,
            @Nullable CheckpointManager checkpointManager,
            WriteDirtyPage flushDirtyPageForReplacement
    ) {
        return new PersistentPageMemory(
                PersistentDataRegionConfiguration.builder().pageSize(PAGE_SIZE).size(dataRegionSize).build(),
                new PersistentPageMemoryMetricSource("test"),
                ioRegistry,
                segmentSizes,
                checkpointBufferSize,
                filePageStoreManager == null ? new TestPageReadWriteManager() : filePageStoreManager,
                flushDirtyPageForReplacement,
                checkpointManager == null ? mockCheckpointTimeoutLock(true) : checkpointManager.checkpointTimeoutLock(),
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL),
                checkpointManager == null ? new PartitionDestructionLockManager() : checkpointManager.partitionDestructionLockManager()
        );
    }

    private DirtyFullPageId createDirtyPage(PersistentPageMemory pageMemory) throws Exception {
        FullPageId fullPageId = allocatePage(pageMemory);

        long page = pageMemory.acquirePage(fullPageId.groupId(), fullPageId.pageId());

        try {
            writePage(pageMemory, fullPageId, page, 100);

            return new DirtyFullPageId(fullPageId.pageId(), fullPageId.groupId(), partitionGeneration(page));
        } finally {
            pageMemory.releasePage(fullPageId.groupId(), fullPageId.pageId(), page);
        }
    }

    private static long[] defaultSegmentSizes() {
        return LongStream.range(0, 9).map(i -> 5 * MiB).toArray();
    }

    private static long defaultCheckpointBufferSize() {
        return 5 * MiB;
    }

    private static WriteDirtyPage shouldNotHappenFlushDirtyPageForReplacement() {
        return (fullPageId, buf, tag) -> fail("Should not happen");
    }

    private CheckpointManager createCheckpointManager(
            CheckpointConfiguration checkpointConfig,
            FilePageStoreManager filePageStoreManager,
            PartitionMetaManager partitionMetaManager,
            Collection<DataRegion<PersistentPageMemory>> dataRegions
    ) throws Exception {
        return new CheckpointManager(
                "test",
                null,
                mock(FailureManager.class),
                checkpointConfig,
                filePageStoreManager,
                partitionMetaManager,
                dataRegions,
                ioRegistry,
                mock(LogSyncer.class),
                executorService,
                new CheckpointMetricSource("test"),
                PAGE_SIZE
        );
    }

    private static FilePageStoreManager createFilePageStoreManager(Path storagePath) {
        return new FilePageStoreManager(
                "test",
                storagePath,
                new RandomAccessFileIoFactory(),
                PAGE_SIZE,
                mock(FailureManager.class));
    }

    private static void initGroupFilePageStores(
            FilePageStoreManager filePageStoreManager,
            PartitionMetaManager partitionMetaManager,
            CheckpointManager checkpointManager,
            PersistentPageMemory pageMemory
    ) throws Exception {
        int partitions = PARTITION_ID + 1;

        checkpointManager.checkpointTimeoutLock().checkpointReadLock();

        ByteBuffer buffer = null;

        try {
            buffer = allocateBuffer(PAGE_SIZE);

            for (int partition = 0; partition < partitions; partition++) {
                GroupPartitionId groupPartitionId = new GroupPartitionId(GRP_ID, partition);

                FilePageStore filePageStore = filePageStoreManager.readOrCreateStore(groupPartitionId, buffer.rewind());

                filePageStore.ensure();

                CheckpointProgress lastCheckpointProgress = checkpointManager.lastCheckpointProgress();

                StoragePartitionMeta partitionMeta = (StoragePartitionMeta) partitionMetaManager.readOrCreateMeta(
                        lastCheckpointProgress == null ? null : lastCheckpointProgress.id(),
                        groupPartitionId,
                        filePageStore,
                        buffer.rewind(),
                        pageMemory.partGeneration(groupPartitionId.getGroupId(), groupPartitionId.getPartitionId())
                );

                filePageStore.setPageAllocationListener(pageIdx -> {
                    assert checkpointManager.checkpointTimeoutLock().checkpointLockIsHeldByThread();

                    CheckpointProgress last = checkpointManager.lastCheckpointProgress();

                    partitionMeta.incrementPageCount(last == null ? null : last.id());
                });

                filePageStore.pages(partitionMeta.pageCount());

                filePageStoreManager.addStore(groupPartitionId, filePageStore);
                partitionMetaManager.addMeta(groupPartitionId, partitionMeta);
            }
        } finally {
            ByteBuffer bufferToClose = buffer;

            closeAll(
                    bufferToClose == null ? null : () -> freeBuffer(bufferToClose),
                    () -> checkpointManager.checkpointTimeoutLock().checkpointReadUnlock()
            );
        }
    }

    @Test
    public void testLoadedPagesCount() {
        PageMemory mem = memory();

        int expPages = MAX_MEMORY_SIZE / mem.systemPageSize();

        try {
            assertDoesNotThrow(() -> {
                for (int i = 0; i < expPages * 2; i++) {
                    allocatePage(mem);
                }
            });
        } finally {
            mem.stop(true);
        }
    }

    @Test
    void testPartitionGenerationAfterAllocatePage() throws Exception {
        runWithStartedPersistentPageMemory(mem -> {
            FullPageId fullPageId = allocatePage(mem);

            // Absolute memory pointer to page with header.
            long absPtr = mem.acquirePage(fullPageId.groupId(), fullPageId.pageId());

            assertEquals(1, partitionGeneration(absPtr));
        });
    }

    @Test
    void testPartitionGenerationAfterAllocatePageAndInvalidatePartition() throws Exception {
        runWithStartedPersistentPageMemory(mem -> {
            assertEquals(2, mem.invalidate(GRP_ID, PARTITION_ID));

            FullPageId fullPageId = allocatePage(mem);

            // Absolute memory pointer to page with header.
            long absPtr = mem.acquirePage(fullPageId.groupId(), fullPageId.pageId());

            assertEquals(2, partitionGeneration(absPtr));
        });
    }

    @Test
    void testPartitionGenerationAfterCheckpointWritePageAndInvalidatePartition() throws Exception {
        runWithStartedPersistentPageMemory(mem -> {
            DirtyFullPageId fullPageId = allocateDirtyPage(mem);

            mem.beginCheckpoint(new CheckpointProgressImpl(42));

            assertEquals(2, mem.invalidate(GRP_ID, PARTITION_ID));

            mem.checkpointWritePage(
                    fullPageId,
                    ByteBuffer.allocate(mem.pageSize()),
                    (fullPageId1, buf, tag) -> {},
                    new CheckpointMetricsTracker(),
                    true
            );

            // Absolute memory pointer to page with header.
            long absPtr = mem.acquirePage(fullPageId.groupId(), fullPageId.pageId());

            assertEquals(2, partitionGeneration(absPtr));
        });
    }

    private void runWithStartedPersistentPageMemory(ConsumerX<PersistentPageMemory> c) throws Exception {
        PersistentPageMemory mem = (PersistentPageMemory) memory();

        try {
            c.accept(mem);
        } finally {
            mem.stop(true);
        }
    }

    @FunctionalInterface
    private interface ConsumerX<T> {
        void accept(T t) throws Exception;
    }

    /**
     * Allocates dirty page.
     *
     * @param mem Memory.
     * @throws IgniteInternalCheckedException If failed.
     */
    public static DirtyFullPageId allocateDirtyPage(PersistentPageMemory mem) throws IgniteInternalCheckedException {
        long pageId = mem.allocatePageNoReuse(GRP_ID, PARTITION_ID, PageIdAllocator.FLAG_DATA);

        long page = mem.acquirePage(GRP_ID, pageId);

        try {
            return new DirtyFullPageId(pageId, GRP_ID, partitionGeneration(page));
        } finally {
            mem.releasePage(GRP_ID, pageId, page);
        }
    }
}
