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
import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.MUST_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.NOT_REQUIRED;
import static org.apache.ignite.internal.pagememory.persistence.CheckpointUrgency.SHOULD_TRIGGER;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.PAGE_OVERHEAD;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SORTED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.mockCheckpointTimeoutLock;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.stream.LongStream;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.AbstractPageMemoryNoLoadSelfTest;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileChange;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta.PartitionMetaSnapshot;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.TestPageReadWriteManager;
import org.apache.ignite.internal.pagememory.persistence.WriteDirtyPage;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@link PersistentPageMemory}.
 */
@ExtendWith({WorkDirectoryExtension.class, ConfigurationExtension.class})
public class PersistentPageMemoryNoLoadTest extends AbstractPageMemoryNoLoadSelfTest {
    private static PageIoRegistry ioRegistry;

    @InjectConfiguration(
            polymorphicExtensions = PersistentPageMemoryProfileConfigurationSchema.class,
            value = "mock.engine = aipersist"
    )
    private StorageProfileConfiguration dataRegionCfg;

    @BeforeAll
    static void beforeAll() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    @BeforeEach
    void setUp() throws Exception {
        dataRegionCfg.change(c -> ((PersistentPageMemoryProfileChange) c).changeSize(MAX_MEMORY_SIZE)).get(1, SECONDS);
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
    void testDirtyPages(
            @InjectConfiguration PageMemoryCheckpointConfiguration checkpointConfig,
            @WorkDirectory Path workDir
    ) throws Exception {
        FilePageStoreManager filePageStoreManager = createFilePageStoreManager(workDir);

        PartitionMetaManager<StoragePartitionMeta, StoragePartitionMetaIo> partitionMetaManager =
                new PartitionMetaManager<>(ioRegistry, PAGE_SIZE,
                        StoragePartitionMeta.FACTORY, StoragePartitionMetaIo.VERSIONS);

        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(
                checkpointConfig,
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

        dataRegions.add(() -> pageMemory);

        filePageStoreManager.start();

        checkpointManager.start();

        pageMemory.start();

        try {
            initGroupFilePageStores(filePageStoreManager, partitionMetaManager, checkpointManager);

            checkpointManager.checkpointTimeoutLock().checkpointReadLock();

            try {
                Set<FullPageId> dirtyPages = Set.of(createDirtyPage(pageMemory), createDirtyPage(pageMemory));

                assertThat(pageMemory.dirtyPages(), equalTo(dirtyPages));
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
    void testCheckpointUrgency(
            @InjectConfiguration PageMemoryCheckpointConfiguration checkpointConfig,
            @WorkDirectory Path workDir
    ) throws Exception {
        FilePageStoreManager filePageStoreManager = createFilePageStoreManager(workDir);

        PartitionMetaManager<StoragePartitionMeta, StoragePartitionMetaIo> partitionMetaManager =
                new PartitionMetaManager<>(ioRegistry, PAGE_SIZE,
                        StoragePartitionMeta.FACTORY, StoragePartitionMetaIo.VERSIONS);

        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(
                checkpointConfig,
                filePageStoreManager,
                partitionMetaManager,
                dataRegions
        );

        long systemPageSize = PAGE_SIZE + PAGE_OVERHEAD;

        dataRegionCfg.change(c -> ((PersistentPageMemoryProfileChange) c).changeSize(128 * systemPageSize)).get(1, SECONDS);

        PersistentPageMemory pageMemory = createPageMemory(
                new long[]{100 * systemPageSize},
                28 * systemPageSize,
                filePageStoreManager,
                checkpointManager,
                shouldNotHappenFlushDirtyPageForReplacement()
        );

        dataRegions.add(() -> pageMemory);

        filePageStoreManager.start();

        checkpointManager.start();

        pageMemory.start();

        try {
            initGroupFilePageStores(filePageStoreManager, partitionMetaManager, checkpointManager);

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
    void testDeltaFilePageStore(
            @InjectConfiguration PageMemoryCheckpointConfiguration checkpointConfig,
            @WorkDirectory Path workDir
    ) throws Exception {
        FilePageStoreManager filePageStoreManager = spy(createFilePageStoreManager(workDir));

        PartitionMetaManager<StoragePartitionMeta, StoragePartitionMetaIo> partitionMetaManager =
                new PartitionMetaManager<>(ioRegistry, PAGE_SIZE,
                        StoragePartitionMeta.FACTORY, StoragePartitionMetaIo.VERSIONS);

        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(
                checkpointConfig,
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

        dataRegions.add(() -> pageMemory);

        filePageStoreManager.start();

        checkpointManager.start();

        pageMemory.start();

        try {
            initGroupFilePageStores(filePageStoreManager, partitionMetaManager, checkpointManager);

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
    void testPageReplacement(
            @InjectConfiguration("mock.checkpointThreads=1") PageMemoryCheckpointConfiguration checkpointConfig,
            @WorkDirectory Path workDir
    ) throws Exception {
        FilePageStoreManager filePageStoreManager = createFilePageStoreManager(workDir);

        PartitionMetaManager<StoragePartitionMeta, StoragePartitionMetaIo> partitionMetaManager =
                new PartitionMetaManager<>(ioRegistry, PAGE_SIZE,
                        StoragePartitionMeta.FACTORY, StoragePartitionMetaIo.VERSIONS);
        partitionMetaManager = spy(partitionMetaManager);

        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(
                checkpointConfig,
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

        dataRegions.add(() -> pageMemory);

        filePageStoreManager.start();

        checkpointManager.start();

        pageMemory.start();

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
            initGroupFilePageStores(filePageStoreManager, partitionMetaManager, checkpointManager);

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

    protected PersistentPageMemory createPageMemory(
            long[] segmentSizes,
            long checkpointBufferSize,
            @Nullable FilePageStoreManager filePageStoreManager,
            @Nullable CheckpointManager checkpointManager,
            WriteDirtyPage flushDirtyPageForReplacement
    ) {
        return new PersistentPageMemory(
                (PersistentPageMemoryProfileConfiguration) fixConfiguration(dataRegionCfg),
                ioRegistry,
                segmentSizes,
                checkpointBufferSize,
                filePageStoreManager == null ? new TestPageReadWriteManager() : filePageStoreManager,
                null,
                flushDirtyPageForReplacement,
                checkpointManager == null ? mockCheckpointTimeoutLock(true) : checkpointManager.checkpointTimeoutLock(),
                PAGE_SIZE
        );
    }

    protected FullPageId createDirtyPage(PersistentPageMemory pageMemory) throws Exception {
        FullPageId fullPageId = allocatePage(pageMemory);

        long page = pageMemory.acquirePage(fullPageId.groupId(), fullPageId.pageId());

        try {
            writePage(pageMemory, fullPageId, page, 100);
        } finally {
            pageMemory.releasePage(fullPageId.groupId(), fullPageId.pageId(), page);
        }

        return fullPageId;
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

    private static CheckpointManager createCheckpointManager(
            PageMemoryCheckpointConfiguration checkpointConfig,
            FilePageStoreManager filePageStoreManager,
            PartitionMetaManager partitionMetaManager,
            Collection<DataRegion<PersistentPageMemory>> dataRegions
    ) throws Exception {
        return new CheckpointManager(
                "test",
                null,
                null,
                mock(FailureProcessor.class),
                checkpointConfig,
                filePageStoreManager,
                partitionMetaManager,
                dataRegions,
                ioRegistry,
                mock(LogSyncer.class),
                PAGE_SIZE
        );
    }

    private static FilePageStoreManager createFilePageStoreManager(Path storagePath) {
        return new FilePageStoreManager(
                "test",
                storagePath,
                new RandomAccessFileIoFactory(),
                PAGE_SIZE,
                mock(FailureProcessor.class));
    }

    private static void initGroupFilePageStores(
            FilePageStoreManager filePageStoreManager,
            PartitionMetaManager<StoragePartitionMeta, StoragePartitionMetaIo> partitionMetaManager,
            CheckpointManager checkpointManager
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

                StoragePartitionMeta partitionMeta = partitionMetaManager.readOrCreateMeta(
                        lastCheckpointProgress == null ? null : lastCheckpointProgress.id(),
                        groupPartitionId,
                        filePageStore,
                        buffer.rewind()
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
}
