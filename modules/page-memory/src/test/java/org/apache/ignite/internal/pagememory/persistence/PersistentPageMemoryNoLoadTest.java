/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagememory.persistence;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.PAGE_OVERHEAD;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.mockCheckpointTimeoutLock;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.LongStream;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemoryNoLoadSelfTest;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@link PersistentPageMemory}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class PersistentPageMemoryNoLoadTest extends VolatilePageMemoryNoLoadSelfTest {
    @InjectConfiguration(polymorphicExtensions = UnsafeMemoryAllocatorConfigurationSchema.class)
    private PersistentPageMemoryDataRegionConfiguration dataRegionCfg;

    @BeforeEach
    void setUp() throws Exception {
        dataRegionCfg.change(c -> c.changeSize(MAX_MEMORY_SIZE)).get(1, SECONDS);
    }

    /** {@inheritDoc} */
    @Override
    protected PageMemory memory() {
        return createPageMemoryImpl(defaultSegmentSizes(), defaultCheckpointBufferSize(), null, null);
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

        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(checkpointConfig, workDir, filePageStoreManager, dataRegions);

        PersistentPageMemory pageMemory = createPageMemoryImpl(
                defaultSegmentSizes(),
                defaultCheckpointBufferSize(),
                filePageStoreManager,
                checkpointManager
        );

        dataRegions.add(() -> pageMemory);

        filePageStoreManager.start();

        checkpointManager.start();

        pageMemory.start();

        try {
            initGroupFilePageStores(filePageStoreManager);

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
    void testSafeToUpdate(
            @InjectConfiguration PageMemoryCheckpointConfiguration checkpointConfig,
            @WorkDirectory Path workDir
    ) throws Exception {
        FilePageStoreManager filePageStoreManager = createFilePageStoreManager(workDir);

        Collection<DataRegion<PersistentPageMemory>> dataRegions = new ArrayList<>();

        CheckpointManager checkpointManager = createCheckpointManager(checkpointConfig, workDir, filePageStoreManager, dataRegions);

        long systemPageSize = PAGE_SIZE + PAGE_OVERHEAD;

        dataRegionCfg.change(c -> c.changeSize(128 * systemPageSize)).get(1, SECONDS);

        PersistentPageMemory pageMemory = createPageMemoryImpl(
                new long[]{100 * systemPageSize},
                28 * systemPageSize,
                filePageStoreManager,
                checkpointManager
        );

        dataRegions.add(() -> pageMemory);

        filePageStoreManager.start();

        checkpointManager.start();

        pageMemory.start();

        try {
            initGroupFilePageStores(filePageStoreManager);

            long maxPages = pageMemory.totalPages();

            long maxDirtyPages = (maxPages * 3 / 4);

            assertThat(maxDirtyPages, greaterThanOrEqualTo(50L));

            checkpointManager.checkpointTimeoutLock().checkpointReadLock();

            try {
                for (int i = 0; i < maxDirtyPages - 1; i++) {
                    createDirtyPage(pageMemory);

                    assertTrue(pageMemory.safeToUpdate(), "i=" + i);
                }

                for (int i = (int) maxDirtyPages - 1; i < maxPages; i++) {
                    createDirtyPage(pageMemory);

                    assertFalse(pageMemory.safeToUpdate(), "i=" + i);
                }
            } finally {
                checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
            }

            checkpointManager
                    .forceCheckpoint("for_test_safe_to_update")
                    .futureFor(FINISHED)
                    .get(1, SECONDS);

            assertTrue(pageMemory.safeToUpdate());
        } finally {
            closeAll(
                    () -> pageMemory.stop(true),
                    checkpointManager::stop,
                    filePageStoreManager::stop
            );
        }
    }

    protected PersistentPageMemory createPageMemoryImpl(
            long[] segmentSizes,
            long checkpointBufferSize,
            @Nullable FilePageStoreManager filePageStoreManager,
            @Nullable CheckpointManager checkpointManager
    ) {
        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PersistentPageMemory(
                dataRegionCfg,
                ioRegistry,
                segmentSizes,
                checkpointBufferSize,
                filePageStoreManager == null ? new TestPageReadWriteManager() : filePageStoreManager,
                null,
                (fullPageId, buf, tag) -> fail("Should not happen"),
                checkpointManager == null ? mockCheckpointTimeoutLock(log, true) : checkpointManager.checkpointTimeoutLock(),
                PAGE_SIZE
        );
    }

    protected FullPageId createDirtyPage(PersistentPageMemory pageMemoryImpl) throws Exception {
        FullPageId fullPageId = allocatePage(pageMemoryImpl);

        long page = pageMemoryImpl.acquirePage(fullPageId.groupId(), fullPageId.pageId());

        try {
            writePage(pageMemoryImpl, fullPageId, page, 100);
        } finally {
            pageMemoryImpl.releasePage(fullPageId.groupId(), fullPageId.pageId(), page);
        }

        return fullPageId;
    }

    private static long[] defaultSegmentSizes() {
        return LongStream.range(0, 9).map(i -> 5 * MiB).toArray();
    }

    private static long defaultCheckpointBufferSize() {
        return 5 * MiB;
    }

    private static CheckpointManager createCheckpointManager(
            PageMemoryCheckpointConfiguration checkpointConfig,
            Path storagePath,
            FilePageStoreManager filePageStoreManager,
            Collection<DataRegion<PersistentPageMemory>> dataRegions
    ) throws Exception {
        return new CheckpointManager(
                "test",
                null,
                null,
                checkpointConfig,
                filePageStoreManager,
                dataRegions,
                storagePath,
                PAGE_SIZE
        );
    }

    private static FilePageStoreManager createFilePageStoreManager(Path storagePath) throws Exception {
        return new FilePageStoreManager(log, "test", storagePath, new RandomAccessFileIoFactory(), PAGE_SIZE);
    }

    private static void initGroupFilePageStores(FilePageStoreManager filePageStoreManager) throws Exception {
        filePageStoreManager.initialize("Test", GRP_ID, PARTITION_ID + 1);

        for (FilePageStore filePageStore : filePageStoreManager.getStores(GRP_ID)) {
            filePageStore.ensure();
        }
    }
}
