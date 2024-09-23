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

package org.apache.ignite.internal.pagememory.tree.persistence;

import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestSimpleValuePageIo;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.persistence.FakePartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Integration tests for testing page replacement. */
@ExtendWith({WorkDirectoryExtension.class, ConfigurationExtension.class})
public class ItPageReplacementTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "test";

    private static final int GROUP_ID = 1;

    private static final int PARTITION_ID = 0;

    private static final int PARTITION_COUNT = 1;

    private static final int PAGE_SIZE = 512;

    private static final int PAGE_COUNT = 1024;

    private static final int MAX_MEMORY_SIZE = PAGE_COUNT * PAGE_SIZE;

    private static final int CPUS = Math.min(4, Runtime.getRuntime().availableProcessors());

    @WorkDirectory
    private Path workDir;

    @InjectConfiguration
    private PageMemoryCheckpointConfiguration checkpointConfig;

    @InjectConfiguration(
            polymorphicExtensions = PersistentPageMemoryProfileConfigurationSchema.class,
            value = "mock = {"
                    + "engine=aipersist, "
                    + "size=" + MAX_MEMORY_SIZE
                    + "}"
    )
    private StorageProfileConfiguration storageProfileCfg;

    private FilePageStoreManager filePageStoreManager;

    private PartitionMetaManager partitionMetaManager;

    private CheckpointManager checkpointManager;

    private PersistentPageMemory pageMemory;

    @BeforeEach
    void setUp() throws Exception {
        FailureManager failureManager = mock(FailureManager.class);

        var ioRegistry = new TestPageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        filePageStoreManager = new FilePageStoreManager(
                NODE_NAME,
                workDir,
                new RandomAccessFileIoFactory(),
                PAGE_SIZE,
                failureManager
        );

        partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, FakePartitionMeta.FACTORY);

        var dataRegionList = new ArrayList<DataRegion<PersistentPageMemory>>();

        checkpointManager = new CheckpointManager(
                NODE_NAME,
                null,
                null,
                failureManager,
                checkpointConfig,
                filePageStoreManager,
                partitionMetaManager,
                dataRegionList,
                ioRegistry,
                mock(LogSyncer.class),
                PAGE_SIZE
        );

        pageMemory = new PersistentPageMemory(
                (PersistentPageMemoryProfileConfiguration) fixConfiguration(storageProfileCfg),
                ioRegistry,
                LongStream.range(0, CPUS).map(i -> MAX_MEMORY_SIZE / CPUS).toArray(),
                10 * MiB,
                filePageStoreManager,
                null,
                (pageMemory0, fullPageId, buf) -> checkpointManager.writePageToDeltaFilePageStore(pageMemory0, fullPageId, buf, true),
                checkpointManager.checkpointTimeoutLock(),
                PAGE_SIZE
        );

        dataRegionList.add(() -> pageMemory);

        filePageStoreManager.start();
        checkpointManager.start();
        pageMemory.start();

        createPartitionFilePageStoresIfMissing();
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                checkpointManager == null ? null : checkpointManager::stop,
                pageMemory == null ? null : () -> pageMemory.stop(true),
                filePageStoreManager == null ? null : filePageStoreManager::stop
        );
    }

    @Test
    void testPageReplacement() throws Exception {
        int pageCountToCreate = 128 * PAGE_COUNT;
        int createPageThreadCount = 4;

        CompletableFuture<Long> createPagesFuture = runMultiThreadedAsync(
                () -> {
                    // The values are taken empirically, if make the batch size more than 10, then sometimes it falls with IOOM.
                    createAndFillTestSimpleValuePages(pageCountToCreate, 10);

                    return null;
                },
                createPageThreadCount,
                "allocate-page-thread"
        );

        assertThat(createPagesFuture, willCompleteSuccessfully());

        assertTrue(pageMemory.isPageReplacementOccurs());

        // Let's flush everything to disk just in case, so as not to lose anything.
        assertThat(checkpointManager.forceCheckpoint("after write all pages").futureFor(FINISHED), willCompleteSuccessfully());

        // Let's restart everything, but we won't delete anything.
        restartAll();

        // Let's read all the pages and make sure they have the correct content.
        for (int pageIdx = 1; pageIdx < pageCountToCreate * createPageThreadCount; pageIdx++) {
            readAndCheckContentTestSimpleValuePage(pageIdx);
        }
    }

    private void createAndFillTestSimpleValuePages(int pageCountToCreate, int batchSize) throws Exception {
        for (int i = 0; i < pageCountToCreate; ) {
            checkpointManager.checkpointTimeoutLock().checkpointReadLock();

            try {
                for (int j = 0; j < batchSize && i < pageCountToCreate; j++, i++) {
                    createAndFillTestSimpleValuePage(pageMemory.allocatePage(null, GROUP_ID, PARTITION_ID, FLAG_DATA));
                }
            } finally {
                checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
            }
        }
    }

    private void createAndFillTestSimpleValuePage(long pageId) throws Exception {
        long page = pageMemory.acquirePage(GROUP_ID, pageId);

        try {
            long pageAddr = pageMemory.writeLock(GROUP_ID, pageId, page);

            try {
                new TestSimpleValuePageIo().initNewPage(pageAddr, pageId, PAGE_SIZE);

                TestSimpleValuePageIo.setLongValue(pageAddr, pageIndex(pageId) * 3L);
            } finally {
                pageMemory.writeUnlock(GROUP_ID, pageId, page, true);
            }
        } finally {
            pageMemory.releasePage(GROUP_ID, pageId, page);
        }
    }

    private void readAndCheckContentTestSimpleValuePage(int pageIdx) throws Exception {
        long pageId = pageId(PARTITION_ID, FLAG_DATA, pageIdx);
        long page = pageMemory.acquirePage(GROUP_ID, pageId);

        try {
            long pageAddr = pageMemory.readLock(GROUP_ID, pageId, page);

            try {
                assertEquals(pageIdx * 3L, TestSimpleValuePageIo.getLongValue(pageAddr));
            } finally {
                pageMemory.readUnlock(GROUP_ID, pageId, page);
            }
        } finally {
            pageMemory.releasePage(GROUP_ID, pageId, page);
        }
    }

    private void createPartitionFilePageStoresIfMissing() throws Exception {
        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        try {
            for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                var groupPartitionId = new GroupPartitionId(GROUP_ID, partitionId);

                FilePageStore filePageStore = filePageStoreManager.readOrCreateStore(groupPartitionId, buffer.rewind());

                filePageStore.ensure();

                PartitionMeta partitionMeta = partitionMetaManager.readOrCreateMeta(
                        null,
                        groupPartitionId,
                        filePageStore,
                        buffer.rewind()
                );

                filePageStore.pages(partitionMeta.pageCount());

                filePageStore.setPageAllocationListener(pageIdx -> {
                    assert checkpointManager.checkpointTimeoutLock().checkpointLockIsHeldByThread();

                    CheckpointProgress checkpointProgress = checkpointManager.lastCheckpointProgress();

                    partitionMeta.incrementPageCount(checkpointProgress == null ? null : checkpointProgress.id());
                });

                filePageStoreManager.addStore(groupPartitionId, filePageStore);
                partitionMetaManager.addMeta(groupPartitionId, partitionMeta);
            }
        } finally {
            freeBuffer(buffer);
        }
    }

    private void restartAll() throws Exception {
        tearDown();
        setUp();
    }
}
