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

package org.apache.ignite.internal.pagememory.persistence.replacement;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestSimpleValuePageIo;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.CheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.ReplacementMode;
import org.apache.ignite.internal.pagememory.persistence.FakePartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for testing various page replacement policies.
 */
@ExtendWith({ConfigurationExtension.class, ExecutorServiceExtension.class})
public abstract class AbstractPageReplacementTest extends IgniteAbstractTest {
    private static final String NODE_NAME = "test";

    private static final int GROUP_ID = 1;

    private static final int PARTITION_ID = 0;

    private static final int PARTITION_COUNT = 1;

    private static final int PAGE_SIZE = 512;

    private static final int PAGE_COUNT = 1024;

    private static final int MAX_MEMORY_SIZE = PAGE_COUNT * PAGE_SIZE;

    private FilePageStoreManager filePageStoreManager;

    private PartitionMetaManager partitionMetaManager;

    private CheckpointManager checkpointManager;

    private PersistentPageMemory pageMemory;

    @InjectExecutorService
    private ExecutorService executorService;

    protected abstract ReplacementMode replacementMode();

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
                failureManager,
                CheckpointConfiguration.builder().checkpointThreads(1).build(),
                filePageStoreManager,
                partitionMetaManager,
                dataRegionList,
                ioRegistry,
                mock(LogSyncer.class),
                executorService,
                new CheckpointMetricSource("test"),
                PAGE_SIZE
        );

        pageMemory = new PersistentPageMemory(
                PersistentDataRegionConfiguration.builder()
                        .pageSize(PAGE_SIZE).size(MAX_MEMORY_SIZE).replacementMode(replacementMode()).build(),
                new PersistentPageMemoryMetricSource("test"),
                ioRegistry,
                new long[]{MAX_MEMORY_SIZE},
                10 * MiB,
                filePageStoreManager,
                checkpointManager::writePageToFilePageStore,
                checkpointManager.checkpointTimeoutLock(),
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL),
                checkpointManager.partitionDestructionLockManager()
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

    /** Checks that page replacement will occur after the start of the checkpoint and before its end. */
    @Test
    void testPageReplacement() throws Throwable {
        var startWritePagesOnCheckpointFuture = new CompletableFuture<Void>();
        var continueWritePagesOnCheckpointFuture = new CompletableFuture<Void>();

        var startWritePagesOnPageReplacementFuture = new CompletableFuture<Void>();

        CheckpointProgress checkpointProgress = inCheckpointReadLock(() -> {
            // We make only one dirty page for the checkpoint to work.
            createAndFillTestSimpleValuePages(1);

            FilePageStore filePageStore = filePageStoreManager.getStore(new GroupPartitionId(GROUP_ID, PARTITION_ID));

            // Blocking checkpoint at the moment when it tries to create a delta file for the partition's meta page.
            doAnswer(invocation -> {
                startWritePagesOnCheckpointFuture.complete(null);

                assertThat(continueWritePagesOnCheckpointFuture, willCompleteSuccessfully());

                return invocation.callRealMethod();
            }).when(filePageStore).getOrCreateNewDeltaFile(any(), any());

            // After checkpoint is blocked, writes will be done only by page replacements.
            doAnswer(invocation -> {
                if (startWritePagesOnCheckpointFuture.isDone()) {
                    startWritePagesOnPageReplacementFuture.complete(null);
                }

                return invocation.callRealMethod();
            }).when(filePageStore).write(anyLong(), any());

            // Trigger checkpoint so that it writes a meta page and one dirty one. We do it under a read lock to ensure that the background
            // does not start after the lock is released.
            return checkpointManager.forceCheckpoint("for test");
        });

        CompletableFuture<Void> finishCheckpointFuture = checkpointProgress.futureFor(FINISHED);

        // Let's wait for the checkpoint writer to start writing the first page.
        assertThat(startWritePagesOnCheckpointFuture, willCompleteSuccessfully());
        // Let's make sure that no one has tried to write another page.
        assertFalse(startWritePagesOnPageReplacementFuture.isDone());

        // We will create dirty pages until the page replacement occurs.
        // Asynchronously so as not to get into dead locks or something like that.
        assertThat(
                runAsync(() -> inCheckpointReadLock(
                        () -> createAndFillTestSimpleValuePages(() -> !startWritePagesOnPageReplacementFuture.isDone())
                )),
                willCompleteSuccessfully()
        );
        assertFalse(finishCheckpointFuture.isDone());

        continueWritePagesOnCheckpointFuture.complete(null);
        assertThat(finishCheckpointFuture, willCompleteSuccessfully());
        assertTrue(pageMemory.pageReplacementOccurred());
    }

    @Test
    void testFsyncDeltaFilesWillNotStartOnCheckpointUntilPageReplacementIsComplete() throws Exception {
        var startWritePagesOnCheckpointFuture = new CompletableFuture<Void>();
        var continueWritePagesOnCheckpointFuture = new CompletableFuture<Void>();

        var startWritePagesOnPageReplacementFuture = new CompletableFuture<Void>();
        var continueWritePagesOnPageReplacementFuture = new CompletableFuture<Void>();

        var deltaFileIoFuture = new CompletableFuture<DeltaFilePageStoreIo>();

        CheckpointProgress checkpointProgress = inCheckpointReadLock(() -> {
            // We make only one dirty page for the checkpoint to work.
            createAndFillTestSimpleValuePages(1);

            FilePageStore filePageStore = filePageStoreManager.getStore(new GroupPartitionId(GROUP_ID, PARTITION_ID));

            // Blocking checkpoint at the moment when it tries to create a delta file for the partition's meta page.
            doAnswer(invocation -> {
                CompletableFuture<DeltaFilePageStoreIo> callRealMethodResult =
                        (CompletableFuture<DeltaFilePageStoreIo>) invocation.callRealMethod();

                callRealMethodResult = callRealMethodResult
                        .handle((deltaFilePageStoreIo, throwable) -> {
                            if (throwable != null) {
                                deltaFileIoFuture.completeExceptionally(throwable);

                                throw new CompletionException(throwable);
                            } else {
                                deltaFilePageStoreIo = spy(deltaFilePageStoreIo);

                                deltaFileIoFuture.complete(deltaFilePageStoreIo);

                                return deltaFilePageStoreIo;
                            }
                        });

                startWritePagesOnCheckpointFuture.complete(null);

                assertThat(continueWritePagesOnCheckpointFuture, willCompleteSuccessfully());

                return callRealMethodResult;
            }).when(filePageStore).getOrCreateNewDeltaFile(any(), any());

            // After checkpoint is blocked, writes will be done only by page replacements.
            doAnswer(invocation -> {
                if (startWritePagesOnCheckpointFuture.isDone()) {
                    startWritePagesOnPageReplacementFuture.complete(null);

                    assertThat(continueWritePagesOnPageReplacementFuture, willCompleteSuccessfully());
                }

                return invocation.callRealMethod();
            }).when(filePageStore).write(anyLong(), any());

            doReturn(deltaFileIoFuture).when(filePageStore).getNewDeltaFile();

            doAnswer(invocation -> {
                assertThat(deltaFileIoFuture, willCompleteSuccessfully());

                return deltaFileIoFuture.join();
            }).doReturn(null).when(filePageStore).getDeltaFileToCompaction();

            doAnswer(invocation -> {
                DeltaFilePageStoreIo argument = invocation.getArgument(0);

                assertThat(deltaFileIoFuture, willBe(argument));

                return true;
            }).when(filePageStore).removeDeltaFile(any());

            // Trigger checkpoint so that it writes a meta page and one dirty one. We do it under a read lock to ensure that the background
            // does not start after the lock is released.
            return checkpointManager.forceCheckpoint("for test");
        });

        CompletableFuture<Void> finishCheckpointFuture = checkpointProgress.futureFor(FINISHED);

        // Let's wait for the checkpoint writer to start writing the first page.
        assertThat(startWritePagesOnCheckpointFuture, willCompleteSuccessfully());
        assertThat(deltaFileIoFuture, willCompleteSuccessfully());

        // We will create dirty pages until the page replacement occurs.
        // Asynchronously so as not to get into dead locks or something like that.

        CompletableFuture<Void> createPagesForPageReplacementFuture = runAsync(
                () -> inCheckpointReadLock(() -> createAndFillTestSimpleValuePages(() -> !startWritePagesOnPageReplacementFuture.isDone()))
        );
        assertThat(startWritePagesOnPageReplacementFuture, willCompleteSuccessfully());
        assertFalse(createPagesForPageReplacementFuture.isDone());

        // Let's release the checkpoint and make sure that it does not complete and fsync does not occur until the page replacement is
        // complete.
        continueWritePagesOnCheckpointFuture.complete(null);
        assertThat(finishCheckpointFuture, willTimeoutFast());
        // 250 by analogy with willTimeoutFast().
        verify(deltaFileIoFuture.join(), timeout(250).times(0)).sync();
        assertFalse(createPagesForPageReplacementFuture.isDone());

        // Let's release the page replacement and make sure everything ends well.
        continueWritePagesOnPageReplacementFuture.complete(null);

        assertThat(finishCheckpointFuture, willCompleteSuccessfully());
        assertThat(createPagesForPageReplacementFuture, willCompleteSuccessfully());
        verify(deltaFileIoFuture.join()).sync();
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
                        buffer.rewind(),
                        pageMemory.partGeneration(groupPartitionId.getGroupId(), groupPartitionId.getPartitionId())
                );

                filePageStore.pages(partitionMeta.pageCount());

                filePageStore.setPageAllocationListener(pageIdx -> {
                    assert checkpointManager.checkpointTimeoutLock().checkpointLockIsHeldByThread();

                    CheckpointProgress checkpointProgress = checkpointManager.lastCheckpointProgress();

                    partitionMeta.incrementPageCount(checkpointProgress == null ? null : checkpointProgress.id());
                });

                filePageStoreManager.addStore(groupPartitionId, spy(filePageStore));
                partitionMetaManager.addMeta(groupPartitionId, partitionMeta);
            }
        } finally {
            freeBuffer(buffer);
        }
    }

    private <V> V inCheckpointReadLock(Callable<V> callable) throws Exception {
        checkpointManager.checkpointTimeoutLock().checkpointReadLock();

        try {
            return callable.call();
        } finally {
            checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
        }
    }

    private void inCheckpointReadLock(RunnableX runnableX) throws Throwable {
        checkpointManager.checkpointTimeoutLock().checkpointReadLock();

        try {
            runnableX.run();
        } finally {
            checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
        }
    }

    private void createAndFillTestSimpleValuePages(int pageCount) throws Exception {
        for (int i = 0; i < pageCount; i++) {
            createAndFillTestSimpleValuePage(pageMemory.allocatePage(null, GROUP_ID, PARTITION_ID, FLAG_DATA));
        }
    }

    private void createAndFillTestSimpleValuePages(BooleanSupplier continuePredicate) throws Exception {
        while (continuePredicate.getAsBoolean()) {
            createAndFillTestSimpleValuePage(pageMemory.allocatePage(null, GROUP_ID, PARTITION_ID, FLAG_DATA));
        }
    }
}
