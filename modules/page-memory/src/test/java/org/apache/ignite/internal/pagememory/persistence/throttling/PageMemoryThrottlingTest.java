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

package org.apache.ignite.internal.pagememory.persistence.throttling;

import static org.apache.ignite.internal.pagememory.persistence.throttling.PagesWriteThrottlePolicy.DEFAULT_LOGGING_THRESHOLD;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.ignite.internal.components.NoOpLogSyncer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.TestDataRegion;
import org.apache.ignite.internal.pagememory.configuration.CheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.freelist.io.PagesListNodeIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.FakePartitionMeta.FakePartitionMetaFactory;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests {@link PersistentPageMemory} and {@link PagesWriteThrottlePolicy} interactions.
 */
@ExtendWith({ConfigurationExtension.class, ExecutorServiceExtension.class})
public class PageMemoryThrottlingTest extends IgniteAbstractTest {
    private static final int PAGE_SIZE = 4096;

    private static final int SEGMENT_SIZE = 128 * Constants.MiB;

    private static final int CHECKPOINT_BUFFER_SIZE = 16 * Constants.MiB;

    private static final int GROUP_ID = 1;

    private static final int PART_ID = 1;

    private static PageIoRegistry ioRegistry;

    // We use very small readLock timeout here on purpose.
    private CheckpointConfiguration checkpointConfig = CheckpointConfiguration.builder()
            .checkpointThreads(1)
            .readLockTimeoutMillis(() -> 50)
            .build();

    private FilePageStoreManager pageStoreManager;

    private CheckpointManager checkpointManager;

    private PersistentPageMemory pageMemory;

    private FileIoFactory fileIoFactory;

    private DataRegion<PersistentPageMemory> dataRegion;

    @InjectExecutorService
    private ExecutorService executorService;

    @BeforeAll
    static void beforeAll() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    @AfterAll
    static void afterAll() {
        ioRegistry = null;
    }

    void setUp(ThrottlingPolicyFactory throttleFactory) throws Exception {
        FailureManager failureManager = mock(FailureManager.class);
        when(failureManager.process(any())).thenThrow(new AssertionError("Unexpected error"));

        fileIoFactory = spy(new RandomAccessFileIoFactory());
        pageStoreManager = new FilePageStoreManager("test", workDir, fileIoFactory, PAGE_SIZE, failureManager);

        List<DataRegion<PersistentPageMemory>> dataRegions = new CopyOnWriteArrayList<>();

        PartitionMetaManager partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, new FakePartitionMetaFactory());

        checkpointManager = new CheckpointManager(
                "test",
                null,
                failureManager,
                checkpointConfig,
                pageStoreManager,
                partitionMetaManager,
                dataRegions,
                ioRegistry,
                new NoOpLogSyncer(),
                executorService,
                new CheckpointMetricSource("test"),
                PAGE_SIZE
        );

        pageMemory = new PersistentPageMemory(
                PersistentDataRegionConfiguration.builder()
                        .pageSize(PAGE_SIZE)
                        .size(SEGMENT_SIZE + CHECKPOINT_BUFFER_SIZE)
                        .throttlingPolicyFactory(throttleFactory)
                        .build(),
                new PersistentPageMemoryMetricSource("test"),
                ioRegistry,
                new long[]{SEGMENT_SIZE},
                CHECKPOINT_BUFFER_SIZE,
                pageStoreManager,
                (pageMem, pageId, pageBuf) -> {
                    checkpointManager.writePageToFilePageStore(pageMem, pageId, pageBuf);

                    // Almost the same code that happens in data region, but here the region is mocked.
                    CheckpointProgress checkpointProgress = checkpointManager.currentCheckpointProgress();

                    assertNotNull(checkpointProgress);
                    checkpointProgress.evictedPagesCounter().incrementAndGet();
                },
                checkpointManager.checkpointTimeoutLock(),
                new OffheapReadWriteLock(2),
                checkpointManager.partitionDestructionLockManager()
        );

        pageStoreManager.start();
        pageMemory.start();

        dataRegion = new TestDataRegion<>(pageMemory);
        dataRegions.add(dataRegion);

        checkpointManager.start();

        GroupPartitionId groupPartitionId = new GroupPartitionId(GROUP_ID, PART_ID);

        FilePageStore filePageStore = pageStoreManager.readOrCreateStore(groupPartitionId, ByteBuffer.allocate(PAGE_SIZE));
        filePageStore.ensure();

        pageStoreManager.addStore(groupPartitionId, filePageStore);
        PartitionMeta partitionMeta = partitionMetaManager.readOrCreateMeta(
                null,
                groupPartitionId,
                filePageStore,
                ByteBuffer.allocateDirect(PAGE_SIZE).order(ByteOrder.LITTLE_ENDIAN),
                pageMemory.partGeneration(groupPartitionId.getGroupId(), groupPartitionId.getPartitionId())
        );

        partitionMetaManager.addMeta(groupPartitionId, partitionMeta);

        filePageStore.pages(partitionMeta.pageCount());
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                checkpointManager == null ? null : checkpointManager::stop,
                pageMemory == null ? null : () -> pageMemory.stop(true),
                pageStoreManager == null ? null : pageStoreManager::stop
        );
    }

    /**
     * Tests that page allocation results in {@link PagesWriteThrottlePolicy#onMarkDirty(boolean)} call.
     */
    @Test
    void pageAllocationNotifiedThrottler() throws Exception {
        PagesWriteThrottlePolicy writeThrottle = mock(PagesWriteThrottlePolicy.class);

        setUp(pm -> writeThrottle);

        checkpointManager.checkpointTimeoutLock().checkpointReadLock();

        try {
            pageMemory.allocatePageNoReuse(GROUP_ID, PART_ID, PageIdAllocator.FLAG_AUX);

            verify(writeThrottle).onMarkDirty(eq(false));
        } finally {
            checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
        }
    }

    /**
     * Tests that page write lock usage without page modification does not result in {@link PagesWriteThrottlePolicy#onMarkDirty(boolean)}.
     */
    @Test
    void pageUnlockWithoutMarkingDirty() throws Exception {
        PagesWriteThrottlePolicy writeThrottle = mock(PagesWriteThrottlePolicy.class);

        setUp(pm -> writeThrottle);

        long pageId;

        checkpointManager.checkpointTimeoutLock().checkpointReadLock();
        try {
            pageId = pageMemory.allocatePageNoReuse(GROUP_ID, PART_ID, PageIdAllocator.FLAG_AUX);

            initPage(pageId);
        } finally {
            checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
        }

        CheckpointProgress checkpointFuture = checkpointManager.forceCheckpoint("must clean the dirty page");
        assertThat(checkpointFuture.futureFor(CheckpointState.FINISHED), willCompleteSuccessfully());

        checkpointManager.checkpointTimeoutLock().checkpointReadLock();
        try {
            clearInvocations(writeThrottle);

            acquireAndReleaseWriteLock(pageId, false);

            verify(writeThrottle, times(0)).onMarkDirty(anyBoolean());
        } finally {
            checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
        }
    }

    /**
     * Tests that two consecutive page updates lead to a single {@link PagesWriteThrottlePolicy#onMarkDirty(boolean)} call.
     */
    @Test
    void pageMarkedDirtyOnlyOnce() throws Exception {
        PagesWriteThrottlePolicy writeThrottle = mock(PagesWriteThrottlePolicy.class);

        setUp(pm -> writeThrottle);

        AtomicLong pageId = new AtomicLong();

        runInLock(() -> {
            pageId.set(pageMemory.allocatePageNoReuse(GROUP_ID, PART_ID, PageIdAllocator.FLAG_AUX));

            initPage(pageId.get());
        });

        CheckpointProgress checkpointFuture = checkpointManager.forceCheckpoint("must clean the dirty page");
        assertThat(checkpointFuture.futureFor(CheckpointState.FINISHED), willCompleteSuccessfully());

        runInLock(() -> {
            clearInvocations(writeThrottle);

            acquireAndReleaseWriteLock(pageId.get(), true);
            acquireAndReleaseWriteLock(pageId.get(), true);

            verify(writeThrottle).onMarkDirty(anyBoolean());
        });
    }

    /**
     * Tests that checkpoint events are properly propagated to the throttler.
     */
    @Test
    void checkpointEvents() throws Exception {
        PagesWriteThrottlePolicy writeThrottle = mock(PagesWriteThrottlePolicy.class);

        setUp(pm -> writeThrottle);

        AtomicLong pageId = new AtomicLong();

        runInLock(() -> {
            pageId.set(pageMemory.allocatePageNoReuse(GROUP_ID, PART_ID, PageIdAllocator.FLAG_AUX));

            initPage(pageId.get());
        });

        CheckpointProgress checkpointFuture = checkpointManager.forceCheckpoint("must clean the dirty page");
        assertThat(checkpointFuture.futureFor(CheckpointState.FINISHED), willCompleteSuccessfully());

        verify(writeThrottle).onBeginCheckpoint();
        verify(writeThrottle).onFinishCheckpoint();
    }

    /**
     * Tests that page memory wakes up throttled thread while writing checkpoint buffer pages.
     *
     * @throws Exception If failed.
     */
    @Test
    void wakeupThrottledThreads() throws Exception {
        PagesWriteThrottlePolicy writeThrottle = mock(PagesWriteThrottlePolicy.class);

        setUp(pm -> writeThrottle);

        int pages = CHECKPOINT_BUFFER_SIZE / PAGE_SIZE * 9 / 10;
        long[] pageIds = new long[pages];

        runInLock(() -> {
            for (int i = 0; i < pages; i++) {
                long pageId = pageMemory.allocatePageNoReuse(GROUP_ID, PART_ID, PageIdAllocator.FLAG_AUX);

                pageIds[i] = pageId;

                initPage(pageId);
            }
        });

        CountDownLatch cdl = new CountDownLatch(1);

        doAnswer(invocation -> {
            cdl.await();

            return invocation.callRealMethod();
        }).when(fileIoFactory).create(any(), any(OpenOption[].class));

        CheckpointProgress checkpointFuture = checkpointManager.forceCheckpoint("reset dirty pages set in test");
        assertThat(checkpointFuture.futureFor(CheckpointState.LOCK_TAKEN), willCompleteSuccessfully());

        runInLock(() -> {
            for (int i = 0; i < pages; i++) {
                acquireAndReleaseWriteLock(pageIds[i], true);
            }
        });

        cdl.countDown();

        assertThat(checkpointFuture.futureFor(CheckpointState.FINISHED), willCompleteSuccessfully());

        verify(writeThrottle).wakeupThrottledThreads();
    }

    /**
     * Tests that there's no checkpoint read lock timeouts during the high load.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void hugeLoadDoesNotBreakCheckpointReadLock(boolean speedBasedThrottling) throws Exception {
        PersistentPageMemoryMetricSource metricSource = new PersistentPageMemoryMetricSource("test");

        ThrottlingPolicyFactory throttleFactory;
        if (speedBasedThrottling) {
            throttleFactory = pageMemory -> new PagesWriteSpeedBasedThrottle(
                    pageMemory,
                    checkpointManager::currentCheckpointProgress,
                    checkpointManager.checkpointTimeoutLock()::checkpointLockIsHeldByThread,
                    metricSource
            );
        } else {
            throttleFactory = pageMemory -> new TargetRatioPagesWriteThrottle(
                    DEFAULT_LOGGING_THRESHOLD,
                    pageMemory,
                    checkpointManager::currentCheckpointProgress,
                    checkpointManager.checkpointTimeoutLock()::checkpointLockIsHeldByThread,
                    metricSource
            );
        }

        setUp(throttleFactory);

        long[] pageIds = new long[SEGMENT_SIZE / PAGE_SIZE * 2];

        // Unlike regular "for" loop, "forEach" makes "i" effectively final.
        IntStream.range(0, SEGMENT_SIZE / PAGE_SIZE * 2).forEach(i -> runInLock(() -> {
            // TODO https://issues.apache.org/jira/browse/IGNITE-24877 This line should not be necessary.
            checkpointManager.markPartitionAsDirty(dataRegion, GROUP_ID, PART_ID, 1);

            long pageId = pageMemory.allocatePageNoReuse(GROUP_ID, PART_ID, PageIdAllocator.FLAG_AUX);

            pageIds[i] = pageId;

            initPage(pageId);
        }));

        for (int i = 0; i < pageIds.length * 10; i++) {
            runInLock(() -> {
                // TODO https://issues.apache.org/jira/browse/IGNITE-24877 This line should not be necessary.
                checkpointManager.markPartitionAsDirty(dataRegion, GROUP_ID, PART_ID, 1);

                long pageId = pageIds[ThreadLocalRandom.current().nextInt(pageIds.length)];

                acquireAndReleaseWriteLock(pageId, true);
            });
        }
    }

    private void runInLock(RunnableX runnable) {
        checkpointManager.checkpointTimeoutLock().checkpointReadLock();

        try {
            runnable.run();
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable e) {
            fail(e);
        } finally {
            checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
        }
    }

    private void initPage(long pageId) throws IgniteInternalCheckedException {
        long page = pageMemory.acquirePage(GROUP_ID, pageId);

        try {
            long pageAddr = pageMemory.writeLock(GROUP_ID, pageId, page, true);

            try {
                PagesListNodeIo.VERSIONS.latest().initNewPage(pageAddr, pageId, PAGE_SIZE);
            } finally {
                pageMemory.writeUnlock(GROUP_ID, pageId, page, true);
            }
        } finally {
            pageMemory.releasePage(GROUP_ID, pageId, page);
        }
    }

    private void acquireAndReleaseWriteLock(long pageId, boolean markDirty) throws IgniteInternalCheckedException {
        long page = pageMemory.acquirePage(GROUP_ID, pageId);

        try {
            long pageAddr = pageMemory.writeLock(GROUP_ID, pageId, page);

            try {
                assertNotEquals(0L, pageAddr);
            } finally {
                pageMemory.writeUnlock(GROUP_ID, pageId, page, markDirty);
            }
        } finally {
            pageMemory.releasePage(GROUP_ID, pageId, page);
        }
    }
}
