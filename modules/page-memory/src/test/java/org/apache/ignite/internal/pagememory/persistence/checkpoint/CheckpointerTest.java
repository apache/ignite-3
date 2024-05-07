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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static java.lang.System.nanoTime;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.DIRTY_PAGE_COMPARATOR;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.EMPTY;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.configuration.schema.PageMemoryCheckpointConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.WriteDirtyPage;
import org.apache.ignite.internal.pagememory.persistence.compaction.Compactor;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link Checkpointer} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class CheckpointerTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 1024;

    private static PageIoRegistry ioRegistry;

    @InjectConfiguration("mock : {checkpointThreads=1, frequency=1000, frequencyDeviation=0}")
    private PageMemoryCheckpointConfiguration checkpointConfig;

    @BeforeAll
    static void beforeAll() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    @AfterAll
    static void afterAll() {
        ioRegistry = null;
    }

    @Test
    void testStartAndStop() throws Exception {
        Checkpointer checkpointer = new Checkpointer(
                "test",
                null,
                null,
                mock(FailureProcessor.class),
                createCheckpointWorkflow(EMPTY),
                createCheckpointPagesWriterFactory(mock(PartitionMetaManager.class)),
                mock(FilePageStoreManager.class),
                mock(Compactor.class),
                checkpointConfig,
                mock(LogSyncer.class)
        );

        assertNull(checkpointer.runner());

        assertFalse(checkpointer.isShutdownNow());
        assertFalse(checkpointer.isCancelled());
        assertFalse(checkpointer.isDone());

        checkpointer.start();

        assertTrue(waitForCondition(() -> checkpointer.runner() != null, 10, 100));

        checkpointer.stop();

        assertTrue(waitForCondition(() -> checkpointer.runner() == null, 10, 100));

        assertTrue(checkpointer.isShutdownNow());
        assertTrue(checkpointer.isCancelled());
        assertTrue(checkpointer.isDone());
    }

    @Test
    void testScheduleCheckpoint() {
        Checkpointer checkpointer = spy(new Checkpointer(
                "test",
                null,
                null,
                mock(FailureProcessor.class),
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                mock(FilePageStoreManager.class),
                mock(Compactor.class),
                checkpointConfig,
                mock(LogSyncer.class)
        ));

        assertNull(checkpointer.lastCheckpointProgress());

        CheckpointProgressImpl scheduledProgress = (CheckpointProgressImpl) checkpointer.scheduledProgress();

        long onCreateCheckpointerNextCheckpointNanos = scheduledProgress.nextCheckpointNanos();
        String onCreateCheckpointerReason = scheduledProgress.reason();

        assertThat(
                onCreateCheckpointerNextCheckpointNanos - nanoTime(),
                allOf(greaterThan(0L), lessThanOrEqualTo(MILLISECONDS.toNanos(1_000)))
        );

        assertNull(onCreateCheckpointerReason);

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(3000, "test0"));

        assertNull(checkpointer.lastCheckpointProgress());

        assertEquals(onCreateCheckpointerNextCheckpointNanos, scheduledProgress.nextCheckpointNanos());
        assertEquals(onCreateCheckpointerReason, scheduledProgress.reason());

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(100, "test1"));

        assertNull(checkpointer.lastCheckpointProgress());

        assertNotEquals(onCreateCheckpointerNextCheckpointNanos, scheduledProgress.nextCheckpointNanos());
        assertNotEquals(onCreateCheckpointerReason, scheduledProgress.reason());

        assertThat(
                scheduledProgress.nextCheckpointNanos() - nanoTime(),
                allOf(greaterThan(0L), lessThanOrEqualTo(MILLISECONDS.toNanos(100)))
        );

        assertEquals("test1", scheduledProgress.reason());

        long scheduledNextCheckpointNanos = scheduledProgress.nextCheckpointNanos();
        String scheduledReason = scheduledProgress.reason();

        // Checks after the start of a checkpoint.

        checkpointer.startCheckpointProgress();

        assertNull(checkpointer.lastCheckpointProgress());

        checkpointer.updateLastProgressAfterReleaseWriteLock();

        CheckpointProgressImpl currentProgress = (CheckpointProgressImpl) checkpointer.lastCheckpointProgress();

        assertSame(scheduledProgress, currentProgress);

        assertNotSame(scheduledProgress, checkpointer.scheduledProgress());

        verify(checkpointer, times(1)).nextCheckpointInterval();

        scheduledProgress = (CheckpointProgressImpl) checkpointer.scheduledProgress();

        assertThat(
                scheduledProgress.nextCheckpointNanos() - nanoTime(),
                allOf(greaterThan(0L), lessThanOrEqualTo(MILLISECONDS.toNanos(1_000)))
        );

        assertEquals(scheduledNextCheckpointNanos, currentProgress.nextCheckpointNanos());
        assertEquals(scheduledReason, currentProgress.reason());

        assertSame(currentProgress, checkpointer.scheduleCheckpoint(90, "test2"));
        assertSame(currentProgress, checkpointer.lastCheckpointProgress());
        assertSame(scheduledProgress, checkpointer.scheduledProgress());

        assertEquals(scheduledNextCheckpointNanos, currentProgress.nextCheckpointNanos());
        assertEquals(scheduledReason, currentProgress.reason());

        currentProgress.transitTo(LOCK_TAKEN);

        assertSame(scheduledProgress, checkpointer.scheduleCheckpoint(90, "test3"));
        assertSame(currentProgress, checkpointer.lastCheckpointProgress());
        assertSame(scheduledProgress, checkpointer.scheduledProgress());

        assertThat(
                scheduledProgress.nextCheckpointNanos() - nanoTime(),
                allOf(greaterThan(0L), lessThanOrEqualTo(MILLISECONDS.toNanos(90)))
        );

        assertEquals("test3", scheduledProgress.reason());

        assertEquals(scheduledNextCheckpointNanos, currentProgress.nextCheckpointNanos());
        assertEquals(scheduledReason, currentProgress.reason());
    }

    @Test
    void testWaitCheckpointEvent() throws Exception {
        checkpointConfig.frequency().update(200L).get(100, MILLISECONDS);

        Checkpointer checkpointer = new Checkpointer(
                "test",
                null,
                null,
                mock(FailureProcessor.class),
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                mock(FilePageStoreManager.class),
                mock(Compactor.class),
                checkpointConfig,
                mock(LogSyncer.class)
        );

        CompletableFuture<?> waitCheckpointEventFuture = runAsync(checkpointer::waitCheckpointEvent);

        assertThrows(TimeoutException.class, () -> waitCheckpointEventFuture.get(100, MILLISECONDS));

        waitCheckpointEventFuture.get(200, MILLISECONDS);

        ((CheckpointProgressImpl) checkpointer.scheduledProgress()).nextCheckpointNanos(MILLISECONDS.toNanos(10_000));

        checkpointer.stop();

        runAsync(checkpointer::waitCheckpointEvent).get(100, MILLISECONDS);
    }

    @Test
    void testCheckpointBody() throws Exception {
        checkpointConfig.frequency().update(100L).get(100, MILLISECONDS);

        Checkpointer checkpointer = spy(new Checkpointer(
                "test",
                null,
                null,
                mock(FailureProcessor.class),
                createCheckpointWorkflow(EMPTY),
                createCheckpointPagesWriterFactory(mock(PartitionMetaManager.class)),
                mock(FilePageStoreManager.class),
                mock(Compactor.class),
                checkpointConfig,
                mock(LogSyncer.class)
        ));

        ((CheckpointProgressImpl) checkpointer.scheduledProgress())
                .futureFor(FINISHED)
                .whenComplete((unused, throwable) -> {
                    try {
                        checkpointConfig.frequency().update(10_000L).get(100, MILLISECONDS);

                        verify(checkpointer, times(1)).doCheckpoint();

                        checkpointer.shutdownCheckpointer(false);
                    } catch (Exception e) {
                        fail(e);
                    }
                });

        runAsync(checkpointer::body).get(200, MILLISECONDS);

        verify(checkpointer, times(2)).doCheckpoint();

        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> checkpointer.scheduledProgress().futureFor(FINISHED).get(100, MILLISECONDS)
        );

        assertThat(exception.getCause(), instanceOf(NodeStoppingException.class));

        // Checks cancelled checkpointer.

        checkpointer.shutdownCheckpointer(false);

        runAsync(checkpointer::body).get(200, MILLISECONDS);

        verify(checkpointer, times(3)).doCheckpoint();

        exception = assertThrows(
                ExecutionException.class,
                () -> checkpointer.scheduledProgress().futureFor(FINISHED).get(100, MILLISECONDS)
        );

        assertThat(exception.getCause(), instanceOf(NodeStoppingException.class));

        // Checks shutdowned checkpointer.

        checkpointer.shutdownCheckpointer(true);

        runAsync(checkpointer::body).get(200, MILLISECONDS);

        verify(checkpointer, times(3)).doCheckpoint();

        exception = assertThrows(
                ExecutionException.class,
                () -> checkpointer.scheduledProgress().futureFor(FINISHED).get(100, MILLISECONDS)
        );

        assertThat(exception.getCause(), instanceOf(NodeStoppingException.class));
    }

    @Test
    void testDoCheckpoint() throws Exception {
        CheckpointDirtyPages dirtyPages = spy(dirtyPages(
                mock(PersistentPageMemory.class),
                fullPageId(0, 0, 1), fullPageId(0, 0, 2), fullPageId(0, 0, 3)
        ));

        PartitionMetaManager partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE);

        partitionMetaManager.addMeta(
                new GroupPartitionId(0, 0),
                new PartitionMeta(null, 0, 0, 0, 0, 0, 0, 0, 3, 0)
        );

        FilePageStore filePageStore = mock(FilePageStore.class);

        when(filePageStore.getNewDeltaFile()).thenReturn(completedFuture(mock(DeltaFilePageStoreIo.class)));

        Compactor compactor = mock(Compactor.class);

        LogSyncer mockLogSyncer = mock(LogSyncer.class);

        Checkpointer checkpointer = spy(new Checkpointer(
                "test",
                null,
                null,
                mock(FailureProcessor.class),
                createCheckpointWorkflow(dirtyPages),
                createCheckpointPagesWriterFactory(partitionMetaManager),
                createFilePageStoreManager(Map.of(new GroupPartitionId(0, 0), filePageStore)),
                compactor,
                checkpointConfig,
                mockLogSyncer
        ));

        assertDoesNotThrow(checkpointer::doCheckpoint);

        verify(dirtyPages, times(1)).toDirtyPageIdQueue();
        verify(checkpointer, times(1)).startCheckpointProgress();
        verify(compactor, times(1)).triggerCompaction();
        verify(mockLogSyncer, times(1)).sync();

        assertEquals(checkpointer.lastCheckpointProgress().currentCheckpointPagesCount(), 3);

        verify(checkpointer, times(1)).updateLastProgressAfterReleaseWriteLock();
    }

    @Test
    void testDoCheckpointNoDirtyPages() throws Exception {
        CheckpointDirtyPages dirtyPages = spy(EMPTY);

        Compactor compactor = mock(Compactor.class);

        Checkpointer checkpointer = spy(new Checkpointer(
                "test",
                null,
                null,
                mock(FailureProcessor.class),
                createCheckpointWorkflow(dirtyPages),
                createCheckpointPagesWriterFactory(new PartitionMetaManager(ioRegistry, PAGE_SIZE)),
                createFilePageStoreManager(Map.of()),
                compactor,
                checkpointConfig,
                mock(LogSyncer.class)
        ));

        assertDoesNotThrow(checkpointer::doCheckpoint);

        verify(dirtyPages, never()).toDirtyPageIdQueue();
        verify(checkpointer, times(1)).startCheckpointProgress();
        verify(compactor, never()).triggerCompaction();

        assertEquals(checkpointer.lastCheckpointProgress().currentCheckpointPagesCount(), 0);

        verify(checkpointer, times(1)).updateLastProgressAfterReleaseWriteLock();
    }

    @Test
    void testNextCheckpointInterval() throws Exception {
        Checkpointer checkpointer = new Checkpointer(
                "test",
                null,
                null,
                mock(FailureProcessor.class),
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                mock(FilePageStoreManager.class),
                mock(Compactor.class),
                checkpointConfig,
                mock(LogSyncer.class)
        );

        // Checks case 0 deviation.

        checkpointConfig.frequencyDeviation().update(0).get(100, MILLISECONDS);

        checkpointConfig.frequency().update(1_000L).get(100, MILLISECONDS);
        assertEquals(1_000, checkpointer.nextCheckpointInterval());

        checkpointConfig.frequency().update(2_000L).get(100, MILLISECONDS);
        assertEquals(2_000, checkpointer.nextCheckpointInterval());

        // Checks for non-zero deviation.

        checkpointConfig.frequencyDeviation().update(10).get(100, MILLISECONDS);

        assertThat(
                checkpointer.nextCheckpointInterval(),
                allOf(greaterThanOrEqualTo(1_900L), lessThanOrEqualTo(2_100L))
        );

        checkpointConfig.frequencyDeviation().update(20).get(100, MILLISECONDS);

        assertThat(
                checkpointer.nextCheckpointInterval(),
                allOf(greaterThanOrEqualTo(1_800L), lessThanOrEqualTo(2_200L))
        );
    }

    @Test
    void testPrepareToDestroyPartition() throws Exception {
        Checkpointer checkpointer = new Checkpointer(
                "test",
                null,
                null,
                mock(FailureProcessor.class),
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                mock(FilePageStoreManager.class),
                mock(Compactor.class),
                checkpointConfig,
                mock(LogSyncer.class)
        );

        GroupPartitionId groupPartitionId = new GroupPartitionId(0, 0);

        // Everything should be fine as there is no current running checkpoint.
        checkpointer.prepareToDestroyPartition(groupPartitionId).get(1, SECONDS);

        CheckpointProgressImpl checkpointProgress = (CheckpointProgressImpl) checkpointer.scheduledProgress();

        checkpointer.startCheckpointProgress();

        checkpointer.prepareToDestroyPartition(groupPartitionId).get(1, SECONDS);

        checkpointProgress.transitTo(LOCK_RELEASED);
        assertTrue(checkpointProgress.inProgress());

        // Everything should be fine so on a "working" checkpoint we don't process the partition anyhow.
        checkpointer.prepareToDestroyPartition(groupPartitionId).get(1, SECONDS);

        // Let's emulate that we are processing a partition and check that everything will be fine after processing is completed.
        checkpointProgress.onStartPartitionProcessing(groupPartitionId);

        CompletableFuture<?> onPartitionDestructionFuture = checkpointer.prepareToDestroyPartition(groupPartitionId);

        checkpointProgress.onFinishPartitionProcessing(groupPartitionId);

        onPartitionDestructionFuture.get(1, SECONDS);
    }

    private CheckpointDirtyPages dirtyPages(PersistentPageMemory pageMemory, FullPageId... pageIds) {
        Arrays.sort(pageIds, DIRTY_PAGE_COMPARATOR);

        return new CheckpointDirtyPages(List.of(new DataRegionDirtyPages<>(pageMemory, pageIds)));
    }

    private CheckpointWorkflow createCheckpointWorkflow(CheckpointDirtyPages dirtyPages) throws Exception {
        CheckpointWorkflow mock = mock(CheckpointWorkflow.class);

        when(mock.markCheckpointBegin(
                anyLong(),
                any(CheckpointProgressImpl.class),
                any(CheckpointMetricsTracker.class),
                any(Runnable.class),
                any(Runnable.class)
        )).then(answer -> {
            CheckpointProgressImpl progress = answer.getArgument(1);

            if (dirtyPages.dirtyPagesCount() > 0) {
                progress.pagesToWrite(dirtyPages);

                progress.initCounters(dirtyPages.dirtyPagesCount());
            }

            ((Runnable) answer.getArgument(3)).run();
            ((Runnable) answer.getArgument(4)).run();

            return new Checkpoint(dirtyPages, progress);
        });

        doAnswer(answer -> {
            ((Checkpoint) answer.getArgument(0)).progress.transitTo(FINISHED);

            return null;
        })
                .when(mock)
                .markCheckpointEnd(any(Checkpoint.class));

        return mock;
    }

    private CheckpointPagesWriterFactory createCheckpointPagesWriterFactory(PartitionMetaManager partitionMetaManager) {
        return new CheckpointPagesWriterFactory(
                mock(WriteDirtyPage.class),
                ioRegistry,
                partitionMetaManager,
                PAGE_SIZE
        );
    }

    private static FilePageStoreManager createFilePageStoreManager(Map<GroupPartitionId, FilePageStore> pageStores) {
        FilePageStoreManager manager = mock(FilePageStoreManager.class);

        when(manager.getStore(any(GroupPartitionId.class))).then(answer -> pageStores.get(answer.getArgument(0)));

        return manager;
    }

    private static FullPageId fullPageId(int grpId, int partId, int pageIdx) {
        return new FullPageId(pageId(partId, (byte) 0, pageIdx), grpId);
    }
}
