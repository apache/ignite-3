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
import static org.apache.ignite.internal.pagememory.persistence.FakePartitionMeta.FACTORY;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.EMPTY;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.createDirtyPagesAndPartitions;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.awaitility.Awaitility.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.pagememory.configuration.CheckpointConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.FakePartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionDestructionLockManager;
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
 * TODO: https://issues.apache.org/jira/browse/IGNITE-27281 .
 */
@ExtendWith(ConfigurationExtension.class)
public class CheckpointerTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 1024;

    private static PageIoRegistry ioRegistry;

    private final AtomicLong intervalMillis = new AtomicLong(1_000L);

    private final AtomicInteger intervalDeviationPercent = new AtomicInteger(0);

    private final CheckpointConfiguration checkpointConfig = CheckpointConfiguration.builder()
            .checkpointThreads(1)
            .intervalMillis(intervalMillis::get)
            .intervalDeviationPercent(intervalDeviationPercent::get)
            .build();

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
        PartitionMetaManager mockParititonMetaManager = mock(PartitionMetaManager.class);

        var partitionDestructionLockManager = new PartitionDestructionLockManager();

        var checkpointer = new Checkpointer(
                "test",
                null,
                mock(FailureManager.class),
                createCheckpointWorkflow(EMPTY),
                createCheckpointPagesWriterFactory(mockParititonMetaManager, partitionDestructionLockManager),
                mock(FilePageStoreManager.class),
                mockParititonMetaManager,
                mock(Compactor.class),
                PAGE_SIZE,
                checkpointConfig,
                mock(LogSyncer.class),
                partitionDestructionLockManager,
                new CheckpointMetricSource("test")
        );

        assertNull(checkpointer.runner());

        assertFalse(checkpointer.isShutdownNow());
        assertFalse(checkpointer.isCancelled());
        assertFalse(checkpointer.isDone());

        checkpointer.start();

        with().pollInterval(10, MILLISECONDS)
                .await()
                .timeout(100, MILLISECONDS)
                .until(checkpointer::runner, notNullValue());

        checkpointer.stop();

        with().pollInterval(10, MILLISECONDS)
                .await()
                .timeout(100, MILLISECONDS)
                .until(checkpointer::runner, nullValue());

        assertTrue(checkpointer.isShutdownNow());
        assertTrue(checkpointer.isCancelled());
        assertTrue(checkpointer.isDone());
    }

    @Test
    void testScheduleCheckpoint() {
        Checkpointer checkpointer = spy(new Checkpointer(
                "test",
                null,
                mock(FailureManager.class),
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                mock(FilePageStoreManager.class),
                mock(PartitionMetaManager.class),
                mock(Compactor.class),
                PAGE_SIZE,
                checkpointConfig,
                mock(LogSyncer.class),
                new PartitionDestructionLockManager(),
                new CheckpointMetricSource("test")
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
        intervalMillis.set(200L);

        Checkpointer checkpointer = new Checkpointer(
                "test",
                null,
                mock(FailureManager.class),
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                mock(FilePageStoreManager.class),
                mock(PartitionMetaManager.class),
                mock(Compactor.class),
                PAGE_SIZE,
                checkpointConfig,
                mock(LogSyncer.class),
                new PartitionDestructionLockManager(),
                new CheckpointMetricSource("test")
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
        intervalMillis.set(100L);

        var partitionDestructionLockManager = new PartitionDestructionLockManager();

        var checkpointer = spy(new Checkpointer(
                "test",
                null,
                mock(FailureManager.class),
                createCheckpointWorkflow(EMPTY),
                createCheckpointPagesWriterFactory(mock(PartitionMetaManager.class), partitionDestructionLockManager),
                mock(FilePageStoreManager.class),
                mock(PartitionMetaManager.class),
                mock(Compactor.class),
                PAGE_SIZE,
                checkpointConfig,
                mock(LogSyncer.class),
                partitionDestructionLockManager,
                new CheckpointMetricSource("test")
        ));

        checkpointer.scheduledProgress()
                .futureFor(FINISHED)
                .whenComplete((unused, throwable) -> {
                    try {
                        intervalMillis.set(10_000L);

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
                dirtyFullPageId(0, 0, 1), dirtyFullPageId(0, 0, 2), dirtyFullPageId(0, 0, 3)
        ));

        PartitionMetaManager partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, FACTORY);

        partitionMetaManager.addMeta(
                new GroupPartitionId(0, 0),
                new FakePartitionMeta().init(null)
        );

        FilePageStore filePageStore = mock(FilePageStore.class);

        when(filePageStore.getNewDeltaFile()).thenReturn(completedFuture(mock(DeltaFilePageStoreIo.class)));

        Compactor compactor = mock(Compactor.class);

        LogSyncer mockLogSyncer = mock(LogSyncer.class);

        PartitionMetaManager mock = mock(PartitionMetaManager.class);

        FakePartitionMeta meta = new FakePartitionMeta(10, 1);
        meta.init(UUID.randomUUID());

        when(mock.getMeta(any())).thenReturn(meta);

        var partitionDestructionLockManager = new PartitionDestructionLockManager();

        var checkpointer = spy(new Checkpointer(
                "test",
                null,
                mock(FailureManager.class),
                createCheckpointWorkflow(dirtyPages),
                createCheckpointPagesWriterFactory(partitionMetaManager, partitionDestructionLockManager),
                createFilePageStoreManager(Map.of(new GroupPartitionId(0, 0), filePageStore)),
                mock,
                compactor,
                PAGE_SIZE,
                checkpointConfig,
                mockLogSyncer,
                partitionDestructionLockManager,
                new CheckpointMetricSource("test")
        ));

        assertDoesNotThrow(checkpointer::doCheckpoint);

        verify(dirtyPages, times(1)).toDirtyPartitionQueue();
        verify(checkpointer, times(1)).startCheckpointProgress();
        verify(compactor, times(1)).triggerCompaction();
        verify(mockLogSyncer, times(1)).sync();

        assertEquals(3, checkpointer.lastCheckpointProgress().currentCheckpointPagesCount());

        verify(checkpointer, times(1)).updateLastProgressAfterReleaseWriteLock();
    }

    @Test
    void testDoCheckpointNoDirtyPages() throws Exception {
        CheckpointDirtyPages dirtyPages = spy(EMPTY);

        Compactor compactor = mock(Compactor.class);

        var partitionDestructionLockManager = new PartitionDestructionLockManager();

        var checkpointer = spy(new Checkpointer(
                "test",
                null,
                mock(FailureManager.class),
                createCheckpointWorkflow(dirtyPages),
                createCheckpointPagesWriterFactory(
                        new PartitionMetaManager(ioRegistry, PAGE_SIZE, FACTORY),
                        partitionDestructionLockManager
                ),
                createFilePageStoreManager(Map.of()),
                mock(PartitionMetaManager.class),
                compactor,
                PAGE_SIZE,
                checkpointConfig,
                mock(LogSyncer.class),
                partitionDestructionLockManager,
                new CheckpointMetricSource("test")
        ));

        assertDoesNotThrow(checkpointer::doCheckpoint);

        verify(dirtyPages, never()).toDirtyPartitionQueue();
        verify(checkpointer, times(1)).startCheckpointProgress();
        verify(compactor, never()).triggerCompaction();

        assertEquals(0, checkpointer.lastCheckpointProgress().currentCheckpointPagesCount());

        verify(checkpointer, times(1)).updateLastProgressAfterReleaseWriteLock();
    }

    @Test
    void testNextCheckpointInterval() {
        Checkpointer checkpointer = new Checkpointer(
                "test",
                null,
                mock(FailureManager.class),
                mock(CheckpointWorkflow.class),
                mock(CheckpointPagesWriterFactory.class),
                mock(FilePageStoreManager.class),
                mock(PartitionMetaManager.class),
                mock(Compactor.class),
                PAGE_SIZE,
                checkpointConfig,
                mock(LogSyncer.class),
                new PartitionDestructionLockManager(),
                new CheckpointMetricSource("test")
        );

        // Checks case 0 deviation.
        intervalDeviationPercent.set(0);

        intervalMillis.set(1_000L);
        assertEquals(1_000, checkpointer.nextCheckpointInterval());

        intervalMillis.set(2_000L);
        assertEquals(2_000, checkpointer.nextCheckpointInterval());

        // Checks for non-zero deviation.

        intervalDeviationPercent.set(10);

        assertThat(
                checkpointer.nextCheckpointInterval(),
                allOf(greaterThanOrEqualTo(1_900L), lessThanOrEqualTo(2_100L))
        );

        intervalDeviationPercent.set(20);

        assertThat(
                checkpointer.nextCheckpointInterval(),
                allOf(greaterThanOrEqualTo(1_800L), lessThanOrEqualTo(2_200L))
        );
    }

    private static CheckpointDirtyPages dirtyPages(PersistentPageMemory pageMemory, DirtyFullPageId... pageIds) {
        return new CheckpointDirtyPages(List.of(createDirtyPagesAndPartitions(pageMemory, pageIds)));
    }

    private static CheckpointWorkflow createCheckpointWorkflow(CheckpointDirtyPages dirtyPages) throws Exception {
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

    private static CheckpointPagesWriterFactory createCheckpointPagesWriterFactory(
            PartitionMetaManager partitionMetaManager,
            PartitionDestructionLockManager partitionDestructionLockManager
    ) {
        return new CheckpointPagesWriterFactory(
                mock(WriteDirtyPage.class),
                ioRegistry,
                partitionMetaManager,
                PAGE_SIZE,
                partitionDestructionLockManager
        );
    }

    private static FilePageStoreManager createFilePageStoreManager(Map<GroupPartitionId, FilePageStore> pageStores) {
        FilePageStoreManager manager = mock(FilePageStoreManager.class);

        when(manager.getStore(any(GroupPartitionId.class))).then(answer -> pageStores.get(answer.getArgument(0)));

        return manager;
    }

    private static DirtyFullPageId dirtyFullPageId(int grpId, int partId, int pageIdx) {
        return new DirtyFullPageId(pageId(partId, (byte) 0, pageIdx), grpId, 1);
    }
}
