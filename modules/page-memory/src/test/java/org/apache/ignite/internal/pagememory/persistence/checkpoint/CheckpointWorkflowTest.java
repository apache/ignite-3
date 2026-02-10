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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.persistence.PartitionMeta.partitionMetaPageId;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.EMPTY;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGES_SORTED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.toListDirtyPageIds;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.AFTER_CHECKPOINT_END;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.BEFORE_CHECKPOINT_BEGIN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.ON_CHECKPOINT_BEGIN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.ON_MARK_CHECKPOINT_BEGIN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.createDirtyPagesAndPartitions;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.TestDataRegion;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * For {@link CheckpointWorkflow} testing.
 */
@ExtendWith({ExecutorServiceExtension.class, ExecutorServiceExtension.class})
public class CheckpointWorkflowTest extends BaseIgniteAbstractTest {
    @Nullable
    private CheckpointWorkflow workflow;

    @InjectExecutorService
    private ExecutorService executorService;

    @AfterEach
    void tearDown() {
        if (workflow != null) {
            workflow.stop();
        }
    }

    @Test
    void testListeners() {
        PersistentPageMemory pageMemory0 = mock(PersistentPageMemory.class);
        PersistentPageMemory pageMemory1 = mock(PersistentPageMemory.class);
        PersistentPageMemory pageMemory2 = mock(PersistentPageMemory.class);

        DataRegion<PersistentPageMemory> dataRegion0 = new TestDataRegion<>(pageMemory0);
        DataRegion<PersistentPageMemory> dataRegion1 = new TestDataRegion<>(pageMemory1);
        DataRegion<PersistentPageMemory> dataRegion2 = new TestDataRegion<>(pageMemory2);

        workflow = new CheckpointWorkflow(
                "test",
                newReadWriteLock(),
                List.of(dataRegion0, dataRegion1),
                1
        );

        workflow.start();

        CheckpointListener listener0 = mock(CheckpointListener.class);
        CheckpointListener listener1 = mock(CheckpointListener.class);
        CheckpointListener listener2 = mock(CheckpointListener.class);
        CheckpointListener listener3 = mock(CheckpointListener.class);
        CheckpointListener listener4 = mock(CheckpointListener.class);

        workflow.addCheckpointListener(listener0, dataRegion0);
        workflow.addCheckpointListener(listener1, dataRegion0);
        workflow.addCheckpointListener(listener2, dataRegion0);
        workflow.addCheckpointListener(listener3, dataRegion1);
        workflow.addCheckpointListener(listener4, null);

        assertThat(
                Set.copyOf(workflow.collectCheckpointListeners(List.of(dataRegion0))),
                equalTo(Set.of(listener0, listener1, listener2, listener4))
        );

        assertThat(
                Set.copyOf(workflow.collectCheckpointListeners(List.of(dataRegion1))),
                equalTo(Set.of(listener3, listener4))
        );

        assertThat(
                Set.copyOf(workflow.collectCheckpointListeners(List.of(dataRegion2))),
                equalTo(Set.of(listener4))
        );

        assertThat(
                Set.copyOf(workflow.collectCheckpointListeners(List.of(dataRegion0, dataRegion1))),
                equalTo(Set.of(listener0, listener1, listener2, listener3, listener4))
        );

        // Checks remove listener.

        workflow.removeCheckpointListener(listener0);
        workflow.removeCheckpointListener(listener1);
        workflow.removeCheckpointListener(listener3);

        assertThat(
                Set.copyOf(workflow.collectCheckpointListeners(List.of(dataRegion0))),
                equalTo(Set.of(listener2, listener4))
        );

        assertThat(
                Set.copyOf(workflow.collectCheckpointListeners(List.of(dataRegion1))),
                equalTo(Set.of(listener4))
        );

        assertThat(
                Set.copyOf(workflow.collectCheckpointListeners(List.of(dataRegion2))),
                equalTo(Set.of(listener4))
        );

        // Checks empty listeners after stop.

        workflow.stop();

        assertThat(workflow.collectCheckpointListeners(List.of(dataRegion0)), empty());
        assertThat(workflow.collectCheckpointListeners(List.of(dataRegion1)), empty());
        assertThat(workflow.collectCheckpointListeners(List.of(dataRegion2)), empty());
    }

    @Test
    void testMarkCheckpointBegin() throws Exception {
        CheckpointReadWriteLock readWriteLock = newReadWriteLock();

        List<DirtyFullPageId> dirtyPages = List.of(of(0, 0, 1), of(0, 0, 2), of(0, 0, 3));

        PersistentPageMemory pageMemory = newPageMemory(dirtyPages);

        DataRegion<PersistentPageMemory> dataRegion = new TestDataRegion<>(pageMemory);

        workflow = new CheckpointWorkflow(
                "test",
                readWriteLock,
                List.of(dataRegion),
                1
        );

        workflow.start();

        CheckpointProgressImpl progressImpl = mock(CheckpointProgressImpl.class);

        ArgumentCaptor<CheckpointState> checkpointStateArgumentCaptor = ArgumentCaptor.forClass(CheckpointState.class);

        ArgumentCaptor<Integer> pagesCountArgumentCaptor = ArgumentCaptor.forClass(Integer.class);

        doNothing().when(progressImpl).transitTo(checkpointStateArgumentCaptor.capture());

        doNothing().when(progressImpl).currentCheckpointPagesCount(pagesCountArgumentCaptor.capture());

        when(progressImpl.futureFor(PAGES_SORTED)).thenReturn(nullCompletedFuture());

        UUID checkpointId = UUID.randomUUID();

        when(progressImpl.id()).thenReturn(checkpointId);

        List<String> events = new ArrayList<>();

        CheckpointMetricsTracker tracker = mock(CheckpointMetricsTracker.class);

        Runnable onReleaseWriteLock = mock(Runnable.class);

        workflow.addCheckpointListener(new TestCheckpointListener(events) {
            @Override
            public void beforeCheckpointBegin(CheckpointProgress progress, @Nullable Executor exec) throws IgniteInternalCheckedException {
                super.beforeCheckpointBegin(progress, exec);

                assertNull(exec);

                assertSame(progressImpl, progress);

                assertEquals(readWriteLock.getReadHoldCount(), 1);

                assertThat(checkpointStateArgumentCaptor.getAllValues(), empty());

                verify(tracker, never()).onWriteLockWaitStart();
                verify(tracker, never()).onWriteLockWaitEnd();
                verify(tracker, never()).onWriteLockHoldStart();
                verify(tracker, never()).onWriteLockHoldEnd();
                verify(tracker, never()).onMarkCheckpointBeginStart();
                verify(tracker, never()).onMarkCheckpointBeginEnd();
                verify(tracker, never()).onSplitAndSortCheckpointPagesStart();
                verify(tracker, never()).onSplitAndSortCheckpointPagesEnd();

                verify(progressImpl, never()).pagesToWrite(any(CheckpointDirtyPages.class));
                verify(progressImpl, never()).initCounters(anyInt());

                verify(onReleaseWriteLock, never()).run();
            }

            @Override
            public void onMarkCheckpointBegin(CheckpointProgress progress, @Nullable Executor exec) throws IgniteInternalCheckedException {
                super.onMarkCheckpointBegin(progress, exec);

                assertNull(exec);

                assertSame(progressImpl, progress);

                assertTrue(readWriteLock.isWriteLockHeldByCurrentThread());

                assertThat(checkpointStateArgumentCaptor.getAllValues(), equalTo(List.of(LOCK_TAKEN)));

                verify(tracker, times(1)).onWriteLockWaitStart();
                verify(tracker, times(1)).onWriteLockWaitEnd();
                verify(tracker, times(1)).onWriteLockHoldStart();
                verify(tracker, times(1)).onMarkCheckpointBeginStart();
                verify(tracker, never()).onMarkCheckpointBeginEnd();
                verify(tracker, never()).onWriteLockHoldEnd();
                verify(tracker, never()).onSplitAndSortCheckpointPagesStart();
                verify(tracker, never()).onSplitAndSortCheckpointPagesEnd();

                verify(progressImpl, never()).pagesToWrite(any(CheckpointDirtyPages.class));
                verify(progressImpl, never()).initCounters(anyInt());

                verify(onReleaseWriteLock, never()).run();
            }

            @Override
            public void onCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
                super.onCheckpointBegin(progress);

                assertSame(progressImpl, progress);

                assertEquals(0, readWriteLock.getReadHoldCount());

                assertFalse(readWriteLock.isWriteLockHeldByCurrentThread());

                assertThat(checkpointStateArgumentCaptor.getAllValues(), equalTo(List.of(LOCK_TAKEN, PAGES_SNAPSHOT_TAKEN, LOCK_RELEASED)));

                assertThat(pagesCountArgumentCaptor.getAllValues(), equalTo(List.of(3)));

                verify(tracker, times(1)).onWriteLockWaitStart();
                verify(tracker, times(1)).onWriteLockWaitEnd();
                verify(tracker, times(1)).onWriteLockHoldStart();
                verify(tracker, times(1)).onWriteLockHoldEnd();
                verify(tracker, times(1)).onMarkCheckpointBeginStart();
                verify(tracker, times(1)).onMarkCheckpointBeginEnd();
                verify(tracker, never()).onSplitAndSortCheckpointPagesStart();
                verify(tracker, never()).onSplitAndSortCheckpointPagesEnd();

                verify(progressImpl, never()).pagesToWrite(any(CheckpointDirtyPages.class));
                verify(progressImpl, never()).initCounters(anyInt());

                verify(onReleaseWriteLock, times(1)).run();
            }
        }, dataRegion);

        Checkpoint checkpoint = workflow.markCheckpointBegin(
                coarseCurrentTimeMillis(),
                progressImpl,
                tracker,
                () -> {},
                onReleaseWriteLock
        );

        verify(tracker, times(1)).onWriteLockWaitStart();
        verify(tracker, times(1)).onWriteLockWaitEnd();
        verify(tracker, times(1)).onWriteLockHoldStart();
        verify(tracker, times(1)).onWriteLockHoldEnd();
        verify(tracker, times(1)).onMarkCheckpointBeginStart();
        verify(tracker, times(1)).onMarkCheckpointBeginEnd();
        verify(tracker, times(1)).onSplitAndSortCheckpointPagesStart();
        verify(tracker, times(1)).onSplitAndSortCheckpointPagesEnd();

        verify(progressImpl, times(1)).pagesToWrite(any(CheckpointDirtyPages.class));
        verify(progressImpl, times(1)).initCounters(anyInt());

        CheckpointDirtyPagesView dirtyPagesView = checkpoint.dirtyPages.getPartitionView(pageMemory, 0, 0);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(dirtyPages));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegion.pageMemory()));

        assertThat(
                events,
                equalTo(List.of(BEFORE_CHECKPOINT_BEGIN, ON_MARK_CHECKPOINT_BEGIN, ON_CHECKPOINT_BEGIN))
        );

        assertThat(
                checkpointStateArgumentCaptor.getAllValues(),
                equalTo(List.of(LOCK_TAKEN, PAGES_SNAPSHOT_TAKEN, LOCK_RELEASED, PAGES_SORTED))
        );

        verify(onReleaseWriteLock, times(1)).run();
    }

    @Test
    void testMarkCheckpointEnd() throws Exception {
        CheckpointReadWriteLock readWriteLock = newReadWriteLock();

        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        DataRegion<PersistentPageMemory> dataRegion = new TestDataRegion<>(pageMemory);

        workflow = new CheckpointWorkflow(
                "test",
                readWriteLock,
                List.of(dataRegion),
                1
        );

        workflow.start();

        List<String> events = new ArrayList<>();

        ArgumentCaptor<CheckpointState> checkpointStateArgumentCaptor = ArgumentCaptor.forClass(CheckpointState.class);

        CheckpointProgressImpl progressImpl = mock(CheckpointProgressImpl.class);

        doNothing().when(progressImpl).transitTo(checkpointStateArgumentCaptor.capture());

        UUID checkpointId = UUID.randomUUID();

        when(progressImpl.id()).thenReturn(checkpointId);

        TestCheckpointListener checkpointListener = new TestCheckpointListener(events) {
            /** {@inheritDoc} */
            @Override
            public void afterCheckpointEnd(CheckpointProgress progress) throws IgniteInternalCheckedException {
                super.afterCheckpointEnd(progress);

                assertSame(progressImpl, progress);

                assertFalse(readWriteLock.isWriteLockHeldByCurrentThread());

                assertEquals(0, readWriteLock.getReadHoldCount());

                assertThat(checkpointStateArgumentCaptor.getAllValues(), empty());

                verify(progressImpl, times(1)).pagesToWrite(isNull());
                verify(progressImpl, times(1)).clearCounters();
            }
        };

        workflow.addCheckpointListener(checkpointListener, dataRegion);

        workflow.markCheckpointEnd(new Checkpoint(
                new CheckpointDirtyPages(List.of(createDirtyPagesAndPartitions(pageMemory, of(0, 0, 0)))),
                progressImpl
        ));

        assertThat(checkpointStateArgumentCaptor.getAllValues(), equalTo(List.of(FINISHED)));

        assertThat(events, equalTo(List.of(AFTER_CHECKPOINT_END)));

        verify(progressImpl, times(1)).clearCounters();

        verify(pageMemory, times(1)).finishCheckpoint();

        verify(progressImpl, times(1)).pagesToWrite(isNull());

        verify(progressImpl, times(1)).clearCounters();

        // Checks with empty dirty pages.

        workflow.removeCheckpointListener(checkpointListener);

        assertDoesNotThrow(() -> workflow.markCheckpointEnd(new Checkpoint(EMPTY, progressImpl)));
    }

    @Test
    void testCreateAndSortCheckpointDirtyPages() throws Exception {
        PersistentPageMemory pageMemory0 = mock(PersistentPageMemory.class);
        PersistentPageMemory pageMemory1 = mock(PersistentPageMemory.class);

        DataRegionDirtyPages<Collection<DirtyFullPageId>> dataRegionDirtyPages0 = createDataRegionDirtyPages(
                pageMemory0,
                of(10, 10, 2), of(10, 10, 1), of(10, 10, 0),
                of(10, 5, 100), of(10, 5, 99),
                of(10, 1, 50), of(10, 1, 51), of(10, 1, 99)
        );

        DataRegionDirtyPages<Collection<DirtyFullPageId>> dataRegionDirtyPages1 = createDataRegionDirtyPages(
                pageMemory1,
                of(77, 5, 100), of(77, 5, 99),
                of(88, 1, 51), of(88, 1, 50), of(88, 1, 99),
                of(66, 33, 0), of(66, 33, 1), of(66, 33, 2)
        );

        workflow = new CheckpointWorkflow(
                "test",
                newReadWriteLock(),
                List.of(),
                1
        );

        workflow.start();

        CheckpointDirtyPages sortCheckpointDirtyPages = workflow.createAndSortCheckpointDirtyPages(
                new DataRegionsDirtyPages(List.of(dataRegionDirtyPages0, dataRegionDirtyPages1))
        );

        CheckpointDirtyPagesView dirtyPagesView = sortCheckpointDirtyPages.getPartitionView(pageMemory0, 10, 1);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(10, 1, 50), of(10, 1, 51), of(10, 1, 99))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages0.pageMemory));

        dirtyPagesView = sortCheckpointDirtyPages.getPartitionView(pageMemory0, 10, 5);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(10, 5, 99), of(10, 5, 100))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages0.pageMemory));

        dirtyPagesView = sortCheckpointDirtyPages.getPartitionView(pageMemory0, 10, 10);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(10, 10, 0), of(10, 10, 1), of(10, 10, 2))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages0.pageMemory));

        dirtyPagesView = sortCheckpointDirtyPages.getPartitionView(pageMemory1, 66, 33);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(66, 33, 0), of(66, 33, 1), of(66, 33, 2))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages1.pageMemory));

        dirtyPagesView = sortCheckpointDirtyPages.getPartitionView(pageMemory1, 77, 5);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(77, 5, 99), of(77, 5, 100))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages1.pageMemory));

        dirtyPagesView = sortCheckpointDirtyPages.getPartitionView(pageMemory1, 88, 1);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(88, 1, 50), of(88, 1, 51), of(88, 1, 99))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages1.pageMemory));
    }

    @Test
    void testParallelSortDirtyPages() throws Exception {
        int count = CheckpointWorkflow.PARALLEL_SORT_THRESHOLD + 10;

        DirtyFullPageId[] dirtyPages0 = IntStream.range(0, count).mapToObj(i -> of(0, 0, count - i)).toArray(DirtyFullPageId[]::new);
        DirtyFullPageId[] dirtyPages1 = IntStream.range(0, count).mapToObj(i -> of(1, 1, i)).toArray(DirtyFullPageId[]::new);

        workflow = new CheckpointWorkflow(
                "test",
                newReadWriteLock(),
                List.of(),
                1
        );

        workflow.start();

        PersistentPageMemory pageMemory0 = mock(PersistentPageMemory.class);
        PersistentPageMemory pageMemory1 = mock(PersistentPageMemory.class);

        CheckpointDirtyPages sortCheckpointDirtyPages = workflow.createAndSortCheckpointDirtyPages(new DataRegionsDirtyPages(List.of(
                createDataRegionDirtyPages(pageMemory1, dirtyPages1),
                createDataRegionDirtyPages(pageMemory0, dirtyPages0)
        )));

        CheckpointDirtyPagesView dirtyPagesView = sortCheckpointDirtyPages.getPartitionView(pageMemory1, 1, 1);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(dirtyPages1)));

        dirtyPagesView = sortCheckpointDirtyPages.getPartitionView(pageMemory0, 0, 0);

        assertThat(
                toListDirtyPageIds(dirtyPagesView),
                equalTo(IntStream.range(0, count).mapToObj(i -> dirtyPages0[count - i - 1]).collect(toList()))
        );
    }

    /**
     * Tests that dirty partition with no dirty pages will be checkpointed.
     */
    @Test
    void testDirtyPartitionWithoutDirtyPages() throws Exception {
        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        DataRegion<PersistentPageMemory> dataRegion = new TestDataRegion<>(pageMemory);

        workflow = new CheckpointWorkflow(
                "test",
                newReadWriteLock(),
                List.of(dataRegion),
                1
        );

        workflow.start();

        int groupId = 10;
        int partitionId = 20;
        int partitionGeneration = 1;

        var metaPageId = new DirtyFullPageId(partitionMetaPageId(partitionId), groupId, partitionGeneration);

        when(pageMemory.partGeneration(anyInt(), anyInt())).thenReturn(partitionGeneration);
        workflow.markPartitionAsDirty(dataRegion, groupId, partitionId, partitionGeneration);

        Checkpoint checkpoint = workflow.markCheckpointBegin(
                coarseCurrentTimeMillis(),
                mock(CheckpointProgressImpl.class),
                mock(CheckpointMetricsTracker.class),
                () -> {},
                () -> {}
        );

        assertEquals(1, checkpoint.dirtyPagesSize);

        CheckpointDirtyPagesView dirtyPagesView = checkpoint.dirtyPages.getPartitionView(pageMemory, groupId, partitionId);

        assertNotNull(dirtyPagesView);
        assertThat(toListDirtyPageIds(dirtyPagesView), is(List.of(metaPageId)));
    }

    /**
     * Tests that dirty partition with dirty pages will be checkpointed.
     */
    @Test
    void testDirtyPartitionWithDirtyPages() throws Exception {
        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        DataRegion<PersistentPageMemory> dataRegion = new TestDataRegion<>(pageMemory);

        workflow = new CheckpointWorkflow(
                "test",
                newReadWriteLock(),
                List.of(dataRegion),
                1
        );

        workflow.start();

        int groupId = 10;
        int partitionId = 20;
        int partitionGeneration = 1;

        var metaPageId = new DirtyFullPageId(partitionMetaPageId(partitionId), groupId, partitionGeneration);
        var dataPageId = new DirtyFullPageId(pageId(partitionId, FLAG_DATA, 1), groupId, partitionGeneration);

        when(pageMemory.beginCheckpoint(any())).thenReturn(List.of(dataPageId));
        when(pageMemory.partGeneration(anyInt(), anyInt())).thenReturn(partitionGeneration);

        workflow.markPartitionAsDirty(dataRegion, groupId, partitionId, partitionGeneration);

        Checkpoint checkpoint = workflow.markCheckpointBegin(
                coarseCurrentTimeMillis(),
                mock(CheckpointProgressImpl.class),
                mock(CheckpointMetricsTracker.class),
                () -> {},
                () -> {}
        );

        assertEquals(2, checkpoint.dirtyPagesSize);

        CheckpointDirtyPagesView dirtyPagesView = checkpoint.dirtyPages.getPartitionView(pageMemory, groupId, partitionId);

        assertNotNull(dirtyPagesView);
        assertThat(toListDirtyPageIds(dirtyPagesView), is(List.of(metaPageId, dataPageId)));
    }

    @Test
    void testAwaitPendingTasksOfListenerCallback() {
        workflow = new CheckpointWorkflow(
                "test",
                newReadWriteLock(),
                List.of(),
                2
        );

        workflow.start();

        CompletableFuture<?> startTaskBeforeCheckpointBeginFuture = new CompletableFuture<>();
        CompletableFuture<?> finishTaskBeforeCheckpointBeginFuture = new CompletableFuture<>();

        CompletableFuture<?> startTaskOnMarkCheckpointBeginFuture = new CompletableFuture<>();
        CompletableFuture<?> finishTaskOnMarkCheckpointBeginFuture = new CompletableFuture<>();

        workflow.addCheckpointListener(new CheckpointListener() {
            /** {@inheritDoc} */
            @Override
            public void beforeCheckpointBegin(CheckpointProgress progress, @Nullable Executor executor) {
                assertNotNull(executor);

                executor.execute(() -> {
                    startTaskBeforeCheckpointBeginFuture.complete(null);

                    await(finishTaskBeforeCheckpointBeginFuture, 1, SECONDS);
                });
            }

            /** {@inheritDoc} */
            @Override
            public void onMarkCheckpointBegin(CheckpointProgress progress, @Nullable Executor executor) {
                assertNotNull(executor);

                executor.execute(() -> {
                    startTaskOnMarkCheckpointBeginFuture.complete(null);

                    await(finishTaskOnMarkCheckpointBeginFuture, 1, SECONDS);
                });
            }
        }, null);

        Runnable updateHeartbeat = mock(Runnable.class);

        CompletableFuture<Checkpoint> markCheckpointBeginFuture = runAsync(() -> workflow.markCheckpointBegin(
                coarseCurrentTimeMillis(),
                mock(CheckpointProgressImpl.class),
                mock(CheckpointMetricsTracker.class),
                updateHeartbeat,
                () -> {}
        ));

        await(startTaskBeforeCheckpointBeginFuture, 1, SECONDS);

        assertFalse(markCheckpointBeginFuture.isDone());
        assertFalse(startTaskOnMarkCheckpointBeginFuture.isDone());
        verify(updateHeartbeat, times(1)).run();

        finishTaskBeforeCheckpointBeginFuture.complete(null);

        await(startTaskOnMarkCheckpointBeginFuture, 1, SECONDS);

        assertFalse(markCheckpointBeginFuture.isDone());
        verify(updateHeartbeat, times(3)).run();

        finishTaskOnMarkCheckpointBeginFuture.complete(null);

        await(markCheckpointBeginFuture, 1, SECONDS);

        verify(updateHeartbeat, times(5)).run();
    }

    private static PersistentPageMemory newPageMemory(Collection<DirtyFullPageId> pageIds) {
        PersistentPageMemory mock = mock(PersistentPageMemory.class);

        when(mock.beginCheckpoint(any(CheckpointProgress.class))).thenReturn(pageIds);

        return mock;
    }

    private static DataRegionDirtyPages<Collection<DirtyFullPageId>> createDataRegionDirtyPages(
            PersistentPageMemory pageMemory,
            DirtyFullPageId... pageIds
    ) {
        return new DataRegionDirtyPages<>(pageMemory, List.of(pageIds));
    }

    private static DirtyFullPageId of(int grpId, int partId, int pageIdx) {
        return new DirtyFullPageId(pageId(partId, (byte) 0, pageIdx), grpId, 1);
    }

    /**
     * Test listener implementation that simply collects events.
     */
    static class TestCheckpointListener implements CheckpointListener {
        static final String ON_MARK_CHECKPOINT_BEGIN = "onMarkCheckpointBegin";

        static final String ON_CHECKPOINT_BEGIN = "onCheckpointBegin";

        static final String BEFORE_CHECKPOINT_BEGIN = "beforeCheckpointBegin";

        static final String AFTER_CHECKPOINT_END = "afterCheckpointEnd";

        final List<String> events;

        /**
         * Constructor.
         *
         * @param events For recording events.
         */
        TestCheckpointListener(List<String> events) {
            this.events = events;
        }

        /** {@inheritDoc} */
        @Override
        public void onMarkCheckpointBegin(CheckpointProgress progress, @Nullable Executor executor) throws IgniteInternalCheckedException {
            events.add(ON_MARK_CHECKPOINT_BEGIN);
        }

        /** {@inheritDoc} */
        @Override
        public void onCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
            events.add(ON_CHECKPOINT_BEGIN);
        }

        /** {@inheritDoc} */
        @Override
        public void beforeCheckpointBegin(CheckpointProgress progress, @Nullable Executor executor) throws IgniteInternalCheckedException {
            events.add(BEFORE_CHECKPOINT_BEGIN);
        }

        /** {@inheritDoc} */
        @Override
        public void afterCheckpointEnd(CheckpointProgress progress) throws IgniteInternalCheckedException {
            events.add(AFTER_CHECKPOINT_END);
        }
    }

    private CheckpointReadWriteLock newReadWriteLock() {
        return CheckpointTestUtils.newReadWriteLock(log, executorService);
    }
}
