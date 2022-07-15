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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.EMPTY;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.MARKER_STORED_TO_DISK;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGE_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.createFilePageStoreManager;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.newReadWriteLock;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.toListDirtyPageIds;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.AFTER_CHECKPOINT_END;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.BEFORE_CHECKPOINT_BEGIN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.ON_CHECKPOINT_BEGIN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.ON_MARK_CHECKPOINT_BEGIN;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.DirtyPagesCollection;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesView;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.persistence.store.PartitionFilePageStore;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue.Result;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * For {@link CheckpointWorkflow} testing.
 */
public class CheckpointWorkflowTest {
    private final IgniteLogger log = Loggers.forClass(CheckpointWorkflowTest.class);

    @Nullable
    private CheckpointWorkflow workflow;

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

        DataRegion<PersistentPageMemory> dataRegion0 = () -> pageMemory0;
        DataRegion<PersistentPageMemory> dataRegion1 = () -> pageMemory1;
        DataRegion<PersistentPageMemory> dataRegion2 = () -> pageMemory2;

        workflow = new CheckpointWorkflow(
                "test",
                mock(CheckpointMarkersStorage.class),
                newReadWriteLock(log),
                List.of(dataRegion0, dataRegion1),
                mock(FilePageStoreManager.class)
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
        CheckpointMarkersStorage markersStorage = mock(CheckpointMarkersStorage.class);

        CheckpointReadWriteLock readWriteLock = newReadWriteLock(log);

        List<FullPageId> dirtyPages = List.of(of(0, 0, 1), of(0, 0, 2), of(0, 0, 3));

        PersistentPageMemory pageMemory = newPageMemory(dirtyPages);

        DataRegion<PersistentPageMemory> dataRegion = () -> pageMemory;

        PartitionFilePageStore partitionFilePageStore = spy(new PartitionFilePageStore(
                mock(FilePageStore.class),
                mock(PartitionMeta.class))
        );

        workflow = new CheckpointWorkflow(
                "test",
                markersStorage,
                readWriteLock,
                List.of(dataRegion),
                createFilePageStoreManager(Map.of(new GroupPartitionId(0, 0), partitionFilePageStore))
        );

        workflow.start();

        CheckpointProgressImpl progressImpl = mock(CheckpointProgressImpl.class);

        ArgumentCaptor<CheckpointState> checkpointStateArgumentCaptor = ArgumentCaptor.forClass(CheckpointState.class);

        ArgumentCaptor<Integer> pagesCountArgumentCaptor = ArgumentCaptor.forClass(Integer.class);

        doNothing().when(progressImpl).transitTo(checkpointStateArgumentCaptor.capture());

        doNothing().when(progressImpl).currentCheckpointPagesCount(pagesCountArgumentCaptor.capture());

        when(progressImpl.futureFor(MARKER_STORED_TO_DISK)).thenReturn(completedFuture(null));

        UUID checkpointId = UUID.randomUUID();

        when(progressImpl.id()).thenReturn(checkpointId);

        List<String> events = new ArrayList<>();

        CheckpointMetricsTracker tracker = mock(CheckpointMetricsTracker.class);

        workflow.addCheckpointListener(new TestCheckpointListener(events) {
            /** {@inheritDoc} */
            @Override
            public void beforeCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
                super.beforeCheckpointBegin(progress);

                assertSame(progressImpl, progress);

                assertEquals(readWriteLock.getReadHoldCount(), 1);

                assertThat(checkpointStateArgumentCaptor.getAllValues(), empty());

                verify(markersStorage, times(0)).onCheckpointBegin(checkpointId);

                verify(tracker, times(0)).onWriteLockWaitStart();
                verify(tracker, times(0)).onMarkCheckpointBeginStart();
                verify(tracker, times(0)).onMarkCheckpointBeginEnd();
                verify(tracker, times(0)).onWriteLockRelease();
                verify(tracker, times(0)).onSplitAndSortCheckpointPagesStart();
                verify(tracker, times(0)).onSplitAndSortCheckpointPagesEnd();
            }

            /** {@inheritDoc} */
            @Override
            public void onMarkCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
                super.onMarkCheckpointBegin(progress);

                assertSame(progressImpl, progress);

                assertTrue(readWriteLock.isWriteLockHeldByCurrentThread());

                assertThat(checkpointStateArgumentCaptor.getAllValues(), equalTo(List.of(LOCK_TAKEN)));

                verify(markersStorage, times(0)).onCheckpointBegin(checkpointId);

                verify(tracker, times(1)).onWriteLockWaitStart();
                verify(tracker, times(1)).onMarkCheckpointBeginStart();
                verify(tracker, times(0)).onMarkCheckpointBeginEnd();
                verify(tracker, times(0)).onWriteLockRelease();
                verify(tracker, times(0)).onSplitAndSortCheckpointPagesStart();
                verify(tracker, times(0)).onSplitAndSortCheckpointPagesEnd();
            }

            /** {@inheritDoc} */
            @Override
            public void onCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
                super.onCheckpointBegin(progress);

                assertSame(progressImpl, progress);

                assertEquals(0, readWriteLock.getReadHoldCount());

                assertFalse(readWriteLock.isWriteLockHeldByCurrentThread());

                assertThat(checkpointStateArgumentCaptor.getAllValues(), equalTo(List.of(LOCK_TAKEN, PAGE_SNAPSHOT_TAKEN, LOCK_RELEASED)));

                assertThat(pagesCountArgumentCaptor.getAllValues(), equalTo(List.of(3)));

                verify(markersStorage, times(0)).onCheckpointBegin(checkpointId);

                verify(tracker, times(1)).onWriteLockWaitStart();
                verify(tracker, times(1)).onMarkCheckpointBeginStart();
                verify(tracker, times(1)).onMarkCheckpointBeginEnd();
                verify(tracker, times(1)).onWriteLockRelease();
                verify(tracker, times(0)).onSplitAndSortCheckpointPagesStart();
                verify(tracker, times(0)).onSplitAndSortCheckpointPagesEnd();
            }
        }, dataRegion);

        Checkpoint checkpoint = workflow.markCheckpointBegin(coarseCurrentTimeMillis(), progressImpl, tracker);

        verify(tracker, times(1)).onWriteLockWaitStart();
        verify(tracker, times(1)).onMarkCheckpointBeginStart();
        verify(tracker, times(1)).onMarkCheckpointBeginEnd();
        verify(tracker, times(1)).onWriteLockRelease();
        verify(tracker, times(1)).onSplitAndSortCheckpointPagesStart();
        verify(tracker, times(1)).onSplitAndSortCheckpointPagesEnd();

        CheckpointDirtyPagesView dirtyPagesView = checkpoint.dirtyPages.nextView(null);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(dirtyPages));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegion.pageMemory()));

        assertThat(
                collect(checkpoint.dirtyPages.toDirtyPartitionIdQueue()),
                contains(new GroupPartitionId(0, 0))
        );

        assertThat(
                events,
                equalTo(List.of(BEFORE_CHECKPOINT_BEGIN, ON_MARK_CHECKPOINT_BEGIN, ON_CHECKPOINT_BEGIN))
        );

        assertThat(
                checkpointStateArgumentCaptor.getAllValues(),
                equalTo(List.of(LOCK_TAKEN, PAGE_SNAPSHOT_TAKEN, LOCK_RELEASED, MARKER_STORED_TO_DISK))
        );

        verify(markersStorage, times(1)).onCheckpointBegin(checkpointId);
        verify(partitionFilePageStore, times(1)).updateMetaPageCount();
    }

    @Test
    void testMarkCheckpointEnd() throws Exception {
        CheckpointMarkersStorage markersStorage = mock(CheckpointMarkersStorage.class);

        CheckpointReadWriteLock readWriteLock = newReadWriteLock(log);

        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        DataRegion<PersistentPageMemory> dataRegion = () -> pageMemory;

        workflow = new CheckpointWorkflow(
                "test",
                markersStorage,
                readWriteLock,
                List.of(dataRegion),
                mock(FilePageStoreManager.class)
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

                verify(markersStorage, times(1)).onCheckpointEnd(checkpointId);
            }
        };

        workflow.addCheckpointListener(checkpointListener, dataRegion);

        workflow.markCheckpointEnd(new Checkpoint(
                new CheckpointDirtyPages(List.of(createCheckpointDirtyPages(pageMemory, of(0, 0, 0)))),
                progressImpl
        ));

        assertThat(checkpointStateArgumentCaptor.getAllValues(), equalTo(List.of(FINISHED)));

        assertThat(events, equalTo(List.of(AFTER_CHECKPOINT_END)));

        verify(progressImpl, times(1)).clearCounters();

        verify(pageMemory, times(1)).finishCheckpoint();

        // Checks with empty dirty pages.

        workflow.removeCheckpointListener(checkpointListener);

        assertDoesNotThrow(() -> workflow.markCheckpointEnd(new Checkpoint(EMPTY, progressImpl)));

        verify(markersStorage, times(1)).onCheckpointEnd(checkpointId);
    }

    @Test
    void testCreateAndSortCheckpointDirtyPages() throws Exception {
        DataRegionDirtyPages<DirtyPagesCollection> dataRegionDirtyPages0 = createDataRegionDirtyPages(
                mock(PersistentPageMemory.class),
                of(10, 10, 2), of(10, 10, 1), of(10, 10, 0),
                of(10, 5, 100), of(10, 5, 99),
                of(10, 1, 50), of(10, 1, 51), of(10, 1, 99)
        );

        DataRegionDirtyPages<DirtyPagesCollection> dataRegionDirtyPages1 = createDataRegionDirtyPages(
                mock(PersistentPageMemory.class),
                of(77, 5, 100), of(77, 5, 99),
                of(88, 1, 51), of(88, 1, 50), of(88, 1, 99),
                of(66, 33, 0), of(66, 33, 1), of(66, 33, 2)
        );

        workflow = new CheckpointWorkflow(
                "test",
                mock(CheckpointMarkersStorage.class),
                newReadWriteLock(log),
                List.of(),
                mock(FilePageStoreManager.class)
        );

        workflow.start();

        CheckpointDirtyPages sortCheckpointDirtyPages = workflow.createAndSortCheckpointDirtyPages(
                new DataRegionsDirtyPages(List.of(dataRegionDirtyPages0, dataRegionDirtyPages1))
        );

        CheckpointDirtyPagesView dirtyPagesView = sortCheckpointDirtyPages.nextView(null);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(10, 1, 50), of(10, 1, 51), of(10, 1, 99))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages0.pageMemory));

        dirtyPagesView = sortCheckpointDirtyPages.nextView(dirtyPagesView);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(10, 5, 99), of(10, 5, 100))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages0.pageMemory));

        dirtyPagesView = sortCheckpointDirtyPages.nextView(dirtyPagesView);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(10, 10, 0), of(10, 10, 1), of(10, 10, 2))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages0.pageMemory));

        dirtyPagesView = sortCheckpointDirtyPages.nextView(dirtyPagesView);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(66, 33, 0), of(66, 33, 1), of(66, 33, 2))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages1.pageMemory));

        dirtyPagesView = sortCheckpointDirtyPages.nextView(dirtyPagesView);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(77, 5, 99), of(77, 5, 100))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages1.pageMemory));

        dirtyPagesView = sortCheckpointDirtyPages.nextView(dirtyPagesView);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(of(88, 1, 50), of(88, 1, 51), of(88, 1, 99))));
        assertThat(dirtyPagesView.pageMemory(), equalTo(dataRegionDirtyPages1.pageMemory));
    }

    @Test
    void testParallelSortDirtyPages() throws Exception {
        int count = CheckpointWorkflow.PARALLEL_SORT_THRESHOLD + 10;

        FullPageId[] dirtyPages0 = IntStream.range(0, count).mapToObj(i -> of(0, 0, count - i)).toArray(FullPageId[]::new);
        FullPageId[] dirtyPages1 = IntStream.range(0, count).mapToObj(i -> of(1, 1, i)).toArray(FullPageId[]::new);

        workflow = new CheckpointWorkflow(
                "test",
                mock(CheckpointMarkersStorage.class),
                newReadWriteLock(log),
                List.of(),
                mock(FilePageStoreManager.class)
        );

        workflow.start();

        CheckpointDirtyPages sortCheckpointDirtyPages = workflow.createAndSortCheckpointDirtyPages(new DataRegionsDirtyPages(List.of(
                createDataRegionDirtyPages(mock(PersistentPageMemory.class), dirtyPages1),
                createDataRegionDirtyPages(mock(PersistentPageMemory.class), dirtyPages0)
        )));

        CheckpointDirtyPagesView dirtyPagesView = sortCheckpointDirtyPages.nextView(null);

        assertThat(toListDirtyPageIds(dirtyPagesView), equalTo(List.of(dirtyPages1)));

        dirtyPagesView = sortCheckpointDirtyPages.nextView(dirtyPagesView);

        assertThat(
                toListDirtyPageIds(dirtyPagesView),
                equalTo(IntStream.range(0, count).mapToObj(i -> dirtyPages0[count - i - 1]).collect(toList()))
        );
    }

    private static PersistentPageMemory newPageMemory(Collection<FullPageId> pageIds) {
        PersistentPageMemory mock = mock(PersistentPageMemory.class);

        List<GroupPartitionId> partitionIds = List.of(partitionIds(pageIds.toArray(FullPageId[]::new)));

        when(mock.beginCheckpoint(any(CompletableFuture.class))).thenReturn(new DirtyPagesCollection(pageIds, partitionIds));

        return mock;
    }

    private static DataRegionDirtyPages<DirtyPagesArray> createCheckpointDirtyPages(
            PersistentPageMemory pageMemory,
            FullPageId... pageIds
    ) {
        return new DataRegionDirtyPages<>(pageMemory, new DirtyPagesArray(pageIds, partitionIds(pageIds)));
    }

    private static DataRegionDirtyPages<DirtyPagesCollection> createDataRegionDirtyPages(
            PersistentPageMemory pageMemory,
            FullPageId... pageIds
    ) {
        return new DataRegionDirtyPages<>(pageMemory, new DirtyPagesCollection(List.of(pageIds), List.of(partitionIds(pageIds))));
    }

    private static FullPageId of(int grpId, int partId, int pageIdx) {
        return new FullPageId(PageIdUtils.pageId(partId, (byte) 0, pageIdx), grpId);
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
        public void onMarkCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
            events.add(ON_MARK_CHECKPOINT_BEGIN);
        }

        /** {@inheritDoc} */
        @Override
        public void onCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
            events.add(ON_CHECKPOINT_BEGIN);
        }

        /** {@inheritDoc} */
        @Override
        public void beforeCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
            events.add(BEFORE_CHECKPOINT_BEGIN);
        }

        /** {@inheritDoc} */
        @Override
        public void afterCheckpointEnd(CheckpointProgress progress) throws IgniteInternalCheckedException {
            events.add(AFTER_CHECKPOINT_END);
        }
    }

    private static GroupPartitionId[] partitionIds(FullPageId... fullPageIds) {
        return Stream.of(fullPageIds)
                .map(fullPageId -> new GroupPartitionId(fullPageId.groupId(), fullPageId.partitionId()))
                .distinct()
                .toArray(GroupPartitionId[]::new);
    }

    private static List<GroupPartitionId> collect(IgniteConcurrentMultiPairQueue<PersistentPageMemory, GroupPartitionId> queue) {
        return IntStream.range(0, queue.size())
                .mapToObj(i -> {
                    Result<PersistentPageMemory, GroupPartitionId> result = new Result<>();

                    queue.next(result);

                    return result.getValue();
                })
                .collect(toList());
    }
}
