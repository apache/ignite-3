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

import static java.util.Comparator.comparing;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.MARKER_STORED_TO_DISK;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.PAGE_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.newDataRegion;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.newReadWriteLock;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.AFTER_CHECKPOINT_END;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.BEFORE_CHECKPOINT_BEGIN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.ON_CHECKPOINT_BEGIN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflowTest.TestCheckpointListener.ON_MARK_CHECKPOINT_BEGIN;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWriteOrder.RANDOM;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWriteOrder.SEQUENTIAL;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.IgniteConcurrentMultiPairQueue.EMPTY;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.IgniteConcurrentMultiPairQueue.Result;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * For {@link CheckpointWorkflow} testing.
 */
public class CheckpointWorkflowTest {
    private final IgniteLogger log = IgniteLogger.forClass(CheckpointWorkflowTest.class);

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
        PageMemoryDataRegion dataRegion0 = newDataRegion(true, mock(PageMemoryImpl.class));
        PageMemoryDataRegion dataRegion1 = newDataRegion(true, mock(PageMemoryImpl.class));
        PageMemoryDataRegion dataRegion2 = newDataRegion(true, mock(PageMemoryImpl.class));

        workflow = new CheckpointWorkflow(
                mock(CheckpointMarkersStorage.class),
                newReadWriteLock(log),
                RANDOM,
                List.of(dataRegion0, dataRegion1)
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

        List<FullPageId> dirtyPages = List.of(new FullPageId(0, 0), new FullPageId(1, 0), new FullPageId(2, 0));

        PageMemoryDataRegion dataRegion = newDataRegion(true, newPageMemoryImpl(dirtyPages));

        workflow = new CheckpointWorkflow(markersStorage, readWriteLock, RANDOM, List.of(dataRegion));

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

        List<IgniteBiTuple<PageMemoryImpl, FullPageId>> pairs = collect(checkpoint.dirtyPages);

        assertThat(
                pairs.stream().map(IgniteBiTuple::getKey).collect(toSet()),
                equalTo(Set.of(dataRegion.pageMemory()))
        );

        assertThat(
                pairs.stream().map(IgniteBiTuple::getValue).collect(toList()),
                equalTo(dirtyPages)
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
    }

    @Test
    void testMarkCheckpointBeginRandom() throws Exception {
        List<FullPageId> dirtyPages = List.of(new FullPageId(1, 0), new FullPageId(0, 0), new FullPageId(2, 0));

        PageMemoryDataRegion dataRegion = newDataRegion(true, newPageMemoryImpl(dirtyPages));

        workflow = new CheckpointWorkflow(
                mock(CheckpointMarkersStorage.class),
                newReadWriteLock(log),
                RANDOM,
                List.of(dataRegion)
        );

        workflow.start();

        CheckpointProgressImpl progressImpl = mock(CheckpointProgressImpl.class);

        when(progressImpl.futureFor(MARKER_STORED_TO_DISK)).thenReturn(completedFuture(null));

        Checkpoint checkpoint = workflow.markCheckpointBegin(
                coarseCurrentTimeMillis(),
                progressImpl,
                mock(CheckpointMetricsTracker.class)
        );

        assertThat(
                collect(checkpoint.dirtyPages).stream().map(IgniteBiTuple::getValue).collect(toList()),
                equalTo(dirtyPages)
        );
    }

    @Test
    void testMarkCheckpointBeginSequential() throws Exception {
        List<FullPageId> dirtyPages = List.of(new FullPageId(1, 0), new FullPageId(0, 0), new FullPageId(2, 0));

        PageMemoryDataRegion dataRegion = newDataRegion(true, newPageMemoryImpl(dirtyPages));

        workflow = new CheckpointWorkflow(
                mock(CheckpointMarkersStorage.class),
                newReadWriteLock(log),
                SEQUENTIAL,
                List.of(dataRegion)
        );

        CheckpointProgressImpl progressImpl = mock(CheckpointProgressImpl.class);

        when(progressImpl.futureFor(MARKER_STORED_TO_DISK)).thenReturn(completedFuture(null));

        Checkpoint checkpoint = workflow.markCheckpointBegin(
                coarseCurrentTimeMillis(),
                progressImpl,
                mock(CheckpointMetricsTracker.class)
        );

        assertThat(
                collect(checkpoint.dirtyPages).stream().map(IgniteBiTuple::getValue).collect(toList()),
                equalTo(dirtyPages.stream().sorted(comparing(FullPageId::effectivePageId)).collect(toList()))
        );
    }

    @Test
    void testMarkCheckpointEnd() throws Exception {
        CheckpointMarkersStorage markersStorage = mock(CheckpointMarkersStorage.class);

        CheckpointReadWriteLock readWriteLock = newReadWriteLock(log);

        PageMemoryImpl pageMemory = mock(PageMemoryImpl.class);

        PageMemoryDataRegion dataRegion = newDataRegion(true, pageMemory);

        workflow = new CheckpointWorkflow(markersStorage, readWriteLock, RANDOM, List.of(dataRegion));

        workflow.start();

        List<String> events = new ArrayList<>();

        ArgumentCaptor<CheckpointState> checkpointStateArgumentCaptor = ArgumentCaptor.forClass(CheckpointState.class);

        CheckpointProgressImpl progressImpl = mock(CheckpointProgressImpl.class);

        doNothing().when(progressImpl).transitTo(checkpointStateArgumentCaptor.capture());

        UUID checkpointId = UUID.randomUUID();

        when(progressImpl.id()).thenReturn(checkpointId);

        workflow.addCheckpointListener(new TestCheckpointListener(events) {
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
        }, dataRegion);

        workflow.markCheckpointEnd(new Checkpoint(EMPTY, progressImpl));

        assertThat(checkpointStateArgumentCaptor.getAllValues(), equalTo(List.of(FINISHED)));

        assertThat(events, equalTo(List.of(AFTER_CHECKPOINT_END)));

        verify(progressImpl, times(1)).clearCounters();

        verify(pageMemory, times(1)).finishCheckpoint();
    }

    private List<IgniteBiTuple<PageMemoryImpl, FullPageId>> collect(IgniteConcurrentMultiPairQueue<PageMemoryImpl, FullPageId> queue) {
        List<IgniteBiTuple<PageMemoryImpl, FullPageId>> res = new ArrayList<>();

        Result<PageMemoryImpl, FullPageId> result = new Result<>();

        while (queue.next(result)) {
            res.add(new IgniteBiTuple<>(result.getKey(), result.getValue()));
        }

        return res;
    }

    private PageMemoryImpl newPageMemoryImpl(Collection<FullPageId> dirtyPages) {
        PageMemoryImpl mock = mock(PageMemoryImpl.class);

        when(mock.beginCheckpoint(any(CompletableFuture.class))).thenReturn(dirtyPages);

        return mock;
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
}
