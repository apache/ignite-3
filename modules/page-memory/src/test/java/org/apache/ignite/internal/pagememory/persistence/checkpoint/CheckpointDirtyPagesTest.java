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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.DIRTY_PAGE_COMPARATOR;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.EMPTY;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesQueue;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesView;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.QueueResult;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.Test;

/**
 * For {@link CheckpointDirtyPages} testing.
 */
public class CheckpointDirtyPagesTest {
    @Test
    void testDirtyPagesCount() {
        var dirtyPages0 = createDirtyPages(of(0, 0, 0), of(0, 0, 1));
        var dirtyPages1 = createDirtyPages(of(1, 0, 0), of(1, 0, 1), of(1, 0, 2));

        assertEquals(0, EMPTY.dirtyPagesCount());
        assertEquals(2, new CheckpointDirtyPages(List.of(dirtyPages0)).dirtyPagesCount());
        assertEquals(3, new CheckpointDirtyPages(List.of(dirtyPages1)).dirtyPagesCount());
        assertEquals(5, new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1)).dirtyPagesCount());
    }

    @Test
    void testToQueue() {
        assertTrue(EMPTY.toQueue().isEmpty());

        var dirtyPages0 = createDirtyPages(of(0, 0, 0));
        var dirtyPages1 = createDirtyPages(of(1, 0, 0), of(1, 0, 1));
        var dirtyPages2 = createDirtyPages(of(2, 0, 0), of(2, 1, 0), of(3, 2, 2));

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1, dirtyPages2));

        CheckpointDirtyPagesQueue queue0 = checkpointDirtyPages.toQueue();

        assertFalse(queue0.isEmpty());
        assertEquals(6, queue0.size());

        assertThat(toListDirtyPagePair(queue0), equalTo(toListDirtyPagePair(dirtyPages0, dirtyPages1, dirtyPages2)));

        CheckpointDirtyPagesQueue queue1 = checkpointDirtyPages.toQueue();

        assertNotSame(queue0, queue1);

        assertFalse(queue1.isEmpty());
        assertEquals(6, queue1.size());
    }

    @Test
    void testFindView() {
        assertNull(EMPTY.findView(0, 0));

        var dirtyPages0 = createDirtyPages(of(0, 0, 0));
        var dirtyPages1 = createDirtyPages(of(5, 0, 0));
        var dirtyPages2 = createDirtyPages(of(1, 0, 0), of(1, 0, 1));
        var dirtyPages3 = createDirtyPages(of(2, 0, 0), of(2, 0, 1), of(2, 1, 1), of(3, 2, 2), of(3, 2, 3));

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1, dirtyPages2, dirtyPages3));

        assertNull(checkpointDirtyPages.findView(4, 0));
        assertNull(checkpointDirtyPages.findView(2, 2));
        assertNull(checkpointDirtyPages.findView(3, 1));
        assertNull(checkpointDirtyPages.findView(3, 3));

        assertThat(toListDirtyPagePair(checkpointDirtyPages.findView(0, 0)), equalTo(toListDirtyPagePair(dirtyPages0)));
        assertThat(toListDirtyPagePair(checkpointDirtyPages.findView(5, 0)), equalTo(toListDirtyPagePair(dirtyPages1)));
        assertThat(toListDirtyPagePair(checkpointDirtyPages.findView(1, 0)), equalTo(toListDirtyPagePair(dirtyPages2)));

        assertThat(
                toListDirtyPagePair(checkpointDirtyPages.findView(2, 0)),
                equalTo(toListDirtyPagePair(equalsByGroupAndPartition(2, 0), dirtyPages3))
        );

        assertThat(
                toListDirtyPagePair(checkpointDirtyPages.findView(2, 1)),
                equalTo(toListDirtyPagePair(equalsByGroupAndPartition(2, 1), dirtyPages3))
        );

        assertThat(
                toListDirtyPagePair(checkpointDirtyPages.findView(3, 2)),
                equalTo(toListDirtyPagePair(equalsByGroupAndPartition(3, 2), dirtyPages3))
        );
    }

    @Test
    void testNextView() {
        assertNull(EMPTY.nextView(null));

        var dirtyPages0 = createDirtyPages(of(0, 0, 0));
        var dirtyPages1 = createDirtyPages(of(5, 0, 0));
        var dirtyPages2 = createDirtyPages(of(1, 0, 0), of(1, 0, 1));
        var dirtyPages3 = createDirtyPages(of(2, 0, 0), of(2, 0, 1), of(2, 1, 1), of(3, 2, 2), of(3, 2, 3));

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1, dirtyPages2, dirtyPages3));

        CheckpointDirtyPagesView view = checkpointDirtyPages.nextView(null);

        assertThat(toListDirtyPagePair(view), equalTo(toListDirtyPagePair(dirtyPages0)));
        assertThat(toListDirtyPagePair(view = checkpointDirtyPages.nextView(view)), equalTo(toListDirtyPagePair(dirtyPages1)));
        assertThat(toListDirtyPagePair(view = checkpointDirtyPages.nextView(view)), equalTo(toListDirtyPagePair(dirtyPages2)));

        assertThat(
                toListDirtyPagePair(view = checkpointDirtyPages.nextView(view)),
                equalTo(toListDirtyPagePair(equalsByGroupAndPartition(2, 0), dirtyPages3))
        );

        assertThat(
                toListDirtyPagePair(view = checkpointDirtyPages.nextView(view)),
                equalTo(toListDirtyPagePair(equalsByGroupAndPartition(2, 1), dirtyPages3))
        );

        assertThat(
                toListDirtyPagePair(view = checkpointDirtyPages.nextView(view)),
                equalTo(toListDirtyPagePair(equalsByGroupAndPartition(3, 2), dirtyPages3))
        );

        assertNull(checkpointDirtyPages.nextView(view));
    }

    @Test
    void testSortDirtyPageIds() {
        List<FullPageId> pageIds = new ArrayList<>(List.of(
                of(10, 10, 10), of(10, 10, 9), of(10, 10, 0), of(10, 0, 10), of(10, 0, 0),
                of(0, 1, 3), of(0, 1, 2), of(0, 1, 1), of(0, 1, 0), of(0, 0, 0),
                of(1, 2, 0), of(1, 2, 1)
        ));

        pageIds.sort(DIRTY_PAGE_COMPARATOR);

        assertThat(
                pageIds,
                equalTo(List.of(
                        of(0, 0, 0), of(0, 1, 0), of(0, 1, 1), of(0, 1, 2), of(0, 1, 3),
                        of(1, 2, 0), of(1, 2, 1),
                        of(10, 0, 0), of(10, 0, 10), of(10, 10, 0), of(10, 10, 9), of(10, 10, 10)
                ))
        );
    }

    @Test
    void testQueueNextElementInDifferentThreads() throws Exception {
        var dirtyPages0 = createDirtyPages(of(0, 0, 0));
        var dirtyPages1 = createDirtyPages(of(1, 0, 0));
        var dirtyPages2 = createDirtyPages(of(2, 0, 0), of(2, 0, 1));
        var dirtyPages3 = createDirtyPages(of(3, 0, 0), of(3, 1, 0), of(4, 2, 2));

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1, dirtyPages2, dirtyPages3));

        CheckpointDirtyPagesQueue queue = checkpointDirtyPages.toQueue();

        runAsync(() -> {
            QueueResult queueResult0 = new QueueResult();

            assertTrue(queue.next(queueResult0));
            assertEquals(dirtyPages0.getKey(), queueResult0.pageMemory());
            assertEquals(dirtyPages0.getValue()[0], queueResult0.dirtyPage());

            QueueResult queueResult1 = new QueueResult();

            assertTrue(queue.next(queueResult1));
            assertEquals(dirtyPages1.getKey(), queueResult1.pageMemory());
            assertEquals(dirtyPages1.getValue()[0], queueResult1.dirtyPage());

            assertTrue(queue.next(queueResult0));
            assertEquals(dirtyPages2.getKey(), queueResult0.pageMemory());
            assertEquals(dirtyPages2.getValue()[0], queueResult0.dirtyPage());

            assertTrue(queue.next(queueResult1));
            assertEquals(dirtyPages2.getKey(), queueResult1.pageMemory());
            assertEquals(dirtyPages2.getValue()[1], queueResult1.dirtyPage());
        }).get(1, TimeUnit.SECONDS);

        QueueResult queueResult0 = new QueueResult();

        assertTrue(queue.next(queueResult0));
        assertEquals(dirtyPages3.getKey(), queueResult0.pageMemory());
        assertEquals(dirtyPages3.getValue()[0], queueResult0.dirtyPage());

        QueueResult queueResult1 = new QueueResult();

        assertTrue(queue.next(queueResult1));
        assertEquals(dirtyPages3.getKey(), queueResult1.pageMemory());
        assertEquals(dirtyPages3.getValue()[1], queueResult1.dirtyPage());

        assertTrue(queue.next(queueResult0));
        assertEquals(dirtyPages3.getKey(), queueResult0.pageMemory());
        assertEquals(dirtyPages3.getValue()[2], queueResult0.dirtyPage());
    }

    private static IgniteBiTuple<PersistentPageMemory, FullPageId[]> createDirtyPages(FullPageId... dirtyPages) {
        Arrays.sort(dirtyPages, DIRTY_PAGE_COMPARATOR);

        return new IgniteBiTuple<>(mock(PersistentPageMemory.class), dirtyPages);
    }

    private static FullPageId of(int groupId, int partId, int pageIdx) {
        return new FullPageId(PageIdUtils.pageId(partId, (byte) 0, pageIdx), groupId);
    }

    private static List<IgniteBiTuple<PersistentPageMemory, FullPageId>> toListDirtyPagePair(CheckpointDirtyPagesQueue queue) {
        if (queue.isEmpty()) {
            return List.of();
        }

        QueueResult queueResult = new QueueResult();

        List<IgniteBiTuple<PersistentPageMemory, FullPageId>> result = new ArrayList<>(queue.size());

        while (queue.next(queueResult)) {
            result.add(new IgniteBiTuple<>(queueResult.pageMemory(), queueResult.dirtyPage()));
        }

        return result;
    }

    private static List<IgniteBiTuple<PersistentPageMemory, FullPageId>> toListDirtyPagePair(
            IgniteBiTuple<PersistentPageMemory, FullPageId[]>... dirtyPages
    ) {
        return toListDirtyPagePair(dirtyPageId -> true, dirtyPages);
    }

    private static List<IgniteBiTuple<PersistentPageMemory, FullPageId>> toListDirtyPagePair(
            Predicate<FullPageId> dirtyPagePredicate,
            IgniteBiTuple<PersistentPageMemory, FullPageId[]>... dirtyPages
    ) {
        if (dirtyPages.length == 0) {
            return List.of();
        }

        return Stream.of(dirtyPages)
                .flatMap(pages -> Stream.of(pages.getValue()).filter(dirtyPagePredicate).map(p -> new IgniteBiTuple<>(pages.getKey(), p)))
                .collect(toList());
    }

    private static List<IgniteBiTuple<PersistentPageMemory, FullPageId>> toListDirtyPagePair(CheckpointDirtyPagesView view) {
        if (view.size() == 0) {
            return List.of();
        }

        return IntStream.range(0, view.size()).mapToObj(i -> new IgniteBiTuple<>(view.pageMemory(), view.get(i))).collect(toList());
    }

    private static List<IgniteBiTuple<PersistentPageMemory, FullPageId>> toListDirtyPagePair(
            Map<PersistentPageMemory, List<FullPageId>> dirtyPages
    ) {
        if (dirtyPages.isEmpty()) {
            return List.of();
        }

        return dirtyPages.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(pageId -> new IgniteBiTuple<>(entry.getKey(), pageId)))
                .collect(toList());
    }

    private static Predicate<FullPageId> equalsByGroupAndPartition(int grpId, int partId) {
        return fullPageId -> fullPageId.groupId() == grpId && partitionId(fullPageId.pageId()) == partId;
    }
}
