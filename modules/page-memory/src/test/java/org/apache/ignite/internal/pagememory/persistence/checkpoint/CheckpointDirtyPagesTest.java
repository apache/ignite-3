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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.DIRTY_PAGE_COMPARATOR;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.EMPTY;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesView;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.IgniteConcurrentMultiPairQueue.Result;
import org.junit.jupiter.api.Test;

/**
 * For {@link CheckpointDirtyPages} testing.
 */
public class CheckpointDirtyPagesTest extends BaseIgniteAbstractTest {
    @Test
    void testDirtyPagesCount() {
        DataRegionDirtyPages<FullPageId[]> dirtyPages0 = createDirtyPages(of(0, 0, 0), of(0, 0, 1));
        DataRegionDirtyPages<FullPageId[]> dirtyPages1 = createDirtyPages(of(1, 0, 0), of(1, 0, 1), of(1, 0, 2));

        assertEquals(0, EMPTY.dirtyPagesCount());
        assertEquals(2, new CheckpointDirtyPages(List.of(dirtyPages0)).dirtyPagesCount());
        assertEquals(3, new CheckpointDirtyPages(List.of(dirtyPages1)).dirtyPagesCount());
        assertEquals(5, new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1)).dirtyPagesCount());
    }

    @Test
    void testToDirtyPageIdQueue() {
        assertTrue(EMPTY.toDirtyPageIdQueue().isEmpty());

        DataRegionDirtyPages<FullPageId[]> dirtyPages0 = createDirtyPages(of(0, 0, 0));
        DataRegionDirtyPages<FullPageId[]> dirtyPages1 = createDirtyPages(of(1, 0, 0), of(1, 0, 1));
        DataRegionDirtyPages<FullPageId[]> dirtyPages2 = createDirtyPages(of(2, 0, 0), of(2, 1, 0), of(3, 2, 2));

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1, dirtyPages2));

        assertThat(
                toListPair(checkpointDirtyPages.toDirtyPageIdQueue()),
                equalTo(toListPair(dirtyPages0, dirtyPages1, dirtyPages2))
        );
    }

    @Test
    void testGetPartitionViewByPageMemory() {
        assertThrows(IllegalArgumentException.class, () -> EMPTY.getPartitionView(mock(PersistentPageMemory.class), 0, 0));

        DataRegionDirtyPages<FullPageId[]> dirtyPages0 = createDirtyPages(of(0, 0, 0));
        DataRegionDirtyPages<FullPageId[]> dirtyPages1 = createDirtyPages(of(5, 0, 0));
        DataRegionDirtyPages<FullPageId[]> dirtyPages2 = createDirtyPages(of(1, 0, 0), of(1, 0, 1));
        DataRegionDirtyPages<FullPageId[]> dirtyPages3 = createDirtyPages(
                of(2, 0, 0), of(2, 0, 1),
                of(2, 1, 1),
                of(3, 2, 2), of(3, 2, 3)
        );

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1, dirtyPages2, dirtyPages3));

        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages0.pageMemory, 4, 0));
        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages1.pageMemory, 4, 0));
        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages2.pageMemory, 2, 2));
        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages3.pageMemory, 2, 2));
        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages1.pageMemory, 0, 0));
        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages2.pageMemory, 5, 0));

        assertThat(
                toListPair(checkpointDirtyPages.getPartitionView(dirtyPages0.pageMemory, 0, 0)),
                equalTo(toListPair(dirtyPages0))
        );

        assertThat(
                toListPair(checkpointDirtyPages.getPartitionView(dirtyPages1.pageMemory, 5, 0)),
                equalTo(toListPair(dirtyPages1))
        );

        assertThat(
                toListPair(checkpointDirtyPages.getPartitionView(dirtyPages2.pageMemory, 1, 0)),
                equalTo(toListPair(dirtyPages2))
        );

        assertThat(
                toListPair(checkpointDirtyPages.getPartitionView(dirtyPages3.pageMemory, 2, 0)),
                equalTo(toListPair(equalsByGroupAndPartition(2, 0), dirtyPages3))
        );

        assertThat(
                toListPair(checkpointDirtyPages.getPartitionView(dirtyPages3.pageMemory, 2, 1)),
                equalTo(toListPair(equalsByGroupAndPartition(2, 1), dirtyPages3))
        );

        assertThat(
                toListPair(checkpointDirtyPages.getPartitionView(dirtyPages3.pageMemory, 3, 2)),
                equalTo(toListPair(equalsByGroupAndPartition(3, 2), dirtyPages3))
        );
    }

    @Test
    void testNextPartitionView() {
        assertNull(EMPTY.nextPartitionView(null));

        DataRegionDirtyPages<FullPageId[]> dirtyPages0 = createDirtyPages(of(0, 0, 0));
        DataRegionDirtyPages<FullPageId[]> dirtyPages1 = createDirtyPages(of(5, 0, 0));
        DataRegionDirtyPages<FullPageId[]> dirtyPages2 = createDirtyPages(of(1, 0, 0), of(1, 0, 1));
        DataRegionDirtyPages<FullPageId[]> dirtyPages3 = createDirtyPages(
                of(2, 0, 0), of(2, 0, 1),
                of(2, 1, 1),
                of(3, 2, 2), of(3, 2, 3)
        );

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1, dirtyPages2, dirtyPages3));

        CheckpointDirtyPagesView view = checkpointDirtyPages.nextPartitionView(null);

        assertThat(toListPair(view), equalTo(toListPair(dirtyPages0)));

        assertThat(
                toListPair(view = checkpointDirtyPages.nextPartitionView(view)),
                equalTo(toListPair(dirtyPages1))
        );

        assertThat(
                toListPair(view = checkpointDirtyPages.nextPartitionView(view)),
                equalTo(toListPair(dirtyPages2))
        );

        assertThat(
                toListPair(view = checkpointDirtyPages.nextPartitionView(view)),
                equalTo(toListPair(equalsByGroupAndPartition(2, 0), dirtyPages3))
        );

        assertThat(
                toListPair(view = checkpointDirtyPages.nextPartitionView(view)),
                equalTo(toListPair(equalsByGroupAndPartition(2, 1), dirtyPages3))
        );

        assertThat(
                toListPair(view = checkpointDirtyPages.nextPartitionView(view)),
                equalTo(toListPair(equalsByGroupAndPartition(3, 2), dirtyPages3))
        );

        assertNull(checkpointDirtyPages.nextPartitionView(view));
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

    private static DataRegionDirtyPages<FullPageId[]> createDirtyPages(FullPageId... pageIds) {
        Arrays.sort(pageIds, DIRTY_PAGE_COMPARATOR);

        return new DataRegionDirtyPages<>(mock(PersistentPageMemory.class), pageIds);
    }

    private static FullPageId of(int groupId, int partId, int pageIdx) {
        return new FullPageId(PageIdUtils.pageId(partId, (byte) 0, pageIdx), groupId);
    }

    private static <T> List<IgniteBiTuple<PersistentPageMemory, T>> toListPair(
            IgniteConcurrentMultiPairQueue<PersistentPageMemory, T> queue
    ) {
        return IntStream.range(0, queue.size())
                .mapToObj(i -> {
                    Result<PersistentPageMemory, T> queueResult = new Result<>();

                    queue.next(queueResult);

                    return new IgniteBiTuple<>(queueResult.getKey(), queueResult.getValue());
                })
                .collect(toList());
    }

    private static List<IgniteBiTuple<PersistentPageMemory, FullPageId>> toListPair(DataRegionDirtyPages<FullPageId[]>... dirtyPages) {
        return toListPair(dirtyPageId -> true, dirtyPages);
    }

    private static List<IgniteBiTuple<PersistentPageMemory, FullPageId>> toListPair(
            Predicate<FullPageId> predicate,
            DataRegionDirtyPages<FullPageId[]>... dirtyPages
    ) {
        return Stream.of(dirtyPages)
                .flatMap(pages -> Stream.of(pages.dirtyPages)
                        .filter(predicate)
                        .map(pageId -> new IgniteBiTuple<>(pages.pageMemory, pageId))
                )
                .collect(toList());
    }

    private static List<IgniteBiTuple<PersistentPageMemory, FullPageId>> toListPair(CheckpointDirtyPagesView view) {
        return IntStream.range(0, view.size()).mapToObj(i -> new IgniteBiTuple<>(view.pageMemory(), view.get(i))).collect(toList());
    }

    private static Predicate<FullPageId> equalsByGroupAndPartition(int grpId, int partId) {
        return fullPageId -> fullPageId.groupId() == grpId && partitionId(fullPageId.pageId()) == partId;
    }
}
