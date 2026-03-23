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
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.PageIndexesWithPartitionGeneration.pageIndexesWithPartGen;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.dirtyFullPageIds;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.TestCheckpointUtils.union;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
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
        DirtyPagesAndPartitions dirtyPages0 = createDirtyPagesAndPartitions(of(0, 0, 0), of(0, 0, 1));
        DirtyPagesAndPartitions dirtyPages1 = createDirtyPagesAndPartitions(of(1, 0, 0), of(1, 0, 1), of(1, 0, 2));

        assertEquals(0, EMPTY.dirtyPagesCount());
        assertEquals(2, new CheckpointDirtyPages(List.of(dirtyPages0)).dirtyPagesCount());
        assertEquals(3, new CheckpointDirtyPages(List.of(dirtyPages1)).dirtyPagesCount());
        assertEquals(5, new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1)).dirtyPagesCount());
    }

    @Test
    void testToDirtyPartitionQueue() {
        assertTrue(EMPTY.toDirtyPartitionQueue().isEmpty());

        DirtyPagesAndPartitions dirtyPages0 = createDirtyPagesAndPartitions(of(0, 0, 0));
        DirtyPagesAndPartitions dirtyPages1 = createDirtyPagesAndPartitions(of(1, 0, 0), of(1, 0, 1));
        DirtyPagesAndPartitions dirtyPages2 = createDirtyPagesAndPartitions(of(2, 0, 0), of(2, 1, 0), of(3, 2, 2));

        var checkpointDirtyPages = new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1, dirtyPages2));

        assertThat(
                toListPair(checkpointDirtyPages.toDirtyPartitionQueue()),
                equalTo(toListDirtyPartitionPair(dirtyPages0, dirtyPages1, dirtyPages2))
        );
    }

    @Test
    void testGetPartitionViewByPageMemory() {
        assertThrows(IllegalArgumentException.class, () -> EMPTY.getPartitionView(mock(PersistentPageMemory.class), 0, 0));

        DirtyPagesAndPartitions dirtyPages0 = createDirtyPagesAndPartitions(of(0, 0, 0));
        DirtyPagesAndPartitions dirtyPages1 = createDirtyPagesAndPartitions(of(5, 0, 0));
        DirtyPagesAndPartitions dirtyPages2 = createDirtyPagesAndPartitions(of(1, 0, 0), of(1, 0, 1));
        DirtyPagesAndPartitions dirtyPages3 = createDirtyPagesAndPartitions(
                of(2, 0, 0), of(2, 0, 1),
                of(2, 1, 1),
                of(3, 2, 2), of(3, 2, 3)
        );

        var checkpointDirtyPages = new CheckpointDirtyPages(List.of(dirtyPages0, dirtyPages1, dirtyPages2, dirtyPages3));

        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages0.pageMemory, 4, 0));
        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages1.pageMemory, 4, 0));
        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages2.pageMemory, 2, 2));
        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages3.pageMemory, 2, 2));
        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages1.pageMemory, 0, 0));
        assertNull(checkpointDirtyPages.getPartitionView(dirtyPages2.pageMemory, 5, 0));

        assertThat(
                toListDirtyPagePair(checkpointDirtyPages.getPartitionView(dirtyPages0.pageMemory, 0, 0)),
                equalTo(toListDirtyPagePair(dirtyPages0))
        );

        assertThat(
                toListDirtyPagePair(checkpointDirtyPages.getPartitionView(dirtyPages1.pageMemory, 5, 0)),
                equalTo(toListDirtyPagePair(dirtyPages1))
        );

        assertThat(
                toListDirtyPagePair(checkpointDirtyPages.getPartitionView(dirtyPages2.pageMemory, 1, 0)),
                equalTo(toListDirtyPagePair(dirtyPages2))
        );

        assertThat(
                toListDirtyPagePair(checkpointDirtyPages.getPartitionView(dirtyPages3.pageMemory, 2, 0)),
                equalTo(toListDirtyPagePair(equalsByGroupAndPartition(2, 0), dirtyPages3))
        );

        assertThat(
                toListDirtyPagePair(checkpointDirtyPages.getPartitionView(dirtyPages3.pageMemory, 2, 1)),
                equalTo(toListDirtyPagePair(equalsByGroupAndPartition(2, 1), dirtyPages3))
        );

        assertThat(
                toListDirtyPagePair(checkpointDirtyPages.getPartitionView(dirtyPages3.pageMemory, 3, 2)),
                equalTo(toListDirtyPagePair(equalsByGroupAndPartition(3, 2), dirtyPages3))
        );
    }

    @Test
    void testSortDirtyPageIds() {
        List<DirtyFullPageId> pageIds = new ArrayList<>(List.of(
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
    void testSortDirtyPageIdsWithDifferentPartitionGeneration() {
        DirtyFullPageId[] dirtyFullPageIds0 = dirtyFullPageIds(
                2,
                1,
                pageIndexesWithPartGen(1, 0, 1, 2),
                pageIndexesWithPartGen(2, 0, 1, 3),
                pageIndexesWithPartGen(3, 3, 4)
        );

        DirtyFullPageId[] dirtyFullPageIds1 = dirtyFullPageIds(
                2,
                0,
                pageIndexesWithPartGen(1, 0, 6, 7)
        );

        DirtyFullPageId[] dirtyFullPageIds2 = dirtyFullPageIds(
                0,
                2,
                pageIndexesWithPartGen(2, 8, 9)
        );

        DirtyFullPageId[] union = union(dirtyFullPageIds0, dirtyFullPageIds1, dirtyFullPageIds2);

        Arrays.sort(union, DIRTY_PAGE_COMPARATOR);

        DirtyFullPageId[] exp = {
                // GroupId=0, partitionId=2.
                of(0, 2, 8, 2), of(0, 2, 9, 2),
                // GroupId=2, partitionId=0.
                of(2, 0, 0, 1), of(2, 0, 6, 1), of(2, 0, 7, 1),
                // GroupId=2, partitionId=1.
                of(2, 1, 0, 1), of(2, 1, 0, 2),
                of(2, 1, 1, 1), of(2, 1, 1, 2),
                of(2, 1, 2, 1),
                of(2, 1, 3, 2), of(2, 1, 3, 3),
                of(2, 1, 4, 3)
        };

        assertArrayEquals(exp, union, "Sorting is expected to be: groupId -> partitionId -> pageIdx -> partitionGeneration.");
    }

    @Test
    void testContainsMetaPage() {
        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        DirtyFullPageId[] dirtyPages = dirtyFullPageIds(
                2,
                1,
                pageIndexesWithPartGen(1, 0, 1, 2),
                pageIndexesWithPartGen(2, 0, 1, 3),
                pageIndexesWithPartGen(3, 3, 4)
        );

        var checkpointDirtyPages = new CheckpointDirtyPages(
                List.of(TestCheckpointUtils.createDirtyPagesAndPartitions(pageMemory, dirtyPages))
        );

        CheckpointDirtyPagesView partitionView = checkpointDirtyPages.getPartitionView(pageMemory, 2, 1);
        assertNotNull(partitionView);

        assertFalse(partitionView.containsMetaPage(0));
        assertTrue(partitionView.containsMetaPage(1));
        assertTrue(partitionView.containsMetaPage(2));
        assertFalse(partitionView.containsMetaPage(3));
    }

    @Test
    void testCountUpdatedPages() {
        PersistentPageMemory pageMemory = mock(PersistentPageMemory.class);

        DirtyFullPageId[] dirtyPages = dirtyFullPageIds(
                2,
                1,
                pageIndexesWithPartGen(1, 0, 1, 2),
                pageIndexesWithPartGen(2, 0, 1, 3),
                pageIndexesWithPartGen(3, 3, 4)
        );

        var checkpointDirtyPages = new CheckpointDirtyPages(
                List.of(TestCheckpointUtils.createDirtyPagesAndPartitions(pageMemory, dirtyPages))
        );

        CheckpointDirtyPagesView partitionView = checkpointDirtyPages.getPartitionView(pageMemory, 2, 1);
        assertNotNull(partitionView);

        assertEquals(0, partitionView.countAlteredPages(0, 0));
        assertEquals(0, partitionView.countAlteredPages(0, 1));

        assertEquals(0, partitionView.countAlteredPages(1, 0));
        assertEquals(1, partitionView.countAlteredPages(1, 1));
        assertEquals(2, partitionView.countAlteredPages(1, 2));
        assertEquals(3, partitionView.countAlteredPages(1, 3));

        assertEquals(0, partitionView.countAlteredPages(2, 0));
        assertEquals(1, partitionView.countAlteredPages(2, 1));
        assertEquals(2, partitionView.countAlteredPages(2, 2));
        assertEquals(2, partitionView.countAlteredPages(2, 3));
        assertEquals(3, partitionView.countAlteredPages(2, 4));
        assertEquals(3, partitionView.countAlteredPages(2, 5));

        assertEquals(0, partitionView.countAlteredPages(3, 0));
        assertEquals(0, partitionView.countAlteredPages(3, 1));
        assertEquals(0, partitionView.countAlteredPages(3, 2));
        assertEquals(0, partitionView.countAlteredPages(3, 3));
        assertEquals(1, partitionView.countAlteredPages(3, 4));
        assertEquals(2, partitionView.countAlteredPages(3, 5));
        assertEquals(2, partitionView.countAlteredPages(3, 6));
    }

    private static DirtyPagesAndPartitions createDirtyPagesAndPartitions(DirtyFullPageId... pageIds) {
        return TestCheckpointUtils.createDirtyPagesAndPartitions(mock(PersistentPageMemory.class), pageIds);
    }

    private static DirtyFullPageId of(int groupId, int partId, int pageIdx) {
        return new DirtyFullPageId(PageIdUtils.pageId(partId, (byte) 0, pageIdx), groupId, 1);
    }

    private static DirtyFullPageId of(int groupId, int partId, int pageIdx, int partGen) {
        return new DirtyFullPageId(PageIdUtils.pageId(partId, (byte) 0, pageIdx), groupId, partGen);
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

    private static List<IgniteBiTuple<PersistentPageMemory, GroupPartitionId>> toListDirtyPartitionPair(
            DirtyPagesAndPartitions... dirtyPagesAndPartitions
    ) {
        return Stream.of(dirtyPagesAndPartitions)
                .flatMap(pages -> pages.dirtyPartitions.stream().map(partitionId -> new IgniteBiTuple<>(pages.pageMemory, partitionId)))
                .collect(toList());
    }

    private static List<IgniteBiTuple<PersistentPageMemory, DirtyFullPageId>> toListDirtyPagePair(DirtyPagesAndPartitions... dirtyPages) {
        return toListDirtyPagePair(dirtyPageId -> true, dirtyPages);
    }

    private static List<IgniteBiTuple<PersistentPageMemory, DirtyFullPageId>> toListDirtyPagePair(
            Predicate<DirtyFullPageId> predicate,
            DirtyPagesAndPartitions... dirtyPages
    ) {
        return Stream.of(dirtyPages)
                .flatMap(pages -> Stream.of(pages.dirtyPages)
                        .filter(predicate)
                        .map(pageId -> new IgniteBiTuple<>(pages.pageMemory, pageId))
                )
                .collect(toList());
    }

    private static List<IgniteBiTuple<PersistentPageMemory, DirtyFullPageId>> toListDirtyPagePair(CheckpointDirtyPagesView view) {
        return IntStream.range(0, view.size()).mapToObj(i -> new IgniteBiTuple<>(view.pageMemory(), view.get(i))).collect(toList());
    }

    private static Predicate<DirtyFullPageId> equalsByGroupAndPartition(int grpId, int partId) {
        return fullPageId -> fullPageId.groupId() == grpId && partitionId(fullPageId.pageId()) == partId;
    }
}
