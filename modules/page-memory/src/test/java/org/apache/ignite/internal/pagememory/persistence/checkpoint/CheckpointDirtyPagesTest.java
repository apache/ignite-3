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
import java.util.stream.Stream;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.CheckpointDirtyPagesQueue;
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
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages0 = createDirtyPages(of(0, 0, 0), of(0, 0, 1));
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages1 = createDirtyPages(of(1, 0, 0), of(1, 0, 1), of(1, 0, 2));

        assertEquals(0, EMPTY.dirtyPagesCount());
        assertEquals(2, new CheckpointDirtyPages(dirtyPages0).dirtyPagesCount());
        assertEquals(3, new CheckpointDirtyPages(dirtyPages1).dirtyPagesCount());
        assertEquals(5, new CheckpointDirtyPages(dirtyPages0, dirtyPages1).dirtyPagesCount());
    }

    @Test
    void testToQueue() {
        assertTrue(EMPTY.toQueue().isEmpty());

        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages0 = createDirtyPages(of(0, 0, 0));
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages1 = createDirtyPages(of(1, 0, 0), of(1, 0, 1));
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages2 = createDirtyPages(of(2, 0, 0), of(2, 1, 0), of(3, 2, 2));

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(dirtyPages0, dirtyPages1, dirtyPages2);

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

        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages0 = createDirtyPages(of(0, 0, 0));
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages1 = createDirtyPages(of(1, 0, 0), of(1, 0, 1));
        IgniteBiTuple<PersistentPageMemory, FullPageId[]> dirtyPages2 = createDirtyPages(of(2, 0, 0), of(2, 1, 0), of(3, 2, 2));

        CheckpointDirtyPages checkpointDirtyPages = new CheckpointDirtyPages(dirtyPages0, dirtyPages1, dirtyPages2);

        assertNull(checkpointDirtyPages.findView(4, 0));
        assertNull(checkpointDirtyPages.findView(3, 3));

        // TODO: continue

        checkpointDirtyPages.findView(0, 0);
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
        if (dirtyPages.length == 0) {
            return List.of();
        }

        return Stream.of(dirtyPages)
                .flatMap(pages -> Stream.of(pages.getValue()).map(p -> new IgniteBiTuple<>(pages.getKey(), p)))
                .collect(toList());
    }
}
