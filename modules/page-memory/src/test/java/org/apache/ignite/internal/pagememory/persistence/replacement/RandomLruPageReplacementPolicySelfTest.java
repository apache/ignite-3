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

package org.apache.ignite.internal.pagememory.persistence.replacement;

import static org.apache.ignite.internal.pagememory.persistence.PageHeader.PAGE_OVERHEAD;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.dirtyFullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.fullPageId;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryProvider;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryRegion;
import org.apache.ignite.internal.pagememory.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.LoadedPagesMap;
import org.apache.ignite.internal.pagememory.persistence.PageHeader;
import org.apache.ignite.internal.pagememory.persistence.PagePool;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.Segment;
import org.apache.ignite.internal.pagememory.persistence.ReplaceCandidate;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointPages;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for some internal properties of the {@link RandomLruPageReplacementPolicy}.
 */
@ExtendWith(MockitoExtension.class)
class RandomLruPageReplacementPolicySelfTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 16 * 1024;

    @Mock
    private Segment segment;

    @Mock
    private LoadedPagesMap loadedPagesMap;

    @Mock
    private CheckpointPages checkpointPages;

    private PagePool pagePool;

    private RandomLruPageReplacementPolicy policy;

    private final DirectMemoryProvider memoryProvider = new UnsafeMemoryProvider(null);

    @BeforeEach
    void setUp(
            @Mock OffheapReadWriteLock readWriteLock,
            @Mock PageIoRegistry pageIoRegistry,
            @Mock PageIo pageIo
    ) throws IgniteInternalCheckedException {
        memoryProvider.initialize(new long[] { MiB });

        DirectMemoryRegion region = memoryProvider.nextRegion();

        assertNotNull(region);

        pagePool = new PagePool(0, region, PAGE_SIZE + PAGE_OVERHEAD, readWriteLock);

        when(segment.loadedPages()).thenReturn(loadedPagesMap);
        when(segment.checkpointPages()).thenReturn(checkpointPages);
        when(segment.pool()).thenReturn(pagePool);

        when(segment.absolute(anyLong())).thenAnswer(invocationOnMock -> {
            long relPtr = invocationOnMock.getArgument(0);

            return pagePool.absolute(relPtr);
        });

        lenient().when(segment.ioRegistry()).thenReturn(pageIoRegistry);
        lenient().when(pageIoRegistry.resolve(anyLong())).thenReturn(pageIo);

        policy = new RandomLruPageReplacementPolicy(segment);
    }

    @AfterEach
    void tearDown() {
        memoryProvider.shutdown(true);
    }

    @Test
    void dirtyPageThatWillBeCheckpointedCanBeReplaced() throws IgniteInternalCheckedException {
        when(loadedPagesMap.capacity()).thenReturn(1);
        when(segment.tryToRemovePage(any(), anyLong())).thenReturn(true);

        long pageRelPtr = pagePool.borrowOrAllocateFreePage(0);

        long pageAbsPtr = pagePool.absolute(pageRelPtr);

        PageHeader.dirty(pageAbsPtr, true);

        FullPageId fullPageId = fullPageId(pageAbsPtr);
        DirtyFullPageId dirtyFullPageId = dirtyFullPageId(pageAbsPtr);

        when(checkpointPages.contains(dirtyFullPageId)).thenReturn(true);

        when(loadedPagesMap.getNearestAt(anyInt())).thenReturn(new ReplaceCandidate(42, pageRelPtr, fullPageId));

        assertThat(policy.replace(), is(pageRelPtr));
    }

    @RepeatedTest(5)
    void dirtyPageThatWillNotBeCheckpointedCanNotBeReplaced() throws IgniteInternalCheckedException {
        // Create pages: one is dirty and one is not. We then expect that the dirty page will not be replaced, because it is not a part
        // of an ongoing checkpoint.
        when(loadedPagesMap.capacity()).thenReturn(2);

        long pageRelPtr = pagePool.borrowOrAllocateFreePage(0);
        long dirtyPageRelPtr = pagePool.borrowOrAllocateFreePage(0);

        long pageAbsPtr = pagePool.absolute(pageRelPtr);
        long dirtyPageAbsPtr = pagePool.absolute(dirtyPageRelPtr);

        when(segment.tryToRemovePage(any(), eq(pageAbsPtr)))
                .thenReturn(true);

        PageHeader.dirty(dirtyPageAbsPtr, true);

        when(loadedPagesMap.getNearestAt(0))
                .thenReturn(new ReplaceCandidate(42, pageRelPtr, fullPageId(pageAbsPtr)));
        when(loadedPagesMap.getNearestAt(1))
                .thenReturn(new ReplaceCandidate(42, dirtyPageRelPtr, fullPageId(dirtyPageAbsPtr)));

        // Only gets called during fallback to the sequential search.
        lenient().when(segment.getWriteHoldCount()).thenReturn(1);

        assertThat(policy.replace(), is(pageRelPtr));
    }
}
