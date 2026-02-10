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

package org.apache.ignite.internal.pagememory.persistence.compaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionDestructionLockManager;
import org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIo;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap;
import org.apache.ignite.internal.pagememory.persistence.store.LongOperationAsyncExecutor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * For {@link Compactor} testing.
 */
public class CompactorTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 1024;

    @Test
    void testStartAndStop() throws Exception {
        Compactor compactor = newCompactor();

        assertNull(compactor.runner());

        assertFalse(compactor.isCancelled());
        assertFalse(compactor.isDone());
        assertFalse(Thread.currentThread().isInterrupted());

        compactor.start();

        assertTrue(waitForCondition(() -> compactor.runner() != null, 10, 1_000));

        assertFalse(compactor.isCancelled());
        assertFalse(compactor.isDone());
        assertFalse(Thread.currentThread().isInterrupted());

        compactor.stop();

        assertTrue(waitForCondition(() -> compactor.runner() == null, 10, 1_000));

        assertTrue(compactor.isCancelled());
        assertTrue(compactor.isDone());
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    void testMergeDeltaFileToMainFile() throws Throwable {
        Compactor compactor = newCompactor();

        DeltaFilePageStoreIo deltaFilePageStoreIo = createDeltaFilePageStoreIo();
        FilePageStore filePageStore = createFilePageStore(deltaFilePageStoreIo);

        compactor.mergeDeltaFileToMainFile(filePageStore, deltaFilePageStoreIo, new CompactionMetricsTracker());

        verify(deltaFilePageStoreIo, times(1)).readWithMergedToFilePageStoreCheck(eq(0L), eq(0L), any(ByteBuffer.class), anyBoolean());
        verify(filePageStore, times(1)).write(eq(1L), any(ByteBuffer.class));

        verify(filePageStore, times(1)).sync();
        verify(filePageStore, times(1)).removeDeltaFile(eq(deltaFilePageStoreIo));

        verify(deltaFilePageStoreIo, times(1)).markMergedToFilePageStore();
        verify(deltaFilePageStoreIo, times(1)).stop(eq(true));
    }

    @Test
    void testMergeDeltaFileToMainFileWithNewerDeltaFile() throws Throwable {
        Compactor compactor = newCompactor();

        DeltaFilePageStoreIo deltaFilePageStoreIo = createDeltaFilePageStoreIo(new int[]{0, 1});
        when(deltaFilePageStoreIo.fileIndex()).thenReturn(2);
        FilePageStore filePageStore = createFilePageStore(deltaFilePageStoreIo);

        DeltaFilePageStoreIo newerDeltaFile = createDeltaFilePageStoreIo(new int[]{1, 2});
        when(newerDeltaFile.fileIndex()).thenReturn(3);
        DeltaFilePageStoreIo olderDeltaFile = createDeltaFilePageStoreIo(new int[]{0, 1});
        when(olderDeltaFile.fileIndex()).thenReturn(1);

        when(filePageStore.getCompletedDeltaFiles()).thenReturn(List.of(deltaFilePageStoreIo, olderDeltaFile, newerDeltaFile));

        compactor.mergeDeltaFileToMainFile(filePageStore, deltaFilePageStoreIo, new CompactionMetricsTracker());

        verify(deltaFilePageStoreIo, times(1)).readWithMergedToFilePageStoreCheck(eq(0L), eq(0L), any(ByteBuffer.class), anyBoolean());

        verify(filePageStore, times(1)).getCompletedDeltaFiles();
        verify(filePageStore, times(1)).write(eq(1L), any(ByteBuffer.class));

        verify(filePageStore, times(1)).sync();
        verify(filePageStore, times(1)).removeDeltaFile(eq(deltaFilePageStoreIo));

        verify(deltaFilePageStoreIo, times(1)).markMergedToFilePageStore();
        verify(deltaFilePageStoreIo, times(1)).stop(eq(true));
    }

    @Test
    void testMergeDeltaFileWithMultipleNewerDeltaFiles() throws Throwable {
        Compactor compactor = newCompactor();

        DeltaFilePageStoreIo deltaFilePageStoreIo = createDeltaFilePageStoreIo(new int[]{0, 1, 2, 3, 4, 5});
        when(deltaFilePageStoreIo.fileIndex()).thenReturn(1);

        // Check processing the first page index.
        DeltaFilePageStoreIo newerDeltaFile1 = createDeltaFilePageStoreIo(new int[]{0});
        when(newerDeltaFile1.fileIndex()).thenReturn(2);

        // Check processing the last page index.
        DeltaFilePageStoreIo newerDeltaFile2 = createDeltaFilePageStoreIo(new int[]{5});
        when(newerDeltaFile2.fileIndex()).thenReturn(3);

        // No matches.
        DeltaFilePageStoreIo newerDeltaFileNoMatches = createDeltaFilePageStoreIo(new int[]{30});
        when(newerDeltaFileNoMatches.fileIndex()).thenReturn(4);

        // Multiple page matches.
        DeltaFilePageStoreIo newerDeltaFile3 = createDeltaFilePageStoreIo(new int[]{2, 3});
        when(newerDeltaFile3.fileIndex()).thenReturn(5);

        FilePageStore filePageStore = createFilePageStore(deltaFilePageStoreIo);
        when(filePageStore.getCompletedDeltaFiles()).thenReturn(List.of(
                deltaFilePageStoreIo, newerDeltaFile1, newerDeltaFileNoMatches, newerDeltaFile2, newerDeltaFile3
        ));

        CompactionMetricsTracker tracker = new CompactionMetricsTracker();
        compactor.mergeDeltaFileToMainFile(filePageStore, deltaFilePageStoreIo, tracker);

        // Pages 1, 4 were in no newer delta files, so they should be written.
        verify(filePageStore, times(2)).write(eq(1L), any(ByteBuffer.class));

        verify(deltaFilePageStoreIo, times(1)).readWithMergedToFilePageStoreCheck(eq(1L), anyLong(), any(ByteBuffer.class), anyBoolean());
        verify(deltaFilePageStoreIo, times(1)).readWithMergedToFilePageStoreCheck(eq(4L), anyLong(), any(ByteBuffer.class), anyBoolean());

        verify(filePageStore, times(1)).sync();
        verify(filePageStore, times(1)).removeDeltaFile(eq(deltaFilePageStoreIo));

        assertThat(tracker.dataPagesSkipped(), is(4));
        assertThat(tracker.dataPagesWritten(), is(2));
    }

    @Test
    void testMergeDeltaFileWhenAllPagesSkipped() throws Throwable {
        Compactor compactor = newCompactor();

        // Delta file to compact has pages [0, 1]
        DeltaFilePageStoreIo deltaFilePageStoreIo = createDeltaFilePageStoreIo(new int[]{0, 1});
        when(deltaFilePageStoreIo.fileIndex()).thenReturn(1);

        DeltaFilePageStoreIo newerDeltaFile = createDeltaFilePageStoreIo(new int[]{0, 1});
        when(newerDeltaFile.fileIndex()).thenReturn(2);

        FilePageStore filePageStore = createFilePageStore(deltaFilePageStoreIo);
        when(filePageStore.getCompletedDeltaFiles()).thenReturn(List.of(deltaFilePageStoreIo, newerDeltaFile));

        compactor.mergeDeltaFileToMainFile(filePageStore, deltaFilePageStoreIo, new CompactionMetricsTracker());

        verify(filePageStore, never()).write(anyLong(), any(ByteBuffer.class));

        verify(deltaFilePageStoreIo, never()).readWithMergedToFilePageStoreCheck(anyLong(), anyLong(), any(ByteBuffer.class), anyBoolean());

        verify(filePageStore, never()).sync();

        verify(filePageStore, times(1)).removeDeltaFile(eq(deltaFilePageStoreIo));
        verify(deltaFilePageStoreIo, times(1)).markMergedToFilePageStore();
        verify(deltaFilePageStoreIo, times(1)).stop(eq(true));
    }

    @Test
    void testDoCompaction() throws Throwable {
        FilePageStore filePageStore = mock(FilePageStore.class);

        AtomicReference<DeltaFilePageStoreIo> deltaFilePageStoreIoRef = new AtomicReference<>(mock(DeltaFilePageStoreIo.class));

        when(filePageStore.getDeltaFileToCompaction()).then(answer -> deltaFilePageStoreIoRef.get());

        var groupPageStoresMap = new GroupPageStoresMap<FilePageStore>(new LongOperationAsyncExecutor("test", log));

        groupPageStoresMap.put(new GroupPartitionId(0, 0), filePageStore);

        FilePageStoreManager filePageStoreManager = newFilePageStoreManager(groupPageStoresMap);

        Compactor compactor = spy(newCompactor(filePageStoreManager));

        doAnswer(answer -> {
            assertSame(filePageStore, answer.getArgument(0));
            assertSame(deltaFilePageStoreIoRef.get(), answer.getArgument(1));

            deltaFilePageStoreIoRef.set(null);

            return null;
        })
                .when(compactor)
                .mergeDeltaFileToMainFile(any(FilePageStore.class), any(DeltaFilePageStoreIo.class), any(CompactionMetricsTracker.class));

        compactor.doCompaction();

        verify(filePageStore, times(2)).getDeltaFileToCompaction();

        verify(compactor, times(1)).mergeDeltaFileToMainFile(
                any(FilePageStore.class),
                any(DeltaFilePageStoreIo.class),
                any(CompactionMetricsTracker.class)
        );
    }

    @Test
    void testBody() throws Exception {
        Compactor compactor = spy(newCompactor());

        doNothing().when(compactor).waitDeltaFiles();

        doAnswer(answer -> {
            compactor.cancel();

            return null;
        }).when(compactor).doCompaction();

        compactor.body();

        verify(compactor, times(3)).isCancelled();
        verify(compactor, times(1)).waitDeltaFiles();
        verify(compactor, times(1)).doCompaction();
    }

    @Test
    void testWaitDeltaFiles() throws Exception {
        Compactor compactor = spy(newCompactor());

        CompletableFuture<?> waitDeltaFilesFuture = runAsync(compactor::waitDeltaFiles);

        assertThrows(TimeoutException.class, () -> waitDeltaFilesFuture.get(100, MILLISECONDS));

        compactor.triggerCompaction();

        waitDeltaFilesFuture.get(100, MILLISECONDS);
    }

    @Test
    void testCancel() throws Exception {
        Compactor compactor = spy(newCompactor());

        assertFalse(compactor.isCancelled());

        CompletableFuture<?> waitDeltaFilesFuture = runAsync(compactor::waitDeltaFiles);

        assertThrows(TimeoutException.class, () -> waitDeltaFilesFuture.get(100, MILLISECONDS));

        compactor.cancel();

        assertTrue(compactor.isCancelled());
        assertFalse(Thread.currentThread().isInterrupted());

        waitDeltaFilesFuture.get(100, MILLISECONDS);
    }

    @Test
    void testNotifyCheckpointStartAndFinish() throws Exception {
        var groupPageStoresMap = new GroupPageStoresMap<FilePageStore>(new LongOperationAsyncExecutor("test", log));

        FilePageStoreManager filePageStoreManager = newFilePageStoreManager(groupPageStoresMap);

        Compactor compactor = spy(newCompactor(filePageStoreManager));

        compactor.notifyCheckpointStart();

        DeltaFilePageStoreIo deltaFilePageStoreIo = createDeltaFilePageStoreIo();
        FilePageStore filePageStore = createFilePageStore(deltaFilePageStoreIo);

        groupPageStoresMap.put(new GroupPartitionId(0, 0), filePageStore);

        assertThat(runAsync(compactor::doCompaction), willCompleteSuccessfully());

        verify(filePageStore, never()).write(anyLong(), any());
        verify(filePageStore, never()).sync();
        verify(filePageStore, never()).removeDeltaFile(any());

        verify(deltaFilePageStoreIo, never()).readWithMergedToFilePageStoreCheck(anyLong(), anyLong(), any(), anyBoolean());
        verify(deltaFilePageStoreIo, never()).markMergedToFilePageStore();
        verify(deltaFilePageStoreIo, never()).stop(anyBoolean());

        compactor.notifyCheckpointFinish();

        assertThat(runAsync(compactor::doCompaction), willCompleteSuccessfully());

        verify(filePageStore).write(anyLong(), any());
        verify(filePageStore).sync();
        verify(filePageStore).removeDeltaFile(any());

        verify(deltaFilePageStoreIo).readWithMergedToFilePageStoreCheck(anyLong(), anyLong(), any(), anyBoolean());
        verify(deltaFilePageStoreIo).markMergedToFilePageStore();
        verify(deltaFilePageStoreIo).stop(anyBoolean());
    }

    @Test
    void testTriggerCompactionAfterNotifyCheckpointStartAndFinish() {
        Compactor compactor = spy(newCompactor());

        compactor.notifyCheckpointStart();

        CompletableFuture<?> waitDeltaFilesFuture = runAsync(compactor::waitDeltaFiles);
        assertThat(waitDeltaFilesFuture, willTimeoutFast());

        compactor.triggerCompaction();
        assertThat(waitDeltaFilesFuture, willCompleteSuccessfully());
    }

    @Test
    void testWaitDeltaFilesAfterResume() {
        Compactor compactor = spy(newCompactor());

        CompletableFuture<?> waitDeltaFilesFuture = runAsync(compactor::waitDeltaFiles);
        assertThat(waitDeltaFilesFuture, willTimeoutFast());

        compactor.notifyCheckpointFinish();
        assertThat(waitDeltaFilesFuture, willCompleteSuccessfully());
    }

    private Compactor newCompactor() {
        return newCompactor(mock(FilePageStoreManager.class));
    }

    private Compactor newCompactor(FilePageStoreManager filePageStoreManager) {
        return new Compactor(
                log,
                "test",
                1,
                filePageStoreManager,
                PAGE_SIZE,
                mock(FailureManager.class),
                new PartitionDestructionLockManager()
        );
    }

    private static DeltaFilePageStoreIo createDeltaFilePageStoreIo() throws Exception {
        return createDeltaFilePageStoreIo(new int[]{0});
    }

    private static DeltaFilePageStoreIo createDeltaFilePageStoreIo(int[] pageIndexes) throws Exception {
        DeltaFilePageStoreIo deltaFilePageStoreIo = mock(DeltaFilePageStoreIo.class);

        when(deltaFilePageStoreIo.pageIndexes()).thenReturn(pageIndexes);

        when(deltaFilePageStoreIo.readWithMergedToFilePageStoreCheck(anyLong(), anyLong(), any(ByteBuffer.class), anyBoolean()))
                .then(answer -> {
                    ByteBuffer buffer = answer.getArgument(2);

                    PageIo.setPageId(bufferAddress(buffer), 1);

                    return true;
                });

        return deltaFilePageStoreIo;
    }

    private static FilePageStore createFilePageStore(DeltaFilePageStoreIo deltaFilePageStoreIo) {
        FilePageStore filePageStore = mock(FilePageStore.class);

        var removedDeltaFile = new AtomicBoolean();

        when(filePageStore.removeDeltaFile(eq(deltaFilePageStoreIo))).then(invocation -> {
            removedDeltaFile.set(true);

            return true;
        });
        when(filePageStore.getDeltaFileToCompaction()).then(invocation -> removedDeltaFile.get() ? null : deltaFilePageStoreIo);
        when(filePageStore.deltaFileCount()).thenReturn(1);

        return filePageStore;
    }

    private static FilePageStoreManager newFilePageStoreManager(GroupPageStoresMap<FilePageStore> map) {
        FilePageStoreManager manager = mock(FilePageStoreManager.class);

        when(manager.allPageStores()).then(invocation -> map.getAll());

        return manager;
    }
}
