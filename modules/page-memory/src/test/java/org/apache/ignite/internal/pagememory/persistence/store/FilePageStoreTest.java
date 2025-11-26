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

package org.apache.ignite.internal.pagememory.persistence.store;

import static java.nio.ByteOrder.nativeOrder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.DELTA_FILE_VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.arr;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createDataPageId;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createPageByteBuffer;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.util.ArrayUtils.INT_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStore} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Test
    void testAllocatePage() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks allocation without writing pages.

        try (FilePageStore filePageStore0 = createFilePageStore(testFilePath)) {
            assertEquals(0, filePageStore0.pages());

            assertEquals(0, filePageStore0.allocatePage());

            assertEquals(1, filePageStore0.pages());

            assertEquals(1, filePageStore0.allocatePage());

            assertEquals(2, filePageStore0.pages());
        }

        try (FilePageStore filePageStore1 = createFilePageStore(testFilePath)) {
            filePageStore1.pages(2);

            assertEquals(2, filePageStore1.pages());

            filePageStore1.ensure();

            assertEquals(2, filePageStore1.pages());

            assertEquals(2, filePageStore1.allocatePage());

            assertEquals(3, filePageStore1.pages());
        }

        // Checks allocation with writing pages.

        try (FilePageStore filePageStore2 = createFilePageStore(testFilePath)) {
            filePageStore2.pages(0);

            assertEquals(0, filePageStore2.pages());

            long pageId0 = createDataPageId(filePageStore2::allocatePage);

            filePageStore2.write(pageId0, createPageByteBuffer(pageId0, PAGE_SIZE));

            assertEquals(1, filePageStore2.pages());

            long pageId1 = createDataPageId(filePageStore2::allocatePage);

            filePageStore2.write(pageId1, createPageByteBuffer(pageId1, PAGE_SIZE));

            assertEquals(2, filePageStore2.pages());
        }

        try (FilePageStore filePageStore3 = createFilePageStore(testFilePath)) {
            assertEquals(0, filePageStore3.pages());

            filePageStore3.ensure();

            assertEquals(0, filePageStore3.pages());
        }

        try (FilePageStore filePageStore4 = createFilePageStore(testFilePath)) {
            filePageStore4.pages(2);

            assertEquals(2, filePageStore4.pages());

            filePageStore4.ensure();

            assertEquals(2, filePageStore4.pages());

            assertEquals(2, filePageStore4.allocatePage());

            assertEquals(3, filePageStore4.pages());
        }
    }

    @Test
    void testPageAllocationListener() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
            ConcurrentLinkedQueue<Integer> allocatedPageIdx = new ConcurrentLinkedQueue<>();

            filePageStore.setPageAllocationListener(allocatedPageIdx::add);

            filePageStore.allocatePage();

            assertThat(allocatedPageIdx, contains(0));

            filePageStore.allocatePage();

            assertThat(allocatedPageIdx, contains(0, 1));
        }
    }

    @Test
    void testGetOrCreateNewDeltaFile() throws Exception {
        try (FilePageStore filePageStore = createFilePageStore(workDir.resolve("test"))) {
            int[] pageIndexes = arr(0, 1, 2);

            Supplier<int[]> pageIndexesSupplier = spy(new Supplier<>() {
                /** {@inheritDoc} */
                @Override
                public int[] get() {
                    return pageIndexes;
                }
            });

            IntFunction<Path> deltaFilePathFunction = spy(new IntFunction<>() {
                /** {@inheritDoc} */
                @Override
                public Path apply(int index) {
                    return workDir.resolve("testDelta" + index);
                }
            });

            CompletableFuture<DeltaFilePageStoreIo> future0 = filePageStore.getOrCreateNewDeltaFile(
                    deltaFilePathFunction,
                    pageIndexesSupplier
            );

            CompletableFuture<DeltaFilePageStoreIo> future1 = filePageStore.getOrCreateNewDeltaFile(
                    deltaFilePathFunction,
                    pageIndexesSupplier
            );

            assertSame(future0, future1);

            assertDoesNotThrow(() -> future0.get(1, SECONDS));

            verify(deltaFilePathFunction, times(1)).apply(0);
            verify(deltaFilePathFunction, times(1)).apply(anyInt());

            verify(pageIndexesSupplier, times(1)).get();

            DeltaFilePageStoreIo deltaIo = future0.join();

            assertEquals(PAGE_SIZE, deltaIo.pageSize());
            assertEquals(0, deltaIo.fileIndex());
        }
    }

    @Test
    void testStop() throws Exception {
        FilePageStoreIo filePageStoreIo = mock(FilePageStoreIo.class);

        DeltaFilePageStoreIo deltaFilePageStoreIo = mock(DeltaFilePageStoreIo.class);

        try (FilePageStore filePageStore = new FilePageStore(filePageStoreIo, deltaFilePageStoreIo)) {
            filePageStore.stop(true);
            filePageStore.stop(false);

            verify(filePageStoreIo, times(1)).stop(true);
            verify(filePageStoreIo, times(1)).stop(false);

            verify(deltaFilePageStoreIo, times(1)).stop(true);
            verify(deltaFilePageStoreIo, times(1)).stop(false);
        }
    }

    @Test
    void testClose() throws Exception {
        FilePageStoreIo filePageStoreIo = mock(FilePageStoreIo.class);

        DeltaFilePageStoreIo deltaFilePageStoreIo = mock(DeltaFilePageStoreIo.class);

        try (FilePageStore filePageStore = new FilePageStore(filePageStoreIo, deltaFilePageStoreIo)) {
            filePageStore.close();

            verify(filePageStoreIo, times(1)).close();
            verify(filePageStoreIo, times(1)).close();
        }
    }

    @Test
    void testReadWithDeltaFile() throws Exception {
        RandomAccessFileIoFactory ioFactory = new RandomAccessFileIoFactory();

        FilePageStoreHeader filePageStoreHeader = new FilePageStoreHeader(VERSION_1, PAGE_SIZE);

        DeltaFilePageStoreIoHeader deltaHeader0 = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(1));
        DeltaFilePageStoreIoHeader deltaHeader1 = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(2));

        Path filePageStoreFilePath = workDir.resolve("filePageStore");

        try (
                DeltaFilePageStoreIo deltaIo0 = spy(new DeltaFilePageStoreIo(ioFactory, deltaFilePath(0), deltaHeader0));
                FilePageStoreIo filePageStoreIo = spy(new FilePageStoreIo(ioFactory, filePageStoreFilePath, filePageStoreHeader));
                FilePageStore filePageStore = new FilePageStore(filePageStoreIo, deltaIo0);
        ) {
            DeltaFilePageStoreIo deltaIo1 = filePageStore
                    .getOrCreateNewDeltaFile(this::deltaFilePath, deltaHeader1::pageIndexes)
                    .get(1, SECONDS);

            // Fills with data.
            long pageId0 = createDataPageId(filePageStore::allocatePage);
            long pageId1 = createDataPageId(filePageStore::allocatePage);
            long pageId2 = createDataPageId(filePageStore::allocatePage);

            filePageStoreIo.write(pageId0, createPageByteBuffer(pageId0, PAGE_SIZE));
            deltaIo0.write(pageId1, createPageByteBuffer(pageId1, PAGE_SIZE));
            deltaIo1.write(pageId2, createPageByteBuffer(pageId2, PAGE_SIZE));

            filePageStoreIo.sync();
            deltaIo0.sync();
            filePageStore.completeNewDeltaFile();

            // Checks page reading.
            ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

            try {
                // Check read page from filePageStoreIo.
                filePageStore.read(pageId0, buffer, true);

                assertEquals(pageId0, PageIo.getPageId(buffer.rewind()));

                verify(filePageStoreIo, times(1)).read(eq(pageId0), anyLong(), eq(buffer), eq(true));

                // Check read page from deltaIo0.
                filePageStore.read(pageId1, buffer.rewind(), true);

                assertEquals(pageId1, PageIo.getPageId(buffer.rewind()));

                verify(filePageStoreIo, times(0)).read(eq(pageId1), anyLong(), eq(buffer), eq(true));

                // Check read page from deltaIo1.
                filePageStore.read(pageId2, buffer.rewind(), true);

                assertEquals(pageId2, PageIo.getPageId(buffer.rewind()));

                verify(filePageStoreIo, times(0)).read(eq(pageId2), anyLong(), eq(buffer), eq(true));
            } finally {
                freeBuffer(buffer);
            }
        }
    }

    @Test
    void testReadDirectlyWithoutPageIdCheck() throws Exception {
        RandomAccessFileIoFactory ioFactory = new RandomAccessFileIoFactory();

        FilePageStoreHeader filePageStoreHeader = new FilePageStoreHeader(VERSION_1, PAGE_SIZE);

        DeltaFilePageStoreIoHeader deltaHeader0 = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(1));
        DeltaFilePageStoreIoHeader deltaHeader1 = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(2));

        Path filePageStoreFilePath = workDir.resolve("filePageStore");

        try (
                DeltaFilePageStoreIo deltaIo0 = spy(new DeltaFilePageStoreIo(ioFactory, deltaFilePath(0), deltaHeader0));
                FilePageStoreIo filePageStoreIo = spy(new FilePageStoreIo(ioFactory, filePageStoreFilePath, filePageStoreHeader));
                FilePageStore filePageStore = new FilePageStore(filePageStoreIo, deltaIo0);
        ) {
            DeltaFilePageStoreIo deltaIo1 = filePageStore
                    .getOrCreateNewDeltaFile(this::deltaFilePath, deltaHeader1::pageIndexes)
                    .get(1, SECONDS);

            // Fills with data.
            long pageId0 = createDataPageId(filePageStore::allocatePage);
            long pageId1 = createDataPageId(filePageStore::allocatePage);
            long pageId2 = createDataPageId(filePageStore::allocatePage);

            filePageStoreIo.write(pageId0, createPageByteBuffer(pageId0, PAGE_SIZE));
            deltaIo0.write(pageId1, createPageByteBuffer(pageId1, PAGE_SIZE));
            deltaIo1.write(pageId2, createPageByteBuffer(pageId2, PAGE_SIZE));

            filePageStoreIo.sync();
            deltaIo0.sync();
            filePageStore.completeNewDeltaFile();

            // Checks page reading.
            ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

            try {
                // Check read page from filePageStoreIo.
                filePageStore.readWithoutPageIdCheck(pageId0, buffer, true);

                assertEquals(pageId0, PageIo.getPageId(buffer.rewind()));

                verify(filePageStoreIo, times(1)).read(eq(pageId0), anyLong(), eq(buffer), eq(true));

                // Check read page from deltaIo0.
                filePageStore.readWithoutPageIdCheck(pageId1, buffer.rewind(), true);

                assertEquals(pageId1, PageIo.getPageId(buffer.rewind()));

                verify(filePageStoreIo, times(0)).read(eq(pageId1), anyLong(), eq(buffer), eq(true));

                // Check read page from deltaIo1.
                filePageStore.readWithoutPageIdCheck(pageId2, buffer.rewind(), true);

                assertEquals(pageId2, PageIo.getPageId(buffer.rewind()));

                verify(filePageStoreIo, times(0)).read(eq(pageId2), anyLong(), eq(buffer), eq(true));

                // Let's check the reading of a non-existent page.
                long pageId3 = createDataPageId(() -> 3);

                filePageStore.readWithoutPageIdCheck(pageId3, buffer.rewind(), true);

                assertEquals(0, PageIo.getPageId(buffer.rewind()));

                verify(filePageStoreIo, times(1)).read(eq(pageId3), anyLong(), eq(buffer), eq(true));
            } finally {
                freeBuffer(buffer);
            }
        }
    }

    @Test
    void testDeltaFileCount() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
            assertEquals(0, filePageStore.deltaFileCount());

            filePageStore.getOrCreateNewDeltaFile(this::deltaFilePath, TestPageStoreUtils::arr).get(1, SECONDS);

            assertEquals(1, filePageStore.deltaFileCount());
        }

        try (FilePageStore filePageStore = createFilePageStore(testFilePath, mock(DeltaFilePageStoreIo.class))) {
            assertEquals(1, filePageStore.deltaFileCount());

            filePageStore.getOrCreateNewDeltaFile(this::deltaFilePath, TestPageStoreUtils::arr).get(1, SECONDS);

            assertEquals(2, filePageStore.deltaFileCount());
        }
    }

    @Test
    void testGetNewDeltaFile() throws Exception {
        try (FilePageStore filePageStore = createFilePageStore(workDir.resolve("test"))) {
            assertNull(filePageStore.getNewDeltaFile());

            CompletableFuture<DeltaFilePageStoreIo> future = filePageStore.getOrCreateNewDeltaFile(
                    this::deltaFilePath,
                    TestPageStoreUtils::arr
            );

            future.get(1, SECONDS);

            assertSame(future, filePageStore.getNewDeltaFile());

            filePageStore.completeNewDeltaFile();

            assertNull(filePageStore.getNewDeltaFile());
        }
    }

    @Test
    void testRemoveDeltaFile() throws Exception {
        DeltaFilePageStoreIo deltaFile0 = mock(DeltaFilePageStoreIo.class);
        DeltaFilePageStoreIo deltaFile1 = mock(DeltaFilePageStoreIo.class);

        try (FilePageStore filePageStore = createFilePageStore(workDir.resolve("test"), deltaFile0, deltaFile1)) {
            assertEquals(2, filePageStore.deltaFileCount());

            assertTrue(filePageStore.removeDeltaFile(deltaFile0));
            assertFalse(filePageStore.removeDeltaFile(deltaFile0));

            assertEquals(1, filePageStore.deltaFileCount());

            assertTrue(filePageStore.removeDeltaFile(deltaFile1));
            assertFalse(filePageStore.removeDeltaFile(deltaFile1));

            assertEquals(0, filePageStore.deltaFileCount());
        }
    }

    @Test
    void testGetDeltaFileToCompaction() throws Exception {
        DeltaFilePageStoreIo deltaFile0 = mock(DeltaFilePageStoreIo.class);
        DeltaFilePageStoreIo deltaFile1 = mock(DeltaFilePageStoreIo.class);

        when(deltaFile0.fileIndex()).thenReturn(0);
        when(deltaFile0.fileIndex()).thenReturn(1);

        try (FilePageStore filePageStore = createFilePageStore(workDir.resolve("test"), deltaFile0, deltaFile1)) {
            assertSame(deltaFile1, filePageStore.getDeltaFileToCompaction());

            CompletableFuture<DeltaFilePageStoreIo> createNewDeltaFileFuture = filePageStore.getOrCreateNewDeltaFile(
                    index -> workDir.resolve("delta" + index),
                    TestPageStoreUtils::arr
            );

            createNewDeltaFileFuture.get(1, SECONDS);

            assertSame(deltaFile1, filePageStore.getDeltaFileToCompaction());

            filePageStore.removeDeltaFile(deltaFile1);

            assertSame(deltaFile0, filePageStore.getDeltaFileToCompaction());

            filePageStore.removeDeltaFile(deltaFile0);

            assertNull(filePageStore.getDeltaFileToCompaction());

            filePageStore.completeNewDeltaFile();

            assertSame(createNewDeltaFileFuture.join(), filePageStore.getDeltaFileToCompaction());

            filePageStore.removeDeltaFile(createNewDeltaFileFuture.join());

            assertNull(filePageStore.getDeltaFileToCompaction());
        }
    }

    @Test
    void testReadWithMergedDeltaFiles() throws Exception {
        RandomAccessFileIoFactory ioFactory = new RandomAccessFileIoFactory();

        FilePageStoreHeader header = new FilePageStoreHeader(VERSION_1, PAGE_SIZE);
        DeltaFilePageStoreIoHeader deltaHeader = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(0));

        try (
                DeltaFilePageStoreIo deltaIo = spy(new DeltaFilePageStoreIo(ioFactory, deltaFilePath(0), deltaHeader));
                FilePageStoreIo storeIo = spy(new FilePageStoreIo(ioFactory, workDir.resolve("test"), header));
                FilePageStore filePageStore = new FilePageStore(storeIo, deltaIo);
        ) {
            long pageId = createDataPageId(filePageStore::allocatePage);

            ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE).order(nativeOrder());

            filePageStore.read(pageId, buffer, true);

            verify(deltaIo, times(1)).readWithMergedToFilePageStoreCheck(eq(pageId), anyLong(), eq(buffer), eq(true));
            verify(storeIo, times(0)).read(eq(pageId), anyLong(), eq(buffer), eq(true));

            deltaIo.markMergedToFilePageStore();

            filePageStore.read(pageId, buffer.rewind(), true);

            verify(deltaIo, times(2)).readWithMergedToFilePageStoreCheck(eq(pageId), anyLong(), eq(buffer), eq(true));
            verify(storeIo, times(1)).read(eq(pageId), anyLong(), eq(buffer), eq(true));
        }
    }

    @RepeatedTest(100)
    void testConcurrentAddNewAndCompactDeltaFile() throws Exception {
        try (FilePageStore filePageStore = createFilePageStore(workDir.resolve("test"), mock(DeltaFilePageStoreIo.class))) {
            DeltaFilePageStoreIo deltaFileToCompaction = filePageStore.getDeltaFileToCompaction();
            Path newDeltaFilePath = workDir.resolve("test_2");

            runRace(
                    () -> filePageStore.getOrCreateNewDeltaFile(newDeltaFileIndex -> newDeltaFilePath, () -> INT_EMPTY_ARRAY),
                    () -> filePageStore.removeDeltaFile(deltaFileToCompaction)
            );
        }
    }

    @Test
    void testFullSize() throws Exception {
        DeltaFilePageStoreIo deltaFile0 = mock(DeltaFilePageStoreIo.class);
        DeltaFilePageStoreIo deltaFile1 = mock(DeltaFilePageStoreIo.class);

        when(deltaFile0.size()).thenReturn(100L);
        when(deltaFile1.size()).thenReturn(200L);

        try (FilePageStore filePageStore = createFilePageStore(workDir.resolve("test"), deltaFile0, deltaFile1)) {
            assertEquals(300L, filePageStore.fullSize());

            filePageStore.ensure();

            assertEquals(300L + PAGE_SIZE, filePageStore.fullSize());

            long pageId0 = createDataPageId(filePageStore::allocatePage);

            filePageStore.write(pageId0, createPageByteBuffer(pageId0, PAGE_SIZE));

            assertEquals(300L + PAGE_SIZE * 2, filePageStore.fullSize());
        }
    }

    private static FilePageStore createFilePageStore(Path filePath) {
        return createFilePageStore(filePath, new FilePageStoreHeader(VERSION_1, PAGE_SIZE));
    }

    private static FilePageStore createFilePageStore(Path filePath, DeltaFilePageStoreIo... deltaIos) {
        return createFilePageStore(filePath, new FilePageStoreHeader(VERSION_1, PAGE_SIZE), deltaIos);
    }

    private static FilePageStore createFilePageStore(
            Path filePath,
            FilePageStoreHeader header,
            DeltaFilePageStoreIo... deltaFilePageStoreIos
    ) {
        return new FilePageStore(new FilePageStoreIo(new RandomAccessFileIoFactory(), filePath, header), deltaFilePageStoreIos);
    }

    private Path deltaFilePath(int index) {
        return workDir.resolve("delta" + index);
    }
}
