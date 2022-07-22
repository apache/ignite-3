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

package org.apache.ignite.internal.pagememory.persistence.store;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.DELTA_FILE_VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.arr;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createDataPageId;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createPageByteBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStore} testing.
 */
// TODO: IGNITE-17372 добавить/поправить тесты
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreTest {
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

            filePageStore2.write(pageId0, createPageByteBuffer(pageId0, PAGE_SIZE), true);

            assertEquals(1, filePageStore2.pages());

            long pageId1 = createDataPageId(filePageStore2::allocatePage);

            filePageStore2.write(pageId1, createPageByteBuffer(pageId1, PAGE_SIZE), true);

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
            DeltaFilePageStoreIoFactory factory = spy(new DeltaFilePageStoreIoFactory() {
                /** {@inheritDoc} */
                @Override
                public DeltaFilePageStoreIo create(int index, int[] pageIndexes) {
                    return mock(DeltaFilePageStoreIo.class);
                }
            });

            filePageStore.setDeltaFilePageStoreIoFactory(factory);

            int[] pageIndexes = arr(0, 1, 2);

            Supplier<int[]> pageIndexesSupplier = spy(new Supplier<int[]>() {
                /** {@inheritDoc} */
                @Override
                public int[] get() {
                    return pageIndexes;
                }
            });

            CompletableFuture<DeltaFilePageStoreIo> future0 = filePageStore.getOrCreateNewDeltaFile(pageIndexesSupplier);
            CompletableFuture<DeltaFilePageStoreIo> future1 = filePageStore.getOrCreateNewDeltaFile(pageIndexesSupplier);

            assertSame(future0, future1);

            assertDoesNotThrow(() -> future0.get(1, SECONDS));

            verify(factory, times(1)).create(0, pageIndexes);
            verify(factory, times(1)).create(anyInt(), any(int[].class));

            verify(pageIndexesSupplier, times(1)).get();
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

        Path deltaFilePath0 = workDir.resolve("delta0");
        Path deltaFilePath1 = workDir.resolve("delta1");

        try (
                DeltaFilePageStoreIo deltaIo0 = spy(new DeltaFilePageStoreIo(ioFactory, deltaFilePath0, deltaHeader0));
                FilePageStoreIo filePageStoreIo = spy(new FilePageStoreIo(ioFactory, filePageStoreFilePath, filePageStoreHeader));
                FilePageStore filePageStore = new FilePageStore(filePageStoreIo, deltaIo0);
        ) {
            filePageStore.setDeltaFilePageStoreIoFactory((index, pageIndexes) -> {
                assertEquals(1, index);
                assertArrayEquals(deltaHeader1.pageIndexes(), pageIndexes);

                return spy(new DeltaFilePageStoreIo(ioFactory, deltaFilePath1, deltaHeader1));
            });

            filePageStore.setCompleteCreationDeltaFilePageStoreIoCallback(AbstractFilePageStoreIo::sync);

            DeltaFilePageStoreIo deltaIo1 = filePageStore.getOrCreateNewDeltaFile(deltaHeader1::pageIndexes).get(1, SECONDS);

            // Fills with data.
            long pageId0 = createDataPageId(filePageStore::allocatePage);
            long pageId1 = createDataPageId(filePageStore::allocatePage);
            long pageId2 = createDataPageId(filePageStore::allocatePage);

            filePageStoreIo.write(pageId0, createPageByteBuffer(pageId0, PAGE_SIZE), true);
            deltaIo0.write(pageId1, createPageByteBuffer(pageId1, PAGE_SIZE), true);
            deltaIo1.write(pageId2, createPageByteBuffer(pageId2, PAGE_SIZE), true);

            filePageStoreIo.sync();
            deltaIo0.sync();
            filePageStore.completeNewDeltaFile();

            // Checks page reading.
            ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

            try {
                // Check read page from filePageStoreIo.
                filePageStore.read(pageId0, buffer, true);

                assertEquals(pageId0, PageIo.getPageId(buffer.rewind()));

                verify(deltaIo0, times(0)).read(eq(pageId0), anyLong(), eq(buffer), eq(true));
                verify(deltaIo1, times(0)).read(eq(pageId0), anyLong(), eq(buffer), eq(true));
                verify(filePageStoreIo, times(1)).read(eq(pageId0), anyLong(), eq(buffer), eq(true));

                // Check read page from deltaIo0.
                filePageStore.read(pageId1, buffer.rewind(), true);

                assertEquals(pageId1, PageIo.getPageId(buffer.rewind()));

                verify(deltaIo0, times(1)).read(eq(pageId1), anyLong(), eq(buffer), eq(true));
                verify(deltaIo1, times(0)).read(eq(pageId1), anyLong(), eq(buffer), eq(true));
                verify(filePageStoreIo, times(0)).read(eq(pageId1), anyLong(), eq(buffer), eq(true));

                // Check read page from deltaIo1.
                filePageStore.read(pageId2, buffer.rewind(), true);

                assertEquals(pageId2, PageIo.getPageId(buffer.rewind()));

                verify(deltaIo0, times(0)).read(eq(pageId2), anyLong(), eq(buffer), eq(true));
                verify(deltaIo1, times(1)).read(eq(pageId2), anyLong(), eq(buffer), eq(true));
                verify(filePageStoreIo, times(0)).read(eq(pageId2), anyLong(), eq(buffer), eq(true));
            } finally {
                freeBuffer(buffer);
            }
        }
    }

    @Test
    void testReadDirectlyWithoutPageIdCheck() throws Exception {
        DeltaFilePageStoreIo deltaIo0 = mock(DeltaFilePageStoreIo.class);
        DeltaFilePageStoreIo deltaIo1 = mock(DeltaFilePageStoreIo.class);

        FilePageStoreIo filePageStoreIo = spy(new FilePageStoreIo(
                new RandomAccessFileIoFactory(),
                workDir.resolve("test"),
                new FilePageStoreHeader(VERSION_1, PAGE_SIZE)
        ));

        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        try (FilePageStore filePageStore = new FilePageStore(filePageStoreIo, deltaIo0)) {
            filePageStore.setDeltaFilePageStoreIoFactory((index, pageIndexes) -> deltaIo1);
            filePageStore.setCompleteCreationDeltaFilePageStoreIoCallback(deltaFilePageStoreIo -> {});

            filePageStore.getOrCreateNewDeltaFile(TestPageStoreUtils::arr).get(1, SECONDS);
            filePageStore.completeNewDeltaFile();

            long pageId = createDataPageId(() -> 101);

            assertDoesNotThrow(() -> filePageStore.readDirectlyWithoutPageIdCheck(pageId, buffer, true));

            verify(filePageStoreIo, times(1)).read(eq(pageId), anyLong(), eq(buffer), eq(true));
            verify(deltaIo0, times(0)).read(eq(pageId), anyLong(), eq(buffer), eq(true));
            verify(deltaIo1, times(0)).read(eq(pageId), anyLong(), eq(buffer), eq(true));
        } finally {
            freeBuffer(buffer);
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
}
