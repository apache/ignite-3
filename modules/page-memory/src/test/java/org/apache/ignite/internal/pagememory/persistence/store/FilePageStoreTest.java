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

import static java.nio.ByteOrder.nativeOrder;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.io.PageIo.getCrc;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.GridUnsafe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStore} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreTest {
    private static final int PAGE_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Test
    void testStop() throws Exception {
        Path testFilePath0 = workDir.resolve("test0");
        Path testFilePath1 = workDir.resolve("test1");

        // Checks uninitialized store.

        try (
                FilePageStore filePageStore0 = createFilePageStore(testFilePath0);
                FilePageStore filePageStore1 = createFilePageStore(testFilePath1);
        ) {
            assertDoesNotThrow(() -> filePageStore0.stop(false));
            assertDoesNotThrow(() -> filePageStore1.stop(true));

            assertFalse(Files.exists(testFilePath0));
            assertFalse(Files.exists(testFilePath1));
        }

        // Checks initialized store.

        Path testFilePath2 = workDir.resolve("test2");
        Path testFilePath3 = workDir.resolve("test3");

        try (
                FilePageStore filePageStore2 = createFilePageStore(testFilePath2);
                FilePageStore filePageStore3 = createFilePageStore(testFilePath3);
        ) {
            filePageStore2.ensure();
            filePageStore3.ensure();

            assertDoesNotThrow(() -> filePageStore2.stop(false));
            assertTrue(Files.exists(testFilePath2));

            assertDoesNotThrow(() -> filePageStore3.stop(true));
            assertFalse(Files.exists(testFilePath3));
        }
    }

    @Test
    void testClose() throws Exception {
        Path testFilePath0 = workDir.resolve("test0");

        // Checks uninitialized store.

        try (FilePageStore filePageStore0 = createFilePageStore(testFilePath0)) {
            assertDoesNotThrow(filePageStore0::close);
            assertFalse(Files.exists(testFilePath0));
        }

        // Checks initialized store.

        Path testFilePath1 = workDir.resolve("test0");

        try (FilePageStore filePageStore1 = createFilePageStore(testFilePath1)) {
            filePageStore1.ensure();

            assertDoesNotThrow(filePageStore1::close);
            assertTrue(Files.exists(testFilePath1));
        }
    }

    @Test
    void testExist() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks uninitialized store with not exists file.

        try (FilePageStore filePageStore0 = createFilePageStore(testFilePath)) {
            assertFalse(filePageStore0.exists());
        }

        // Checks uninitialized store with existent file.

        try (FilePageStore filePageStore1 = createFilePageStore(Files.createFile(testFilePath))) {
            assertFalse(filePageStore1.exists());
        }

        // Checks initialized store.

        try (FilePageStore filePageStore2 = createFilePageStore(testFilePath)) {
            filePageStore2.ensure();

            assertTrue(filePageStore2.exists());
        }

        // Checks after closing the initialized store.

        try (FilePageStore filePageStore3 = createFilePageStore(testFilePath)) {
            assertTrue(filePageStore3.exists());
        }
    }

    @Test
    void testEnsure() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStore filePageStore0 = createFilePageStore(testFilePath)) {
            assertDoesNotThrow(filePageStore0::ensure);
            assertDoesNotThrow(filePageStore0::ensure);
        }

        try (FilePageStore filePageStore1 = createFilePageStore(testFilePath)) {
            assertDoesNotThrow(filePageStore1::ensure);
            assertDoesNotThrow(filePageStore1::ensure);
        }
    }

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

            filePageStore2.write(createPageId(filePageStore2), createPageByteBuffer(), true);

            assertEquals(1, filePageStore2.pages());

            filePageStore2.write(createPageId(filePageStore2), createPageByteBuffer(), true);

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
    void testSync() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
            assertDoesNotThrow(filePageStore::sync);

            filePageStore.write(createPageId(filePageStore), createPageByteBuffer(), true);

            assertDoesNotThrow(filePageStore::sync);
            assertEquals(2 * PAGE_SIZE, testFilePath.toFile().length());
        }
    }

    @Test
    void testSize() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStore filePageStore0 = createFilePageStore(testFilePath)) {
            assertEquals(0, filePageStore0.size());

            filePageStore0.ensure();

            assertEquals(PAGE_SIZE, filePageStore0.size());

            filePageStore0.write(createPageId(filePageStore0), createPageByteBuffer(), true);

            assertEquals(2 * PAGE_SIZE, filePageStore0.size());
        }

        try (FilePageStore filePageStore1 = createFilePageStore(testFilePath)) {
            assertEquals(0, filePageStore1.size());

            filePageStore1.ensure();

            assertEquals(2 * PAGE_SIZE, filePageStore1.size());
        }
    }

    @Test
    void testPageOffset() throws Exception {
        try (FilePageStore filePageStore = createFilePageStore(workDir.resolve("test"))) {
            assertEquals(PAGE_SIZE, filePageStore.pageOffset(pageId(0, FLAG_DATA, 0)));
            assertEquals(2 * PAGE_SIZE, filePageStore.pageOffset(pageId(0, FLAG_DATA, 1)));
            assertEquals(3 * PAGE_SIZE, filePageStore.pageOffset(pageId(0, FLAG_DATA, 2)));
        }
    }

    @Test
    void testWrite() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
            filePageStore.ensure();

            long expPageId = createPageId(filePageStore);

            ByteBuffer pageByteBuffer = createPageByteBuffer();

            filePageStore.write(expPageId, pageByteBuffer, true);

            assertEquals(2 * PAGE_SIZE, testFilePath.toFile().length());

            assertEquals(0, PageIo.getCrc(pageByteBuffer));
        }
    }

    @Test
    void testRead() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
            filePageStore.ensure();

            long expPageId = createPageId(filePageStore);

            ByteBuffer pageByteBuffer = createPageByteBuffer();

            // Puts random bytes after: type (2 byte) + version (2 byte) + crc (4 byte).
            pageByteBuffer.position(8).put(randomBytes(128));

            filePageStore.write(expPageId, pageByteBuffer.rewind(), true);

            ByteBuffer readBuffer = ByteBuffer.allocate(PAGE_SIZE).order(pageByteBuffer.order());

            assertTrue(filePageStore.read(expPageId, readBuffer, false));
            assertEquals(pageByteBuffer.rewind(), readBuffer.rewind());
            assertEquals(0, getCrc(readBuffer));

            readBuffer = ByteBuffer.allocate(PAGE_SIZE).order(pageByteBuffer.order());

            assertTrue(filePageStore.read(expPageId, readBuffer, true));
            assertNotEquals(0, getCrc(readBuffer));
        }
    }

    @Test
    void testHeaderSize() {
        Path testFilePath = workDir.resolve("test");

        assertEquals(PAGE_SIZE, createFilePageStore(testFilePath).headerSize());

        assertEquals(
                2 * PAGE_SIZE,
                createFilePageStore(testFilePath, new FilePageStoreHeader(VERSION_1, 2 * PAGE_SIZE)).headerSize()
        );

        assertEquals(
                3 * PAGE_SIZE,
                createFilePageStore(testFilePath, new FilePageStoreHeader(VERSION_1, 3 * PAGE_SIZE)).headerSize()
        );
    }

    private static byte[] randomBytes(int len) {
        byte[] res = new byte[len];

        ThreadLocalRandom.current().nextBytes(res);

        return res;
    }

    private static ByteBuffer createPageByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE).order(nativeOrder());

        new TestPageIo().initNewPage(GridUnsafe.bufferAddress(buffer), 0, PAGE_SIZE);

        return buffer;
    }

    private static long createPageId(FilePageStore filePageStore) throws Exception {
        return pageId(pageId(0, FLAG_DATA, (int) filePageStore.allocatePage()));
    }

    private static FilePageStore createFilePageStore(Path filePath) {
        return createFilePageStore(filePath, new FilePageStoreHeader(VERSION_1, PAGE_SIZE));
    }

    private static FilePageStore createFilePageStore(Path filePath, FilePageStoreHeader header) {
        return new FilePageStore(
                header.version(),
                header.pageSize(),
                header.headerSize(),
                filePath,
                new RandomAccessFileIoFactory()
        );
    }
}
