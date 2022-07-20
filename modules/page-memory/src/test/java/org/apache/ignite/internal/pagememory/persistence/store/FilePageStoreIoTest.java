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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.io.PageIo.getCrc;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createDataPageId;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createPageByteBuffer;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.randomBytes;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStoreIo} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreIoTest {
    private static final int PAGE_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Test
    void testStop() throws Exception {
        Path testFilePath0 = workDir.resolve("test0");
        Path testFilePath1 = workDir.resolve("test1");

        // Checks uninitialized store.

        try (
                FilePageStoreIo filePageStore0 = createFilePageStoreIo(testFilePath0);
                FilePageStoreIo filePageStore1 = createFilePageStoreIo(testFilePath1);
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
                FilePageStoreIo filePageStore2 = createFilePageStoreIo(testFilePath2);
                FilePageStoreIo filePageStore3 = createFilePageStoreIo(testFilePath3);
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

        try (FilePageStoreIo filePageStore0 = createFilePageStoreIo(testFilePath0)) {
            assertDoesNotThrow(filePageStore0::close);
            assertFalse(Files.exists(testFilePath0));
        }

        // Checks initialized store.

        Path testFilePath1 = workDir.resolve("test0");

        try (FilePageStoreIo filePageStore1 = createFilePageStoreIo(testFilePath1)) {
            filePageStore1.ensure();

            assertDoesNotThrow(filePageStore1::close);
            assertTrue(Files.exists(testFilePath1));
        }
    }

    @Test
    void testExist() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks uninitialized store with not exists file.

        try (FilePageStoreIo filePageStore0 = createFilePageStoreIo(testFilePath)) {
            assertFalse(filePageStore0.exists());
        }

        // Checks uninitialized store with existent file.

        try (FilePageStoreIo filePageStore1 = createFilePageStoreIo(Files.createFile(testFilePath))) {
            assertFalse(filePageStore1.exists());
        }

        // Checks initialized store.

        try (FilePageStoreIo filePageStore2 = createFilePageStoreIo(testFilePath)) {
            filePageStore2.ensure();

            assertTrue(filePageStore2.exists());
        }

        // Checks after closing the initialized store.

        try (FilePageStoreIo filePageStore3 = createFilePageStoreIo(testFilePath)) {
            assertTrue(filePageStore3.exists());
        }
    }

    @Test
    void testEnsure() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStoreIo filePageStore0 = createFilePageStoreIo(testFilePath)) {
            assertDoesNotThrow(filePageStore0::ensure);
            assertDoesNotThrow(filePageStore0::ensure);
        }

        try (FilePageStoreIo filePageStore1 = createFilePageStoreIo(testFilePath)) {
            assertDoesNotThrow(filePageStore1::ensure);
            assertDoesNotThrow(filePageStore1::ensure);
        }
    }

    @Test
    void testSync() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStoreIo filePageStore = createFilePageStoreIo(testFilePath)) {
            assertDoesNotThrow(filePageStore::sync);

            filePageStore.write(createDataPageId(() -> 0), createPageByteBuffer(PAGE_SIZE), true);

            assertDoesNotThrow(filePageStore::sync);
            assertEquals(2 * PAGE_SIZE, testFilePath.toFile().length());
        }
    }

    @Test
    void testSize() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStoreIo filePageStore0 = createFilePageStoreIo(testFilePath)) {
            assertEquals(0, filePageStore0.size());

            filePageStore0.ensure();

            assertEquals(PAGE_SIZE, filePageStore0.size());

            filePageStore0.write(createDataPageId(() -> 0), createPageByteBuffer(PAGE_SIZE), true);

            assertEquals(2 * PAGE_SIZE, filePageStore0.size());
        }

        try (FilePageStoreIo filePageStore1 = createFilePageStoreIo(testFilePath)) {
            assertEquals(0, filePageStore1.size());

            filePageStore1.ensure();

            assertEquals(2 * PAGE_SIZE, filePageStore1.size());
        }
    }

    @Test
    void testPageOffset() throws Exception {
        try (FilePageStoreIo filePageStore = createFilePageStoreIo(workDir.resolve("test"))) {
            assertEquals(PAGE_SIZE, filePageStore.pageOffset(pageId(0, FLAG_DATA, 0)));
            assertEquals(2 * PAGE_SIZE, filePageStore.pageOffset(pageId(0, FLAG_DATA, 1)));
            assertEquals(3 * PAGE_SIZE, filePageStore.pageOffset(pageId(0, FLAG_DATA, 2)));
        }
    }

    @Test
    void testWrite() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStoreIo filePageStore = createFilePageStoreIo(testFilePath)) {
            filePageStore.ensure();

            long expPageId = createDataPageId(() -> 0);

            ByteBuffer pageByteBuffer = createPageByteBuffer(PAGE_SIZE);

            filePageStore.write(expPageId, pageByteBuffer, true);

            assertEquals(2 * PAGE_SIZE, testFilePath.toFile().length());

            assertEquals(0, PageIo.getCrc(pageByteBuffer));
        }
    }

    @Test
    void testRead() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStoreIo filePageStore = createFilePageStoreIo(testFilePath)) {
            filePageStore.ensure();

            long expPageId = createDataPageId(() -> 0);

            ByteBuffer pageByteBuffer = createPageByteBuffer(PAGE_SIZE);

            // Puts random bytes after: type (2 byte) + version (2 byte) + crc (4 byte).
            pageByteBuffer.position(8).put(randomBytes(128));

            filePageStore.write(expPageId, pageByteBuffer.rewind(), true);

            ByteBuffer readBuffer = ByteBuffer.allocate(PAGE_SIZE).order(pageByteBuffer.order());

            filePageStore.read(expPageId, readBuffer, false);

            assertEquals(pageByteBuffer.rewind(), readBuffer.rewind());
            assertEquals(0, getCrc(readBuffer));

            readBuffer = ByteBuffer.allocate(PAGE_SIZE).order(pageByteBuffer.order());

            filePageStore.read(expPageId, readBuffer, true);

            assertNotEquals(0, getCrc(readBuffer));
        }
    }

    @Test
    void testHeaderSize() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStoreIo filePageStore = createFilePageStoreIo(testFilePath)) {
            assertEquals(PAGE_SIZE, filePageStore.headerSize());
        }

        try (FilePageStoreIo filePageStore = createFilePageStoreIo(testFilePath, new FilePageStoreHeader(VERSION_1, 2 * PAGE_SIZE))) {
            assertEquals(2 * PAGE_SIZE, filePageStore.headerSize());
        }

        try (FilePageStoreIo filePageStore = createFilePageStoreIo(testFilePath, new FilePageStoreHeader(VERSION_1, 3 * PAGE_SIZE))) {
            assertEquals(3 * PAGE_SIZE, filePageStore.headerSize());
        }
    }

    private static FilePageStoreIo createFilePageStoreIo(Path filePath) {
        return createFilePageStoreIo(filePath, new FilePageStoreHeader(VERSION_1, PAGE_SIZE));
    }

    private static FilePageStoreIo createFilePageStoreIo(Path filePath, FilePageStoreHeader header) {
        return new FilePageStoreIo(new RandomAccessFileIoFactory(), filePath, header);
    }
}
