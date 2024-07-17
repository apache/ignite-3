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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.DELTA_FILE_VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.arr;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createDataPageId;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createPageByteBuffer;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.randomBytes;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.stream.IntStream;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.fileio.RandomAccessFileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.junit.jupiter.api.Test;

/**
 * For {@link DeltaFilePageStoreIo} testing.
 */
public class DeltaFilePageStoreIoTest extends AbstractFilePageStoreIoTest {
    @Test
    void testPageOffset() throws Exception {
        Path testFilePath = workDir.resolve("test");

        DeltaFilePageStoreIoHeader header = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(0, 1, 2));

        try (DeltaFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath, header)) {
            assertEquals(PAGE_SIZE, filePageStoreIo.pageOffset(pageId(0, FLAG_DATA, 0)));
            assertEquals(PAGE_SIZE, filePageStoreIo.pageOffset(0));

            assertEquals(2 * PAGE_SIZE, filePageStoreIo.pageOffset(pageId(0, FLAG_DATA, 1)));
            assertEquals(2 * PAGE_SIZE, filePageStoreIo.pageOffset(1));

            assertEquals(3 * PAGE_SIZE, filePageStoreIo.pageOffset(pageId(0, FLAG_DATA, 2)));
            assertEquals(3 * PAGE_SIZE, filePageStoreIo.pageOffset(2));

            assertEquals(-1, filePageStoreIo.pageOffset(pageId(0, FLAG_DATA, 3)));
            assertEquals(-1, filePageStoreIo.pageOffset(3));

            assertEquals(-1, filePageStoreIo.pageOffset(pageId(0, FLAG_DATA, 4)));
            assertEquals(-1, filePageStoreIo.pageOffset(4));
        }
    }

    @Test
    void testHeaderSize() throws Exception {
        Path testFilePath = workDir.resolve("test");

        DeltaFilePageStoreIoHeader header = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(0, 1, 2));

        try (DeltaFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath, header)) {
            assertEquals(PAGE_SIZE, filePageStoreIo.headerSize());
        }

        header = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, IntStream.range(0, PAGE_SIZE / 4).toArray());

        try (DeltaFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath, header)) {
            assertEquals(2 * PAGE_SIZE, filePageStoreIo.headerSize());
        }
    }

    @Test
    void testHeaderBuffer() throws Exception {
        Path testFilePath = workDir.resolve("test");

        DeltaFilePageStoreIoHeader header = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(0, 1, 2));

        try (DeltaFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath, header)) {
            assertEquals(header.toByteBuffer().rewind(), filePageStoreIo.headerBuffer().rewind());
        }
    }

    @Test
    void testCheckHeader() throws Exception {
        Path testFilePath = workDir.resolve("test");

        DeltaFilePageStoreIoHeader header = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(0, 1, 2));

        try (
                DeltaFilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath, header);
                FileIo fileIo = new RandomAccessFileIo(testFilePath, READ, WRITE, CREATE);
        ) {
            Exception exception = assertThrows(IOException.class, () -> filePageStoreIo.checkHeader(fileIo));
            assertThat(exception.getMessage(), startsWith("Missing file header"));

            fileIo.writeFully(new DeltaFilePageStoreIoHeader(-1, 1, PAGE_SIZE, arr(0, 1, 2)).toByteBuffer().rewind(), 0);
            fileIo.force();

            exception = assertThrows(IOException.class, () -> filePageStoreIo.checkHeader(fileIo));
            assertThat(exception.getMessage(), startsWith("Invalid file version"));

            fileIo.writeFully(new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 2, PAGE_SIZE, arr(0, 1, 2)).toByteBuffer().rewind(), 0);
            fileIo.force();

            exception = assertThrows(IOException.class, () -> filePageStoreIo.checkHeader(fileIo));
            assertThat(exception.getMessage(), startsWith("Invalid file indexes"));

            fileIo.writeFully(
                    new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, 2 * PAGE_SIZE, arr(0, 1, 2)).toByteBuffer().rewind(),
                    0
            );

            fileIo.force();

            exception = assertThrows(IOException.class, () -> filePageStoreIo.checkHeader(fileIo));
            assertThat(exception.getMessage(), startsWith("Invalid file pageSize"));

            fileIo.writeFully(new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(2, 1, 0)).toByteBuffer().rewind(), 0);
            fileIo.force();

            exception = assertThrows(IOException.class, () -> filePageStoreIo.checkHeader(fileIo));
            assertThat(exception.getMessage(), startsWith("Invalid file pageIndexes"));

            fileIo.writeFully(header.toByteBuffer().rewind(), 0);
            fileIo.force();

            assertDoesNotThrow(() -> filePageStoreIo.checkHeader(fileIo));
        }
    }

    @Test
    void testMergedToFilePageStore() throws Exception {
        DeltaFilePageStoreIoHeader header = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(0, 1, 2));

        try (DeltaFilePageStoreIo filePageStoreIo = createFilePageStoreIo(workDir.resolve("test"), header)) {
            // Preparation for reading.
            long pageId = createDataPageId(() -> 0);

            ByteBuffer pageByteBuffer = createPageByteBuffer(pageId, PAGE_SIZE);

            // Puts random bytes after: type (2 byte) + version (2 byte) + crc (4 byte).
            pageByteBuffer.position(8).put(randomBytes(128));

            filePageStoreIo.write(pageId, pageByteBuffer.rewind(), true);

            filePageStoreIo.sync();

            // Checking readings.
            ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE).order(pageByteBuffer.order());

            long pageOff = filePageStoreIo.pageOffset(pageId);

            assertTrue(filePageStoreIo.readWithMergedToFilePageStoreCheck(pageId, pageOff, buffer, false));

            assertEquals(pageByteBuffer.rewind(), buffer.rewind());

            buffer.rewind().put(new byte[PAGE_SIZE]);

            filePageStoreIo.markMergedToFilePageStore();

            assertFalse(filePageStoreIo.readWithMergedToFilePageStoreCheck(pageId, pageOff, buffer.rewind(), false));

            assertEquals(ByteBuffer.allocateDirect(PAGE_SIZE).order(pageByteBuffer.order()), buffer.rewind());
        }
    }

    @Test
    void testPageIndexes() throws Exception {
        DeltaFilePageStoreIoHeader header = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(0, 1, 2));

        try (DeltaFilePageStoreIo filePageStoreIo = createFilePageStoreIo(workDir.resolve("test"), header)) {
            assertArrayEquals(arr(0, 1, 2), filePageStoreIo.pageIndexes());
        }
    }

    @Override
    DeltaFilePageStoreIo createFilePageStoreIo(Path filePath, FileIoFactory ioFactory) {
        return new DeltaFilePageStoreIo(
                ioFactory,
                filePath,
                new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(0, 1, 2, 3, 5, 6, 7, 8, 9))
        );
    }

    private static DeltaFilePageStoreIo createFilePageStoreIo(Path filePath, DeltaFilePageStoreIoHeader header) {
        return new DeltaFilePageStoreIo(new RandomAccessFileIoFactory(), filePath, header);
    }
}
