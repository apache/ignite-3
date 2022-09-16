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
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIoHeader.checkFileIndex;
import static org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIoHeader.checkFilePageIndexes;
import static org.apache.ignite.internal.pagememory.persistence.store.DeltaFilePageStoreIoHeader.readHeader;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.DELTA_FILE_VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.arr;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIo;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link DeltaFilePageStoreIoHeader} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class DeltaFilePageStoreIoHeaderTest {
    private static final int PAGE_SIZE = 32;

    @Test
    void testCheckFilePageIndexes() {
        assertDoesNotThrow(() -> checkFilePageIndexes(arr(1, 2, 3), arr(1, 2, 3)));

        Exception exception = assertThrows(IOException.class, () -> checkFilePageIndexes(arr(1, 2), arr(1, 2, 3)));

        assertThat(exception.getMessage(), startsWith("Invalid file pageIndexes"));

        exception = assertThrows(IOException.class, () -> checkFilePageIndexes(arr(1, 2, 4), arr(1, 2, 3)));

        assertThat(exception.getMessage(), startsWith("Invalid file pageIndexes"));
    }

    @Test
    void testHeaderSize() {
        assertEquals(PAGE_SIZE, new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr()).headerSize());
        assertEquals(PAGE_SIZE, new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(1)).headerSize());
        assertEquals(PAGE_SIZE, new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(1, 2)).headerSize());
        assertEquals(PAGE_SIZE, new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(1, 2)).headerSize());

        assertEquals(2 * PAGE_SIZE, new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(1, 2, 3)).headerSize());

        assertEquals(2 * PAGE_SIZE, new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(1, 2, 3, 4)).headerSize());

        assertEquals(2 * PAGE_SIZE,
                new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(1, 2, 3, 4, 5, 6, 7)).headerSize()
        );

        assertEquals(
                2 * PAGE_SIZE,
                new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).headerSize()
        );

        assertEquals(
                3 * PAGE_SIZE,
                new DeltaFilePageStoreIoHeader(
                        DELTA_FILE_VERSION_1,
                        0,
                        PAGE_SIZE,
                        arr(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
                ).headerSize()
        );
    }

    @Test
    void testToByteBuffer() {
        int[] arr = arr();

        assertThat(
                new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr).toByteBuffer().rewind(),
                equalTo(toByteBuffer(PAGE_SIZE, DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr).rewind())
        );

        arr = arr(1, 2);

        assertThat(
                new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr).toByteBuffer().rewind(),
                equalTo(toByteBuffer(PAGE_SIZE, DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr).rewind())
        );

        arr = arr(1, 2, 3, 4);

        assertThat(
                new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr).toByteBuffer().rewind(),
                equalTo(toByteBuffer(2 * PAGE_SIZE, DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr).rewind())
        );
    }

    @Test
    void testReadHeaderFailure(@WorkDirectory Path workDir) throws Exception {
        try (FileIo fileIo = createFileIo(workDir.resolve("test"))) {
            ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE).order(nativeOrder());

            buffer.putLong(-1);

            fileIo.writeFully(buffer.rewind(), 0);

            fileIo.force();

            Exception exception = assertThrows(IOException.class, () -> readHeader(fileIo, buffer.rewind()));

            assertThat(exception.getMessage(), startsWith("Invalid file signature"));
        }
    }

    @Test
    void testReadHeader(@WorkDirectory Path workDir) throws Exception {
        try (FileIo fileIo = createFileIo(workDir.resolve("test0"))) {
            ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE).order(nativeOrder());

            assertNull(readHeader(fileIo, buffer));

            DeltaFilePageStoreIoHeader header = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr());

            fileIo.writeFully(header.toByteBuffer().rewind(), 0);

            fileIo.force();

            assertHeaderEquals(header, readHeader(fileIo, buffer.rewind()));
        }

        try (FileIo fileIo = createFileIo(workDir.resolve("test1"))) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE).order(nativeOrder());

            DeltaFilePageStoreIoHeader header = new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr(1, 2));

            fileIo.writeFully(header.toByteBuffer().rewind(), 0);

            fileIo.force();

            assertHeaderEquals(header, readHeader(fileIo, buffer));
        }

        try (FileIo fileIo = createFileIo(workDir.resolve("test2"))) {
            ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

            try {
                DeltaFilePageStoreIoHeader header = new DeltaFilePageStoreIoHeader(
                        DELTA_FILE_VERSION_1,
                        0,
                        PAGE_SIZE,
                        arr(1, 2, 3, 4, 5, 6)
                );

                fileIo.writeFully(header.toByteBuffer().rewind(), 0);

                fileIo.force();

                assertHeaderEquals(header, readHeader(fileIo, buffer));
            } finally {
                freeBuffer(buffer);
            }
        }
    }

    @Test
    void testCheckFileIndex() {
        assertDoesNotThrow(() -> checkFileIndex(0, 0));

        Exception exception = assertThrows(IOException.class, () -> checkFileIndex(1, 2));

        assertThat(exception.getMessage(), startsWith("Invalid file indexes"));
    }

    @Test
    void testIndex() {
        assertEquals(0, new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 0, PAGE_SIZE, arr()).index());
        assertEquals(1, new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr()).index());
    }

    private static ByteBuffer toByteBuffer(int capacity, int version, int index, int pageSize, int[] pageIndexes) {
        ByteBuffer buffer = ByteBuffer.allocate(capacity).order(nativeOrder())
                .putLong(0xDEAFAEE072020173L)
                .putInt(version)
                .putInt(index)
                .putInt(pageSize)
                .putInt(pageIndexes.length);

        if (pageIndexes.length > 0) {
            buffer.asIntBuffer().put(pageIndexes);
        }

        return buffer;
    }

    private static FileIo createFileIo(Path filePath) throws Exception {
        return new RandomAccessFileIo(filePath, CREATE, WRITE, READ);
    }

    private static void assertHeaderEquals(DeltaFilePageStoreIoHeader exp, DeltaFilePageStoreIoHeader act) {
        assertEquals(exp.version(), act.version());
        assertEquals(exp.pageSize(), act.pageSize());
        assertEquals(exp.headerSize(), act.headerSize());

        assertArrayEquals(exp.pageIndexes(), act.pageIndexes());
    }
}
