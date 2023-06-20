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
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.fileio.RandomAccessFileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStoreIo} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreIoTest extends AbstractFilePageStoreIoTest {
    @Test
    void testPageOffset() throws Exception {
        try (FilePageStoreIo filePageStoreIo = createFilePageStoreIo(workDir.resolve("test"))) {
            assertEquals(PAGE_SIZE, filePageStoreIo.pageOffset(pageId(0, FLAG_DATA, 0)));
            assertEquals(2 * PAGE_SIZE, filePageStoreIo.pageOffset(pageId(0, FLAG_DATA, 1)));
            assertEquals(3 * PAGE_SIZE, filePageStoreIo.pageOffset(pageId(0, FLAG_DATA, 2)));
        }
    }

    @Test
    void testHeaderSize() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath)) {
            assertEquals(PAGE_SIZE, filePageStoreIo.headerSize());
        }

        try (FilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath, new FilePageStoreHeader(VERSION_1, 2 * PAGE_SIZE))) {
            assertEquals(2 * PAGE_SIZE, filePageStoreIo.headerSize());
        }

        try (FilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath, new FilePageStoreHeader(VERSION_1, 3 * PAGE_SIZE))) {
            assertEquals(3 * PAGE_SIZE, filePageStoreIo.headerSize());
        }
    }

    @Test
    void testHeaderBuffer() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FilePageStoreHeader header = new FilePageStoreHeader(VERSION_1, PAGE_SIZE);

        try (FilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath, header)) {
            assertEquals(header.toByteBuffer().rewind(), filePageStoreIo.headerBuffer().rewind());
        }
    }

    @Test
    void testCheckHeader() throws Exception {
        Path testFilePath = workDir.resolve("test");

        FilePageStoreHeader header = new FilePageStoreHeader(VERSION_1, PAGE_SIZE);

        try (
                FilePageStoreIo filePageStoreIo = createFilePageStoreIo(testFilePath, header);
                FileIo fileIo = new RandomAccessFileIo(testFilePath, READ, WRITE, CREATE);
        ) {
            Exception exception = assertThrows(IOException.class, () -> filePageStoreIo.checkHeader(fileIo));
            assertThat(exception.getMessage(), startsWith("Missing file header"));

            fileIo.writeFully(new FilePageStoreHeader(-1, PAGE_SIZE).toByteBuffer().rewind(), 0);
            fileIo.force();

            exception = assertThrows(IOException.class, () -> filePageStoreIo.checkHeader(fileIo));
            assertThat(exception.getMessage(), startsWith("Invalid file version"));

            fileIo.writeFully(new FilePageStoreHeader(VERSION_1, 2 * PAGE_SIZE).toByteBuffer().rewind(), 0);
            fileIo.force();

            exception = assertThrows(IOException.class, () -> filePageStoreIo.checkHeader(fileIo));
            assertThat(exception.getMessage(), startsWith("Invalid file pageSize"));

            fileIo.writeFully(header.toByteBuffer().rewind(), 0);
            fileIo.force();

            assertDoesNotThrow(() -> filePageStoreIo.checkHeader(fileIo));
        }
    }

    @Override
    protected FilePageStoreIo createFilePageStoreIo(Path filePath, FileIoFactory ioFactory) {
        return new FilePageStoreIo(ioFactory, filePath, new FilePageStoreHeader(VERSION_1, PAGE_SIZE));
    }

    private static FilePageStoreIo createFilePageStoreIo(Path filePath, FilePageStoreHeader header) {
        return new FilePageStoreIo(new RandomAccessFileIoFactory(), filePath, header);
    }
}
