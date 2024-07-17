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
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.DELTA_FILE_VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreHeader.readHeader;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.arr;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStoreFactory} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreFactoryTest {
    private static final int PAGE_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Test
    void testUnknownFilePageStoreVersion() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FileIo fileIo = createFileIo(testFilePath)) {
            fileIo.writeFully(new FilePageStoreHeader(-1, PAGE_SIZE).toByteBuffer().rewind(), 0);

            Exception exception = assertThrows(
                    IgniteInternalCheckedException.class,
                    () -> createFilePageStoreFactory().createPageStore(ByteBuffer.allocate(PAGE_SIZE).order(nativeOrder()), testFilePath)
            );

            assertThat(exception.getMessage(), containsString("Unknown version of file page store"));
        }
    }

    @Test
    void testCreateNewFilePageStore() throws Exception {
        Path testFilePath = workDir.resolve("test");

        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE).order(nativeOrder());

        try (
                FilePageStore filePageStore = createFilePageStoreFactory().createPageStore(buffer, testFilePath);
                FileIo fileIo = createFileIo(testFilePath)
        ) {
            assertNull(readHeader(fileIo, buffer.rewind()));

            assertEquals(0, filePageStore.pages());

            filePageStore.ensure();

            checkHeader(readHeader(fileIo, buffer.rewind()));

            assertEquals(0, filePageStore.pages());
        }
    }

    @Test
    void testCreateExistsFilePageStore() throws Exception {
        FilePageStoreFactory filePageStoreFactory = createFilePageStoreFactory();

        Path testFilePath = workDir.resolve("test");

        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE).order(nativeOrder());

        try (FilePageStore pageStore = filePageStoreFactory.createPageStore(buffer, testFilePath)) {
            pageStore.ensure();
        }

        try (
                FilePageStore filePageStore = filePageStoreFactory.createPageStore(buffer.rewind(), testFilePath);
                FileIo fileIo = createFileIo(testFilePath);
        ) {
            assertEquals(0, filePageStore.pages());

            checkHeader(readHeader(fileIo, buffer.rewind()));
        }
    }

    @Test
    void testUnknownDeltaFilePageStoreVersion() throws Exception {
        Path filePageStorePath = workDir.resolve("test");

        Path deltaFilePageStorePath = workDir.resolve("testDelta");

        try (
                FileIo filePageStoreIo = createFileIo(filePageStorePath);
                FileIo deltaFilePageStoreIo = createFileIo(deltaFilePageStorePath)
        ) {
            filePageStoreIo.writeFully(new FilePageStoreHeader(VERSION_1, PAGE_SIZE).toByteBuffer().rewind(), 0);
            deltaFilePageStoreIo.writeFully(new DeltaFilePageStoreIoHeader(-1, 1, PAGE_SIZE, arr(0)).toByteBuffer().rewind(), 0);

            ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE).order(nativeOrder());

            Exception exception = assertThrows(
                    IgniteInternalCheckedException.class,
                    () -> createFilePageStoreFactory().createPageStore(buffer, filePageStorePath, deltaFilePageStorePath)
            );

            assertThat(exception.getMessage(), containsString("Unknown version of delta file page store"));
        }
    }

    @Test
    void testCreateFilePageStoreWithDeltaFiles() throws Exception {
        Path filePageStorePath = workDir.resolve("test");

        Path deltaPageStorePath0 = workDir.resolve("testDelta0");
        Path deltaPageStorePath1 = workDir.resolve("testDelta1");

        try (
                FileIo filePageStoreIo = createFileIo(filePageStorePath);
                FileIo deltaFilePageStoreIo0 = createFileIo(deltaPageStorePath0);
                FileIo deltaFilePageStoreIo1 = createFileIo(deltaPageStorePath1);
        ) {
            filePageStoreIo.writeFully(new FilePageStoreHeader(VERSION_1, PAGE_SIZE).toByteBuffer().rewind(), 0);

            deltaFilePageStoreIo0.writeFully(
                    new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 1, PAGE_SIZE, arr(0)).toByteBuffer().rewind(),
                    0
            );

            deltaFilePageStoreIo1.writeFully(
                    new DeltaFilePageStoreIoHeader(DELTA_FILE_VERSION_1, 2, PAGE_SIZE, arr(1)).toByteBuffer().rewind(),
                    0
            );

            assertDoesNotThrow(() -> createFilePageStoreFactory().createPageStore(
                    ByteBuffer.allocateDirect(PAGE_SIZE).order(nativeOrder()),
                    filePageStorePath,
                    deltaPageStorePath0, deltaPageStorePath1
            ));
        }
    }

    private static FilePageStoreFactory createFilePageStoreFactory() {
        return new FilePageStoreFactory(new RandomAccessFileIoFactory(), PAGE_SIZE);
    }

    private static FileIo createFileIo(Path filePath) throws Exception {
        return new RandomAccessFileIo(filePath, CREATE, WRITE, READ);
    }

    private static void checkHeader(FilePageStoreHeader header) {
        // Check that creates a file page store with the latest version.
        assertEquals(VERSION_1, header.version());
        assertEquals(header.pageSize(), PAGE_SIZE);
    }
}
