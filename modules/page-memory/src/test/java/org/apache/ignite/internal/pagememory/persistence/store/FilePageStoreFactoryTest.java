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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreHeader.readHeader;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteInternalCheckedException;
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
    void testInvalidFilePageSize() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FileIo fileIo = createFileIo(testFilePath)) {
            fileIo.writeFully(new FilePageStoreHeader(VERSION_1, PAGE_SIZE * 2).toByteBuffer(), 0);

            Exception exception = assertThrows(
                    IgniteInternalCheckedException.class,
                    () -> createFilePageStoreFactory().createPageStore(testFilePath)
            );

            assertThat(exception.getCause().getMessage(), startsWith("Invalid file pageSize"));
        }
    }

    @Test
    void testUnknownFilePageStoreVersion() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FileIo fileIo = createFileIo(testFilePath)) {
            fileIo.writeFully(new FilePageStoreHeader(-1, PAGE_SIZE).toByteBuffer(), 0);

            Exception exception = assertThrows(
                    IgniteInternalCheckedException.class,
                    () -> createFilePageStoreFactory().createPageStore(testFilePath)
            );

            assertThat(exception.getMessage(), containsString("Unknown version of file page store"));
        }
    }

    @Test
    void testCreateNewFilePageStore() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (
                FilePageStore filePageStore = createFilePageStoreFactory().createPageStore(testFilePath);
                FileIo fileIo = createFileIo(testFilePath)
        ) {
            checkHeader(readHeader(fileIo));
        }
    }

    @Test
    void testCreateExistsFilePageStore() throws Exception {
        FilePageStoreFactory filePageStoreFactory = createFilePageStoreFactory();

        Path testFilePath = workDir.resolve("test");

        filePageStoreFactory.createPageStore(testFilePath).close();

        try (
                FilePageStore filePageStore = createFilePageStoreFactory().createPageStore(testFilePath);
                FileIo fileIo = createFileIo(testFilePath)
        ) {
            checkHeader(readHeader(fileIo));
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
