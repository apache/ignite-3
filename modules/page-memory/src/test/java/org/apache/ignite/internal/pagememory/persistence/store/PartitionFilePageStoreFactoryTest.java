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
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link PartitionFilePageStoreFactory} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class PartitionFilePageStoreFactoryTest {
    private static final int PAGE_SIZE = 1024;

    private static PageIoRegistry ioRegistry;

    @WorkDirectory
    private Path workDir;

    @BeforeAll
    static void beforeAll() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    @AfterAll
    static void afterAll() {
        ioRegistry = null;
    }

    @Test
    void testUnknownFilePageStoreVersion() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FileIo fileIo = createFileIo(testFilePath)) {
            fileIo.writeFully(new FilePageStoreHeader(-1, PAGE_SIZE).toByteBuffer(), 0);

            Exception exception = assertThrows(
                    IgniteInternalCheckedException.class,
                    () -> createFilePageStoreFactory().createPageStore(testFilePath, ByteBuffer.allocateDirect(PAGE_SIZE))
            );

            assertThat(exception.getMessage(), containsString("Unknown version of file page store"));
        }
    }

    @Test
    void testCreateNewFilePageStore() throws Exception {
        Path testFilePath = workDir.resolve("test");

        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);

        try (
                PartitionFilePageStore filePageStore = createFilePageStoreFactory().createPageStore(testFilePath, buffer);
                FileIo fileIo = createFileIo(testFilePath)
        ) {
            assertNull(readHeader(fileIo));

            assertEquals(1, filePageStore.pages());

            filePageStore.ensure();

            checkHeader(readHeader(fileIo));

            assertEquals(1, filePageStore.pages());
        }
    }

    @Test
    void testCreateExistsFilePageStore() throws Exception {
        PartitionFilePageStoreFactory filePageStoreFactory = createFilePageStoreFactory();

        Path testFilePath = workDir.resolve("test");

        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);

        try (PartitionFilePageStore pageStore = filePageStoreFactory.createPageStore(testFilePath, buffer)) {
            pageStore.ensure();
        }

        try (
                PartitionFilePageStore filePageStore = filePageStoreFactory.createPageStore(testFilePath, buffer.rewind());
                FileIo fileIo = createFileIo(testFilePath);
        ) {
            assertEquals(1, filePageStore.pages());

            checkHeader(readHeader(fileIo));
        }
    }

    @Test
    void testCreateExistsFilePageStoreWithChangePageCount() throws Exception {
        PartitionFilePageStoreFactory filePageStoreFactory = createFilePageStoreFactory();

        Path testFilePath = workDir.resolve("test");

        ByteBuffer buffer = ByteBuffer.allocateDirect(PAGE_SIZE);

        try (
                PartitionFilePageStore pageStore = filePageStoreFactory.createPageStore(testFilePath, buffer);
                FileIo fileIo = createFileIo(testFilePath);
        ) {
            pageStore.ensure();

            writePageCount(fileIo, 3);

            fileIo.force();
        }

        try (
                PartitionFilePageStore filePageStore = filePageStoreFactory.createPageStore(testFilePath, buffer.rewind());
                FileIo fileIo = createFileIo(testFilePath);
        ) {
            assertEquals(3, filePageStore.pages());

            checkHeader(readHeader(fileIo));
        }
    }

    private static PartitionFilePageStoreFactory createFilePageStoreFactory() {
        return new PartitionFilePageStoreFactory(new RandomAccessFileIoFactory(), ioRegistry, PAGE_SIZE);
    }

    private static FileIo createFileIo(Path filePath) throws Exception {
        return new RandomAccessFileIo(filePath, CREATE, WRITE, READ);
    }

    private static void writePageCount(FileIo fileIo, int pageCount) throws Exception {
        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        try {
            PartitionMetaIo partitionMetaIo = PartitionMetaIo.VERSIONS.latest();

            partitionMetaIo.initNewPage(
                    bufferAddress(buffer),
                    pageId(0, (byte) 0, 0),
                    PAGE_SIZE
            );

            partitionMetaIo.setPageCount(bufferAddress(buffer), pageCount);

            // Must be after the file header.
            fileIo.writeFully(buffer.rewind(), PAGE_SIZE);
        } finally {
            freeBuffer(buffer);
        }
    }

    private static void checkHeader(FilePageStoreHeader header) {
        // Check that creates a file page store with the latest version.
        assertEquals(VERSION_1, header.version());
        assertEquals(header.pageSize(), PAGE_SIZE);
    }
}
