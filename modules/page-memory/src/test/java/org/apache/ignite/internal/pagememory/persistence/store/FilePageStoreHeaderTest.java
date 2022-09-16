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
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.RandomAccessFileIo;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStoreHeader} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreHeaderTest {
    private static final int PAGE_SIZE = 1024;

    @Test
    void testSimple() {
        FilePageStoreHeader header = new FilePageStoreHeader(100500, 1024);

        assertEquals(100500, header.version());
        assertEquals(1024, header.pageSize());
        assertEquals(1024, header.headerSize());
    }

    @Test
    void testToByteBuffer() {
        ByteBuffer header = new FilePageStoreHeader(VERSION_1, 512).toByteBuffer();

        // Checks that the buffer size should be equal to the page size (header size).
        assertEquals(512, header.rewind().remaining());

        // Checks the file page store signature.
        assertEquals(0xF19AC4FE60C530B8L, header.getLong());

        // Checks the file page store version.
        assertEquals(VERSION_1, header.getInt());

        // Checks the page size in bytes.
        assertEquals(512, header.getInt());
    }

    @Test
    void testReadHeader(@WorkDirectory Path workDir) throws Exception {
        Path testFile = Files.createFile(workDir.resolve("test"));

        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE).order(nativeOrder());

        try (FileIo fileIo = new RandomAccessFileIo(testFile, CREATE, WRITE, READ)) {
            assertNull(FilePageStoreHeader.readHeader(fileIo, buffer));

            ByteBuffer headerBuffer = new FilePageStoreHeader(VERSION_1, 256).toByteBuffer();

            fileIo.writeFully(headerBuffer.rewind());

            fileIo.force();

            FilePageStoreHeader header = FilePageStoreHeader.readHeader(fileIo, buffer.rewind());

            assertEquals(VERSION_1, header.version());
            assertEquals(256, header.pageSize());
            assertEquals(256, header.headerSize());

            fileIo.writeFully(headerBuffer.rewind().putLong(-1).rewind(), 0);

            fileIo.force();

            Exception exception = assertThrows(IOException.class, () -> FilePageStoreHeader.readHeader(fileIo, buffer.rewind()));

            assertThat(exception.getMessage(), startsWith("Invalid file signature"));
        }
    }
}
