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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.FileIo;
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
    void testSuccessCreateFilePageStore() throws Exception {
        FilePageStoreFactory filePageStoreFactory = createFilePageStoreFactory();

        FilePageStore filePageStore = filePageStoreFactory.createPageStore(Files.createFile(workDir.resolve("test")));

        checkCommonHeader(filePageStore);

        filePageStore.close();

        checkCommonHeader(filePageStoreFactory.createPageStore(workDir.resolve("test")));
    }

    @Test
    void testFailCreateFilePageStore() throws Exception {
        FilePageStoreFactory filePageStoreFactory = createFilePageStoreFactory();

        // Breaks the file page store version.

        Path testFilePath = workDir.resolve("test");

        FilePageStore filePageStore = filePageStoreFactory.createPageStore(testFilePath);

        ByteBuffer headerBuffer = ByteBuffer.allocate(PAGE_SIZE).order(nativeOrder());

        filePageStore.readHeader(headerBuffer);

        headerBuffer.rewind().putInt(8, -1);

        try (FileIo fileIo = new RandomAccessFileIoFactory().create(testFilePath)) {
            fileIo.writeFully(headerBuffer.rewind());
        }

        // Checks that there will be an error when creating an unknown version of file page store.

        IgniteInternalCheckedException exception = assertThrows(
                IgniteInternalCheckedException.class,
                () -> filePageStoreFactory.createPageStore(testFilePath)
        );

        assertThat(exception.getMessage(), containsString("Unknown version of file page store"));
    }

    private void checkCommonHeader(FilePageStore filePageStore) throws Exception {
        ByteBuffer headerBuffer = ByteBuffer.allocate(PAGE_SIZE).order(nativeOrder());

        filePageStore.readHeader(headerBuffer);

        // Skip signature.
        headerBuffer.rewind().getLong();

        assertEquals(FilePageStore.VERSION_1, headerBuffer.getInt());
        assertEquals(PAGE_SIZE, headerBuffer.getInt());
    }

    private FilePageStoreFactory createFilePageStoreFactory() {
        return new FilePageStoreFactory(new RandomAccessFileIoFactory(), PAGE_SIZE);
    }
}
