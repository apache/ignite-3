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

import static org.apache.ignite.internal.pagememory.persistence.store.PageStore.TYPE_DATA;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStore} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreTest {
    @WorkDirectory
    private Path workDir;

    @Test
    void testStop() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks uninitialized store with non-existent file.

        FilePageStore filePageStore0 = new FilePageStore(TYPE_DATA, testFilePath, new RandomAccessFileIoFactory(), 1024);

        assertDoesNotThrow(() -> filePageStore0.stop(false));
        assertDoesNotThrow(() -> filePageStore0.stop(true));

        // Checks uninitialized store with existent file.

        Files.write(testFilePath, new byte[1024]);

        FilePageStore filePageStore1 = new FilePageStore(TYPE_DATA, testFilePath, new RandomAccessFileIoFactory(), 1024);

        assertDoesNotThrow(() -> filePageStore1.stop(false));
        assertTrue(Files.exists(testFilePath));

        assertDoesNotThrow(() -> filePageStore1.stop(true));
        assertFalse(Files.exists(testFilePath));

        // Checks initialized store.

        FilePageStore filePageStore2 = new FilePageStore(TYPE_DATA, testFilePath, new RandomAccessFileIoFactory(), 1024);

        filePageStore2.ensure();

        assertDoesNotThrow(() -> filePageStore2.stop(false));
        assertTrue(Files.exists(testFilePath));

        assertDoesNotThrow(() -> filePageStore2.stop(true));
        assertFalse(Files.exists(testFilePath));
    }

    @Test
    void testClose() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks uninitialized store with non-existent file.

        FilePageStore filePageStore0 = new FilePageStore(TYPE_DATA, testFilePath, new RandomAccessFileIoFactory(), 1024);

        assertDoesNotThrow(filePageStore0::close);

        // Checks uninitialized store with existent file.

        Files.write(testFilePath, new byte[1024]);

        FilePageStore filePageStore1 = new FilePageStore(TYPE_DATA, testFilePath, new RandomAccessFileIoFactory(), 1024);

        assertDoesNotThrow(filePageStore1::close);
        assertTrue(Files.exists(testFilePath));

        // Checks initialized store.

        Files.delete(testFilePath);

        FilePageStore filePageStore2 = new FilePageStore(TYPE_DATA, testFilePath, new RandomAccessFileIoFactory(), 1024);

        filePageStore2.ensure();

        assertDoesNotThrow(filePageStore2::close);
        assertTrue(Files.exists(testFilePath));
    }

    // TODO: 20.05.2022 Continue
}
