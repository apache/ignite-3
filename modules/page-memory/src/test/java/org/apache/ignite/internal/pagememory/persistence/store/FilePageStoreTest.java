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

import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.VERSION_1;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createDataPageId;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createPageByteBuffer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStore} testing.
 */
// TODO: IGNITE-17372 добавить/поправить тесты
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreTest {
    private static final int PAGE_SIZE = 1024;

    @WorkDirectory
    private Path workDir;

    @Test
    void testAllocatePage() throws Exception {
        Path testFilePath = workDir.resolve("test");

        // Checks allocation without writing pages.

        try (FilePageStore filePageStore0 = createFilePageStore(testFilePath)) {
            assertEquals(0, filePageStore0.pages());

            assertEquals(0, filePageStore0.allocatePage());

            assertEquals(1, filePageStore0.pages());

            assertEquals(1, filePageStore0.allocatePage());

            assertEquals(2, filePageStore0.pages());
        }

        try (FilePageStore filePageStore1 = createFilePageStore(testFilePath)) {
            filePageStore1.pages(2);

            assertEquals(2, filePageStore1.pages());

            filePageStore1.ensure();

            assertEquals(2, filePageStore1.pages());

            assertEquals(2, filePageStore1.allocatePage());

            assertEquals(3, filePageStore1.pages());
        }

        // Checks allocation with writing pages.

        try (FilePageStore filePageStore2 = createFilePageStore(testFilePath)) {
            filePageStore2.pages(0);

            assertEquals(0, filePageStore2.pages());

            filePageStore2.write(createDataPageId(filePageStore2::allocatePage), createPageByteBuffer(PAGE_SIZE), true);

            assertEquals(1, filePageStore2.pages());

            filePageStore2.write(createDataPageId(filePageStore2::allocatePage), createPageByteBuffer(PAGE_SIZE), true);

            assertEquals(2, filePageStore2.pages());
        }

        try (FilePageStore filePageStore3 = createFilePageStore(testFilePath)) {
            assertEquals(0, filePageStore3.pages());

            filePageStore3.ensure();

            assertEquals(0, filePageStore3.pages());
        }

        try (FilePageStore filePageStore4 = createFilePageStore(testFilePath)) {
            filePageStore4.pages(2);

            assertEquals(2, filePageStore4.pages());

            filePageStore4.ensure();

            assertEquals(2, filePageStore4.pages());

            assertEquals(2, filePageStore4.allocatePage());

            assertEquals(3, filePageStore4.pages());
        }
    }

    @Test
    void testPageAllocationListener() throws Exception {
        Path testFilePath = workDir.resolve("test");

        try (FilePageStore filePageStore = createFilePageStore(testFilePath)) {
            ConcurrentLinkedQueue<Integer> allocatedPageIdx = new ConcurrentLinkedQueue<>();

            filePageStore.setPageAllocationListener(allocatedPageIdx::add);

            filePageStore.allocatePage();

            assertThat(allocatedPageIdx, contains(0));

            filePageStore.allocatePage();

            assertThat(allocatedPageIdx, contains(0, 1));
        }
    }

    private static FilePageStore createFilePageStore(Path filePath) {
        return createFilePageStore(filePath, new FilePageStoreHeader(VERSION_1, PAGE_SIZE));
    }

    private static FilePageStore createFilePageStore(Path filePath, FilePageStoreHeader header) {
        return new FilePageStore(new FilePageStoreIo(new RandomAccessFileIoFactory(), filePath, header));
    }
}
