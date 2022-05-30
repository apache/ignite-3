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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagememory.persistence.store.PageStore.TYPE_DATA;
import static org.apache.ignite.internal.pagememory.persistence.store.PageStore.TYPE_IDX;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStoreManager} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreManagerTest {
    private final IgniteLogger log = IgniteLogger.forClass(FilePageStoreManagerTest.class);

    @WorkDirectory
    private Path workDir;

    @Test
    void testCreateManager() throws Exception {
        // Checks if the manager was successfully created.

        assertDoesNotThrow(this::createManager);

        Path dbDir = workDir.resolve("db");

        assertTrue(Files.isDirectory(dbDir));

        // Checks for failed manager creation.

        Files.delete(dbDir);

        Files.createFile(dbDir);

        IgniteInternalCheckedException exception = assertThrows(IgniteInternalCheckedException.class, this::createManager);

        assertThat(exception.getMessage(), startsWith("Could not create work directory for page stores"));
    }

    @Test
    void testStartAndStop() throws Exception {
        FilePageStoreManager manager = spy(createManager());

        assertDoesNotThrow(manager::start);

        assertDoesNotThrow(manager::stop);

        verify(manager, times(1)).stopAllGroupFilePageStores(false);
    }

    @Test
    void testInitialize() throws Exception {
        FilePageStoreManager manager = createManager();

        try {
            Files.createDirectories(workDir.resolve("db"));

            Path testGroupDir = workDir.resolve("db/group-test");

            Files.createFile(testGroupDir);

            IgniteInternalCheckedException exception = assertThrows(
                    IgniteInternalCheckedException.class,
                    () -> manager.initialize("test", 0, 2)
            );

            assertThat(exception.getMessage(), startsWith("Failed to initialize group working directory"));

            Files.delete(testGroupDir);

            assertDoesNotThrow(() -> manager.initialize("test", 0, 2));

            assertTrue(Files.isDirectory(testGroupDir));
            assertThat(Files.list(testGroupDir).collect(toList()), empty());

            for (FilePageStore filePageStore : manager.getStores(0)) {
                filePageStore.ensure();
            }

            assertThat(
                    Files.list(testGroupDir).map(Path::getFileName).map(Path::toString).collect(toSet()),
                    equalTo(Set.of("index.bin", "part-0.bin", "part-1.bin"))
            );
        } finally {
            manager.stop();
        }
    }

    @Test
    void testGetStores() throws Exception {
        FilePageStoreManager manager = createManager();

        try {
            manager.initialize("test", 0, 2);

            // Checks getStores.

            assertNull(manager.getStores(1));

            Collection<FilePageStore> stores = manager.getStores(0);

            assertNotNull(stores);
            assertEquals(3, stores.size());
            assertDoesNotThrow(() -> Set.copyOf(stores));

            // Checks getStore.

            Set<FilePageStore> pageStores = new HashSet<>();

            FilePageStore partitionPageStore0 = manager.getStore(0, 0);

            assertTrue(pageStores.add(partitionPageStore0));
            assertTrue(stores.contains(partitionPageStore0));

            assertEquals(TYPE_DATA, partitionPageStore0.type());
            assertTrue(partitionPageStore0.filePath().endsWith("db/group-test/part-0.bin"));

            FilePageStore partitionPageStore1 = manager.getStore(0, 1);

            assertTrue(pageStores.add(partitionPageStore1));
            assertTrue(stores.contains(partitionPageStore1));

            assertEquals(TYPE_DATA, partitionPageStore1.type());
            assertTrue(partitionPageStore1.filePath().endsWith("db/group-test/part-1.bin"));

            FilePageStore idxPageStore = manager.getStore(0, INDEX_PARTITION);

            assertTrue(pageStores.add(idxPageStore));
            assertTrue(stores.contains(idxPageStore));

            assertEquals(TYPE_IDX, idxPageStore.type());
            assertTrue(idxPageStore.filePath().endsWith("db/group-test/index.bin"));

            IgniteInternalCheckedException exception = assertThrows(IgniteInternalCheckedException.class, () -> manager.getStore(1, 0));

            assertThat(
                    exception.getMessage(),
                    startsWith("Failed to get file page store for the given group ID (group has not been started)")
            );

            exception = assertThrows(IgniteInternalCheckedException.class, () -> manager.getStore(0, 2));

            assertThat(
                    exception.getMessage(),
                    startsWith("Failed to get file page store for the given partition ID (partition has not been created)")
            );
        } finally {
            manager.stop();
        }
    }

    @Test
    void testStopAllGroupFilePageStores() throws Exception {
        // Checks without clean files.

        FilePageStoreManager manager0 = createManager();

        try {
            manager0.initialize("test0", 0, 1);

            for (FilePageStore filePageStore : manager0.getStores(0)) {
                filePageStore.ensure();
            }

            manager0.stopAllGroupFilePageStores(false);

            assertFalse(waitForCondition(
                    () -> workDir.resolve("db/group-test0").toFile().listFiles().length == 0,
                    10,
                    100
            ));

            assertThat(
                    Files.list(workDir.resolve("db/group-test0")).map(Path::getFileName).map(Path::toString).collect(toSet()),
                    equalTo(Set.of("index.bin", "part-0.bin"))
            );
        } finally {
            manager0.stop();
        }

        // Checks with clean files.

        FilePageStoreManager manager1 = createManager();

        try {
            manager1.initialize("test1", 1, 1);

            for (FilePageStore filePageStore : manager1.getStores(1)) {
                filePageStore.ensure();
            }

            manager1.stopAllGroupFilePageStores(true);

            assertTrue(waitForCondition(
                    () -> workDir.resolve("db/group-test1").toFile().listFiles().length == 0,
                    10,
                    100
            ));
        } finally {
            manager1.stop();
        }
    }

    private FilePageStoreManager createManager() throws Exception {
        return new FilePageStoreManager(log, "test", workDir, new RandomAccessFileIoFactory(), 1024);
    }
}
