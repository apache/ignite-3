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
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.PART_DELTA_FILE_TEMPLATE;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.TMP_FILE_SUFFIX;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.TMP_PART_DELTA_FILE_TEMPLATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
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
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FilePageStoreManager} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreManagerTest {
    private final IgniteLogger log = Loggers.forClass(FilePageStoreManagerTest.class);

    @WorkDirectory
    private Path workDir;

    @Test
    void testCreateManager() {
        // Checks if the manager was successfully created.

        assertDoesNotThrow(this::createManager);

        Path dbDir = workDir.resolve("db");

        assertFalse(Files.exists(dbDir));
    }

    @Test
    void testStartAndStop() throws Exception {
        FilePageStoreManager manager = spy(createManager());

        assertDoesNotThrow(manager::start);

        assertDoesNotThrow(manager::stop);

        verify(manager, times(1)).stopAllGroupFilePageStores(false);
    }

    @Test
    void testStartFailed() throws Exception {
        Files.createFile(workDir.resolve("db"));

        FilePageStoreManager manager = createManager();

        IgniteInternalCheckedException exception = assertThrows(IgniteInternalCheckedException.class, manager::start);

        assertThat(exception.getMessage(), containsString("Could not create work directory for page stores"));

    }

    @Test
    void testInitialize() throws Exception {
        FilePageStoreManager manager = createManager();

        try {
            Files.createDirectories(workDir.resolve("db"));

            Path testGroupDir = workDir.resolve("db/table-0");

            Files.createFile(testGroupDir);

            IgniteInternalCheckedException exception = assertThrows(
                    IgniteInternalCheckedException.class,
                    () -> manager.initialize("test", 0, 2)
            );

            assertThat(exception.getMessage(), containsString("Failed to initialize group working directory"));

            Files.delete(testGroupDir);

            assertDoesNotThrow(() -> manager.initialize("test", 0, 2));

            assertTrue(Files.isDirectory(testGroupDir));

            try (Stream<Path> files = Files.list(testGroupDir)) {
                assertThat(files.count(), is(0L));
            }

            for (FilePageStore filePageStore : manager.getStores(0)) {
                filePageStore.ensure();
            }

            try (Stream<Path> files = Files.list(testGroupDir)) {
                assertThat(
                        files.map(Path::getFileName).map(Path::toString).collect(toSet()),
                        containsInAnyOrder("part-0.bin", "part-1.bin")
                );
            }
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
            assertEquals(2, stores.size());
            assertDoesNotThrow(() -> Set.copyOf(stores));

            // Checks getStore.

            Set<FilePageStore> pageStores = new HashSet<>();

            FilePageStore partitionPageStore0 = manager.getStore(0, 0);

            assertTrue(pageStores.add(partitionPageStore0));
            assertTrue(stores.contains(partitionPageStore0));

            assertTrue(partitionPageStore0.filePath().endsWith("db/table-0/part-0.bin"));

            FilePageStore partitionPageStore1 = manager.getStore(0, 1);

            assertTrue(pageStores.add(partitionPageStore1));
            assertTrue(stores.contains(partitionPageStore1));

            assertTrue(partitionPageStore1.filePath().endsWith("db/table-0/part-1.bin"));

            IgniteInternalCheckedException exception = assertThrows(IgniteInternalCheckedException.class, () -> manager.getStore(1, 0));

            assertThat(
                    exception.getMessage(),
                    containsString("Failed to get file page store for the given group ID (group has not been started)")
            );

            exception = assertThrows(IgniteInternalCheckedException.class, () -> manager.getStore(0, 2));

            assertThat(
                    exception.getMessage(),
                    containsString("Failed to get file page store for the given partition ID (partition has not been created)")
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
        } finally {
            // Waits for all asynchronous operations to complete.
            manager0.stop();
        }

        try (Stream<Path> files = Files.list(workDir.resolve("db/table-0"))) {
            assertThat(
                    files.map(Path::getFileName).map(Path::toString).collect(toSet()),
                    containsInAnyOrder("part-0.bin")
            );
        }

        // Checks with clean files.

        FilePageStoreManager manager1 = createManager();

        try {
            manager1.initialize("test1", 1, 1);

            for (FilePageStore filePageStore : manager1.getStores(1)) {
                filePageStore.ensure();
            }

            manager1.stopAllGroupFilePageStores(true);
        } finally {
            // Waits for all asynchronous operations to complete.
            manager1.stop();
        }

        assertThat(workDir.resolve("db/table-1").toFile().listFiles(), emptyArray());
    }

    @Test
    void testRemoveTmpFilesOnStart() throws Exception {
        FilePageStoreManager manager = createManager();

        manager.start();

        manager.initialize("test0", 1, 1);
        manager.initialize("test1", 2, 1);

        Path grpDir0 = workDir.resolve("db/table-1");
        Path grpDir1 = workDir.resolve("db/table-2");

        Files.createFile(grpDir0.resolve(String.format(PART_FILE_TEMPLATE, 1) + TMP_FILE_SUFFIX));
        Files.createFile(grpDir0.resolve(String.format(TMP_PART_DELTA_FILE_TEMPLATE, 1, 1)));
        Files.createFile(grpDir1.resolve(String.format(PART_FILE_TEMPLATE, 3) + TMP_FILE_SUFFIX));
        Files.createFile(grpDir1.resolve(String.format(TMP_PART_DELTA_FILE_TEMPLATE, 3, 1)));

        assertDoesNotThrow(manager::start);

        try (
                Stream<Path> grpFileStream0 = Files.list(grpDir0);
                Stream<Path> grpFileStream1 = Files.list(grpDir1);
        ) {
            assertThat(grpFileStream0.collect(toList()), empty());
            assertThat(grpFileStream1.collect(toList()), empty());
        }
    }

    @Test
    void testFindPartitionDeltaFiles() throws Exception {
        FilePageStoreManager manager = createManager();

        manager.start();

        manager.initialize("test0", 1, 1);
        manager.initialize("test1", 2, 1);

        Path grpDir0 = workDir.resolve("db/table-1");
        Path grpDir1 = workDir.resolve("db/table-2");

        Path grp0Part1deltaFilePath0 = grpDir0.resolve(String.format(PART_DELTA_FILE_TEMPLATE, 1, 0));
        Path grp0Part1deltaFilePath1 = grpDir0.resolve(String.format(PART_DELTA_FILE_TEMPLATE, 1, 1));
        Path grp0Part1deltaFilePath2 = grpDir0.resolve(String.format(PART_DELTA_FILE_TEMPLATE, 1, 2));

        Path grp0Part2deltaFilePath0 = grpDir0.resolve(String.format(PART_DELTA_FILE_TEMPLATE, 2, 0));

        Path grp1Part4deltaFilePath3 = grpDir1.resolve(String.format(PART_DELTA_FILE_TEMPLATE, 4, 3));

        Files.createFile(grp0Part1deltaFilePath0);
        Files.createFile(grp0Part1deltaFilePath1);
        Files.createFile(grp0Part1deltaFilePath2);

        Files.createFile(grp0Part2deltaFilePath0);

        Files.createFile(grp1Part4deltaFilePath3);

        assertThat(
                manager.findPartitionDeltaFiles(grpDir0, 1),
                arrayContainingInAnyOrder(grp0Part1deltaFilePath0, grp0Part1deltaFilePath1, grp0Part1deltaFilePath2)
        );

        assertThat(
                manager.findPartitionDeltaFiles(grpDir0, 2),
                arrayContainingInAnyOrder(grp0Part2deltaFilePath0)
        );

        assertThat(
                manager.findPartitionDeltaFiles(grpDir1, 4),
                arrayContainingInAnyOrder(grp1Part4deltaFilePath3)
        );

        assertThat(manager.findPartitionDeltaFiles(grpDir0, 100), emptyArray());
        assertThat(manager.findPartitionDeltaFiles(grpDir1, 100), emptyArray());
    }

    @Test
    void testTmpDeltaFilePageStorePath() throws Exception {
        FilePageStoreManager manager = createManager();

        Path tableDir = workDir.resolve("db/table-0");

        assertEquals(
                tableDir.resolve(String.format(TMP_PART_DELTA_FILE_TEMPLATE, 0, 0)),
                manager.tmpDeltaFilePageStorePath(0, 0, 0)
        );

        assertEquals(
                tableDir.resolve(String.format(TMP_PART_DELTA_FILE_TEMPLATE, 1, 2)),
                manager.tmpDeltaFilePageStorePath(0, 1, 2)
        );
    }

    @Test
    void testDeltaFilePageStorePath() throws Exception {
        FilePageStoreManager manager = createManager();

        Path tableDir = workDir.resolve("db/table-0");

        assertEquals(
                tableDir.resolve(String.format(PART_DELTA_FILE_TEMPLATE, 0, 0)),
                manager.deltaFilePageStorePath(0, 0, 0)
        );

        assertEquals(
                tableDir.resolve(String.format(PART_DELTA_FILE_TEMPLATE, 1, 2)),
                manager.deltaFilePageStorePath(0, 1, 2)
        );
    }

    @Test
    void testAllPageStores() throws Exception {
        FilePageStoreManager manager = createManager();

        manager.start();

        manager.initialize("test0", 1, 1);
        manager.initialize("test1", 2, 1);

        assertThat(
                manager.allPageStores().stream().flatMap(List::stream).map(FilePageStore::filePath).collect(toList()),
                containsInAnyOrder(
                        workDir.resolve("db/table-1").resolve("part-0.bin"),
                        workDir.resolve("db/table-2").resolve("part-0.bin")
                )
        );
    }

    private FilePageStoreManager createManager() throws Exception {
        return new FilePageStoreManager(log, "test", workDir, new RandomAccessFileIoFactory(), 1024);
    }
}
