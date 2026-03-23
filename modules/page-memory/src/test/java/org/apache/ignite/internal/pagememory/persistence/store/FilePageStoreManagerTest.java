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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.DEL_PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.GROUP_DIR_PREFIX;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.PART_DELTA_FILE_TEMPLATE;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.TMP_FILE_SUFFIX;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.TMP_PART_DELTA_FILE_TEMPLATE;
import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager.findPartitionDeltaFiles;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap.GroupPartitionPageStore;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * For {@link FilePageStoreManager} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FilePageStoreManagerTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 1024;

    /** To be used in a loop. {@link RepeatedTest} cannot be combined with {@link ParameterizedTest}. */
    private static final int REPEATS = 100;

    @WorkDirectory
    private Path workDir;

    private final Collection<FilePageStoreManager> managers = new ConcurrentLinkedQueue<>();

    @AfterEach
    void tearDown() throws Exception {
        closeAll(managers.stream().filter(Objects::nonNull).map(manager -> manager::stop));
    }

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
    void testReadOrCreateStore() throws Exception {
        FilePageStoreManager manager = createManager();

        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        try {
            Files.createDirectories(workDir.resolve("db"));

            Path testGroupDir = workDir.resolve("db/table-0");

            Files.createFile(testGroupDir);

            GroupPartitionId groupPartitionId00 = new GroupPartitionId(0, 0);

            IgniteTestUtils.assertThrows(
                    IgniteInternalCheckedException.class,
                    () -> manager.readOrCreateStore(groupPartitionId00, buffer.rewind()),
                    "Failed to initialize group working directory"
            );

            Files.delete(testGroupDir);

            GroupPartitionId groupPartitionId01 = new GroupPartitionId(0, 1);

            FilePageStore filePageStore0 = manager.readOrCreateStore(groupPartitionId00, buffer.rewind());
            FilePageStore filePageStore1 = manager.readOrCreateStore(groupPartitionId01, buffer.rewind());

            assertNull(manager.getStore(groupPartitionId00));
            assertNull(manager.getStore(groupPartitionId01));

            assertTrue(Files.isDirectory(testGroupDir));

            try (Stream<Path> files = Files.list(testGroupDir)) {
                assertThat(files.count(), is(0L));
            }

            filePageStore0.ensure();
            filePageStore1.ensure();

            try (Stream<Path> files = Files.list(testGroupDir)) {
                assertThat(
                        files.map(Path::getFileName).map(Path::toString).collect(toSet()),
                        containsInAnyOrder("part-0.bin", "part-1.bin")
                );
            }
        } finally {
            freeBuffer(buffer);
        }
    }

    @Test
    void testStopAllGroupFilePageStores() throws Exception {
        // Checks without clean files.

        FilePageStoreManager manager0 = createManager();

        try {
            GroupPartitionId groupPartitionId00 = new GroupPartitionId(0, 0);

            createAndAddFilePageStore(manager0, groupPartitionId00);

            manager0.getStore(groupPartitionId00).ensure();

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
            GroupPartitionId groupPartitionId10 = new GroupPartitionId(1, 0);

            createAndAddFilePageStore(manager1, groupPartitionId10);

            manager1.getStore(groupPartitionId10).ensure();

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

        GroupPartitionId groupPartitionId10 = new GroupPartitionId(1, 0);
        GroupPartitionId groupPartitionId20 = new GroupPartitionId(2, 0);

        createAndAddFilePageStore(manager, groupPartitionId10);
        createAndAddFilePageStore(manager, groupPartitionId20);

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

        GroupPartitionId groupPartitionId10 = new GroupPartitionId(1, 0);
        GroupPartitionId groupPartitionId20 = new GroupPartitionId(2, 0);

        createAndAddFilePageStore(manager, groupPartitionId10);
        createAndAddFilePageStore(manager, groupPartitionId20);

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
                findPartitionDeltaFiles(grpDir0, 1),
                arrayContainingInAnyOrder(grp0Part1deltaFilePath0, grp0Part1deltaFilePath1, grp0Part1deltaFilePath2)
        );

        assertThat(
                findPartitionDeltaFiles(grpDir0, 2),
                arrayContainingInAnyOrder(grp0Part2deltaFilePath0)
        );

        assertThat(
                findPartitionDeltaFiles(grpDir1, 4),
                arrayContainingInAnyOrder(grp1Part4deltaFilePath3)
        );

        assertThat(findPartitionDeltaFiles(grpDir0, 100), emptyArray());
        assertThat(findPartitionDeltaFiles(grpDir1, 100), emptyArray());
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

        createAndAddFilePageStore(manager, new GroupPartitionId(1, 0));
        createAndAddFilePageStore(manager, new GroupPartitionId(2, 0));

        List<Path> allPageStoreFiles = manager.allPageStores()
                .map(GroupPartitionPageStore::pageStore)
                .map(FilePageStore::filePath)
                .collect(toList());

        assertThat(
                allPageStoreFiles,
                containsInAnyOrder(
                        workDir.resolve("db/table-1").resolve("part-0.bin"),
                        workDir.resolve("db/table-2").resolve("part-0.bin")
                )
        );
    }

    @Test
    void testDestroyPartition() throws Exception {
        FilePageStoreManager manager = createManager();

        manager.start();

        GroupPartitionId groupPartitionId00 = new GroupPartitionId(0, 0);
        GroupPartitionId groupPartitionId10 = new GroupPartitionId(1, 0);

        createAndAddFilePageStore(manager, groupPartitionId00);
        createAndAddFilePageStore(manager, groupPartitionId10);

        FilePageStore filePageStore0 = manager.getStore(groupPartitionId00);
        FilePageStore filePageStore1 = manager.getStore(groupPartitionId10);

        filePageStore0.ensure();
        filePageStore1.ensure();

        filePageStore0
                .getOrCreateNewDeltaFile(value -> manager.deltaFilePageStorePath(0, 0, 0), () -> new int[0])
                .get(1, TimeUnit.SECONDS)
                .ensure();

        Path startPath = workDir.resolve("db");

        assertThat(collectFilesOnly(startPath), hasSize(3));

        filePageStore0.markToDestroy();
        filePageStore1.markToDestroy();

        assertThat(manager.destroyPartition(groupPartitionId00), willCompleteSuccessfully());
        assertThat(manager.destroyPartition(groupPartitionId10), willCompleteSuccessfully());

        assertThat(collectFilesOnly(startPath), empty());
    }

    /**
     * Tests the situation when we could crash in the middle of deleting files when calling
     * {@link FilePageStoreManager#destroyPartition(GroupPartitionId)} )}, i.e. delete everything not completely and at the start of the
     * component we will delete everything that could not be completely deleted.
     *
     * @throws Exception If failed.
     */
    @Test
    void testFullyRemovePartitionOnStart() throws Exception {
        FilePageStoreManager manager = createManager();

        manager.start();

        GroupPartitionId groupPartitionId00 = new GroupPartitionId(0, 0);
        GroupPartitionId groupPartitionId01 = new GroupPartitionId(0, 1);
        GroupPartitionId groupPartitionId10 = new GroupPartitionId(1, 0);
        GroupPartitionId groupPartitionId11 = new GroupPartitionId(1, 1);

        createAndAddFilePageStore(manager, groupPartitionId00);
        createAndAddFilePageStore(manager, groupPartitionId01);
        createAndAddFilePageStore(manager, groupPartitionId10);
        createAndAddFilePageStore(manager, groupPartitionId11);

        FilePageStore filePageStore00 = manager.getStore(groupPartitionId00);
        FilePageStore filePageStore01 = manager.getStore(groupPartitionId01);
        FilePageStore filePageStore10 = manager.getStore(groupPartitionId10);
        FilePageStore filePageStore11 = manager.getStore(groupPartitionId11);

        filePageStore00.ensure();
        filePageStore01.ensure();
        filePageStore10.ensure();
        filePageStore11.ensure();

        filePageStore00
                .getOrCreateNewDeltaFile(value -> manager.deltaFilePageStorePath(0, 0, 0), () -> new int[0])
                .get(1, TimeUnit.SECONDS)
                .ensure();

        filePageStore01
                .getOrCreateNewDeltaFile(value -> manager.deltaFilePageStorePath(0, 1, 0), () -> new int[0])
                .get(1, TimeUnit.SECONDS)
                .ensure();

        DeltaFilePageStoreIo deltaFilePageStoreIo11 = filePageStore11
                .getOrCreateNewDeltaFile(value -> manager.deltaFilePageStorePath(1, 1, 0), () -> new int[0])
                .get(1, TimeUnit.SECONDS);

        deltaFilePageStoreIo11.ensure();

        manager.stop();

        Path dbDir = workDir.resolve("db");

        Path groupDir0 = dbDir.resolve(GROUP_DIR_PREFIX + 0);
        Path groupDir1 = dbDir.resolve(GROUP_DIR_PREFIX + 1);

        // Let's leave only the delta file.
        IgniteUtils.deleteIfExists(filePageStore01.filePath());

        // Let's create marker files to remove partitions and delta files.
        Files.createFile(groupDir0.resolve(String.format(DEL_PART_FILE_TEMPLATE, 0)));
        Files.createFile(groupDir0.resolve(String.format(DEL_PART_FILE_TEMPLATE, 1)));
        Files.createFile(groupDir1.resolve(String.format(DEL_PART_FILE_TEMPLATE, 0)));

        // Let's run the component and see what happens.
        manager.start();

        assertThat(
                collectFilesOnly(dbDir),
                containsInAnyOrder(filePageStore11.filePath(), deltaFilePageStoreIo11.filePath())
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 10, 25, 50})
    @Timeout(value = 1, unit = MINUTES)
    void testConcurrentFindAndRemoveTmpDeltaFile(int tmpDeltaFileCount) throws Exception {
        for (int i = 0; i < REPEATS; i++) {
            Path tableDir = createGroupWorkDir(workDir, 13);

            int partitionId = 69;

            for (int j = 0; j < tmpDeltaFileCount; j++) {
                Path tmpDeltaFilePath = createTmpDeltaFilePath(tableDir, partitionId, j);

                Files.createFile(tmpDeltaFilePath);
            }

            runRace(
                    () -> findPartitionTmpDeltaFilesWhileExists(tableDir, partitionId),
                    () -> findPartitionTmpDeltaFilesWhileExists(tableDir, partitionId),
                    () -> findPartitionTmpDeltaFilesWhileExists(tableDir, partitionId),
                    () -> removePartitionTmpDeltaFilesWhileExists(tableDir, partitionId, tmpDeltaFileCount)
            );

            assertThat(collectFilesOnly(tableDir), empty());
        }
    }

    @Test
    void testAddStore() throws Exception {
        FilePageStoreManager manager = createManager();

        GroupPartitionId groupPartitionId0 = new GroupPartitionId(0, 0);
        GroupPartitionId groupPartitionId1 = new GroupPartitionId(0, 1);
        GroupPartitionId groupPartitionId2 = new GroupPartitionId(1, 0);

        FilePageStore filePageStore = mock(FilePageStore.class);

        manager.addStore(groupPartitionId0, filePageStore);

        assertSame(filePageStore, manager.getStore(groupPartitionId0));
        assertNull(manager.getStore(groupPartitionId1));
        assertNull(manager.getStore(groupPartitionId2));

        assertThat(
                manager.allPageStores().map(GroupPartitionPageStore::pageStore).collect(toList()),
                contains(filePageStore)
        );
    }

    @Test
    void testDestroyGroupIfExists() throws Exception {
        FilePageStoreManager manager = createManager();

        manager.start();

        int groupId = 1;

        Path dbDir = workDir.resolve("db");

        // Directory does not exist.
        assertDoesNotThrow(() -> manager.destroyGroupIfExists(groupId));
        assertThat(collectAllSubFileAndDirs(dbDir), empty());

        createAndAddFilePageStore(manager, new GroupPartitionId(groupId, 0));
        assertThat(collectAllSubFileAndDirs(dbDir), not(empty()));

        manager.destroyGroupIfExists(groupId);
        assertThat(collectAllSubFileAndDirs(dbDir), empty());
    }

    private FilePageStoreManager createManager() {
        FilePageStoreManager manager = new FilePageStoreManager(
                "test",
                workDir,
                new RandomAccessFileIoFactory(),
                PAGE_SIZE,
                mock(FailureManager.class)
        );

        managers.add(manager);

        return manager;
    }

    private static List<Path> collectFilesOnly(Path start) throws Exception {
        try (Stream<Path> fileStream = Files.find(start, Integer.MAX_VALUE, (path, basicFileAttributes) -> Files.isRegularFile(path))) {
            return fileStream.collect(toList());
        }
    }

    private static List<Path> collectAllSubFileAndDirs(Path start) throws Exception {
        try (Stream<Path> fileStream = Files.walk(start)) {
            return fileStream.filter(not(start::equals)).collect(toList());
        }
    }

    private static Path createGroupWorkDir(Path workDir, int tableId) throws Exception {
        Path groupWorkDir = workDir.resolve("db/table-" + tableId);

        Files.createDirectories(groupWorkDir);

        return groupWorkDir;
    }

    private static Path createTmpDeltaFilePath(Path groupWorkDir, int partitionId, int i) {
        return groupWorkDir.resolve(String.format(TMP_PART_DELTA_FILE_TEMPLATE, partitionId, i));
    }

    private static void findPartitionTmpDeltaFilesWhileExists(Path groupWorkDir, int partitionId) throws Exception {
        while (true) {
            Path[] partitionDeltaFiles = findPartitionDeltaFiles(groupWorkDir, partitionId);

            if (ArrayUtils.nullOrEmpty(partitionDeltaFiles)) {
                break;
            }
        }
    }

    private static void removePartitionTmpDeltaFilesWhileExists(Path groupWorkDir, int partitionId, int count) {
        for (int i = 0; i < count; i++) {
            Path tmpDeltaFile = createTmpDeltaFilePath(groupWorkDir, partitionId, i);

            assertTrue(IgniteUtils.deleteIfExists(tmpDeltaFile), tmpDeltaFile.toString());
        }
    }

    private static void createAndAddFilePageStore(FilePageStoreManager manager, GroupPartitionId groupPartitionId) throws Exception {
        ByteBuffer buffer = allocateBuffer(PAGE_SIZE);

        try {
            FilePageStore filePageStore = manager.readOrCreateStore(groupPartitionId, buffer);

            manager.addStore(groupPartitionId, filePageStore);
        } finally {
            freeBuffer(buffer);
        }
    }
}
