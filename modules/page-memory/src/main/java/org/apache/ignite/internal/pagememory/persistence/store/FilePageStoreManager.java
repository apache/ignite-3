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

import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createFile;
import static java.nio.file.Files.delete;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.deleteIfExistsThrowable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PageReadWriteManager;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap.GroupPartitionPageStore;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.jetbrains.annotations.Nullable;

/**
 * Partition file page store manager.
 */
public class FilePageStoreManager implements PageReadWriteManager {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(FilePageStoreManager.class);

    /** File suffix. */
    public static final String FILE_SUFFIX = ".bin";

    /** Suffix of the temporary file. */
    public static final String TMP_FILE_SUFFIX = ".tmp";

    /** File suffix to delete. */
    public static final String DEL_FILE_SUFFIX = ".del";

    /** Partition file prefix. */
    public static final String PART_FILE_PREFIX = "part-";

    /** Partition delta file prefix. */
    public static final String PART_DELTA_FILE_PREFIX = PART_FILE_PREFIX + "%d-delta-";

    /** Partition file template, example "part-1.bin". */
    public static final String PART_FILE_TEMPLATE = PART_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** Partition file template to be removed, example "part-1.del". */
    public static final String DEL_PART_FILE_TEMPLATE = PART_FILE_PREFIX + "%d" + DEL_FILE_SUFFIX;

    /** Regexp for the partition file to be deleted, example "part-1.del". */
    public static final String DEL_PART_FILE_REGEXP = PART_FILE_PREFIX + "(\\d+)" + DEL_FILE_SUFFIX;

    /** Partition temporary delta file template, example "part-1-delta-1.bin". */
    public static final String PART_DELTA_FILE_TEMPLATE = PART_DELTA_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** Partition delta file template, example "part-1-delta-1.bin.tmp". */
    public static final String TMP_PART_DELTA_FILE_TEMPLATE = PART_DELTA_FILE_TEMPLATE + TMP_FILE_SUFFIX;

    /** Group directory prefix. */
    public static final String GROUP_DIR_PREFIX = "table-";

    /** Starting directory for all file page stores, for example: 'db/group-123/index.bin'. */
    private final Path dbDir;

    /** Executor to disallow running code that modifies data in {@link #groupPageStores} concurrently with cleanup of file page store. */
    private final LongOperationAsyncExecutor cleanupAsyncExecutor;

    /** Mapping: group ID -> group page stores. */
    private final GroupPageStoresMap<FilePageStore> groupPageStores;

    /** {@link FilePageStore} factory. */
    private final FilePageStoreFactory filePageStoreFactory;

    /** Failure processor. */
    private final FailureManager failureManager;

    /**
     * Constructor.
     *
     * @param igniteInstanceName Name of the Ignite instance.
     * @param storagePath Storage path.
     * @param filePageStoreFileIoFactory {@link FileIo} factory for file page store.
     * @param pageSize Page size in bytes.
     * @param failureManager Failure processor that is used to handler critical errors.
     */
    public FilePageStoreManager(
            String igniteInstanceName,
            Path storagePath,
            FileIoFactory filePageStoreFileIoFactory,
            // TODO: IGNITE-17017 Move to common config
            int pageSize,
            FailureManager failureManager
    ) {
        this.dbDir = storagePath.resolve("db");
        this.failureManager = failureManager;

        cleanupAsyncExecutor = new LongOperationAsyncExecutor(igniteInstanceName, LOG);

        groupPageStores = new GroupPageStoresMap<>(cleanupAsyncExecutor);

        filePageStoreFactory = new FilePageStoreFactory(filePageStoreFileIoFactory, pageSize);
    }

    /**
     * Starts the file page store manager.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    public void start() throws IgniteInternalCheckedException {
        try {
            createDirectories(dbDir);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Could not create work directory for page stores: " + dbDir, e);
        }

        if (LOG.isWarnEnabled()) {
            String tmpDir = System.getProperty("java.io.tmpdir");

            if (tmpDir != null && this.dbDir.startsWith(tmpDir)) {
                LOG.warn("Persistence store directory is in the temp directory and may be cleaned. "
                        + "To avoid this, change location of persistence directories [currentDir={}]", this.dbDir);
            }
        }

        List<Path> toDelete = new ArrayList<>();

        try (Stream<Path> tmpFileStream = Files.find(
                dbDir,
                Integer.MAX_VALUE,
                (path, basicFileAttributes) -> path.getFileName().toString().endsWith(TMP_FILE_SUFFIX))
        ) {
            toDelete.addAll(tmpFileStream.collect(toList()));
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Error while searching temporary files:" + dbDir, e);
        }

        Pattern delPartitionFilePattern = Pattern.compile(DEL_PART_FILE_REGEXP);

        try (Stream<Path> delFileStream = Files.find(
                dbDir,
                Integer.MAX_VALUE,
                (path, basicFileAttributes) -> path.getFileName().toString().endsWith(DEL_FILE_SUFFIX))
        ) {
            delFileStream.forEach(delFilePath -> {
                Matcher matcher = delPartitionFilePattern.matcher(delFilePath.getFileName().toString());

                if (!matcher.matches()) {
                    throw new IgniteInternalException("Unknown file: " + delFilePath);
                }

                Path tableWorkDir = delFilePath.getParent();

                int partitionId = Integer.parseInt(matcher.group(1));

                toDelete.add(tableWorkDir.resolve(String.format(PART_FILE_TEMPLATE, partitionId)));

                try {
                    toDelete.addAll(List.of(findPartitionDeltaFiles(tableWorkDir, partitionId)));
                } catch (IgniteInternalCheckedException e) {
                    throw new IgniteInternalException("Error when searching delta files for partition:" + delFilePath, e);
                }

                toDelete.add(delFilePath);
            });
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Error while searching temporary files:" + dbDir, e);
        }

        if (!toDelete.isEmpty()) {
            LOG.info("Files to be deleted: {}", toDelete);

            toDelete.forEach(IgniteUtils::deleteIfExists);
        }
    }

    /**
     * Stops the file page store manager.
     */
    public void stop() throws Exception {
        stopAllGroupFilePageStores(false);

        cleanupAsyncExecutor.awaitAsyncTaskCompletion(false);
    }

    @Override
    public void read(int grpId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        try {
            FilePageStore pageStore = getStoreWithCheckExists(new GroupPartitionId(grpId, partitionId(pageId)));

            pageStore.read(pageId, pageBuf, keepCrc);
        } catch (IgniteInternalCheckedException e) {
            failureManager.process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    @Override
    public PageStore write(
            int grpId,
            long pageId,
            ByteBuffer pageBuf
    ) throws IgniteInternalCheckedException {
        try {
            FilePageStore pageStore = getStoreWithCheckExists(new GroupPartitionId(grpId, partitionId(pageId)));

            pageStore.write(pageId, pageBuf);

            return pageStore;
        } catch (IgniteInternalCheckedException e) {
            failureManager.process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    @Override
    public long allocatePage(int grpId, int partId, byte flags) throws IgniteInternalCheckedException {
        assert partId >= 0 && partId <= MAX_PARTITION_ID : partId;

        try {
            FilePageStore pageStore = getStoreWithCheckExists(new GroupPartitionId(grpId, partId));

            int pageIdx = pageStore.allocatePage();

            return pageId(partId, flags, pageIdx);
        } catch (IgniteInternalCheckedException e) {
            failureManager.process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /**
     * Returns view for all page stores of all groups.
     */
    public Stream<GroupPartitionPageStore<FilePageStore>> allPageStores() {
        return groupPageStores.getAll();
    }

    /**
     * Returns partition file page store for the corresponding parameters.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @return Partition file page store, {@code null} if not initialized or has been removed.
     */
    public @Nullable FilePageStore getStore(GroupPartitionId groupPartitionId) {
        return groupPageStores.get(groupPartitionId);
    }

    /**
     * Returns partition file page store for the corresponding parameters.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @throws IgniteInternalCheckedException If the partition file page store does not exist.
     */
    private FilePageStore getStoreWithCheckExists(GroupPartitionId groupPartitionId) throws IgniteInternalCheckedException {
        FilePageStore filePageStore = getStore(groupPartitionId);

        if (filePageStore == null) {
            throw new IgniteInternalCheckedException(IgniteStringFormatter.format(
                    "Partition file page store is either not initialized or deleted: [groupId={}, partitionId={}]",
                    groupPartitionId.getGroupId(),
                    groupPartitionId.getPartitionId()
            ));
        }

        return filePageStore;
    }

    /**
     * Stops the all group file page stores.
     *
     * @param cleanFiles Delete files.
     */
    void stopAllGroupFilePageStores(boolean cleanFiles) {
        List<FilePageStore> partitionPageStores = groupPageStores.getAll()
                .map(GroupPartitionPageStore::pageStore)
                .collect(toList());

        groupPageStores.clear();

        Runnable stopPageStores = () -> {
            try {
                stopGroupFilePageStores(partitionPageStores, cleanFiles);

                LOG.info("Cleanup cache stores [total={}, cleanFiles={}]", partitionPageStores.size(), cleanFiles);
            } catch (Exception e) {
                LOG.info("Failed to gracefully stop page store managers", e);
            }
        };

        if (cleanFiles) {
            cleanupAsyncExecutor.async(stopPageStores, "file-page-stores-cleanup");
        } else {
            stopPageStores.run();
        }
    }

    private static void stopGroupFilePageStores(
            List<FilePageStore> partitionPageStores,
            boolean cleanFiles
    ) throws IgniteInternalCheckedException {
        try {
            List<AutoCloseable> closePageStores = partitionPageStores.stream()
                    .map(pageStore -> (AutoCloseable) () -> pageStore.stop(cleanFiles))
                    .collect(toList());

            closeAll(closePageStores);
        } catch (IgniteInternalCheckedException e) {
            throw e;
        } catch (Exception e) {
            throw new IgniteInternalCheckedException(e);
        }
    }

    private Path ensureGroupWorkDir(int groupId) throws IgniteInternalCheckedException {
        Path groupWorkDir = groupDir(groupId);

        try {
            createDirectories(groupWorkDir);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Failed to initialize group working directory "
                    + "(failed to create, make sure the work folder has correct permissions): "
                    + groupWorkDir, e);
        }

        return groupWorkDir;
    }

    /**
     * Returns paths (unsorted) to delta files for the requested partition.
     *
     * @param groupWorkDir Group directory.
     * @param partitionId Partition ID.
     */
    static Path[] findPartitionDeltaFiles(Path groupWorkDir, int partitionId) throws IgniteInternalCheckedException {
        String partitionDeltaFilePrefix = String.format(PART_DELTA_FILE_PREFIX, partitionId);

        // Files#find is not used on purpose, because if a file is deleted from the directory in parallel, then NoSuchFileException may
        // appear on unix os.
        File[] files = groupWorkDir.toFile().listFiles((dir, name) -> name.startsWith(partitionDeltaFilePrefix));

        assert files != null : groupWorkDir;

        return Stream.of(files)
                .map(File::toPath)
                .toArray(Path[]::new);
    }

    /**
     * Returns the temporary path to the delta file page store, like "part-1-delta-1.bin.tmp".
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     * @param index Index of the delta file page store.
     */
    public Path tmpDeltaFilePageStorePath(int groupId, int partitionId, int index) {
        return groupDir(groupId).resolve(String.format(TMP_PART_DELTA_FILE_TEMPLATE, partitionId, index));
    }

    /**
     * Returns the path to the delta file page store, like "part-1-delta-1.bin".
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     * @param index Index of the delta file page store.
     */
    public Path deltaFilePageStorePath(int groupId, int partitionId, int index) {
        return groupDir(groupId).resolve(String.format(PART_DELTA_FILE_TEMPLATE, partitionId, index));
    }

    /**
     * Destruction of the partition file and its delta files.
     *
     * <p>Deletes the partition pages store and all its delta files. Before that, it creates a marker file (for example,
     * "table-1/part-1.del") so as not to get into the situation that we deleted only part of the data/files, the node restarted and we have
     * something left. At the start, we will delete all partition files with delta files that will have a marker.
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @return Future on completion of which the partition file and its delta files will be destroyed.
     */
    public CompletableFuture<Void> destroyPartition(GroupPartitionId groupPartitionId) {
        FilePageStore removed = groupPageStores.remove(groupPartitionId);

        assert removed != null : IgniteStringFormatter.format(
                "Parallel deletion is not allowed: [groupId={}, partitionId={}]",
                groupPartitionId.getGroupId(),
                groupPartitionId.getPartitionId()
        );

        assert removed.isMarkedToDestroy() : IgniteStringFormatter.format(
                "Wasn't marked for deletion: [groupId={}, partitionId={}]",
                groupPartitionId.getGroupId(),
                groupPartitionId.getPartitionId()
        );

        return cleanupAsyncExecutor.async((RunnableX) () -> {
            Path partitionDeleteFilePath = createFile(
                    groupDir(groupPartitionId.getGroupId())
                            .resolve(String.format(DEL_PART_FILE_TEMPLATE, groupPartitionId.getPartitionId()))
            );

            removed.stop(true);

            delete(partitionDeleteFilePath);
        }, "destroy-group-" + groupPartitionId.getGroupId() + "-partition-" + groupPartitionId.getPartitionId());
    }

    /**
     * Reads a partition file page store from the file system with its delta files if it exists, otherwise creates a new one but without
     * saving it to the file system.
     *
     * <p>Also does not initialize the storage, i.e. does not call {@link FilePageStore#ensure()}.</p>
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @param readBuffer Buffer for reading file headers and other supporting information from files.
     */
    public FilePageStore readOrCreateStore(
            GroupPartitionId groupPartitionId,
            ByteBuffer readBuffer
    ) throws IgniteInternalCheckedException {
        Path tableWorkDir = ensureGroupWorkDir(groupPartitionId.getGroupId());

        Path partFilePath = tableWorkDir.resolve(String.format(PART_FILE_TEMPLATE, groupPartitionId.getPartitionId()));

        Path[] partDeltaFiles = findPartitionDeltaFiles(tableWorkDir, groupPartitionId.getPartitionId());

        return filePageStoreFactory.createPageStore(readBuffer.rewind(), partFilePath, partDeltaFiles);
    }

    /**
     * Adds a partition file page storage.
     *
     * <p>It is expected that the storage has not been added previously and is also ready to be used by other components such as checkpoint
     * or delta file compactor.</p>
     *
     * @param groupPartitionId Pair of group ID with partition ID.
     * @param filePageStore Partition file page store.
     */
    public void addStore(GroupPartitionId groupPartitionId, FilePageStore filePageStore) {
        groupPageStores.compute(groupPartitionId, oldFilePageStore -> {
            assert oldFilePageStore == null : groupPartitionId;

            return filePageStore;
        });
    }

    /**
     * Destroys the group directory with all sub-directories and files if it exists.
     *
     * @throws IOException If there was an I/O error when deleting a directory.
     */
    public void destroyGroupIfExists(int groupId) throws IOException {
        Path groupDir = groupDir(groupId);

        try {
            deleteIfExistsThrowable(groupDir);
        } catch (IOException e) {
            throw new IOException("Failed to delete group directory: " + groupDir, e);
        }
    }

    private Path groupDir(int groupId) {
        return dbDir.resolve(GROUP_DIR_PREFIX + groupId);
    }

    /**
     * Scans the working directory to find IDs of all groups for which directories still remain.
     *
     * @return IDs of groups.
     */
    public Set<Integer> allGroupIdsOnFs() {
        try (Stream<Path> tableDirs = Files.list(dbDir)) {
            return tableDirs
                    .filter(path -> Files.isDirectory(path) && path.getFileName().toString().startsWith(GROUP_DIR_PREFIX))
                    .map(FilePageStoreManager::extractTableId)
                    .collect(toUnmodifiableSet());
        } catch (IOException e) {
            throw new IgniteInternalException(Common.INTERNAL_ERR, "Cannot scan for groupIDs", e);
        }
    }

    private static int extractTableId(Path tableDir) {
        Path fileName = tableDir.getFileName();

        assert fileName.toString().startsWith(GROUP_DIR_PREFIX) : tableDir;

        String idString = fileName.toString().substring(GROUP_DIR_PREFIX.length());
        return Integer.parseInt(idString);
    }
}
