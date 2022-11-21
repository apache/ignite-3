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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.persistence.PageReadWriteManager;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap.GroupPageStores;
import org.apache.ignite.internal.pagememory.persistence.store.GroupPageStoresMap.PartitionPageStore;
import org.apache.ignite.internal.util.IgniteStripedLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Partition file page store manager.
 */
// TODO: IGNITE-17132 Don't forget to delete partition files
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

    /** Partition temporary delta file template, example "part-1-delta-1.bin". */
    public static final String PART_DELTA_FILE_TEMPLATE = PART_DELTA_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** Partition delta file template, example "part-1-delta-1.bin.tmp". */
    public static final String TMP_PART_DELTA_FILE_TEMPLATE = PART_DELTA_FILE_TEMPLATE + TMP_FILE_SUFFIX;

    /** Group directory prefix. */
    public static final String GROUP_DIR_PREFIX = "table-";

    /** Starting directory for all file page stores, for example: 'db/group-123/index.bin'. */
    private final Path dbDir;

    /** Page size in bytes. */
    private final int pageSize;

    /** Executor to disallow running code that modifies data in {@link #groupPageStores} concurrently with cleanup of file page store. */
    private final LongOperationAsyncExecutor cleanupAsyncExecutor;

    /** Mapping: group ID -> group page stores. */
    private final GroupPageStoresMap<FilePageStore> groupPageStores;

    /** Striped lock. */
    private final IgniteStripedLock stripedLock = new IgniteStripedLock(Math.max(Runtime.getRuntime().availableProcessors(), 8));

    /** {@link FilePageStore} factory. */
    private final FilePageStoreFactory filePageStoreFactory;

    /**
     * Constructor.
     *
     * @param igniteInstanceName Name of the Ignite instance.
     * @param storagePath Storage path.
     * @param filePageStoreFileIoFactory {@link FileIo} factory for file page store.
     * @param pageSize Page size in bytes.
     * @throws IgniteInternalCheckedException If failed.
     */
    public FilePageStoreManager(
            String igniteInstanceName,
            Path storagePath,
            FileIoFactory filePageStoreFileIoFactory,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) throws IgniteInternalCheckedException {
        this.dbDir = storagePath.resolve("db");
        this.pageSize = pageSize;

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
                        + "To avoid this change location of persistence directories [currentDir={}]", this.dbDir);
            }
        }

        try (Stream<Path> tmpFileStream = Files.find(
                dbDir,
                Integer.MAX_VALUE,
                (path, basicFileAttributes) -> path.getFileName().toString().endsWith(TMP_FILE_SUFFIX)
        )) {
            List<Path> tmpFiles = tmpFileStream.collect(toList());

            if (!tmpFiles.isEmpty()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Temporary files to be deleted: {}", tmpFiles.size());
                }

                tmpFiles.forEach(IgniteUtils::deleteIfExists);
            }
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Could not create work directory for page stores: " + dbDir, e);
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
        FilePageStore pageStore = getStore(grpId, partitionId(pageId));

        try {
            pageStore.read(pageId, pageBuf, keepCrc);
        } catch (IgniteInternalCheckedException e) {
            // TODO: IGNITE-16899 By analogy with 2.0, fail a node

            throw e;
        }
    }

    @Override
    public PageStore write(
            int grpId,
            long pageId,
            ByteBuffer pageBuf,
            boolean calculateCrc
    ) throws IgniteInternalCheckedException {
        FilePageStore pageStore = getStore(grpId, partitionId(pageId));

        try {
            pageStore.write(pageId, pageBuf, calculateCrc);
        } catch (IgniteInternalCheckedException e) {
            // TODO: IGNITE-16899 By analogy with 2.0, fail a node

            throw e;
        }

        return pageStore;
    }

    @Override
    public long allocatePage(int grpId, int partId, byte flags) throws IgniteInternalCheckedException {
        assert partId >= 0 && partId <= MAX_PARTITION_ID : partId;

        FilePageStore pageStore = getStore(grpId, partId);

        try {
            int pageIdx = pageStore.allocatePage();

            return pageId(partId, flags, pageIdx);
        } catch (IgniteInternalCheckedException e) {
            // TODO: IGNITE-16899 By analogy with 2.0, fail a node

            throw e;
        }
    }

    /**
     * Initialization of the page storage for the group partition.
     *
     * @param tableName Table name.
     * @param tableId Integer table id.
     * @param partitionId Partition ID, must be between {@code 0} (inclusive) and {@link PageIdAllocator#MAX_PARTITION_ID} (inclusive).
     * @throws IgniteInternalCheckedException If failed.
     */
    public void initialize(String tableName, int tableId, int partitionId) throws IgniteInternalCheckedException {
        assert partitionId >= 0 && partitionId <= MAX_PARTITION_ID : partitionId;

        stripedLock.lock(tableId + partitionId);

        try {
            if (!groupPageStores.contains(tableId, partitionId)) {
                Path tableWorkDir = ensureGroupWorkDir(tableId);

                ByteBuffer buffer = allocateBuffer(pageSize);

                try {
                    Path partFilePath = tableWorkDir.resolve(String.format(PART_FILE_TEMPLATE, partitionId));

                    Path[] partDeltaFiles = findPartitionDeltaFiles(tableWorkDir, partitionId);

                    FilePageStore filePageStore = filePageStoreFactory.createPageStore(buffer.rewind(), partFilePath, partDeltaFiles);

                    FilePageStore previous = groupPageStores.put(tableId, partitionId, filePageStore);

                    assert previous == null : IgniteStringFormatter.format(
                            "Parallel creation is not allowed: [tableName={}, tableId={}, partitionId={}]",
                            tableName,
                            tableId,
                            partitionId
                    );
                } finally {
                    freeBuffer(buffer);
                }
            }
        } catch (IgniteInternalCheckedException e) {
            // TODO: IGNITE-16899 By analogy with 2.0, fail a node

            throw e;
        } finally {
            stripedLock.unlock(tableId + partitionId);
        }
    }

    /**
     * Returns the group partition page stores.
     *
     * @param grpId Group ID.
     */
    public @Nullable GroupPageStores<FilePageStore> getStores(int grpId) {
        return groupPageStores.get(grpId);
    }

    /**
     * Returns view for all page stores of all groups.
     */
    public Collection<GroupPageStores<FilePageStore>> allPageStores() {
        return groupPageStores.getAll();
    }

    /**
     * Returns partition file page store for the corresponding parameters.
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID, from {@code 0} (inclusive) to {@link PageIdAllocator#MAX_PARTITION_ID} (inclusive).
     */
    public @Nullable FilePageStore getStore(int groupId, int partitionId) {
        assert partitionId >= 0 && partitionId <= MAX_PARTITION_ID : partitionId;

        GroupPageStores<FilePageStore> groupPageStores = this.groupPageStores.get(groupId);

        if (groupPageStores == null) {
            return null;
        }

        PartitionPageStore<FilePageStore> partitionPageStore = groupPageStores.get(partitionId);

        return partitionPageStore == null ? null : partitionPageStore.pageStore();
    }

    /**
     * Stops the all group file page stores.
     *
     * @param cleanFiles Delete files.
     */
    void stopAllGroupFilePageStores(boolean cleanFiles) {
        List<FilePageStore> partitionPageStores = groupPageStores.getAll().stream()
                .map(GroupPageStores::getAll)
                .flatMap(Collection::stream)
                .map(PartitionPageStore::pageStore)
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
        Path groupWorkDir = dbDir.resolve(GROUP_DIR_PREFIX + groupId);

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
     * @param partition Partition number.
     */
    Path[] findPartitionDeltaFiles(Path groupWorkDir, int partition) throws IgniteInternalCheckedException {
        assert partition >= 0 : partition;

        String partitionDeltaFilePrefix = String.format(PART_DELTA_FILE_PREFIX, partition);

        try (Stream<Path> deltaFileStream = Files.find(
                groupWorkDir,
                1,
                (path, basicFileAttributes) -> path.getFileName().toString().startsWith(partitionDeltaFilePrefix))
        ) {
            return deltaFileStream.toArray(Path[]::new);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException(
                    IgniteStringFormatter.format(
                            "Error while searching delta partition files [groupDir={}, partition={}]",
                            groupWorkDir,
                            partition
                    ),
                    e
            );
        }
    }

    /**
     * Returns the temporary path to the delta file page store, like "part-1-delta-1.bin.tmp".
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     * @param index Index of the delta file page store.
     */
    public Path tmpDeltaFilePageStorePath(int groupId, int partitionId, int index) {
        return dbDir.resolve(GROUP_DIR_PREFIX + groupId).resolve(String.format(TMP_PART_DELTA_FILE_TEMPLATE, partitionId, index));
    }

    /**
     * Returns the path to the delta file page store, like "part-1-delta-1.bin".
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     * @param index Index of the delta file page store.
     */
    public Path deltaFilePageStorePath(int groupId, int partitionId, int index) {
        return dbDir.resolve(GROUP_DIR_PREFIX + groupId).resolve(String.format(PART_DELTA_FILE_TEMPLATE, partitionId, index));
    }

    /**
     * Callback on destruction of the partition of the corresponding group.
     *
     * <p>TODO: IGNITE-17132 добавить описание
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     */
    public void onPartitionDestruction(int groupId, int partitionId) {
        // TODO: IGNITE-17132 реализовать, аккуратно
    }
}
