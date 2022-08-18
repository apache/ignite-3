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

import static java.nio.file.Files.createDirectories;
import static java.util.Collections.unmodifiableList;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.persistence.PageReadWriteManager;
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
    /** File suffix. */
    public static final String FILE_SUFFIX = ".bin";

    /** Suffix of the temporary file. */
    public static final String TMP_FILE_SUFFIX = ".tmp";

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

    /** Logger. */
    private final IgniteLogger log;

    /** Starting directory for all file page stores, for example: 'db/group-123/index.bin'. */
    private final Path dbDir;

    /** Page size in bytes. */
    private final int pageSize;

    /**
     * Executor to disallow running code that modifies data in {@link #groupPageStores} concurrently with cleanup of file page store.
     */
    private final LongOperationAsyncExecutor cleanupAsyncExecutor;

    /** Mapping: group ID -> page store list. */
    private final GroupPageStoresMap<FilePageStore> groupPageStores;

    /** Group directory initialization lock. */
    private final IgniteStripedLock initGroupDirLock = new IgniteStripedLock(Math.max(Runtime.getRuntime().availableProcessors(), 8));

    /** {@link FilePageStore} factory. */
    private final FilePageStoreFactory filePageStoreFactory;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param igniteInstanceName Name of the Ignite instance.
     * @param storagePath Storage path.
     * @param filePageStoreFileIoFactory {@link FileIo} factory for file page store.
     * @param pageSize Page size in bytes.
     * @throws IgniteInternalCheckedException If failed.
     */
    public FilePageStoreManager(
            IgniteLogger log,
            String igniteInstanceName,
            Path storagePath,
            FileIoFactory filePageStoreFileIoFactory,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) throws IgniteInternalCheckedException {
        this.log = log;
        this.dbDir = storagePath.resolve("db");
        this.pageSize = pageSize;

        cleanupAsyncExecutor = new LongOperationAsyncExecutor(igniteInstanceName, log);

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

        if (log.isWarnEnabled()) {
            String tmpDir = System.getProperty("java.io.tmpdir");

            if (tmpDir != null && this.dbDir.startsWith(tmpDir)) {
                log.warn("Persistence store directory is in the temp directory and may be cleaned. "
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
                if (log.isInfoEnabled()) {
                    log.info("Temporary files to be deleted: {}", tmpFiles.size());
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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
     * Initializing the file page stores for a group.
     *
     * @param tableName Table name.
     * @param tableId Integer table id.
     * @param partitions Partition number, must be greater than {@code 0} and less {@link PageIdAllocator#MAX_PARTITION_ID}.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void initialize(String tableName, int tableId, int partitions) throws IgniteInternalCheckedException {
        assert partitions > 0 && partitions < MAX_PARTITION_ID : partitions;

        initGroupDirLock.lock(tableId);

        try {
            if (!groupPageStores.containsPageStores(tableId)) {
                List<FilePageStore> partitionFilePageStores = createFilePageStores(tableId, partitions);

                List<FilePageStore> old = groupPageStores.put(tableId, partitionFilePageStores);

                assert old == null : tableName;
            }
        } catch (IgniteInternalCheckedException e) {
            // TODO: IGNITE-16899 By analogy with 2.0, fail a node

            throw e;
        } finally {
            initGroupDirLock.unlock(tableId);
        }
    }

    /**
     * Returns collection of related partition file page stores for group.
     *
     * @param grpId Group ID.
     */
    public @Nullable List<FilePageStore> getStores(int grpId) {
        return groupPageStores.get(grpId);
    }

    /**
     * Returns all page stores of all groups.
     */
    public Collection<List<FilePageStore>> allPageStores() {
        return groupPageStores.allPageStores();
    }

    /**
     * Returns partition file page store for the corresponding parameters.
     *
     * @param grpId Group ID.
     * @param partId Partition ID, from {@code 0} to {@link PageIdAllocator#MAX_PARTITION_ID} (inclusive).
     * @throws IgniteInternalCheckedException If group or partition with the given ID was not created.
     */
    public FilePageStore getStore(int grpId, int partId) throws IgniteInternalCheckedException {
        assert partId >= 0 && partId <= MAX_PARTITION_ID : partId;

        List<FilePageStore> holder = groupPageStores.get(grpId);

        if (holder == null) {
            throw new IgniteInternalCheckedException(
                    "Failed to get file page store for the given group ID (group has not been started): " + grpId
            );
        }

        if (partId >= holder.size()) {
            throw new IgniteInternalCheckedException(String.format(
                    "Failed to get file page store for the given partition ID (partition has not been created) [grpId=%s, partId=%s]",
                    grpId,
                    partId
            ));
        }

        return holder.get(partId);
    }

    /**
     * Stops the all group file page stores.
     *
     * @param cleanFiles Delete files.
     */
    void stopAllGroupFilePageStores(boolean cleanFiles) {
        List<FilePageStore> partitionPageStores = groupPageStores.allPageStores().stream()
                .flatMap(Collection::stream)
                .collect(toList());

        groupPageStores.clear();

        Runnable stopPageStores = () -> {
            try {
                stopGroupFilePageStores(partitionPageStores, cleanFiles);

                log.info("Cleanup cache stores [total={}, cleanFiles={}]", partitionPageStores.size(), cleanFiles);
            } catch (Exception e) {
                log.info("Failed to gracefully stop page store managers", e);
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
            Files.createDirectories(groupWorkDir);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Failed to initialize group working directory "
                    + "(failed to create, make sure the work folder has correct permissions): "
                    + groupWorkDir, e);
        }

        return groupWorkDir;
    }

    private List<FilePageStore> createFilePageStores(
            int groupId,
            int partitions
    ) throws IgniteInternalCheckedException {
        Path groupWorkDir = ensureGroupWorkDir(groupId);

        List<FilePageStore> partitionFilePageStores = new ArrayList<>(partitions);

        ByteBuffer buffer = allocateBuffer(pageSize);

        try {
            for (int i = 0; i < partitions; i++) {
                int part = i;

                Path partFilePath = groupWorkDir.resolve(String.format(PART_FILE_TEMPLATE, part));

                Path[] partDeltaFiles = findPartitionDeltaFiles(groupWorkDir, part);

                FilePageStore filePageStore = filePageStoreFactory.createPageStore(buffer.rewind(), partFilePath, partDeltaFiles);

                partitionFilePageStores.add(filePageStore);
            }

            return unmodifiableList(partitionFilePageStores);
        } finally {
            freeBuffer(buffer);
        }
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
}
