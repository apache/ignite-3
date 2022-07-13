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
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.PageReadWriteManager;
import org.apache.ignite.internal.util.IgniteStripedLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Partition file page store manager.
 */
public class PartitionFilePageStoreManager implements PageReadWriteManager {
    /** File suffix. */
    public static final String FILE_SUFFIX = ".bin";

    /** Partition file prefix. */
    public static final String PART_FILE_PREFIX = "part-";

    /** Partition file template. */
    public static final String PART_FILE_TEMPLATE = PART_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** Group directory prefix. */
    public static final String GROUP_DIR_PREFIX = "group-";

    /** Logger. */
    private final IgniteLogger log;

    /** Starting directory for all file page stores, for example: 'db/group-123/index.bin'. */
    private final Path dbDir;

    /** Page size in bytes. */
    private final int pageSize;

    /** Page read write manager. */
    private final PageReadWriteManager pageReadWriteManager = new PageReadWriteManagerImpl(this);

    /**
     * Executor to disallow running code that modifies data in {@link #groupPageStores} concurrently with cleanup of file page store.
     */
    private final LongOperationAsyncExecutor cleanupAsyncExecutor;

    /** Mapping: group ID -> page store list. */
    private final GroupPageStoresMap<PartitionFilePageStore> groupPageStores;

    /** Group directory initialization lock. */
    private final IgniteStripedLock initGroupDirLock = new IgniteStripedLock(Math.max(Runtime.getRuntime().availableProcessors(), 8));

    /** {@link PartitionFilePageStore} factory. */
    private final PartitionFilePageStoreFactory partitionFilePageStoreFactory;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param igniteInstanceName Name of the Ignite instance.
     * @param storagePath Storage path.
     * @param filePageStoreFileIoFactory {@link FileIo} factory for file page store.
     * @param ioRegistry Page IO registry.
     * @param pageSize Page size in bytes.
     * @throws IgniteInternalCheckedException If failed.
     */
    public PartitionFilePageStoreManager(
            IgniteLogger log,
            String igniteInstanceName,
            Path storagePath,
            FileIoFactory filePageStoreFileIoFactory,
            PageIoRegistry ioRegistry,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) throws IgniteInternalCheckedException {
        this.log = log;
        this.dbDir = storagePath.resolve("db");
        this.pageSize = pageSize;

        try {
            createDirectories(dbDir);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Could not create work directory for page stores: " + dbDir, e);
        }

        cleanupAsyncExecutor = new LongOperationAsyncExecutor(igniteInstanceName, log);

        groupPageStores = new GroupPageStoresMap<>(cleanupAsyncExecutor);

        partitionFilePageStoreFactory = new PartitionFilePageStoreFactory(filePageStoreFileIoFactory, ioRegistry, pageSize);
    }

    /**
     * Starts the file page store manager.
     */
    public void start() {
        if (log.isWarnEnabled()) {
            String tmpDir = System.getProperty("java.io.tmpdir");

            if (tmpDir != null && this.dbDir.startsWith(tmpDir)) {
                log.warn("Persistence store directory is in the temp directory and may be cleaned. "
                        + "To avoid this change location of persistence directories [currentDir={}]", this.dbDir);
            }
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
        pageReadWriteManager.read(grpId, pageId, pageBuf, keepCrc);
    }

    /** {@inheritDoc} */
    @Override
    public PageStore write(
            int grpId,
            long pageId,
            ByteBuffer pageBuf,
            int tag,
            boolean calculateCrc
    ) throws IgniteInternalCheckedException {
        return pageReadWriteManager.write(grpId, pageId, pageBuf, tag, calculateCrc);
    }

    /** {@inheritDoc} */
    @Override
    public long allocatePage(int grpId, int partId, byte flags) throws IgniteInternalCheckedException {
        return pageReadWriteManager.allocatePage(grpId, partId, flags);
    }

    /**
     * Initializing the file page stores for a group.
     *
     * @param grpName Group name.
     * @param grpId Group ID.
     * @param partitions Partition number, must be greater than {@code 0} and less {@link PageIdAllocator#MAX_PARTITION_ID}.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void initialize(String grpName, int grpId, int partitions) throws IgniteInternalCheckedException {
        assert partitions > 0 && partitions < MAX_PARTITION_ID : partitions;

        initGroupDirLock.lock(grpId);

        try {
            if (!groupPageStores.containsPageStores(grpId)) {
                List<PartitionFilePageStore> partitionFilePageStores = createPartitionFilePageStores(grpName, partitions);

                List<PartitionFilePageStore> old = groupPageStores.put(grpId, partitionFilePageStores);

                assert old == null : grpName;
            }
        } catch (IgniteInternalCheckedException e) {
            // TODO: IGNITE-16899 By analogy with 2.0, fail a node

            throw e;
        } finally {
            initGroupDirLock.unlock(grpId);
        }
    }

    /**
     * Returns collection of related partition file page stores for group.
     *
     * @param grpId Group ID.
     */
    public @Nullable Collection<PartitionFilePageStore> getStores(int grpId) {
        return groupPageStores.get(grpId);
    }

    /**
     * Returns partition file page store for the corresponding parameters.
     *
     * @param grpId Group ID.
     * @param partId Partition ID, from {@code 0} to {@link PageIdAllocator#MAX_PARTITION_ID} (inclusive).
     * @throws IgniteInternalCheckedException If group or partition with the given ID was not created.
     */
    public PartitionFilePageStore getStore(int grpId, int partId) throws IgniteInternalCheckedException {
        assert partId >= 0 && partId <= MAX_PARTITION_ID : partId;

        List<PartitionFilePageStore> holder = groupPageStores.get(grpId);

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
        List<PartitionFilePageStore> partitionPageStores = groupPageStores.allPageStores().stream()
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
            List<PartitionFilePageStore> partitionPageStores,
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

    private Path ensureGroupWorkDir(String grpName) throws IgniteInternalCheckedException {
        Path groupWorkDir = dbDir.resolve(GROUP_DIR_PREFIX + grpName);

        try {
            Files.createDirectories(groupWorkDir);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Failed to initialize group working directory "
                    + "(failed to create, make sure the work folder has correct permissions): "
                    + groupWorkDir, e);
        }

        return groupWorkDir;
    }

    private List<PartitionFilePageStore> createPartitionFilePageStores(
            String grpName,
            int partitions
    ) throws IgniteInternalCheckedException {
        Path groupWorkDir = ensureGroupWorkDir(grpName);

        List<PartitionFilePageStore> partitionFilePageStores = new ArrayList<>(partitions);

        ByteBuffer buffer = allocateBuffer(pageSize);

        try {
            for (int i = 0; i < partitions; i++) {
                Path partFilePath = groupWorkDir.resolve(String.format(PART_FILE_TEMPLATE, i));

                partitionFilePageStores.add(partitionFilePageStoreFactory.createPageStore(partFilePath, buffer));
            }

            return unmodifiableList(partitionFilePageStores);
        } finally {
            freeBuffer(buffer);
        }
    }
}
