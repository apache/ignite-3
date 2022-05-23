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
import static org.apache.ignite.internal.pagememory.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.persistence.PageReadWriteManager;
import org.apache.ignite.internal.util.IgniteStripedLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * File page store manager.
 */
public class FilePageStoreManager implements IgniteComponent, PageReadWriteManager {
    /** File suffix. */
    public static final String FILE_SUFFIX = ".bin";

    /** Suffix for zip files. */
    public static final String ZIP_SUFFIX = ".zip";

    /** Suffix for tmp files. */
    public static final String TMP_SUFFIX = ".tmp";

    /** Partition file prefix. */
    public static final String PART_FILE_PREFIX = "part-";

    /** Index file prefix. */
    public static final String INDEX_FILE_PREFIX = "index";

    /** Index file name. */
    public static final String INDEX_FILE_NAME = INDEX_FILE_PREFIX + FILE_SUFFIX;

    /** Partition file template. */
    public static final String PART_FILE_TEMPLATE = PART_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** Group directory prefix. */
    public static final String GROUP_DIR_PREFIX = "group-";

    /** Data directory predicate. */
    public static final Predicate<File> GROUP_DIR_FILTER = dir -> dir.getName().startsWith(GROUP_DIR_PREFIX);

    /** Matcher for searching of *.tmp files. */
    public static final PathMatcher TMP_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:**" + TMP_SUFFIX);

    /** Logger. */
    private final IgniteLogger log;

    /** Starting directory for all file page stores, for example: 'db/group-123/index.bin'. */
    private final Path dbDir;

    /** {@link FileIo} factory for file page store. */
    private final FileIoFactory filePageStoreFileIoFactory;

    /** Page read write manager. */
    private final PageReadWriteManagerImpl pageReadWriteManager;

    /**
     * Executor to disallow running code that modifies data in {@link #groupPageStoreHolders} concurrently with cleanup of file page store.
     */
    private final LongOperationAsyncExecutor cleanupAsyncExecutor;

    /** Mapping: group ID -> {@link GroupPageStoreHolder}. */
    private final GroupPageStoreHolderMap<FilePageStore> groupPageStoreHolders;

    /** Group directory initialization lock. */
    private final IgniteStripedLock initGroupDirLock = new IgniteStripedLock(Math.max(Runtime.getRuntime().availableProcessors(), 8));

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param igniteInstanceName Name of the Ignite instance.
     * @param storagePath Storage path.
     * @param filePageStoreFileIoFactory {@link FileIo} factory for file page store.
     * @throws IgniteInternalCheckedException If failed.
     */
    public FilePageStoreManager(
            IgniteLogger log,
            String igniteInstanceName,
            Path storagePath,
            FileIoFactory filePageStoreFileIoFactory
    ) throws IgniteInternalCheckedException {
        this.log = log;
        this.filePageStoreFileIoFactory = filePageStoreFileIoFactory;
        this.dbDir = storagePath.resolve("db");

        try {
            createDirectories(dbDir);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Could not create work directory for page stores: " + dbDir, e);
        }

        cleanupAsyncExecutor = new LongOperationAsyncExecutor(igniteInstanceName, log);

        groupPageStoreHolders = new GroupPageStoreHolderMap(cleanupAsyncExecutor);

        pageReadWriteManager = new PageReadWriteManagerImpl(this);
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (log.isWarnEnabled()) {
            String tmpDir = System.getProperty("java.io.tmpdir");

            if (tmpDir != null && this.dbDir.startsWith(tmpDir)) {
                log.warn("Persistence store directory is in the temp directory and may be cleaned. " +
                        "To avoid this change location of persistence directories. " +
                        "Current persistence store directory is: [" + this.dbDir + "]");
            }
        }
    }

    /** {@inheritDoc} */
    @Override
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
    public void write(int grpId, long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteInternalCheckedException {
        pageReadWriteManager.write(grpId, pageId, pageBuf, tag, calculateCrc);
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
     * @param partitions Partition number.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void initialize(String grpName, int grpId, int partitions) throws IgniteInternalCheckedException {
        assert dbDir != null;

        if (!groupPageStoreHolders.containsKey(cacheId)) {
            GroupPageStoreHolder holder = initDir(
                    new File(dbDir, workingDir),
                    cacheId,
                    partitions,
                    pageMetrics,
                    cctx.cacheContext(cacheId) != null && cctx.cacheContext(cacheId).config().isEncryptionEnabled()
            );

            GroupPageStoreHolder old = groupPageStoreHolders.put(cacheId, holder);

            assert old == null : "Non-null old store holder for cacheId: " + cacheId;
        }
    }

    /**
     * @param cacheWorkDir Work directory.
     * @param grpId Group ID.
     * @param partitions Number of partitions.
     * @param pageMetrics Page metrics.
     * @return Cache store holder.
     * @throws IgniteInternalCheckedException If failed.
     */
    private GroupPageStoreHolder initDir(File cacheWorkDir,
            int grpId,
            int partitions
    ) throws IgniteInternalCheckedException {
        try {
            File idxFile = new File(cacheWorkDir, INDEX_FILE_NAME);

            FilePageStoreFactory pageStoreFactory = getPageStoreFactory(grpId);

            PageStore idxStore =
                    pageStoreFactory.createPageStore(
                            PageStore.TYPE_IDX,
                            idxFile,
                            pageMetrics.totalPages()::add);

            PageStore[] partStores = new PageStore[partitions];

            for (int partId = 0; partId < partStores.length; partId++) {
                final int p = partId;

                PageStore partStore = pageStoreFactory.createPageStore(
                        PageStore.TYPE_DATA,
                        () -> getPartitionFilePath(cacheWorkDir, p),
                        pageMetrics.totalPages()::add
                );

                partStores[partId] = partStore;
            }

            return new GroupPageStoreHolder(idxStore, partStores);
        } catch (IgniteInternalCheckedException e) {
            if (X.hasCause(e, StorageException.class, IOException.class)) {
                cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }

            throw e;
        }
    }

    /**
     * @param cacheWorkDir Cache work directory.
     * @param partId Partition id.
     */
    private Path getPartitionFilePath(File cacheWorkDir, int partId) {
        return new File(cacheWorkDir, partitionFileName(partId)).toPath();
    }

    /**
     * @param workDir Cache work directory.
     * @param cacheDirName Cache directory name.
     * @param partId Partition id.
     * @return Partition file.
     */
    public static File getPartitionFile(File workDir, String cacheDirName, int partId) {
        return new File(cacheWorkDir(workDir, cacheDirName), partitionFileName(partId));
    }

    /** {@inheritDoc} */
    @Override
    public boolean checkAndInitCacheWorkDir(CacheConfiguration cacheCfg) throws IgniteInternalCheckedException {
        return checkAndInitCacheWorkDir(cacheWorkDir(cacheCfg));
    }

    /**
     * @param cacheWorkDir Cache work directory.
     */
    private boolean checkAndInitCacheWorkDir(File cacheWorkDir) throws IgniteInternalCheckedException {
        boolean dirExisted = false;

        Lock lock = initGroupDirLock.getLock(cacheWorkDir.getName().hashCode());

        lock.lock();

        try {
            if (!Files.exists(cacheWorkDir.toPath())) {
                try {
                    Files.createDirectory(cacheWorkDir.toPath());
                } catch (IOException e) {
                    throw new IgniteInternalCheckedException("Failed to initialize cache working directory " +
                            "(failed to create, make sure the work folder has correct permissions): " +
                            cacheWorkDir.getAbsolutePath(), e);
                }
            } else {
                if (cacheWorkDir.isFile()) {
                    throw new IgniteInternalCheckedException("Failed to initialize cache working directory " +
                            "(a file with the same name already exists): " + cacheWorkDir.getAbsolutePath());
                }

                File lockF = new File(cacheWorkDir, IgniteCacheSnapshotManager.SNAPSHOT_RESTORE_STARTED_LOCK_FILENAME);

                Path cacheWorkDirPath = cacheWorkDir.toPath();

                Path tmp = cacheWorkDirPath.getParent().resolve(cacheWorkDir.getName() + TMP_SUFFIX);

                if (Files.exists(tmp) && Files.isDirectory(tmp) &&
                        Files.exists(tmp.resolve(IgniteCacheSnapshotManager.TEMP_FILES_COMPLETENESS_MARKER))) {

                    U.warn(log, "Ignite node crashed during the snapshot restore process " +
                            "(there is a snapshot restore lock file left for cache). But old version of cache was saved. " +
                            "Trying to restore it. Cache - [" + cacheWorkDir.getAbsolutePath() + ']');

                    U.delete(cacheWorkDir);

                    try {
                        Files.move(tmp, cacheWorkDirPath, StandardCopyOption.ATOMIC_MOVE);

                        cacheWorkDirPath.resolve(IgniteCacheSnapshotManager.TEMP_FILES_COMPLETENESS_MARKER).toFile().delete();
                    } catch (IOException e) {
                        throw new IgniteInternalCheckedException(e);
                    }
                } else if (lockF.exists()) {
                    U.warn(log, "Ignite node crashed during the snapshot restore process " +
                            "(there is a snapshot restore lock file left for cache). Will remove both the lock file and " +
                            "incomplete cache directory [cacheDir=" + cacheWorkDir.getAbsolutePath() + ']');

                    boolean deleted = U.delete(cacheWorkDir);

                    if (!deleted) {
                        throw new IgniteInternalCheckedException("Failed to remove obsolete cache working directory " +
                                "(remove the directory manually and make sure the work folder has correct permissions): " +
                                cacheWorkDir.getAbsolutePath());
                    }

                    cacheWorkDir.mkdirs();
                } else {
                    dirExisted = true;
                }

                if (!cacheWorkDir.exists()) {
                    throw new IgniteInternalCheckedException("Failed to initialize cache working directory " +
                            "(failed to create, make sure the work folder has correct permissions): " +
                            cacheWorkDir.getAbsolutePath());
                }

                if (Files.exists(tmp)) {
                    U.delete(tmp);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        return dirExisted;
    }

    /**
     * Returns collection of related file page stores.
     *
     * @param grpId Group ID.
     */
    public @Nullable Collection<FilePageStore> getStores(int grpId) {
        return groupPageStoreHolders.get(grpId);
    }

    /**
     * Returns file page store for the corresponding parameters.
     *
     * @param grpId Group ID.
     * @param partId Partition ID, either {@link PageIdAllocator#INDEX_PARTITION} or {@code 0} to {@link PageIdAllocator#MAX_PARTITION_ID}
     * (inclusive).
     * @throws IgniteInternalCheckedException If group or partition with the given ID was not created.
     */
    public FilePageStore getStore(int grpId, int partId) throws IgniteInternalCheckedException {
        GroupPageStoreHolder<FilePageStore> holder = groupPageStoreHolders.get(grpId);

        if (holder == null) {
            throw new IgniteInternalCheckedException(
                    "Failed to get file page store for the given group ID (group has not been started): " + grpId
            );
        }

        if (partId == INDEX_PARTITION) {
            return holder.idxStore;
        }

        if (partId > MAX_PARTITION_ID) {
            throw new IgniteInternalCheckedException("Partition ID is reserved: " + partId);
        }

        FilePageStore store = holder.partStores[partId];

        if (store == null) {
            throw new IgniteInternalCheckedException(String.format(
                    "Failed to get file page store for the given partition ID (partition has not been created) [grpId=%s, partId=%s]",
                    grpId,
                    partId
            ));
        }

        return store;
    }

    /**
     * Stops the all group file page stores.
     *
     * @param cleanFiles Delete files.
     */
    void stopAllGroupFilePageStores(boolean cleanFiles) {
        List<GroupPageStoreHolder<FilePageStore>> holders = new ArrayList<>(groupPageStoreHolders.size());

        for (Iterator<GroupPageStoreHolder<FilePageStore>> it = groupPageStoreHolders.values().iterator(); it.hasNext(); ) {
            GroupPageStoreHolder<FilePageStore> holder = it.next();

            it.remove();

            holders.add(holder);
        }

        Runnable stopPageStores = () -> {
            try {
                stopGroupFilePageStores(holders, cleanFiles);

                if (log.isInfoEnabled()) {
                    log.info(String.format("Cleanup cache stores [total=%s, cleanFiles=%s]", holders.size(), cleanFiles));
                }
            } catch (Exception e) {
                log.error("Failed to gracefully stop page store managers", e);
            }
        };

        if (cleanFiles) {
            cleanupAsyncExecutor.async(stopPageStores);
        } else {
            stopPageStores.run();
        }
    }

    /**
     * Returns the directory where all file page stores of all groups are stored.
     */
    public Path workDir() {
        return dbDir;
    }

    private static void stopGroupFilePageStores(
            Collection<GroupPageStoreHolder<FilePageStore>> groupFilePageStoreHolders,
            boolean cleanFiles
    ) throws IgniteInternalCheckedException {
        try {
            closeAll(groupFilePageStoreHolders.stream().flatMap(Collection::stream).map(pageStore -> () -> pageStore.stop(cleanFiles)));
        } catch (IgniteInternalCheckedException e) {
            throw e;
        } catch (Exception e) {
            throw new IgniteInternalCheckedException(e);
        }
    }

    /**
     * Returns partition ID from file name, either {@link PageIdAllocator#INDEX_PARTITION} or {@code 0} to {@link
     * PageIdAllocator#MAX_PARTITION_ID} (inclusive)
     *
     * @param partitionFileName Partition file name.
     */
    private static int partId(String partitionFileName) {
        if (partitionFileName.equals(INDEX_FILE_NAME)) {
            return INDEX_PARTITION;
        }

        if (partitionFileName.startsWith(PART_FILE_PREFIX)) {
            return Integer.parseInt(partitionFileName.substring(PART_FILE_PREFIX.length(), partitionFileName.indexOf('.')));
        }

        throw new IllegalStateException("Illegal partition file name: " + partitionFileName);
    }

    /**
     * @param partId Partition id.
     * @return File name.
     */
    private static String partitionFileName(int partId) {
        assert partId >= 0 && (partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION) : partId;

        return partId == INDEX_PARTITION ? INDEX_FILE_NAME : String.format(PART_FILE_TEMPLATE, partId);
    }
}
