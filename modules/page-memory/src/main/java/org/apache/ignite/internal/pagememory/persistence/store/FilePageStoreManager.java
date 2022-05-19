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

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.util.function.Predicate;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.lang.IgniteLogger;

/**
 * File page store manager.
 */
public class FilePageStoreManager {
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
    public static final String GRP_DIR_PREFIX = "group-";

    /** Data directory predicate. */
    public static final Predicate<File> DATA_DIR_FILTER = dir -> dir.getName().startsWith(GRP_DIR_PREFIX);

    /** Name of the group data file. */
    public static final String GROUP_DATA_FILENAME = "group_data.dat";

    /** Name of the tmp group data file. */
    public static final String GROUP_DATA_TMP_FILENAME = GROUP_DATA_FILENAME + TMP_SUFFIX;

    public static final String DFLT_STORE_DIR = "db";

    /** Matcher for searching of *.tmp files. */
    public static final PathMatcher TMP_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:**" + TMP_SUFFIX);

    private final IgniteLogger log;
//
//    /** Listeners of configuration changes e.g. overwrite or remove actions. */
//    private final List<BiConsumer<String, File>> lsnrs = new CopyOnWriteArrayList<>();
//
//    /** Lock which guards configuration changes. */
//    private final ReentrantReadWriteLock chgLock = new ReentrantReadWriteLock();
//
//    /** Marshaller. */
//    private final Marshaller marshaller;
//
//    /** Page manager. */
//    private final PageReadWriteManager pmPageMgr;
//
    /** Executor to disallow running code that modifies data in idxCacheStores concurrently with cleanup of file page store. */
    private final LongOperationAsyncExecutor cleanupAsyncExecutor;

    /** Mapping: group ID -> {@link GroupPageStoreHolder}. */
    private final GroupPageStoreHolderMap groupPageStoreHolders;

    /**
     * File IO factory for page store, by default is taken from {@link #dsCfg}. May be overridden by block read/write.
     */
    private FileIoFactory pageStoreFileIoFactory;

    /**
     * File IO factory for page store V1 and for fast checking page store (non block read). By default is taken from {@link #dsCfg}.
     */
    private FileIoFactory pageStoreV1FileIoFactory;
//
//    /**
//     *
//     */
//    private final DataStorageConfiguration dsCfg;
//
//    /** Absolute directory for file page store. Includes consistent id based folder. */
//    private File storeWorkDir;
//
//    /**
//     *
//     */
//    private final Set<Integer> grpsWithoutIdx = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
//
//    /**
//     *
//     */
//    private final GridStripedReadWriteLock initDirLock = new GridStripedReadWriteLock(Math.max(Runtime.getRuntime().availableProcessors(), 8));
//

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param igniteInstanceName Name of the Ignite instance.
     */
    public FilePageStoreManager(
            IgniteLogger log,
            String igniteInstanceName
    ) {
        this.log = log;

        cleanupAsyncExecutor = new LongOperationAsyncExecutor(igniteInstanceName, log);

        groupPageStoreHolders = new GroupPageStoreHolderMap(cleanupAsyncExecutor);
//
//        pageStoreV1FileIoFactory = pageStoreFileIoFactory = dsCfg.getFileIOFactory();
//
//        marshaller = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
//
//        pmPageMgr = new PageReadWriteManagerImpl(ctx, this, FilePageStoreManager.class.getSimpleName());
    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void start0() throws IgniteInternalCheckedException {
//        final GridKernalContext ctx = cctx.kernalContext();
//
//        if (ctx.clientNode()) {
//            return;
//        }
//
//        final PdsFolderSettings folderSettings = ctx.pdsFolderResolver().resolveFolders();
//
//        storeWorkDir = folderSettings.persistentStoreNodePath();
//
//        U.ensureDirectory(storeWorkDir, "page store work directory", log);
//
//        String tmpDir = System.getProperty("java.io.tmpdir");
//
//        if (tmpDir != null && storeWorkDir.getAbsolutePath().startsWith(tmpDir)) {
//            log.warn("Persistence store directory is in the temp directory and may be cleaned." +
//                    "To avoid this set \"IGNITE_HOME\" environment variable properly or " +
//                    "change location of persistence directories in data storage configuration " +
//                    "(see DataStorageConfiguration#walPath, DataStorageConfiguration#walArchivePath, " +
//                    "DataStorageConfiguration#storagePath properties). " +
//                    "Current persistence store directory is: [" + storeWorkDir.getAbsolutePath() + "]");
//        }
//
//        File[] files = storeWorkDir.listFiles();
//
//        for (File file : files) {
//            if (file.isDirectory()) {
//                File[] tmpFiles = file.listFiles((k, v) -> v.endsWith(GROUP_DATA_TMP_FILENAME));
//
//                if (tmpFiles != null) {
//                    for (File tmpFile : tmpFiles) {
//                        if (!tmpFile.delete()) {
//                            log.warn("Failed to delete temporary cache config file" +
//                                    "(make sure Ignite process has enough rights):" + file.getName());
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void cleanupPersistentSpace(CacheConfiguration cacheConfiguration) throws IgniteInternalCheckedException {
//        try {
//            File cacheWorkDir = cacheWorkDir(cacheConfiguration);
//
//            if (!cacheWorkDir.exists()) {
//                return;
//            }
//
//            try (DirectoryStream<Path> files = newDirectoryStream(cacheWorkDir.toPath(),
//                    new DirectoryStream.Filter<Path>() {
//                        @Override
//                        public boolean accept(Path entry) throws IOException {
//                            return entry.toFile().getName().endsWith(FILE_SUFFIX);
//                        }
//                    })) {
//                for (Path path : files) {
//                    delete(path);
//                }
//            }
//        } catch (IOException e) {
//            throw new IgniteInternalCheckedException("Failed to cleanup persistent directory: ", e);
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void cleanupPersistentSpace() throws IgniteInternalCheckedException {
//        try {
//            try (DirectoryStream<Path> files = newDirectoryStream(
//                    storeWorkDir.toPath(), entry -> {
//                        String name = entry.toFile().getName();
//
//                        return !name.equals(MetaStorage.METASTORAGE_DIR_NAME) &&
//                                (name.startsWith(CACHE_DIR_PREFIX) || name.startsWith(GRP_DIR_PREFIX));
//                    }
//            )) {
//                for (Path path : files) {
//                    U.delete(path);
//                }
//            }
//        } catch (IOException e) {
//            throw new IgniteInternalCheckedException("Failed to cleanup persistent directory: ", e);
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void cleanupPageStoreIfMatch(Predicate<Integer> cacheGrpPred, boolean cleanFiles) {
//        Map<Integer, GroupPageStoreHolder> filteredStores = groupPageStoreHolders.entrySet().stream()
//                .filter(e -> cacheGrpPred.test(e.getKey()))
//                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//
//        groupPageStoreHolders.entrySet().removeIf(e -> cacheGrpPred.test(e.getKey()));
//
//        Runnable doShutdown = () -> {
//            IgniteInternalCheckedException ex = shutdown(filteredStores.values(), cleanFiles);
//
//            if (ex != null) {
//                U.error(log, "Failed to gracefully stop page store managers", ex);
//            }
//
//            U.log(log, "Cleanup cache stores [total=" + filteredStores.keySet().size() +
//                    ", left=" + groupPageStoreHolders.size() + ", cleanFiles=" + cleanFiles + ']');
//        };
//
//        if (cleanFiles) {
//            cleanupAsyncExecutor.async(doShutdown);
//
//            U.log(log, "Cache stores cleanup started asynchronously");
//        } else {
//            doShutdown.run();
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void stop0(boolean cancel) {
//        if (log.isDebugEnabled()) {
//            log.debug("Stopping page store manager.");
//        }
//
//        cleanupPageStoreIfMatch(p -> true, false);
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void onKernalStop0(boolean cancel) {
//        cleanupAsyncExecutor.awaitAsyncTaskCompletion(cancel);
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void onActivate(GridKernalContext kctx) throws IgniteInternalCheckedException {
//        if (log.isDebugEnabled()) {
//            log.debug("Activate page store manager [id=" + cctx.localNodeId() +
//                    " topVer=" + cctx.discovery().topologyVersionEx() + " ]");
//        }
//
//        start0();
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void onDeActivate(GridKernalContext kctx) {
//        if (log.isDebugEnabled()) {
//            log.debug("DeActivate page store manager [id=" + cctx.localNodeId() +
//                    " topVer=" + cctx.discovery().topologyVersionEx() + " ]");
//        }
//
//        stop0(true);
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void initialize(int cacheId, int partitions, String workingDir, PageMetrics pageMetrics)
//            throws IgniteInternalCheckedException {
//        assert storeWorkDir != null;
//
//        if (!groupPageStoreHolders.containsKey(cacheId)) {
//            GroupPageStoreHolder holder = initDir(
//                    new File(storeWorkDir, workingDir),
//                    cacheId,
//                    partitions,
//                    pageMetrics,
//                    cctx.cacheContext(cacheId) != null && cctx.cacheContext(cacheId).config().isEncryptionEnabled()
//            );
//
//            GroupPageStoreHolder old = groupPageStoreHolders.put(cacheId, holder);
//
//            assert old == null : "Non-null old store holder for cacheId: " + cacheId;
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void initializeForCache(CacheGroupDescriptor grpDesc, StoredCacheData cacheData) throws IgniteInternalCheckedException {
//        assert storeWorkDir != null;
//
//        int grpId = grpDesc.groupId();
//
//        if (!groupPageStoreHolders.containsKey(grpId)) {
//            GroupPageStoreHolder holder = initForCache(grpDesc, cacheData.config());
//
//            GroupPageStoreHolder old = groupPageStoreHolders.put(grpId, holder);
//
//            assert old == null : "Non-null old store holder for cache: " + cacheData.config().getName();
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void storeCacheData(StoredCacheData cacheData, boolean overwrite) throws IgniteInternalCheckedException {
//        CacheConfiguration<?, ?> ccfg = cacheData.config();
//        File cacheWorkDir = cacheWorkDir(ccfg);
//
//        checkAndInitCacheWorkDir(cacheWorkDir);
//
//        assert cacheWorkDir.exists() : "Work directory does not exist: " + cacheWorkDir;
//
//        File file = cacheConfigurationFile(ccfg);
//        Path filePath = file.toPath();
//
//        chgLock.readLock().lock();
//
//        try {
//            if (overwrite || !Files.exists(filePath) || Files.size(filePath) == 0) {
//                File tmp = new File(file.getParent(), file.getName() + TMP_SUFFIX);
//
//                if (tmp.exists() && !tmp.delete()) {
//                    log.warning("Failed to delete temporary cache config file" +
//                            "(make sure Ignite process has enough rights):" + file.getName());
//                }
//
//                writeCacheData(cacheData, tmp);
//
//                if (Files.exists(filePath) && Files.size(filePath) > 0) {
//                    for (BiConsumer<String, File> lsnr : lsnrs) {
//                        lsnr.accept(ccfg.getName(), file);
//                    }
//                }
//
//                Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
//            }
//        } catch (IOException ex) {
//            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, ex));
//
//            throw new IgniteInternalCheckedException("Failed to persist cache configuration: " + ccfg.getName(), ex);
//        } finally {
//            chgLock.readLock().unlock();
//        }
//    }
//
//    /**
//     * @param lsnr Instance of listener to add.
//     */
//    public void addConfigurationChangeListener(BiConsumer<String, File> lsnr) {
//        assert chgLock.isWriteLockedByCurrentThread();
//
//        lsnrs.add(lsnr);
//    }
//
//    /**
//     * @param lsnr Instance of listener to remove.
//     */
//    public void removeConfigurationChangeListener(BiConsumer<String, File> lsnr) {
//        lsnrs.remove(lsnr);
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void shutdownForCacheGroup(CacheGroupContext grp, boolean destroy) throws IgniteInternalCheckedException {
//        grpsWithoutIdx.remove(grp.groupId());
//
//        GroupPageStoreHolder old = groupPageStoreHolders.remove(grp.groupId());
//
//        if (old != null) {
//            IgniteInternalCheckedException ex = shutdown(old, /*clean files if destroy*/destroy, null);
//
//            if (destroy) {
//                removeCacheGroupConfigurationData(grp);
//            }
//
//            if (ex != null) {
//                throw ex;
//            }
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void truncate(int grpId, int partId, int tag) throws IgniteInternalCheckedException {
//        assert partId <= MAX_PARTITION_ID;
//
//        PageStore store = getStore(grpId, partId);
//
//        store.truncate(tag);
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void read(int grpId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
//        pmPageMgr.read(grpId, pageId, pageBuf, keepCrc);
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public boolean exists(int grpId, int partId) throws IgniteInternalCheckedException {
//        PageStore store = getStore(grpId, partId);
//
//        return store.exists();
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void readHeader(int grpId, int partId, ByteBuffer buf) throws IgniteInternalCheckedException {
//        PageStore store = getStore(grpId, partId);
//
//        try {
//            store.readHeader(buf);
//        } catch (StorageException e) {
//            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
//
//            throw e;
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public PageStore write(
//            int grpId,
//            long pageId,
//            ByteBuffer pageBuf,
//            int tag,
//            boolean calculateCrc
//    ) throws IgniteInternalCheckedException {
//        return pmPageMgr.write(grpId, pageId, pageBuf, tag, calculateCrc);
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public long pageOffset(int grpId, long pageId) throws IgniteInternalCheckedException {
//        PageStore store = getStore(grpId, PageIdUtils.partId(pageId));
//
//        return store.pageOffset(pageId);
//    }
//
//    /**
//     *
//     */
//    public Path getPath(boolean isSharedGroup, String cacheOrGroupName, int partId) {
//        return getPartitionFilePath(cacheWorkDir(isSharedGroup, cacheOrGroupName), partId);
//    }
//
//    /**
//     * @param grpDesc Cache group descriptor.
//     * @param ccfg Cache configuration.
//     * @return Cache store holder.
//     * @throws IgniteInternalCheckedException If failed.
//     */
//    private GroupPageStoreHolder initForCache(CacheGroupDescriptor grpDesc, CacheConfiguration ccfg) throws IgniteInternalCheckedException {
//        assert !grpDesc.sharedGroup() || ccfg.getGroupName() != null : ccfg.getName();
//
//        File cacheWorkDir = cacheWorkDir(ccfg);
//
//        String dataRegionName = grpDesc.config().getDataRegionName();
//        DataRegion dataRegion = cctx.database().dataRegion(dataRegionName);
//        PageMetrics pageMetrics = dataRegion.metrics().cacheGrpPageMetrics(grpDesc.groupId());
//
//        return initDir(
//                cacheWorkDir,
//                grpDesc.groupId(),
//                grpDesc.config().getAffinity().partitions(),
//                pageMetrics,
//                ccfg.isEncryptionEnabled()
//        );
//    }
//
//    /**
//     * @param grpId Cache group id.
//     * @param encrypted {@code true} if cache group encryption enabled.
//     * @return Factory to create page stores.
//     */
//    public FileVersionCheckingFactory getPageStoreFactory(int grpId, boolean encrypted) {
//        return getPageStoreFactory(grpId, encrypted ? cctx.kernalContext().encryption() : null);
//    }
//
//    /**
//     * @param grpId Cache group id.
//     * @param encrKeyProvider Encryption keys provider for encrypted IO. If {@code null}, no encryption is used.
//     * @return Factory to create page stores with certain encryption keys provider.
//     */
//    public FileVersionCheckingFactory getPageStoreFactory(int grpId, EncryptionCacheKeyProvider encrKeyProvider) {
//        FileIOFactory pageStoreFileIoFactory = this.pageStoreFileIoFactory;
//        FileIOFactory pageStoreV1FileIoFactory = this.pageStoreV1FileIoFactory;
//
//        if (encrKeyProvider != null) {
//            pageStoreFileIoFactory = encryptedFileIoFactory(this.pageStoreFileIoFactory, grpId, encrKeyProvider);
//            pageStoreV1FileIoFactory = encryptedFileIoFactory(this.pageStoreV1FileIoFactory, grpId, encrKeyProvider);
//        }
//
//        FileVersionCheckingFactory pageStoreFactory = new FileVersionCheckingFactory(
//                pageStoreFileIoFactory,
//                pageStoreV1FileIoFactory,
//                igniteCfg.getDataStorageConfiguration()::getPageSize
//        );
//
//        if (encrKeyProvider != null) {
//            int hdrSize = pageStoreFactory.headerSize(pageStoreFactory.latestVersion());
//
//            ((EncryptedFileIOFactory) pageStoreFileIoFactory).headerSize(hdrSize);
//            ((EncryptedFileIOFactory) pageStoreV1FileIoFactory).headerSize(hdrSize);
//        }
//
//        return pageStoreFactory;
//    }
//
//    /**
//     * @param plainFileIOFactory Not-encrypting file io factory.
//     * @param cacheGrpId Cache group id.
//     * @param encrKeyProvider Encryption keys provider for encrypted IO. If {@code null}, no encryption is used.
//     * @return Encrypted file IO factory.
//     */
//    public EncryptedFileIOFactory encryptedFileIoFactory(FileIOFactory plainFileIOFactory, int cacheGrpId,
//            EncryptionCacheKeyProvider encrKeyProvider) {
//        return new EncryptedFileIOFactory(
//                plainFileIOFactory,
//                cacheGrpId,
//                pageSize(),
//                encrKeyProvider,
//                cctx.gridConfig().getEncryptionSpi());
//    }
//
//    /**
//     * @return Encrypted file IO factory with stored internal encryption keys.
//     */
//    public EncryptedFileIOFactory encryptedFileIoFactory(FileIOFactory plainFileIOFactory, int cacheGrpId) {
//        return encryptedFileIoFactory(plainFileIOFactory, cacheGrpId, cctx.kernalContext().encryption());
//    }
//
//    /**
//     * @param cacheWorkDir Work directory.
//     * @param grpId Group ID.
//     * @param partitions Number of partitions.
//     * @param pageMetrics Page metrics.
//     * @param encrypted {@code True} if this cache encrypted.
//     * @return Cache store holder.
//     * @throws IgniteInternalCheckedException If failed.
//     */
//    private GroupPageStoreHolder initDir(File cacheWorkDir,
//            int grpId,
//            int partitions,
//            PageMetrics pageMetrics,
//            boolean encrypted) throws IgniteInternalCheckedException {
//        try {
//            boolean dirExisted = checkAndInitCacheWorkDir(cacheWorkDir);
//
//            if (dirExisted) {
//                MaintenanceRegistry mntcReg = cctx.kernalContext().maintenanceRegistry();
//
//                if (!mntcReg.isMaintenanceMode()) {
//                    DefragmentationFileUtils.beforeInitPageStores(cacheWorkDir, log);
//                }
//            }
//
//            File idxFile = new File(cacheWorkDir, INDEX_FILE_NAME);
//
//            if (dirExisted && !idxFile.exists()) {
//                grpsWithoutIdx.add(grpId);
//            }
//
//            FileVersionCheckingFactory pageStoreFactory = getPageStoreFactory(grpId, encrypted);
//
//            PageStore idxStore =
//                    pageStoreFactory.createPageStore(
//                            PageStore.TYPE_IDX,
//                            idxFile,
//                            pageMetrics.totalPages()::add);
//
//            PageStore[] partStores = new PageStore[partitions];
//
//            for (int partId = 0; partId < partStores.length; partId++) {
//                final int p = partId;
//
//                PageStore partStore =
//                        pageStoreFactory.createPageStore(
//                                PageStore.TYPE_DATA,
//                                () -> getPartitionFilePath(cacheWorkDir, p),
//                                pageMetrics.totalPages()::add);
//
//                partStores[partId] = partStore;
//            }
//
//            return new GroupPageStoreHolder(idxStore, partStores);
//        } catch (IgniteInternalCheckedException e) {
//            if (X.hasCause(e, StorageException.class, IOException.class)) {
//                cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
//            }
//
//            throw e;
//        }
//    }
//
//    /**
//     * @param cacheWorkDir Cache work directory.
//     * @param partId Partition id.
//     */
//    @NotNull
//    private Path getPartitionFilePath(File cacheWorkDir, int partId) {
//        return new File(cacheWorkDir, getPartitionFileName(partId)).toPath();
//    }
//
//    /**
//     * @param workDir Cache work directory.
//     * @param cacheDirName Cache directory name.
//     * @param partId Partition id.
//     * @return Partition file.
//     */
//    @NotNull
//    public static File getPartitionFile(File workDir, String cacheDirName, int partId) {
//        return new File(cacheWorkDir(workDir, cacheDirName), getPartitionFileName(partId));
//    }
//
//    /**
//     * @param partId Partition id.
//     * @return File name.
//     */
//    public static String getPartitionFileName(int partId) {
//        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;
//
//        return partId == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, partId);
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public boolean checkAndInitCacheWorkDir(CacheConfiguration cacheCfg) throws IgniteInternalCheckedException {
//        return checkAndInitCacheWorkDir(cacheWorkDir(cacheCfg));
//    }
//
//    /**
//     * @param cacheWorkDir Cache work directory.
//     */
//    private boolean checkAndInitCacheWorkDir(File cacheWorkDir) throws IgniteInternalCheckedException {
//        boolean dirExisted = false;
//
//        ReadWriteLock lock = initDirLock.getLock(cacheWorkDir.getName().hashCode());
//
//        lock.writeLock().lock();
//
//        try {
//            if (!Files.exists(cacheWorkDir.toPath())) {
//                try {
//                    Files.createDirectory(cacheWorkDir.toPath());
//                } catch (IOException e) {
//                    throw new IgniteInternalCheckedException("Failed to initialize cache working directory " +
//                            "(failed to create, make sure the work folder has correct permissions): " +
//                            cacheWorkDir.getAbsolutePath(), e);
//                }
//            } else {
//                if (cacheWorkDir.isFile()) {
//                    throw new IgniteInternalCheckedException("Failed to initialize cache working directory " +
//                            "(a file with the same name already exists): " + cacheWorkDir.getAbsolutePath());
//                }
//
//                File lockF = new File(cacheWorkDir, IgniteCacheSnapshotManager.SNAPSHOT_RESTORE_STARTED_LOCK_FILENAME);
//
//                Path cacheWorkDirPath = cacheWorkDir.toPath();
//
//                Path tmp = cacheWorkDirPath.getParent().resolve(cacheWorkDir.getName() + TMP_SUFFIX);
//
//                if (Files.exists(tmp) && Files.isDirectory(tmp) &&
//                        Files.exists(tmp.resolve(IgniteCacheSnapshotManager.TEMP_FILES_COMPLETENESS_MARKER))) {
//
//                    U.warn(log, "Ignite node crashed during the snapshot restore process " +
//                            "(there is a snapshot restore lock file left for cache). But old version of cache was saved. " +
//                            "Trying to restore it. Cache - [" + cacheWorkDir.getAbsolutePath() + ']');
//
//                    U.delete(cacheWorkDir);
//
//                    try {
//                        Files.move(tmp, cacheWorkDirPath, StandardCopyOption.ATOMIC_MOVE);
//
//                        cacheWorkDirPath.resolve(IgniteCacheSnapshotManager.TEMP_FILES_COMPLETENESS_MARKER).toFile().delete();
//                    } catch (IOException e) {
//                        throw new IgniteInternalCheckedException(e);
//                    }
//                } else if (lockF.exists()) {
//                    U.warn(log, "Ignite node crashed during the snapshot restore process " +
//                            "(there is a snapshot restore lock file left for cache). Will remove both the lock file and " +
//                            "incomplete cache directory [cacheDir=" + cacheWorkDir.getAbsolutePath() + ']');
//
//                    boolean deleted = U.delete(cacheWorkDir);
//
//                    if (!deleted) {
//                        throw new IgniteInternalCheckedException("Failed to remove obsolete cache working directory " +
//                                "(remove the directory manually and make sure the work folder has correct permissions): " +
//                                cacheWorkDir.getAbsolutePath());
//                    }
//
//                    cacheWorkDir.mkdirs();
//                } else {
//                    dirExisted = true;
//                }
//
//                if (!cacheWorkDir.exists()) {
//                    throw new IgniteInternalCheckedException("Failed to initialize cache working directory " +
//                            "(failed to create, make sure the work folder has correct permissions): " +
//                            cacheWorkDir.getAbsolutePath());
//                }
//
//                if (Files.exists(tmp)) {
//                    U.delete(tmp);
//                }
//            }
//        } finally {
//            lock.writeLock().unlock();
//        }
//
//        return dirExisted;
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void sync(int grpId, int partId) throws IgniteInternalCheckedException {
//        try {
//            getStore(grpId, partId).sync();
//        } catch (StorageException e) {
//            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
//
//            throw e;
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void ensure(int grpId, int partId) throws IgniteInternalCheckedException {
//        try {
//            getStore(grpId, partId).ensure();
//        } catch (StorageException e) {
//            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
//
//            throw e;
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public long allocatePage(int grpId, int partId, byte flags) throws IgniteInternalCheckedException {
//        return pmPageMgr.allocatePage(grpId, partId, flags);
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public int pages(int grpId, int partId) throws IgniteInternalCheckedException {
//        PageStore store = getStore(grpId, partId);
//
//        return store.pages();
//    }
//
//    /**
//     * @param ccfgs List of cache configurations to process.
//     * @param ccfgCons Consumer which accepts found configurations files.
//     */
//    public void readConfigurationFiles(List<CacheConfiguration<?, ?>> ccfgs,
//            BiConsumer<CacheConfiguration<?, ?>, File> ccfgCons) {
//        chgLock.writeLock().lock();
//
//        try {
//            for (CacheConfiguration<?, ?> ccfg : ccfgs) {
//                File cacheDir = cacheWorkDir(ccfg);
//
//                if (!cacheDir.exists()) {
//                    continue;
//                }
//
//                File[] ccfgFiles = cacheDir.listFiles((dir, name) ->
//                        name.endsWith(GROUP_DATA_FILENAME));
//
//                if (ccfgFiles == null) {
//                    continue;
//                }
//
//                for (File ccfgFile : ccfgFiles) {
//                    ccfgCons.accept(ccfg, ccfgFile);
//                }
//            }
//        } finally {
//            chgLock.writeLock().unlock();
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public Map<String, StoredCacheData> readCacheConfigurations() throws IgniteInternalCheckedException {
//        if (cctx.kernalContext().clientNode()) {
//            return Collections.emptyMap();
//        }
//
//        File[] files = storeWorkDir.listFiles();
//
//        if (files == null) {
//            return Collections.emptyMap();
//        }
//
//        Map<String, StoredCacheData> ccfgs = new HashMap<>();
//
//        Arrays.sort(files);
//
//        for (File file : files) {
//            if (file.isDirectory()) {
//                readCacheConfigurations(file, ccfgs);
//            }
//        }
//
//        return ccfgs;
//    }
//
//    /**
//     * @param dir Cache (group) directory.
//     * @param ccfgs Cache configurations.
//     * @throws IgniteInternalCheckedException If failed.
//     */
//    public void readCacheConfigurations(File dir, Map<String, StoredCacheData> ccfgs) throws IgniteInternalCheckedException {
//        if (dir.getName().startsWith(CACHE_DIR_PREFIX)) {
//            File conf = new File(dir, GROUP_DATA_FILENAME);
//
//            if (conf.exists() && conf.length() > 0) {
//                StoredCacheData cacheData = readCacheData(conf);
//
//                String cacheName = cacheData.config().getName();
//
//                if (!ccfgs.containsKey(cacheName)) {
//                    ccfgs.put(cacheName, cacheData);
//                } else {
//                    U.warn(log, "Cache with name=" + cacheName + " is already registered, skipping config file "
//                            + dir.getName());
//                }
//            }
//        } else if (dir.getName().startsWith(GRP_DIR_PREFIX)) {
//            readCacheGroupCaches(dir, ccfgs);
//        }
//    }
//
//    /**
//     * @param dir Directory to check.
//     * @param names Cache group names to filter.
//     * @return Files that match cache or cache group pattern.
//     */
//    public static List<File> cacheDirectories(File dir, Predicate<String> names) {
//        File[] files = dir.listFiles();
//
//        if (files == null) {
//            return Collections.emptyList();
//        }
//
//        return Arrays.stream(files)
//                .sorted()
//                .filter(File::isDirectory)
//                .filter(DATA_DIR_FILTER)
//                .filter(f -> names.test(cacheGroupName(f)))
//                .collect(Collectors.toList());
//    }
//
//    /**
//     * @param dir Directory to check.
//     * @param grpId Cache group id
//     * @return Files that match cache or cache group pattern.
//     */
//    public static File cacheDirectory(File dir, int grpId) {
//        File[] files = dir.listFiles();
//
//        if (files == null) {
//            return null;
//        }
//
//        return Arrays.stream(files)
//                .filter(File::isDirectory)
//                .filter(DATA_DIR_FILTER)
//                .filter(f -> CU.cacheId(cacheGroupName(f)) == grpId)
//                .findAny()
//                .orElse(null);
//    }
//
//    /**
//     * @param partFileName Partition file name.
//     * @return Partition id.
//     */
//    public static int partId(String partFileName) {
//        if (partFileName.equals(INDEX_FILE_NAME)) {
//            return INDEX_PARTITION;
//        }
//
//        if (partFileName.startsWith(PART_FILE_PREFIX)) {
//            return Integer.parseInt(partFileName.substring(PART_FILE_PREFIX.length(), partFileName.indexOf('.')));
//        }
//
//        throw new IllegalStateException("Illegal partition file name: " + partFileName);
//    }
//
//    /**
//     * @param cacheDir Cache directory to check.
//     * @return List of cache partitions in given directory.
//     */
//    public static List<File> cachePartitionFiles(File cacheDir) {
//        File[] files = cacheDir.listFiles();
//
//        if (files == null) {
//            return Collections.emptyList();
//        }
//
//        return Arrays.stream(files)
//                .filter(File::isFile)
//                .filter(f -> f.getName().endsWith(FILE_SUFFIX))
//                .collect(Collectors.toList());
//    }
//
//    /**
//     * @param dir Cache directory on disk.
//     * @return Cache or cache group name.
//     */
//    public static String cacheGroupName(File dir) {
//        String name = dir.getName();
//
//        if (name.startsWith(GRP_DIR_PREFIX)) {
//            return name.substring(GRP_DIR_PREFIX.length());
//        } else if (name.startsWith(CACHE_DIR_PREFIX)) {
//            return name.substring(CACHE_DIR_PREFIX.length());
//        } else if (name.equals(MetaStorage.METASTORAGE_DIR_NAME)) {
//            return MetaStorage.METASTORAGE_CACHE_NAME;
//        } else {
//            throw new IgniteException("Directory doesn't match the cache or cache group prefix: " + dir);
//        }
//    }
//
//    /**
//     * @param grpDir Group directory.
//     * @param ccfgs Cache configurations.
//     * @throws IgniteInternalCheckedException If failed.
//     */
//    private void readCacheGroupCaches(File grpDir, Map<String, StoredCacheData> ccfgs) throws IgniteInternalCheckedException {
//        File[] files = grpDir.listFiles();
//
//        if (files == null) {
//            return;
//        }
//
//        for (File file : files) {
//            if (!file.isDirectory() && file.getName().endsWith(GROUP_DATA_FILENAME) && file.length() > 0) {
//                StoredCacheData cacheData = readCacheData(file);
//
//                String cacheName = cacheData.config().getName();
//
//                if (!ccfgs.containsKey(cacheName)) {
//                    ccfgs.put(cacheName, cacheData);
//                } else {
//                    U.warn(log, "Cache with name=" + cacheName + " is already registered, skipping config file "
//                            + file.getName() + " in group directory " + grpDir.getName());
//                }
//            }
//        }
//    }
//
//    /**
//     * @param conf File with stored cache data.
//     * @return Cache data.
//     * @throws IgniteInternalCheckedException If failed.
//     */
//    public StoredCacheData readCacheData(File conf) throws IgniteInternalCheckedException {
//        try (InputStream stream = new BufferedInputStream(new FileInputStream(conf))) {
//            return marshaller.unmarshal(stream, U.resolveClassLoader(igniteCfg));
//        } catch (IgniteInternalCheckedException | IOException e) {
//            throw new IgniteInternalCheckedException("An error occurred during cache configuration loading from file [file=" +
//                    conf.getAbsolutePath() + "]", e);
//        }
//    }
//
//    /**
//     * @param conf File to store cache data.
//     * @param cacheData Cache data file.
//     * @throws IgniteInternalCheckedException If failed.
//     */
//    public void writeCacheData(StoredCacheData cacheData, File conf) throws IgniteInternalCheckedException {
//        // Pre-existing file will be truncated upon stream open.
//        try (OutputStream stream = new BufferedOutputStream(new FileOutputStream(conf))) {
//            marshaller.marshal(cacheData, stream);
//        } catch (IOException e) {
//            throw new IgniteInternalCheckedException("An error occurred during cache configuration writing to file [file=" +
//                    conf.getAbsolutePath() + "]", e);
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public boolean hasIndexStore(int grpId) {
//        return !grpsWithoutIdx.contains(grpId);
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public long pagesAllocated(int grpId) {
//        GroupPageStoreHolder holder = groupPageStoreHolders.get(grpId);
//
//        if (holder == null) {
//            return 0;
//        }
//
//        long pageCnt = holder.idxStore.pages();
//
//        for (int i = 0; i < holder.partStores.length; i++) {
//            pageCnt += holder.partStores[i].pages();
//        }
//
//        return pageCnt;
//    }
//
//    /**
//     * @return Store work dir. Includes consistent-id based folder
//     */
//    public File workDir() {
//        return storeWorkDir;
//    }
//
//    /**
//     * @param ccfg Cache configuration.
//     * @return Store dir for given cache.
//     */
//    public File cacheWorkDir(CacheConfiguration<?, ?> ccfg) {
//        return cacheWorkDir(storeWorkDir, cacheDirName(ccfg));
//    }
//
//    /**
//     * @param isSharedGroup {@code True} if cache is sharing the same `underlying` cache.
//     * @param cacheOrGroupName Cache name.
//     * @return Store directory for given cache.
//     */
//    public File cacheWorkDir(boolean isSharedGroup, String cacheOrGroupName) {
//        return cacheWorkDir(storeWorkDir, cacheDirName(isSharedGroup, cacheOrGroupName));
//    }
//
//    /**
//     * @param cacheDirName Cache directory name.
//     * @return Store directory for given cache.
//     */
//    public static File cacheWorkDir(File storeWorkDir, String cacheDirName) {
//        return new File(storeWorkDir, cacheDirName);
//    }
//
//    /**
//     * @param isSharedGroup {@code True} if cache is sharing the same `underlying` cache.
//     * @param cacheOrGroupName Cache name.
//     * @return The full cache directory name.
//     */
//    public static String cacheDirName(boolean isSharedGroup, String cacheOrGroupName) {
//        return isSharedGroup ? GRP_DIR_PREFIX + cacheOrGroupName
//                : CACHE_DIR_PREFIX + cacheOrGroupName;
//    }
//
//    /**
//     * @param ccfg Cache configuration.
//     * @return The full cache directory name.
//     */
//    public static String cacheDirName(CacheConfiguration<?, ?> ccfg) {
//        boolean isSharedGrp = ccfg.getGroupName() != null;
//
//        return cacheDirName(isSharedGrp, CU.cacheOrGroupName(ccfg));
//    }
//
//    /**
//     * @param grpId Group id.
//     * @return Name of cache group directory.
//     * @throws IgniteInternalCheckedException If cache group doesn't exist.
//     */
//    public String cacheDirName(int grpId) throws IgniteInternalCheckedException {
//        if (grpId == MetaStorage.METASTORAGE_CACHE_ID) {
//            return MetaStorage.METASTORAGE_DIR_NAME;
//        }
//
//        CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);
//
//        if (gctx == null) {
//            throw new IgniteInternalCheckedException("Cache group context has not found due to the cache group is stopped.");
//        }
//
//        return cacheDirName(gctx.config());
//    }
//
//    /**
//     * @param cleanFiles {@code True} if the stores should delete it's files upon close.
//     */
//    private IgniteInternalCheckedException shutdown(Collection<GroupPageStoreHolder> holders, boolean cleanFiles) {
//        IgniteInternalCheckedException ex = null;
//
//        for (GroupPageStoreHolder holder : holders) {
//            ex = shutdown(holder, cleanFiles, ex);
//        }
//
//        return ex;
//    }
//
//    /**
//     * @param holder Store holder.
//     * @param cleanFile {@code True} if files should be cleaned.
//     * @param aggr Aggregating exception.
//     * @return Aggregating exception, if error occurred.
//     */
//    private IgniteInternalCheckedException shutdown(GroupPageStoreHolder holder, boolean cleanFile,
//            @Nullable IgniteInternalCheckedException aggr) {
//        aggr = shutdown(holder.idxStore, cleanFile, aggr);
//
//        for (PageStore store : holder.partStores) {
//            if (store != null) {
//                aggr = shutdown(store, cleanFile, aggr);
//            }
//        }
//
//        return aggr;
//    }
//
//    /**
//     * Delete caches' configuration data files of cache group.
//     *
//     * @param ctx Cache group context.
//     * @throws IgniteInternalCheckedException If fails.
//     */
//    private void removeCacheGroupConfigurationData(CacheGroupContext ctx) throws IgniteInternalCheckedException {
//        File cacheGrpDir = cacheWorkDir(ctx.sharedGroup(), ctx.cacheOrGroupName());
//
//        if (cacheGrpDir != null && cacheGrpDir.exists()) {
//            DirectoryStream.Filter<Path> cacheCfgFileFilter = new DirectoryStream.Filter<Path>() {
//                @Override
//                public boolean accept(Path path) {
//                    return Files.isRegularFile(path) && path.getFileName().toString().endsWith(GROUP_DATA_FILENAME);
//                }
//            };
//
//            try (DirectoryStream<Path> dirStream = newDirectoryStream(cacheGrpDir.toPath(), cacheCfgFileFilter)) {
//                for (Path path : dirStream) {
//                    Files.deleteIfExists(path);
//                }
//            } catch (IOException e) {
//                throw new IgniteInternalCheckedException("Failed to delete cache configurations of group: " + ctx.toString(), e);
//            }
//        }
//    }
//
//    /** {@inheritDoc} */
//    @Override
//    public void removeCacheData(StoredCacheData cacheData) throws IgniteInternalCheckedException {
//        chgLock.readLock().lock();
//
//        try {
//            CacheConfiguration<?, ?> ccfg = cacheData.config();
//            File file = cacheConfigurationFile(ccfg);
//
//            if (file.exists()) {
//                for (BiConsumer<String, File> lsnr : lsnrs) {
//                    lsnr.accept(ccfg.getName(), file);
//                }
//
//                if (!file.delete()) {
//                    throw new IgniteInternalCheckedException("Failed to delete cache configuration: " + ccfg.getName());
//                }
//            }
//        } finally {
//            chgLock.readLock().unlock();
//        }
//    }
//
//    /**
//     * @param ccfg Cache configuration.
//     * @return Cache configuration file with respect to {@link CacheConfiguration#getGroupName} value.
//     */
//    private File cacheConfigurationFile(CacheConfiguration<?, ?> ccfg) {
//        File cacheWorkDir = cacheWorkDir(ccfg);
//
//        return ccfg.getGroupName() == null ? new File(cacheWorkDir, GROUP_DATA_FILENAME) :
//                new File(cacheWorkDir, ccfg.getName() + GROUP_DATA_FILENAME);
//    }
//
//    /**
//     * @param store Store to shutdown.
//     * @param cleanFile {@code True} if files should be cleaned.
//     * @param aggr Aggregating exception.
//     * @return Aggregating exception, if error occurred.
//     */
//    private IgniteInternalCheckedException shutdown(PageStore store, boolean cleanFile, IgniteInternalCheckedException aggr) {
//        try {
//            if (store != null) {
//                store.stop(cleanFile);
//            }
//        } catch (IgniteInternalCheckedException e) {
//            if (aggr == null) {
//                aggr = new IgniteInternalCheckedException("Failed to gracefully shutdown store");
//            }
//
//            aggr.addSuppressed(e);
//        }
//
//        return aggr;
//    }
//
//    /**
//     * Returns collection of related page stores.
//     *
//     * @param grpId Group ID.
//     * @throws IgniteInternalCheckedException If failed.
//     */
//    @Override
//    public Collection<PageStore> getStores(int grpId) throws IgniteInternalCheckedException {
//        return groupPageStoreHolder(grpId);
//    }
//
//    /**
//     * Returns page store for the corresponding parameters.
//     *
//     * @param grpId Group ID.
//     * @param partId Partition ID.
//     * @throws IgniteInternalCheckedException If group or partition with the given ID was not created.
//     */
//    public PageStore getStore(int grpId, int partId) throws IgniteInternalCheckedException {
//        GroupPageStoreHolder holder = groupPageStoreHolder(grpId);
//
//        if (holder == null) {
//            throw new IgniteInternalCheckedException(
//                    "Failed to get page store for the given group ID (group has not been started): " + grpId
//            );
//        }
//
//        if (partId == INDEX_PARTITION) {
//            return holder.idxStore;
//        }
//
//        if (partId > MAX_PARTITION_ID) {
//            throw new IgniteInternalCheckedException("Partition ID is reserved: " + partId);
//        }
//
//        PageStore store = holder.partStores[partId];
//
//        if (store == null) {
//            throw new IgniteInternalCheckedException(String.format(
//                    "Failed to get page store for the given partition ID (partition has not been created) [grpId=%s, partId=%s]",
//                    grpId,
//                    partId
//            ));
//        }
//
//        return store;
//    }
//
//    private GroupPageStoreHolder groupPageStoreHolder(int grpId) throws IgniteInternalCheckedException {
//        try {
//            return groupPageStoreHolders.computeIfAbsent(grpId, (key) -> {
//                CacheGroupDescriptor gDesc = cctx.cache().cacheGroupDescriptor(grpId);
//
//                GroupPageStoreHolder holder0 = null;
//
//                if (gDesc != null && CU.isPersistentCache(gDesc.config(), cctx.gridConfig().getDataStorageConfiguration())) {
//                    try {
//                        holder0 = initForCache(gDesc, gDesc.config());
//                    } catch (IgniteInternalCheckedException e) {
//                        throw new IgniteException(e);
//                    }
//                }
//
//                TablesConfiguration d = null;
//
//                return holder0;
//            });
//        } catch (IgniteInternalException ex) {
//            if (X.hasCause(ex, IgniteInternalCheckedException.class)) {
//                throw ex.getCause(IgniteInternalCheckedException.class);
//            } else {
//                throw ex;
//            }
//        }
//    }
}
