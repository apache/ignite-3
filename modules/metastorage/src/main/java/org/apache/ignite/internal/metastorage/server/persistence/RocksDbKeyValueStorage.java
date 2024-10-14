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

package org.apache.ignite.internal.metastorage.server.persistence;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.NOT_FOUND;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.assertCompactionRevisionLessThanCurrent;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.assertRequestedRevisionLessThanOrEqualToCurrent;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.indexToCompact;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.isLastIndex;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.maxRevisionIndex;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.minRevisionIndex;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.toUtf8String;
import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.appendLong;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.bytesToLong;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.bytesToValue;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.getAsLongs;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.keyToRocksKey;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.longToBytes;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.longsToBytes;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.revisionFromRocksKey;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.rocksKeyToBytes;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.timestampFromRocksValue;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.valueToBytes;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.DATA;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.INDEX;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.REVISION_TO_CHECKSUM;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.REVISION_TO_TS;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.TS_TO_REVISION;
import static org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler.IDEMPOTENT_COMMAND_PREFIX;
import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;
import static org.apache.ignite.internal.rocksdb.snapshot.ColumnFamilyRange.fullRange;
import static org.apache.ignite.internal.util.ArrayUtils.LONG_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.COMPACTION_ERR;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.OP_EXECUTION_ERR;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.RESTORING_STORAGE_ERR;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.STARTING_STORAGE_ERR;
import static org.rocksdb.util.SizeUnit.MB;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationType;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.Condition;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.MetastorageChecksum;
import org.apache.ignite.internal.metastorage.server.OnRevisionAppliedCallback;
import org.apache.ignite.internal.metastorage.server.Statement;
import org.apache.ignite.internal.metastorage.server.Value;
import org.apache.ignite.internal.metastorage.server.Watch;
import org.apache.ignite.internal.metastorage.server.WatchProcessor;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.rocksdb.snapshot.RocksSnapshotManager;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.rocksdb.AbstractNativeReference;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Key-value storage based on RocksDB. Keys are stored with revision. Values are stored in the default column family with an operation
 * timestamp and a boolean flag which represents whether this record is a tombstone.
 * <br>
 * Key: [8 bytes revision, N bytes key itself].
 * <br>
 * Value: [8 bytes operation timestamp, 1 byte tombstone flag, N bytes value].
 * <br>
 * The mapping from the key to the set of the storage's revisions is stored in the "index" column family. A key represents the key of an
 * entry and the value is a {@code byte[]} that represents a {@code long[]} where every item is a revision of the storage.
 */
public class RocksDbKeyValueStorage implements KeyValueStorage {
    private static final IgniteLogger LOG = Loggers.forClass(RocksDbKeyValueStorage.class);

    /** A revision to store with system entries. */
    private static final long SYSTEM_REVISION_MARKER_VALUE = 0;

    /** Revision key. */
    private static final byte[] REVISION_KEY = keyToRocksKey(
            SYSTEM_REVISION_MARKER_VALUE,
            "SYSTEM_REVISION_KEY".getBytes(UTF_8)
    );

    /** Compaction revision key. */
    private static final byte[] COMPACTION_REVISION_KEY = keyToRocksKey(
            SYSTEM_REVISION_MARKER_VALUE,
            "SYSTEM_COMPACTION_REVISION_KEY".getBytes(UTF_8)
    );

    /** Lexicographic order comparator. */
    private static final Comparator<byte[]> CMP = Arrays::compareUnsigned;

    /** Batch size (number of keys) for storage compaction. The value is arbitrary. */
    private static final int COMPACT_BATCH_SIZE = 10;

    static {
        RocksDB.loadLibrary();
    }

    /** RW lock. */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** Thread-pool for snapshot operations execution. */
    private final ExecutorService snapshotExecutor;

    /** Path to the rocksdb database. */
    private final Path dbPath;

    /** RockDB options. */
    private volatile DBOptions options;

    /** RocksDb instance. */
    private volatile RocksDB db;

    /** Data column family. */
    private volatile ColumnFamily data;

    /** Index column family. */
    private volatile ColumnFamily index;

    /** Timestamp to revision mapping column family. */
    private volatile ColumnFamily tsToRevision;

    /** Revision to timestamp mapping column family. */
    private volatile ColumnFamily revisionToTs;

    /** Revision to checksum mapping column family. */
    private volatile ColumnFamily revisionToChecksum;

    /** Snapshot manager. */
    private volatile RocksSnapshotManager snapshotManager;

    /**
     * Revision listener for recovery only. Notifies {@link MetaStorageManagerImpl} of revision update.
     * Guarded by {@link #rwLock}.
     */
    private @Nullable LongConsumer recoveryRevisionListener;

    /**
     * Revision. Will be incremented for each single-entry or multi-entry update operation.
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private long rev;

    /**
     * Facility to work with checksums.
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private MetastorageChecksum checksum;

    /**
     * Last compaction revision that was set or restored from a snapshot.
     *
     * <p>This field is used by metastorage read methods to determine whether {@link CompactedException} should be thrown.</p>
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private long compactionRevision = -1;

    /** Watch processor. */
    private final WatchProcessor watchProcessor;

    /** Status of the watch recovery process. */
    private enum RecoveryStatus {
        INITIAL,
        IN_PROGRESS,
        DONE
    }

    /**
     * Current status of the watch recovery process. Watch recovery is needed for replaying missed updated when {@link #startWatches}
     * is called.
     */
    private final AtomicReference<RecoveryStatus> recoveryStatus = new AtomicReference<>(RecoveryStatus.INITIAL);

    /**
     * Buffer used to cache new events while an event replay is in progress. After replay finishes, the cache gets drained and is never
     * used again.
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.
     */
    @Nullable
    private List<UpdatedEntries> eventCache;

    /**
     * Current list of updated entries.
     *
     * <p>Since this list gets read and updated only on writes (under a write lock), no extra synchronisation is needed.
     */
    private final UpdatedEntries updatedEntries = new UpdatedEntries();

    /** Tracks RocksDb resources that must be properly closed. */
    private List<AbstractNativeReference> rocksResources = new ArrayList<>();

    /** Metastorage recovery is based on the snapshot & external log. WAL is never used for recovery, and can be safely disabled. */
    private final WriteOptions defaultWriteOptions = new WriteOptions().setDisableWAL(true);

    private final AtomicBoolean stopCompaction = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param dbPath RocksDB path.
     */
    public RocksDbKeyValueStorage(String nodeName, Path dbPath, FailureManager failureManager) {
        this.dbPath = dbPath;

        this.watchProcessor = new WatchProcessor(nodeName, this::get, failureManager);

        this.snapshotExecutor = Executors.newFixedThreadPool(2, NamedThreadFactory.create(nodeName, "metastorage-snapshot-executor", LOG));
    }

    @Override
    public void start() {
        rwLock.writeLock().lock();

        try {
            // Delete existing data, relying on the raft's snapshot and log playback
            destroyRocksDb();

            createDb();
        } catch (IOException | RocksDBException e) {
            closeRocksResources();
            throw new MetaStorageException(STARTING_STORAGE_ERR, "Failed to start the storage", e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private List<ColumnFamilyDescriptor> cfDescriptors() {
        Options baseOptions = new Options()
                .setCreateIfMissing(true)
                // Lowering the desired number of levels will, on average, lead to less lookups in files, making reads faster.
                .setNumLevels(4)
                // Protect ourselves from slower flushes during the peak write load.
                .setMaxWriteBufferNumber(4)
                .setTableFormatConfig(new BlockBasedTableConfig()
                        // Speed-up key lookup in levels by adding a bloom filter and always caching it for level 0.
                        // This improves the access time to keys from lower levels. 12 is chosen to fit into a 4kb memory chunk.
                        // This proved to be big enough to positively affect the performance.
                        .setPinL0FilterAndIndexBlocksInCache(true)
                        .setFilterPolicy(new BloomFilter(12))
                        // Often helps to avoid reading data from the storage device, making reads faster.
                        .setBlockCache(new LRUCache(64 * MB))
                );

        ColumnFamilyOptions dataFamilyOptions = new ColumnFamilyOptions(baseOptions)
                // The prefix is the revision of an entry, so prefix length is the size of a long
                .useFixedLengthPrefixExtractor(Long.BYTES);
        this.rocksResources.add(dataFamilyOptions);

        ColumnFamilyOptions indexFamilyOptions = new ColumnFamilyOptions(baseOptions);
        this.rocksResources.add(indexFamilyOptions);

        ColumnFamilyOptions tsToRevFamilyOptions = new ColumnFamilyOptions(baseOptions);
        this.rocksResources.add(tsToRevFamilyOptions);

        ColumnFamilyOptions revToTsFamilyOptions = new ColumnFamilyOptions(baseOptions);
        this.rocksResources.add(revToTsFamilyOptions);

        ColumnFamilyOptions revToChecksumFamilyOptions = new ColumnFamilyOptions(baseOptions);
        this.rocksResources.add(revToChecksumFamilyOptions);

        return List.of(
                new ColumnFamilyDescriptor(DATA.nameAsBytes(), dataFamilyOptions),
                new ColumnFamilyDescriptor(INDEX.nameAsBytes(), indexFamilyOptions),
                new ColumnFamilyDescriptor(TS_TO_REVISION.nameAsBytes(), tsToRevFamilyOptions),
                new ColumnFamilyDescriptor(REVISION_TO_TS.nameAsBytes(), revToTsFamilyOptions),
                new ColumnFamilyDescriptor(REVISION_TO_CHECKSUM.nameAsBytes(), revToChecksumFamilyOptions)
        );
    }

    private DBOptions createDbOptions() {
        DBOptions options = new DBOptions()
                .setCreateMissingColumnFamilies(true)
                .setCreateIfMissing(true);

        rocksResources.add(options);

        return options;
    }

    private void createDb() throws RocksDBException {
        List<ColumnFamilyDescriptor> descriptors = cfDescriptors();

        assert descriptors.size() == 5 : descriptors.size();

        var handles = new ArrayList<ColumnFamilyHandle>(descriptors.size());

        options = createDbOptions();

        db = RocksDB.open(options, dbPath.toAbsolutePath().toString(), descriptors, handles);
        rocksResources.add(db);
        rocksResources.addAll(handles);

        data = ColumnFamily.wrap(db, handles.get(0));

        index = ColumnFamily.wrap(db, handles.get(1));

        tsToRevision = ColumnFamily.wrap(db, handles.get(2));

        revisionToTs = ColumnFamily.wrap(db, handles.get(3));

        revisionToChecksum = ColumnFamily.wrap(db, handles.get(4));

        snapshotManager = new RocksSnapshotManager(db,
                List.of(fullRange(data), fullRange(index), fullRange(tsToRevision), fullRange(revisionToTs), fullRange(revisionToChecksum)),
                snapshotExecutor
        );

        byte[] revision = data.get(REVISION_KEY);

        if (revision != null) {
            rev = ByteUtils.bytesToLong(revision);
        }

        checksum = new MetastorageChecksum(revision == null ? 0 : checksumByRevision(rev));

        byte[] compactionRevisionBytes = data.get(COMPACTION_REVISION_KEY);

        if (compactionRevisionBytes != null) {
            compactionRevision = ByteUtils.bytesToLong(compactionRevisionBytes);
        }
    }

    private long checksumByRevision(long revision) throws RocksDBException {
        byte[] bytes = revisionToChecksum.get(longToBytes(revision));

        if (bytes == null) {
            throw new CompactedException(revision, compactionRevision);
        }

        return bytesToLong(bytes);
    }

    /**
     * Notifies of revision update.
     * Must be called under the {@link #rwLock}.
     */
    private void notifyRevisionUpdate() {
        if (recoveryRevisionListener != null) {
            // Listener must be invoked only on recovery, after recovery listener must be null.
            recoveryRevisionListener.accept(rev);
        }
    }

    /**
     * Clear the RocksDB instance.
     *
     * @throws IOException If failed.
     */
    protected void destroyRocksDb() throws IOException {
        // For unknown reasons, RocksDB#destroyDB(String, Options) throws RocksDBException with ".../LOCK: No such file or directory".
        IgniteUtils.deleteIfExists(dbPath);

        Files.createDirectories(dbPath);
    }

    @Override
    public void close() throws Exception {
        stopCompaction();

        watchProcessor.close();

        IgniteUtils.shutdownAndAwaitTermination(snapshotExecutor, 10, TimeUnit.SECONDS);

        rwLock.writeLock().lock();
        try {
            IgniteUtils.closeAll(this::closeRocksResources, defaultWriteOptions);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void closeRocksResources() {
        Collections.reverse(rocksResources);
        RocksUtils.closeAll(rocksResources);
        this.rocksResources = new ArrayList<>();
    }

    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        return snapshotManager.createSnapshot(snapshotPath);
    }

    @Override
    public void restoreSnapshot(Path path) {
        rwLock.writeLock().lock();

        try {
            // there's no way to easily remove all data from RocksDB, so we need to re-create it from scratch
            closeRocksResources();

            destroyRocksDb();

            createDb();

            snapshotManager.restoreSnapshot(path);

            rev = bytesToLong(data.get(REVISION_KEY));

            byte[] compactionRevisionBytes = data.get(COMPACTION_REVISION_KEY);

            if (compactionRevisionBytes != null) {
                compactionRevision = bytesToLong(compactionRevisionBytes);
            }

            notifyRevisionUpdate();
        } catch (Exception e) {
            throw new MetaStorageException(RESTORING_STORAGE_ERR, "Failed to restore snapshot", e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public long revision() {
        rwLock.readLock().lock();

        try {
            return rev;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void put(byte[] key, byte[] value, HybridTimestamp opTs) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long newChecksum = checksum.wholePut(key, value);

            long curRev = rev + 1;

            addDataToBatch(batch, key, value, curRev, opTs);

            updateKeysIndex(batch, key, curRev);

            completeAndWriteBatch(batch, curRev, opTs, newChecksum);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Adds a revision to the keys index.
     *
     * @param batch  Write batch.
     * @param key    Key.
     * @param curRev New revision for key.
     */
    private void updateKeysIndex(WriteBatch batch, byte[] key, long curRev) {
        try {
            // Get the revisions current value
            byte @Nullable [] array = index.get(key);

            // Store the new value
            index.put(batch, key, appendLong(array, curRev));
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    /**
     * Fills the batch with system values and writes it to the db.
     *
     * @param batch Write batch.
     * @param newRev New revision.
     * @param ts Operation's timestamp.
     * @param newChecksum Checksum corresponding to the revision.
     * @throws RocksDBException If failed.
     */
    private void completeAndWriteBatch(WriteBatch batch, long newRev, HybridTimestamp ts, long newChecksum) throws RocksDBException {
        byte[] revisionBytes = longToBytes(newRev);

        data.put(batch, REVISION_KEY, revisionBytes);

        byte[] tsBytes = hybridTsToArray(ts);

        tsToRevision.put(batch, tsBytes, revisionBytes);
        revisionToTs.put(batch, revisionBytes, tsBytes);

        validateNoChecksumConflict(newRev, newChecksum);
        revisionToChecksum.put(batch, revisionBytes, longToBytes(newChecksum));

        db.write(defaultWriteOptions, batch);

        rev = newRev;
        checksum.commitRound(newChecksum);
        updatedEntries.ts = ts;

        queueWatchEvent();

        notifyRevisionUpdate();
    }

    private void validateNoChecksumConflict(long newRev, long newChecksum) throws RocksDBException {
        byte[] existingChecksumBytes = revisionToChecksum.get(longToBytes(newRev));

        if (existingChecksumBytes != null) {
            long existingChecksum = bytesToLong(existingChecksumBytes);
            if (existingChecksum != newChecksum) {
                throw new MetaStorageException(
                        INTERNAL_ERR,
                        String.format(
                                "Metastorage revision checksum differs from a checksum for the same revision saved earlier. "
                                        + "This probably means that the Metastorage has diverged. [revision=%d, existingChecksum=%d, "
                                        + "newChecksum=%d]",
                                newRev, existingChecksum, newChecksum
                        )
                );
            }
        }
    }

    private static byte[] hybridTsToArray(HybridTimestamp ts) {
        return longToBytes(ts.longValue());
    }

    private static Entry entry(byte[] key, long revision, Value value) {
        return value.tombstone()
                ? EntryImpl.tombstone(key, revision, value.operationTimestamp())
                : new EntryImpl(key, value.bytes(), revision, value.operationTimestamp());
    }

    @Override
    public void putAll(List<byte[]> keys, List<byte[]> values, HybridTimestamp opTs) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long newChecksum = checksum.wholePutAll(keys, values);

            long curRev = rev + 1;

            addAllToBatch(batch, keys, values, curRev, opTs);

            for (byte[] key : keys) {
                updateKeysIndex(batch, key, curRev);
            }

            completeAndWriteBatch(batch, curRev, opTs, newChecksum);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public Entry get(byte[] key) {
        rwLock.readLock().lock();

        try {
            return doGet(key, rev);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Entry get(byte[] key, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            return doGet(key, revUpperBound);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> get(byte[] key, long revLowerBound, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            return doGet(key, revLowerBound, revUpperBound);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys) {
        rwLock.readLock().lock();

        try {
            return doGetAll(keys, rev);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            return doGetAll(keys, revUpperBound);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void remove(byte[] key, HybridTimestamp opTs) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long newChecksum = checksum.wholeRemove(key);

            long curRev = rev + 1;

            if (addToBatchForRemoval(batch, key, curRev, opTs)) {
                updateKeysIndex(batch, key, curRev);
            }

            completeAndWriteBatch(batch, curRev, opTs, newChecksum);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void removeAll(List<byte[]> keys, HybridTimestamp opTs) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long newChecksum = checksum.wholeRemoveAll(keys);

            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                if (addToBatchForRemoval(batch, key, curRev, opTs)) {
                    existingKeys.add(key);
                }
            }

            for (byte[] key : existingKeys) {
                updateKeysIndex(batch, key, curRev);
            }

            completeAndWriteBatch(batch, curRev, opTs, newChecksum);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public boolean invoke(
            Condition condition,
            List<Operation> success,
            List<Operation> failure,
            HybridTimestamp opTs,
            CommandId commandId
    ) {
        rwLock.writeLock().lock();

        try {
            Entry[] entries = getAll(Arrays.asList(condition.keys())).toArray(new Entry[]{});

            boolean branch = condition.test(entries);
            ByteBuffer updateResult = ByteBuffer.wrap(branch ? INVOKE_RESULT_TRUE_BYTES : INVOKE_RESULT_FALSE_BYTES);

            List<Operation> ops = new ArrayList<>(branch ? success : failure);

            ops.add(Operations.put(
                    new ByteArray(IDEMPOTENT_COMMAND_PREFIX + commandId.toMgKeyAsString()),
                    updateResult
            ));

            applyOperations(ops, opTs, false, updateResult);

            return branch;
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public StatementResult invoke(If iif, HybridTimestamp opTs, CommandId commandId) {
        rwLock.writeLock().lock();

        try {
            If currIf = iif;

            byte maximumNumOfNestedBranch = 100;

            while (true) {
                if (maximumNumOfNestedBranch-- <= 0) {
                    throw new MetaStorageException(
                            OP_EXECUTION_ERR,
                            "Too many nested (" + maximumNumOfNestedBranch + ") statements in multi-invoke command.");
                }

                Entry[] entries = getAll(Arrays.asList(currIf.cond().keys())).toArray(new Entry[]{});

                Statement branch = (currIf.cond().test(entries)) ? currIf.andThen() : currIf.orElse();

                if (branch.isTerminal()) {
                    Update update = branch.update();
                    ByteBuffer updateResult = update.result().result();

                    List<Operation> ops = new ArrayList<>(update.operations());

                    ops.add(Operations.put(
                            new ByteArray(IDEMPOTENT_COMMAND_PREFIX + commandId.toMgKeyAsString()),
                            updateResult
                    ));

                    applyOperations(ops, opTs, true, updateResult);

                    return update.result();
                } else {
                    currIf = branch.iif();
                }
            }
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void applyOperations(List<Operation> ops, HybridTimestamp opTs, boolean multiInvoke, ByteBuffer updateResult)
            throws RocksDBException {
        long curRev = rev + 1;

        List<byte[]> updatedKeys = new ArrayList<>();

        int nonDummyOps = (int) ops.stream()
                .filter(op -> op.type() != OperationType.NO_OP)
                .count();
        checksum.prepareForInvoke(multiInvoke, nonDummyOps, toByteArray(updateResult));

        try (WriteBatch batch = new WriteBatch()) {
            for (Operation op : ops) {
                byte @Nullable [] key = op.key() == null ? null : toByteArray(op.key());

                switch (op.type()) {
                    case PUT:
                        byte[] value = toByteArray(op.value());
                        addDataToBatch(batch, key, value, curRev, opTs);

                        updatedKeys.add(key);

                        checksum.appendPutAsPart(key, value);

                        break;

                    case REMOVE:
                        if (addToBatchForRemoval(batch, key, curRev, opTs)) {
                            updatedKeys.add(key);
                        }

                        checksum.appendRemoveAsPart(key);

                        break;

                    case NO_OP:
                        break;

                    default:
                        throw new MetaStorageException(OP_EXECUTION_ERR, "Unknown operation type: " + op.type());
                }
            }

            for (byte[] key : updatedKeys) {
                updateKeysIndex(batch, key, curRev);
            }

            completeAndWriteBatch(batch, curRev, opTs, checksum.roundValue());
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo) {
        rwLock.readLock().lock();

        try {
            return doRange(keyFrom, keyTo, rev);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            return doRange(keyFrom, keyTo, revUpperBound);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void watchRange(byte[] keyFrom, byte @Nullable [] keyTo, long rev, WatchListener listener) {
        assert keyFrom != null : "keyFrom couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        Predicate<byte[]> rangePredicate = keyTo == null
                ? k -> CMP.compare(keyFrom, k) <= 0
                : k -> CMP.compare(keyFrom, k) <= 0 && CMP.compare(keyTo, k) > 0;

        watchProcessor.addWatch(new Watch(rev, listener, rangePredicate));
    }

    @Override
    public void watchExact(byte[] key, long rev, WatchListener listener) {
        assert key != null : "key couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        Predicate<byte[]> exactPredicate = k -> CMP.compare(k, key) == 0;

        watchProcessor.addWatch(new Watch(rev, listener, exactPredicate));
    }

    @Override
    public void watchExact(Collection<byte[]> keys, long rev, WatchListener listener) {
        assert keys != null && !keys.isEmpty() : "keys couldn't be null or empty: " + keys;
        assert rev > 0 : "rev must be positive.";

        TreeSet<byte[]> keySet = new TreeSet<>(CMP);

        keySet.addAll(keys);

        Predicate<byte[]> inPredicate = keySet::contains;

        watchProcessor.addWatch(new Watch(rev, listener, inPredicate));
    }

    @Override
    public void startWatches(long startRevision, OnRevisionAppliedCallback revisionCallback) {
        assert startRevision != 0 : "First meaningful revision is 1";

        long currentRevision;

        rwLock.readLock().lock();

        try {
            watchProcessor.setRevisionCallback(revisionCallback);

            currentRevision = rev;

            // We update the recovery status under the read lock in order to avoid races between starting watches and applying a snapshot
            // or concurrent writes. Replay of events can be done outside of the read lock relying on RocksDB snapshot isolation.
            if (currentRevision == 0) {
                recoveryStatus.set(RecoveryStatus.DONE);
            } else {
                // If revision is not 0, we need to replay updates that match the existing data.
                recoveryStatus.set(RecoveryStatus.IN_PROGRESS);
            }
        } finally {
            rwLock.readLock().unlock();
        }

        if (currentRevision != 0) {
            replayUpdates(startRevision, currentRevision);
        }
    }

    @Override
    public void removeWatch(WatchListener listener) {
        watchProcessor.removeWatch(listener);
    }

    @Override
    public void compact(long revision) {
        assert revision >= 0 : revision;

        try {
            compactKeys(revision);

            compactAuxiliaryMappings(revision);
        } catch (Throwable t) {
            throw new MetaStorageException(COMPACTION_ERR, "Error during compaction: " + revision, t);
        }
    }

    @Override
    public void stopCompaction() {
        stopCompaction.set(true);
    }

    @Override
    public byte @Nullable [] nextKey(byte[] key) {
        return incrementPrefix(key);
    }

    /**
     * Adds a key to a batch marking the value as a tombstone.
     *
     * @param batch Write batch.
     * @param key Target key.
     * @param curRev Revision.
     * @param opTs Operation timestamp.
     * @return {@code true} if an entry can be deleted.
     * @throws RocksDBException If failed.
     */
    private boolean addToBatchForRemoval(
            WriteBatch batch,
            byte[] key,
            long curRev,
            HybridTimestamp opTs
    ) throws RocksDBException {
        Entry e = doGet(key, curRev);

        if (e.empty() || e.tombstone()) {
            return false;
        }

        addDataToBatch(batch, key, TOMBSTONE, curRev, opTs);

        return true;
    }

    /**
     * Compacts the key, see the documentation of {@link KeyValueStorage#compact} for examples.
     *
     * @param batch Write batch.
     * @param key Target key.
     * @param revs Key revisions.
     * @param compactionRevision Revision up to which (inclusively) the key will be compacted.
     * @throws MetaStorageException If failed.
     */
    private void compactForKey(WriteBatch batch, byte[] key, long[] revs, long compactionRevision) {
        try {
            int indexToCompact = indexToCompact(revs, compactionRevision, revision -> isTombstoneForCompaction(key, revision));

            if (NOT_FOUND == indexToCompact) {
                return;
            }

            for (int revisionIndex = 0; revisionIndex <= indexToCompact; revisionIndex++) {
                // This revision is not needed anymore, remove data.
                data.delete(batch, keyToRocksKey(revs[revisionIndex], key));
            }

            if (indexToCompact == revs.length - 1) {
                index.delete(batch, key);
            } else {
                index.put(batch, key, longsToBytes(revs, indexToCompact + 1));
            }
        } catch (Throwable t) {
            throw new MetaStorageException(
                    COMPACTION_ERR,
                    String.format(
                            "Error during compaction of key: [KeyBytes=%s, keyBytesToUtf8String=%s]",
                            Arrays.toString(key), toUtf8String(key)
                    ),
                    t
            );
        }
    }

    private List<Entry> doGetAll(Collection<byte[]> keys, long revUpperBound) {
        assert !keys.isEmpty();
        assert revUpperBound >= 0 : revUpperBound;

        var res = new ArrayList<Entry>(keys.size());

        for (byte[] key : keys) {
            res.add(doGet(key, revUpperBound));
        }

        return res;
    }

    private Entry doGet(byte[] key, long revUpperBound) {
        assert revUpperBound >= 0 : revUpperBound;

        long[] keyRevisions = getRevisionsForOperation(key);
        int maxRevisionIndex = maxRevisionIndex(keyRevisions, revUpperBound);

        if (maxRevisionIndex == NOT_FOUND) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revUpperBound, compactionRevision);

            return EntryImpl.empty(key);
        }

        long revision = keyRevisions[maxRevisionIndex];

        Value value = getValueForOperation(key, revision);

        if (revUpperBound <= compactionRevision && (!isLastIndex(keyRevisions, maxRevisionIndex) || value.tombstone())) {
            throw new CompactedException(revUpperBound, compactionRevision);
        }

        return EntryImpl.toEntry(key, revision, value);
    }

    private List<Entry> doGet(byte[] key, long revLowerBound, long revUpperBound) {
        assert revLowerBound >= 0 : revLowerBound;
        assert revUpperBound >= 0 : revUpperBound;
        assert revUpperBound >= revLowerBound : "revLowerBound=" + revLowerBound + ", revUpperBound=" + revUpperBound;

        long[] keyRevisions = getRevisionsForOperation(key);

        int minRevisionIndex = minRevisionIndex(keyRevisions, revLowerBound);
        int maxRevisionIndex = maxRevisionIndex(keyRevisions, revUpperBound);

        if (minRevisionIndex == NOT_FOUND || maxRevisionIndex == NOT_FOUND) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revLowerBound, compactionRevision);

            return List.of();
        }

        var entries = new ArrayList<Entry>();

        for (int i = minRevisionIndex; i <= maxRevisionIndex; i++) {
            long revision = keyRevisions[i];

            Value value;

            // More complex check to read less from disk.
            if (revision <= compactionRevision) {
                if (!isLastIndex(keyRevisions, i)) {
                    continue;
                }

                value = getValueForOperation(key, revision);

                if (value.tombstone()) {
                    continue;
                }
            } else {
                value = getValueForOperation(key, revision);
            }

            entries.add(EntryImpl.toEntry(key, revision, value));
        }

        if (entries.isEmpty()) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revLowerBound, compactionRevision);
        }

        return entries;
    }

    /**
     * Returns array of revisions of the entry corresponding to the key.
     *
     * @param key Key.
     * @throws RocksDBException If failed to perform {@link RocksDB#get(ColumnFamilyHandle, byte[])}.
     */
    private long[] getRevisions(byte[] key) throws RocksDBException {
        byte[] revisions = index.get(key);

        if (revisions == null) {
            return LONG_EMPTY_ARRAY;
        }

        return getAsLongs(revisions);
    }

    /**
     * Returns array of revisions of the entry corresponding to the key.
     *
     * @param key Key.
     * @throws MetaStorageException If there was an error while getting the revisions for the key.
     */
    private long[] getRevisionsForOperation(byte[] key) {
        try {
            return getRevisions(key);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, "Failed to get revisions for the key: " + toUtf8String(key), e);
        }
    }

    /**
     * Adds an entry to the batch.
     *
     * @param batch Write batch.
     * @param key Key.
     * @param value Value.
     * @param curRev Revision.
     * @param opTs Operation timestamp.
     * @throws RocksDBException If failed.
     */
    private void addDataToBatch(
            WriteBatch batch,
            byte[] key,
            byte[] value,
            long curRev,
            HybridTimestamp opTs
    ) throws RocksDBException {
        byte[] rocksKey = keyToRocksKey(curRev, key);

        byte[] rocksValue = valueToBytes(value, opTs);

        data.put(batch, rocksKey, rocksValue);

        updatedEntries.add(entry(key, curRev, new Value(value, opTs)));
    }

    /**
     * Adds all entries to the batch.
     *
     * @param batch Write batch.
     * @param keys Keys.
     * @param values Values.
     * @param curRev Revision.
     * @param opTs Operation timestamp.
     * @throws RocksDBException If failed.
     */
    private void addAllToBatch(
            WriteBatch batch,
            List<byte[]> keys,
            List<byte[]> values,
            long curRev,
            HybridTimestamp opTs
    ) throws RocksDBException {
        for (int i = 0; i < keys.size(); i++) {
            byte[] key = keys.get(i);
            byte[] bytes = values.get(i);

            addDataToBatch(batch, key, bytes, curRev, opTs);
        }
    }

    /**
     * Adds modified entries to the watch event queue.
     */
    private void queueWatchEvent() {
        if (updatedEntries.isEmpty()) {
            return;
        }

        switch (recoveryStatus.get()) {
            case INITIAL:
                // Watches haven't been enabled yet, no need to queue any events, they will be replayed upon recovery.
                updatedEntries.clear();

                break;

            case IN_PROGRESS:
                // Buffer the event while event replay is still in progress.
                if (eventCache == null) {
                    eventCache = new ArrayList<>();
                }

                eventCache.add(updatedEntries.transfer());

                break;

            default:
                notifyWatches();

                break;
        }
    }

    private void notifyWatches() {
        UpdatedEntries copy = updatedEntries.transfer();

        assert copy.ts != null;
        watchProcessor.notifyWatches(copy.updatedEntries, copy.ts);
    }

    private void replayUpdates(long lowerRevision, long upperRevision) {
        long minWatchRevision = Math.max(lowerRevision, watchProcessor.minWatchRevision().orElse(-1));

        if (minWatchRevision == -1 || minWatchRevision > upperRevision) {
            // No events to replay, we can start processing more recent events from the event queue.
            finishReplay();

            return;
        }

        var updatedEntries = new ArrayList<Entry>();
        HybridTimestamp ts = null;

        try (
                var upperBound = new Slice(longToBytes(upperRevision + 1));
                var options = new ReadOptions().setIterateUpperBound(upperBound);
                RocksIterator it = data.newIterator(options)
        ) {
            it.seek(longToBytes(minWatchRevision));

            long lastSeenRevision = minWatchRevision;

            for (; it.isValid(); it.next()) {
                byte[] rocksKey = it.key();
                byte[] rocksValue = it.value();

                long revision = revisionFromRocksKey(rocksKey);

                if (revision != lastSeenRevision) {
                    if (!updatedEntries.isEmpty()) {
                        List<Entry> updatedEntriesCopy = List.copyOf(updatedEntries);

                        assert ts != null : revision;

                        watchProcessor.notifyWatches(updatedEntriesCopy, ts);

                        updatedEntries.clear();

                        ts = hybridTimestamp(timestampFromRocksValue(rocksValue));
                    }

                    lastSeenRevision = revision;
                }

                if (ts == null) {
                    // This will only execute on first iteration.
                    ts = hybridTimestamp(timestampFromRocksValue(rocksValue));
                }

                updatedEntries.add(entry(rocksKeyToBytes(rocksKey), revision, bytesToValue(rocksValue)));
            }

            try {
                it.status();
            } catch (RocksDBException e) {
                throw new MetaStorageException(OP_EXECUTION_ERR, e);
            }

            // Notify about the events left after finishing the loop above.
            if (!updatedEntries.isEmpty()) {
                assert ts != null;

                watchProcessor.notifyWatches(updatedEntries, ts);
            }
        }

        finishReplay();
    }

    @Override
    public HybridTimestamp timestampByRevision(long revision) {
        rwLock.readLock().lock();

        try {
            assertRequestedRevisionLessThanOrEqualToCurrent(revision, rev);

            byte[] tsBytes = revisionToTs.get(longToBytes(revision));

            if (tsBytes == null) {
                throw new CompactedException("Requested revision has already been compacted: " + revision);
            }

            return hybridTimestamp(bytesToLong(tsBytes));
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, "Error reading revision timestamp: " + revision, e);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public long revisionByTimestamp(HybridTimestamp timestamp) {
        rwLock.readLock().lock();

        // Find a revision with timestamp lesser or equal to the timestamp.
        try (RocksIterator rocksIterator = tsToRevision.newIterator()) {
            rocksIterator.seekForPrev(hybridTsToArray(timestamp));

            rocksIterator.status();

            byte[] tsValue = rocksIterator.value();

            if (tsValue.length == 0) {
                throw new CompactedException("Revisions less than or equal to the requested one are already compacted: " + timestamp);
            }

            return bytesToLong(tsValue);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void finishReplay() {
        // Take the lock to drain the event cache and prevent new events from being cached. Since event notification is asynchronous,
        // this lock shouldn't be held for long.
        rwLock.writeLock().lock();

        try {
            if (eventCache != null) {
                eventCache.forEach(entries -> {
                    assert entries.ts != null;

                    watchProcessor.notifyWatches(entries.updatedEntries, entries.ts);
                });

                eventCache = null;
            }

            recoveryStatus.set(RecoveryStatus.DONE);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void setRecoveryRevisionListener(@Nullable LongConsumer listener) {
        rwLock.writeLock().lock();

        try {
            this.recoveryRevisionListener = listener;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @TestOnly
    public Path getDbPath() {
        return dbPath;
    }

    private static class UpdatedEntries {
        private final List<Entry> updatedEntries;

        @Nullable
        private HybridTimestamp ts;

        private UpdatedEntries() {
            this.updatedEntries = new ArrayList<>();
        }

        private UpdatedEntries(List<Entry> updatedEntries, HybridTimestamp ts) {
            this.updatedEntries = updatedEntries;
            this.ts = Objects.requireNonNull(ts);
        }

        boolean isEmpty() {
            return updatedEntries.isEmpty();
        }

        void add(Entry entry) {
            updatedEntries.add(entry);
        }

        void clear() {
            updatedEntries.clear();

            ts = null;
        }

        UpdatedEntries transfer() {
            assert ts != null;

            UpdatedEntries transferredValue = new UpdatedEntries(new ArrayList<>(updatedEntries), ts);

            clear();

            return transferredValue;
        }
    }

    @Override
    public void registerRevisionUpdateListener(RevisionUpdateListener listener) {
        watchProcessor.registerRevisionUpdateListener(listener);
    }

    @Override
    public void unregisterRevisionUpdateListener(RevisionUpdateListener listener) {
        watchProcessor.unregisterRevisionUpdateListener(listener);
    }

    @Override
    public CompletableFuture<Void> notifyRevisionUpdateListenerOnStart(long newRevision) {
        return watchProcessor.notifyUpdateRevisionListeners(newRevision);
    }

    @Override
    public void advanceSafeTime(HybridTimestamp newSafeTime) {
        rwLock.writeLock().lock();

        try {
            if (recoveryStatus.get() == RecoveryStatus.DONE) {
                watchProcessor.advanceSafeTime(newSafeTime);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void saveCompactionRevision(long revision) {
        assert revision >= 0 : revision;

        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            assertCompactionRevisionLessThanCurrent(revision, rev);

            data.put(batch, COMPACTION_REVISION_KEY, longToBytes(revision));

            db.write(defaultWriteOptions, batch);
        } catch (Throwable t) {
            throw new MetaStorageException(COMPACTION_ERR, "Error saving compaction revision: " + revision, t);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void setCompactionRevision(long revision) {
        assert revision >= 0 : revision;

        rwLock.writeLock().lock();

        try {
            assertCompactionRevisionLessThanCurrent(revision, rev);

            compactionRevision = revision;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public long getCompactionRevision() {
        rwLock.readLock().lock();

        try {
            return compactionRevision;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public long checksum(long revision) {
        rwLock.readLock().lock();

        try {
            assertRequestedRevisionLessThanOrEqualToCurrent(revision, rev);

            return checksumByRevision(revision);
        } catch (RocksDBException e) {
            throw new MetaStorageException(INTERNAL_ERR, "Cannot get checksum by revision: " + revision, e);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void compactKeys(long compactionRevision) throws RocksDBException {
        compactInBatches(index, (it, batch) -> {
            compactForKey(batch, it.key(), getAsLongs(it.value()), compactionRevision);

            return true;
        });
    }

    private void compactAuxiliaryMappings(long compactionRevision) throws RocksDBException {
        compactInBatches(revisionToTs, (it, batch) -> {
            long revision = bytesToLong(it.key());

            if (revision > compactionRevision) {
                return false;
            }

            revisionToTs.delete(batch, it.key());
            tsToRevision.delete(batch, it.value());

            revisionToChecksum.delete(batch, it.key());

            return true;
        });
    }

    @FunctionalInterface
    private interface CompactionAction {
        /**
         * Performs compaction on the storage at the current iterator pointer. Returns {@code true} if it is necessary to continue
         * iterating, {@link false} if it is necessary to finish with writing the last batch.
         */
        boolean compact(RocksIterator it, WriteBatch batch) throws RocksDBException;
    }

    private void compactInBatches(ColumnFamily columnFamily, CompactionAction compactionAction) throws RocksDBException {
        try (RocksIterator iterator = columnFamily.newIterator()) {
            iterator.seekToFirst();

            boolean continueIterating = true;

            while (continueIterating && iterator.isValid()) {
                rwLock.writeLock().lock();

                try (WriteBatch batch = new WriteBatch()) {
                    assertCompactionRevisionLessThanCurrent(compactionRevision, rev);

                    for (int i = 0; i < COMPACT_BATCH_SIZE && iterator.isValid(); i++, iterator.next()) {
                        if (stopCompaction.get()) {
                            return;
                        }

                        if (!compactionAction.compact(iterator, batch)) {
                            continueIterating = false;

                            break;
                        }
                    }

                    db.write(defaultWriteOptions, batch);
                } finally {
                    rwLock.writeLock().unlock();
                }
            }

            iterator.status();
        }
    }

    private boolean isTombstone(byte[] key, long revision) throws RocksDBException {
        byte[] rocksKey = keyToRocksKey(revision, key);

        byte[] valueBytes = data.get(rocksKey);

        assert valueBytes != null : "key=" + toUtf8String(key) + ", revision=" + revision;

        return bytesToValue(valueBytes).tombstone();
    }

    private boolean isTombstoneForCompaction(byte[] key, long revision) {
        try {
            return isTombstone(key, revision);
        } catch (RocksDBException e) {
            throw new MetaStorageException(
                    COMPACTION_ERR,
                    String.format(
                            "Error getting key value by revision: [KeyBytes=%s, keyBytesToUtf8String=%s, revision=%s]",
                            Arrays.toString(key), toUtf8String(key), revision
                    ),
                    e
            );
        }
    }

    private Value getValueForOperation(byte[] key, long revision) {
        Value value = getValueForOperationNullable(key, revision);

        assert value != null : "key=" + toUtf8String(key) + ", revision=" + revision;

        return value;
    }

    private @Nullable Value getValueForOperationNullable(byte[] key, long revision) {
        try {
            byte[] valueBytes = data.get(keyToRocksKey(revision, key));

            assert valueBytes != null && valueBytes.length != 0 : "key=" + toUtf8String(key) + ", revision=" + revision;

            return ArrayUtils.nullOrEmpty(valueBytes) ? null : bytesToValue(valueBytes);
        } catch (RocksDBException e) {
            throw new MetaStorageException(
                    OP_EXECUTION_ERR,
                    String.format("Failed to get value: [key=%s, revision=%s]", toUtf8String(key), revision),
                    e
            );
        }
    }

    private Cursor<Entry> doRange(byte[] keyFrom, byte @Nullable [] keyTo, long revUpperBound) {
        assert revUpperBound >= 0 : revUpperBound;

        CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revUpperBound, compactionRevision);

        var readOpts = new ReadOptions();

        Slice upperBound = keyTo == null ? null : new Slice(keyTo);

        readOpts.setIterateUpperBound(upperBound);

        RocksIterator iterator = index.newIterator(readOpts);

        iterator.seek(keyFrom);

        long compactionRevisionBeforeCreateCursor = compactionRevision;

        return new RocksIteratorAdapter<>(iterator) {
            /** Cached entry used to filter "empty" values. */
            private @Nullable Entry next;

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                }

                while (next == null && super.hasNext()) {
                    Entry nextCandidate = decodeEntry(it.key(), it.value());

                    it.next();

                    if (!nextCandidate.empty()) {
                        next = nextCandidate;

                        return true;
                    }
                }

                return false;
            }

            @Override
            public Entry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                Entry result = next;

                assert result != null;

                next = null;

                return result;
            }

            @Override
            protected Entry decodeEntry(byte[] key, byte[] keyRevisionsBytes) {
                long[] keyRevisions = getAsLongs(keyRevisionsBytes);

                int maxRevisionIndex = maxRevisionIndex(keyRevisions, revUpperBound);

                if (maxRevisionIndex == NOT_FOUND) {
                    return EntryImpl.empty(key);
                }

                long revision = keyRevisions[maxRevisionIndex];
                Value value = getValueForOperationNullable(key, revision);

                // Value may be null if the compaction has removed it in parallel.
                if (value == null || (revision <= compactionRevisionBeforeCreateCursor && value.tombstone())) {
                    return EntryImpl.empty(key);
                }

                return EntryImpl.toEntry(key, revision, value);
            }

            @Override
            public void close() {
                super.close();

                RocksUtils.closeAll(readOpts, upperBound);
            }
        };
    }
}
