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
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.valueToBytes;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.DATA;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.INDEX;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.REVISION_TO_TS;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.TS_TO_REVISION;
import static org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler.IDEMPOTENT_COMMAND_PREFIX;
import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;
import static org.apache.ignite.internal.rocksdb.snapshot.ColumnFamilyRange.fullRange;
import static org.apache.ignite.internal.util.ArrayUtils.LONG_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.COMPACTION_ERR;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.OP_EXECUTION_ERR;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.RESTORING_STORAGE_ERR;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.STARTING_STORAGE_ERR;
import static org.rocksdb.util.SizeUnit.MB;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.Condition;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
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
 * Key-value storage based on RocksDB. Keys are stored with revision. Values are stored in the default column family with an update counter
 * and a boolean flag which represents whether this record is a tombstone.
 * <br>
 * Key: [8 bytes revision, N bytes key itself].
 * <br>
 * Value: [8 bytes update counter, 1 byte tombstone flag, N bytes value].
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
            "SYSTEM_REVISION_KEY".getBytes(StandardCharsets.UTF_8)
    );

    /** Update counter key. */
    private static final byte[] UPDATE_COUNTER_KEY = keyToRocksKey(
            SYSTEM_REVISION_MARKER_VALUE,
            "SYSTEM_UPDATE_COUNTER_KEY".getBytes(StandardCharsets.UTF_8)
    );

    /** Lexicographic order comparator. */
    private static final Comparator<byte[]> CMP = Arrays::compareUnsigned;

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
     * <p>Multi-threaded access is guarded by {@link #rwLock}.
     */
    private long rev;

    /**
     * Update counter. Will be incremented for each update of any particular entry.
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.
     */
    private long updCntr;

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

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param dbPath RocksDB path.
     */
    public RocksDbKeyValueStorage(String nodeName, Path dbPath, FailureProcessor failureProcessor) {
        this.dbPath = dbPath;

        this.watchProcessor = new WatchProcessor(nodeName, this::get, failureProcessor);

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

        return List.of(
                new ColumnFamilyDescriptor(DATA.nameAsBytes(), dataFamilyOptions),
                new ColumnFamilyDescriptor(INDEX.nameAsBytes(), indexFamilyOptions),
                new ColumnFamilyDescriptor(TS_TO_REVISION.nameAsBytes(), tsToRevFamilyOptions),
                new ColumnFamilyDescriptor(REVISION_TO_TS.nameAsBytes(), revToTsFamilyOptions)
        );
    }

    protected DBOptions createDbOptions() {
        DBOptions options = new DBOptions()
                .setCreateMissingColumnFamilies(true)
                .setCreateIfMissing(true);

        rocksResources.add(options);

        return options;
    }

    protected void createDb() throws RocksDBException {
        List<ColumnFamilyDescriptor> descriptors = cfDescriptors();

        assert descriptors.size() == 4;

        var handles = new ArrayList<ColumnFamilyHandle>(descriptors.size());

        options = createDbOptions();

        db = RocksDB.open(options, dbPath.toAbsolutePath().toString(), descriptors, handles);
        rocksResources.add(db);
        rocksResources.addAll(handles);

        data = ColumnFamily.wrap(db, handles.get(0));

        index = ColumnFamily.wrap(db, handles.get(1));

        tsToRevision = ColumnFamily.wrap(db, handles.get(2));

        revisionToTs = ColumnFamily.wrap(db, handles.get(3));

        snapshotManager = new RocksSnapshotManager(db,
                List.of(fullRange(data), fullRange(index), fullRange(tsToRevision), fullRange(revisionToTs)),
                snapshotExecutor
        );

        byte[] revision = data.get(REVISION_KEY);

        if (revision != null) {
            rev = ByteUtils.bytesToLong(revision);
        }
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
    public void close() {
        watchProcessor.close();

        IgniteUtils.shutdownAndAwaitTermination(snapshotExecutor, 10, TimeUnit.SECONDS);

        rwLock.writeLock().lock();
        try {
            closeRocksResources();
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
        long currentRevision;

        rwLock.writeLock().lock();

        try {
            // there's no way to easily remove all data from RocksDB, so we need to re-create it from scratch
            closeRocksResources();

            destroyRocksDb();

            createDb();

            snapshotManager.restoreSnapshot(path);

            currentRevision = bytesToLong(data.get(REVISION_KEY));

            rev = currentRevision;

            updCntr = bytesToLong(data.get(UPDATE_COUNTER_KEY));

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
    public long updateCounter() {
        rwLock.readLock().lock();

        try {
            return updCntr;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void put(byte[] key, byte[] value, HybridTimestamp opTs) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            long cntr = updCntr + 1;

            addDataToBatch(batch, key, value, curRev, cntr);

            updateKeysIndex(batch, key, curRev);

            fillAndWriteBatch(batch, curRev, cntr, opTs);
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
     * Fills the batch with system values (the update counter and the revision) and writes it to the db.
     *
     * @param batch   Write batch.
     * @param newRev  New revision.
     * @param newCntr New update counter.
     * @param ts      Operation's timestamp.
     * @throws RocksDBException If failed.
     */
    private void fillAndWriteBatch(WriteBatch batch, long newRev, long newCntr, @Nullable HybridTimestamp ts) throws RocksDBException {
        // Meta-storage recovery is based on the snapshot & external log. WAL is never used for recovery, and can be safely disabled.
        try (WriteOptions opts = new WriteOptions().setDisableWAL(true)) {
            byte[] revisionBytes = longToBytes(newRev);

            data.put(batch, UPDATE_COUNTER_KEY, longToBytes(newCntr));
            data.put(batch, REVISION_KEY, revisionBytes);

            if (ts != null) {
                byte[] tsBytes = hybridTsToArray(ts);

                tsToRevision.put(batch, tsBytes, revisionBytes);
                revisionToTs.put(batch, revisionBytes, tsBytes);
            }

            db.write(opts, batch);

            rev = newRev;
            updCntr = newCntr;
        }

        updatedEntries.ts = ts;

        queueWatchEvent();

        notifyRevisionUpdate();
    }

    private static byte[] hybridTsToArray(HybridTimestamp ts) {
        return longToBytes(ts.longValue());
    }

    private static Entry entry(byte[] key, long revision, Value value) {
        return value.tombstone()
                ? EntryImpl.tombstone(key, revision, value.updateCounter())
                : new EntryImpl(key, value.bytes(), revision, value.updateCounter());
    }

    @Override
    public void putAll(List<byte[]> keys, List<byte[]> values, HybridTimestamp opTs) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            long counter = addAllToBatch(batch, keys, values, curRev);

            for (byte[] key : keys) {
                updateKeysIndex(batch, key, curRev);
            }

            fillAndWriteBatch(batch, curRev, counter, opTs);
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
    public Collection<Entry> getAll(List<byte[]> keys) {
        rwLock.readLock().lock();

        try {
            return doGetAll(keys, rev);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Collection<Entry> getAll(List<byte[]> keys, long revUpperBound) {
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
            long curRev = rev + 1;
            long counter = updCntr + 1;

            if (addToBatchForRemoval(batch, key, curRev, counter)) {
                updateKeysIndex(batch, key, curRev);

                fillAndWriteBatch(batch, curRev, counter, opTs);
            }
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
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            long counter = updCntr;

            for (byte[] key : keys) {
                if (addToBatchForRemoval(batch, key, curRev, counter + 1)) {
                    existingKeys.add(key);

                    counter++;
                }
            }

            for (byte[] key : existingKeys) {
                updateKeysIndex(batch, key, curRev);
            }

            fillAndWriteBatch(batch, curRev, counter, opTs);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public boolean invoke(
            Condition condition,
            Collection<Operation> success,
            Collection<Operation> failure,
            HybridTimestamp opTs,
            CommandId commandId
    ) {
        rwLock.writeLock().lock();

        try {
            Entry[] entries = getAll(Arrays.asList(condition.keys())).toArray(new Entry[]{});

            boolean branch = condition.test(entries);

            Collection<Operation> ops = branch ? new ArrayList<>(success) : new ArrayList<>(failure);

            ops.add(Operations.put(
                    new ByteArray(IDEMPOTENT_COMMAND_PREFIX + commandId.toMgKeyAsString()),
                    branch ? INVOKE_RESULT_TRUE_BYTES : INVOKE_RESULT_FALSE_BYTES)
            );

            applyOperations(ops, opTs);

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

                    Collection<Operation> ops = new ArrayList<>(update.operations());

                    ops.add(Operations.put(
                            new ByteArray(IDEMPOTENT_COMMAND_PREFIX + commandId.toMgKeyAsString()),
                            update.result().result())
                    );

                    applyOperations(ops, opTs);

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

    private void applyOperations(Collection<Operation> ops, HybridTimestamp opTs) throws RocksDBException {
        long curRev = rev + 1;

        boolean modified = false;

        long counter = updCntr;

        List<byte[]> updatedKeys = new ArrayList<>();

        try (WriteBatch batch = new WriteBatch()) {
            for (Operation op : ops) {
                byte @Nullable [] key = op.key() == null ? null : toByteArray(op.key());

                switch (op.type()) {
                    case PUT:
                        counter++;

                        addDataToBatch(batch, key, toByteArray(op.value()), curRev, counter);

                        updatedKeys.add(key);

                        modified = true;

                        break;

                    case REMOVE:
                        counter++;

                        boolean removed = addToBatchForRemoval(batch, key, curRev, counter);

                        if (!removed) {
                            counter--;
                        } else {
                            updatedKeys.add(key);
                        }

                        modified |= removed;

                        break;

                    case NO_OP:
                        break;

                    default:
                        throw new MetaStorageException(OP_EXECUTION_ERR, "Unknown operation type: " + op.type());
                }
            }

            if (modified) {
                for (byte[] key : updatedKeys) {
                    updateKeysIndex(batch, key, curRev);
                }

                fillAndWriteBatch(batch, curRev, counter, opTs);
            }
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo) {
        rwLock.readLock().lock();

        try {
            return range(keyFrom, keyTo, rev);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            var readOpts = new ReadOptions();

            var upperBound = keyTo == null ? null : new Slice(keyTo);

            readOpts.setIterateUpperBound(upperBound);

            RocksIterator iterator = index.newIterator(readOpts);

            iterator.seek(keyFrom);

            return new RocksIteratorAdapter<>(iterator) {
                /** Cached entry used to filter "empty" values. */
                @Nullable
                private Entry next;

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
                protected Entry decodeEntry(byte[] key, byte[] value) {
                    long[] revisions = getAsLongs(value);

                    long targetRevision = maxRevision(revisions, revUpperBound);

                    if (targetRevision == -1) {
                        return EntryImpl.empty(key);
                    }

                    // This is not a correct approach for using locks in terms of compaction correctness (we should block compaction for the
                    // whole iteration duration). However, compaction is not fully implemented yet, so this lock is taken for consistency
                    // sake. This part must be rewritten in the future.
                    rwLock.readLock().lock();

                    try {
                        return doGetValue(key, targetRevision);
                    } finally {
                        rwLock.readLock().unlock();
                    }
                }

                @Override
                public void close() {
                    super.close();

                    RocksUtils.closeAll(readOpts, upperBound);
                }
            };
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
    public void compact(HybridTimestamp lowWatermark) {
        rwLock.writeLock().lock();


        try (WriteBatch batch = new WriteBatch()) {
            // Find a revision with timestamp lesser or equal to the watermark.
            long maxRevision = revisionByTimestamp(lowWatermark);

            if (maxRevision == -1) {
                // Nothing to compact yet.
                return;
            }

            try (RocksIterator iterator = index.newIterator()) {
                iterator.seekToFirst();

                RocksUtils.forEach(iterator, (key, value) -> compactForKey(batch, key, getAsLongs(value), maxRevision));
            }

            fillAndWriteBatch(batch, rev, updCntr, null);
        } catch (RocksDBException e) {
            throw new MetaStorageException(COMPACTION_ERR, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public byte @Nullable [] nextKey(byte[] key) {
        return incrementPrefix(key);
    }

    /**
     * Adds a key to a batch marking the value as a tombstone.
     *
     * @param batch   Write batch.
     * @param key     Target key.
     * @param curRev  Revision.
     * @param counter Update counter.
     * @return {@code true} if an entry can be deleted.
     * @throws RocksDBException If failed.
     */
    private boolean addToBatchForRemoval(WriteBatch batch, byte[] key, long curRev, long counter) throws RocksDBException {
        Entry e = doGet(key, curRev);

        if (e.empty() || e.tombstone()) {
            return false;
        }

        addDataToBatch(batch, key, TOMBSTONE, curRev, counter);

        return true;
    }

    /**
     * Compacts all entries by the given key, removing revision that are no longer needed.
     * Last entry with a revision lesser or equal to the {@code minRevisionToKeep} and all consecutive entries will be preserved.
     * If the first entry to keep is a tombstone, it will be removed.
     * Example:
     * <pre>
     * Example 1:
     *     put entry1: revision 5
     *     put entry2: revision 7
     *
     *     do compaction: revision 6
     *
     *     entry1: exists
     *     entry2: exists
     *
     * Example 2:
     *     put entry1: revision 5
     *     put entry2: revision 7
     *
     *     do compaction: revision 7
     *
     *     entry1: doesn't exist
     *     entry2: exists
     * </pre>
     *
     * @param batch Write batch.
     * @param key   Target key.
     * @param revs  Revisions.
     * @param minRevisionToKeep Minimum revision that should be kept.
     * @throws RocksDBException If failed.
     */
    private void compactForKey(WriteBatch batch, byte[] key, long[] revs, long minRevisionToKeep) throws RocksDBException {
        if (revs.length < 2) {
            // If we have less than two revisions, there is no point in compaction.
            return;
        }

        // Index of the first revision we will be keeping in the array of revisions.
        int idxToKeepFrom = 0;

        // Whether there is an entry with the minRevisionToKeep.
        boolean hasMinRevision = false;

        // Traverse revisions, looking for the first revision that needs to be kept.
        for (long rev : revs) {
            if (rev >= minRevisionToKeep) {
                if (rev == minRevisionToKeep) {
                    hasMinRevision = true;
                }
                break;
            }

            idxToKeepFrom++;
        }

        if (!hasMinRevision) {
            // Minimal revision was not encountered, that mean that we are between revisions of a key, so previous revision
            // must be preserved.
            idxToKeepFrom--;
        }

        if (idxToKeepFrom <= 0) {
            // All revisions are still in use.
            return;
        }

        for (int i = 0; i < idxToKeepFrom; i++) {
            // This revision is not needed anymore, remove data.
            data.delete(batch, keyToRocksKey(revs[i], key));
        }

        // Whether we only have last revision (even if it's lesser or equal to watermark).
        boolean onlyLastRevisionLeft = idxToKeepFrom == (revs.length - 1);

        // Get the number of the first revision that will be kept.
        long rev = onlyLastRevisionLeft ? lastRevision(revs) : revs[idxToKeepFrom];

        byte[] rocksKey = keyToRocksKey(rev, key);

        Value value = bytesToValue(data.get(rocksKey));

        if (value.tombstone()) {
            // The first revision we are going to keep is a tombstone, we may delete it.
            data.delete(batch, rocksKey);

            if (!onlyLastRevisionLeft) {
                // First revision was a tombstone, but there are other revisions, that need to be kept,
                // so advance index of the first revision we need to keep.
                idxToKeepFrom++;
            }
        }

        if (onlyLastRevisionLeft && value.tombstone()) {
            // We don't have any previous revisions for this entry and the single existing is a tombstone,
            // so we can remove it from index.
            index.delete(batch, key);
        } else {
            // Keeps revisions starting with idxToKeepFrom.
            index.put(batch, key, longsToBytes(revs, idxToKeepFrom));
        }
    }

    /**
     * Gets all entries with given keys and a revision.
     *
     * @param keys Target keys.
     * @param rev  Target revision.
     * @return Collection of entries.
     */
    private Collection<Entry> doGetAll(Collection<byte[]> keys, long rev) {
        assert keys != null : "keys list can't be null.";
        assert !keys.isEmpty() : "keys list can't be empty.";
        assert rev >= 0;

        var res = new ArrayList<Entry>(keys.size());

        for (byte[] key : keys) {
            res.add(doGet(key, rev));
        }

        return res;
    }

    /**
     * Gets the value by key and revision.
     *
     * @param key            Target key.
     * @param revUpperBound  Target upper bound of revision.
     * @return Value.
     */
    private Entry doGet(byte[] key, long revUpperBound) {
        assert revUpperBound >= 0 : "Invalid arguments: [revUpperBound=" + revUpperBound + ']';

        long[] revs;
        try {
            revs = getRevisions(key);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }

        if (revs.length == 0) {
            return EntryImpl.empty(key);
        }

        long lastRev = maxRevision(revs, revUpperBound);

        // lastRev can be -1 if maxRevision return -1.
        if (lastRev == -1) {
            return EntryImpl.empty(key);
        }

        return doGetValue(key, lastRev);
    }

    /**
     * Returns all entries corresponding to the given key and bounded by given revisions.
     * All these entries are ordered by revisions and have the same key.
     * The lower bound and the upper bound are inclusive.
     *
     * @param key The key.
     * @param revLowerBound The lower bound of revision.
     * @param revUpperBound The upper bound of revision.
     * @return Entries corresponding to the given key.
     */
    private List<Entry> doGet(byte[] key, long revLowerBound, long revUpperBound) {
        assert revLowerBound >= 0 : "Invalid arguments: [revLowerBound=" + revLowerBound + ']';
        assert revUpperBound >= 0 : "Invalid arguments: [revUpperBound=" + revUpperBound + ']';
        assert revUpperBound >= revLowerBound
                : "Invalid arguments: [revLowerBound=" + revLowerBound + ", revUpperBound=" + revUpperBound + ']';
        // TODO: IGNITE-19782 throw CompactedException if revLowerBound is compacted.

        long[] revs;

        try {
            revs = getRevisions(key);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }

        if (revs.length == 0) {
            return Collections.emptyList();
        }

        int firstRevIndex = minRevisionIndex(revs, revLowerBound);
        int lastRevIndex = maxRevisionIndex(revs, revUpperBound);

        // firstRevIndex can be -1 if minRevisionIndex return -1. lastRevIndex can be -1 if maxRevisionIndex return -1.
        if (firstRevIndex == -1 || lastRevIndex == -1) {
            return Collections.emptyList();
        }

        List<Entry> entries = new ArrayList<>();

        for (int i = firstRevIndex; i <= lastRevIndex; i++) {
            entries.add(doGetValue(key, revs[i]));
        }

        return entries;
    }

    /**
     * Get a list of the revisions of the entry corresponding to the key.
     *
     * @param key Key.
     * @return Array of revisions.
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
     * Returns maximum revision which must be less or equal to {@code upperBoundRev}. If there is no such revision then {@code -1} will be
     * returned.
     *
     * @param revs          Revisions list.
     * @param upperBoundRev Revision upper bound.
     * @return Maximum revision or {@code -1} if there is no such revision.
     */
    private static long maxRevision(long[] revs, long upperBoundRev) {
        for (int i = revs.length - 1; i >= 0; i--) {
            long rev = revs[i];

            if (rev <= upperBoundRev) {
                return rev;
            }
        }

        return -1;
    }

    /**
     * Returns index of minimum revision which must be greater or equal to {@code lowerBoundRev}.
     * If there is no such revision then {@code -1} will be returned.
     *
     * @param revs          Revisions list.
     * @param lowerBoundRev Revision lower bound.
     * @return Index of minimum revision or {@code -1} if there is no such revision.
     */
    private static int minRevisionIndex(long[] revs, long lowerBoundRev) {
        for (int i = 0; i < revs.length; i++) {
            long rev = revs[i];

            if (rev >= lowerBoundRev) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Returns index of maximum revision which must be less or equal to {@code upperBoundRev}.
     * If there is no such revision then {@code -1} will be returned.
     *
     * @param revs          Revisions list.
     * @param upperBoundRev Revision upper bound.
     * @return Index of maximum revision or {@code -1} if there is no such revision.
     */
    private static int maxRevisionIndex(long[] revs, long upperBoundRev) {
        for (int i = revs.length - 1; i >= 0; i--) {
            long rev = revs[i];

            if (rev <= upperBoundRev) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Gets the value by a key and a revision.
     *
     * @param key      Target key.
     * @param revision Target revision.
     * @return Entry.
     */
    private Entry doGetValue(byte[] key, long revision) {
        if (revision == 0) {
            return EntryImpl.empty(key);
        }

        byte[] valueBytes;

        try {
            valueBytes = data.get(keyToRocksKey(revision, key));
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }

        if (valueBytes == null || valueBytes.length == 0) {
            return EntryImpl.empty(key);
        }

        Value lastVal = bytesToValue(valueBytes);

        if (lastVal.tombstone()) {
            return EntryImpl.tombstone(key, revision, lastVal.updateCounter());
        }

        return new EntryImpl(key, lastVal.bytes(), revision, lastVal.updateCounter());
    }

    /**
     * Adds an entry to the batch.
     *
     * @param batch  Write batch.
     * @param key    Key.
     * @param value  Value.
     * @param curRev Revision.
     * @param cntr   Update counter.
     * @throws RocksDBException If failed.
     */
    private void addDataToBatch(WriteBatch batch, byte[] key, byte[] value, long curRev, long cntr) throws RocksDBException {
        byte[] rocksKey = keyToRocksKey(curRev, key);

        byte[] rocksValue = valueToBytes(value, cntr);

        data.put(batch, rocksKey, rocksValue);

        updatedEntries.add(entry(key, curRev, new Value(value, cntr)));
    }

    /**
     * Adds all entries to the batch.
     *
     * @param batch  Write batch.
     * @param keys   Keys.
     * @param values Values.
     * @param curRev Revision.
     * @return New update counter value.
     * @throws RocksDBException If failed.
     */
    private long addAllToBatch(WriteBatch batch, List<byte[]> keys, List<byte[]> values, long curRev) throws RocksDBException {
        long counter = this.updCntr;

        for (int i = 0; i < keys.size(); i++) {
            counter++;

            byte[] key = keys.get(i);

            byte[] bytes = values.get(i);

            addDataToBatch(batch, key, bytes, curRev, counter);
        }

        return counter;
    }

    /**
     * Gets last revision from the list.
     *
     * @param revs Revisions.
     * @return Last revision.
     */
    private static long lastRevision(long[] revs) {
        return revs[revs.length - 1];
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
                        var updatedEntriesCopy = List.copyOf(updatedEntries);

                        assert ts != null;

                        watchProcessor.notifyWatches(updatedEntriesCopy, ts);

                        updatedEntries.clear();

                        ts = timestampByRevision(revision);
                    }

                    lastSeenRevision = revision;
                }

                if (ts == null) {
                    // This will only execute on first iteration.
                    ts = timestampByRevision(revision);
                }

                updatedEntries.add(entry(rocksKeyToBytes(rocksKey), revision, bytesToValue(rocksValue)));
            }

            RocksUtils.checkIterator(it);

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
        try {
            byte[] tsBytes = revisionToTs.get(longToBytes(revision));

            assert tsBytes != null;

            return HybridTimestamp.hybridTimestamp(bytesToLong(tsBytes));
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    @Override
    public long revisionByTimestamp(HybridTimestamp timestamp) {
        byte[] tsBytes = hybridTsToArray(timestamp);

        // Find a revision with timestamp lesser or equal to the watermark.
        try (RocksIterator rocksIterator = tsToRevision.newIterator()) {
            rocksIterator.seekForPrev(tsBytes);

            RocksUtils.checkIterator(rocksIterator);

            byte[] tsValue = rocksIterator.value();

            if (tsValue.length == 0) {
                return -1;
            }

            return bytesToLong(tsValue);
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

        public UpdatedEntries() {
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
}
