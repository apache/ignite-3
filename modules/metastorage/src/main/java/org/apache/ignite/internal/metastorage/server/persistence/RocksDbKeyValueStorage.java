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
import static org.apache.ignite.internal.metastorage.MetaStorageManager.LATEST_REVISION;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.NOT_FOUND;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.assertCompactionRevisionLessThanCurrent;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.assertRequestedRevisionLessThanOrEqualToCurrent;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.indexToCompact;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.maxRevisionIndex;
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
import static org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler.toIdempotentCommandKey;
import static org.apache.ignite.internal.rocksdb.snapshot.ColumnFamilyRange.fullRange;
import static org.apache.ignite.internal.util.ArrayUtils.LONG_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
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
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.components.NoOpLogSyncer;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationType;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.server.AbstractKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.ChecksumAndRevisions;
import org.apache.ignite.internal.metastorage.server.Condition;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.KeyValueUpdateContext;
import org.apache.ignite.internal.metastorage.server.MetastorageChecksum;
import org.apache.ignite.internal.metastorage.server.NotifyWatchProcessorEvent;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker.TrackingToken;
import org.apache.ignite.internal.metastorage.server.Statement;
import org.apache.ignite.internal.metastorage.server.UpdateEntriesEvent;
import org.apache.ignite.internal.metastorage.server.UpdateOnlyRevisionEvent;
import org.apache.ignite.internal.metastorage.server.Value;
import org.apache.ignite.internal.metastorage.server.WatchEventHandlingCallback;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.rocksdb.snapshot.RocksSnapshotManager;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
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
public class RocksDbKeyValueStorage extends AbstractKeyValueStorage {
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

    /**
     * Key for storing index and term.
     *
     * @see #setIndexAndTerm(long, long)
     */
    private static final byte[] INDEX_AND_TERM_KEY = keyToRocksKey(
            SYSTEM_REVISION_MARKER_VALUE,
            "SYSTEM_INDEX_AND_TERM_KEY".getBytes(UTF_8)
    );

    /**
     * Key for storing configuration.
     *
     * @see #saveConfiguration(byte[], long, long)
     */
    private static final byte[] CONFIGURATION_KEY = keyToRocksKey(
            SYSTEM_REVISION_MARKER_VALUE,
            "SYSTEM_CONFIGURATION_KEY".getBytes(UTF_8)
    );

    /** Batch size (number of keys) for storage compaction. The value is arbitrary. */
    private static final int COMPACT_BATCH_SIZE = 10;

    /** Key value storage flush delay in mills. Value is taken from the example of default values of other components. */
    private static final int KV_STORAGE_FLUSH_DELAY = 100;

    static {
        RocksDB.loadLibrary();
    }

    private final LongSupplier revSupplier = () -> rev;

    private final LongSupplier compactedRevSupplier = () -> compactionRevision;

    /**
     * Only read or modifying while being synchronized on itself.
     *
     * @see #writeBatch(WriteBatch)
     * @see #writeCompactedBatch(List, WriteBatch, CompactionStatisticsHolder)
     */
    private final WriteBatchProtector writeBatchProtector = new WriteBatchProtector();

    /** Executor for storage operations. */
    private final ExecutorService executor;

    /**
     * Scheduled executor. Needed only for asynchronous start of scheduled operations without performing blocking, long or IO operations.
     */
    private final ScheduledExecutorService scheduledExecutor;

    /** Path to the rocksdb database. */
    private final Path dbPath;

    /** Node name. */
    private final String nodeName;

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

    // TODO: https://issues.apache.org/jira/browse/IGNITE-23910 - either make checksums durable or invent another way to solve
    // the 'divergence going unnoticed' problem.
    /** Revision to checksum mapping column family. */
    private volatile ColumnFamily revisionToChecksum;

    /** Snapshot manager. */
    private volatile RocksSnapshotManager snapshotManager;

    /**
     * Facility to work with checksums.
     */
    private volatile MetastorageChecksum checksum;

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
     * Current list of updated entries.
     *
     * <p>Since this list gets read and updated only on writes (under a write lock), no extra synchronisation is needed.</p>
     */
    private final UpdatedEntries updatedEntries = new UpdatedEntries();

    /** Tracks RocksDb resources that must be properly closed. */
    private List<AbstractNativeReference> rocksResources = new ArrayList<>();

    /**
     * Write options used to write to RocksDB.
     */
    private volatile WriteOptions writeOptions;

    private volatile RocksDbFlusher flusher;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param dbPath RocksDB path.
     * @param failureProcessor Failure processor that is used to handle critical errors.
     * @param readOperationForCompactionTracker Read operation tracker for metastorage compaction.
     * @param scheduledExecutor Scheduled executor. Needed only for asynchronous start of scheduled operations without performing blocking,
     *      long or IO operations.
     */
    public RocksDbKeyValueStorage(
            String nodeName,
            Path dbPath,
            FailureProcessor failureProcessor,
            ReadOperationForCompactionTracker readOperationForCompactionTracker,
            ScheduledExecutorService scheduledExecutor
    ) {
        super(
                nodeName,
                failureProcessor,
                readOperationForCompactionTracker
        );

        this.dbPath = dbPath;
        this.scheduledExecutor = scheduledExecutor;
        this.nodeName = nodeName;

        executor = Executors.newFixedThreadPool(
                2,
                IgniteThreadFactory.create(nodeName, "metastorage-rocksdb-kv-storage-executor", log)
        );
    }

    @Override
    public void start() {
        inBusyLock(busyLock, this::startBusy);
    }

    private void startBusy() {
        try {
            Files.createDirectories(dbPath);

            createDb();
        } catch (IOException | RocksDBException e) {
            closeRocksResources();

            throw new MetaStorageException(STARTING_STORAGE_ERR, "Failed to start the storage", e);
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
                .setAtomicFlush(true)
                .setCreateMissingColumnFamilies(true)
                .setListeners(List.of(flusher.listener()))
                .setCreateIfMissing(true);

        rocksResources.add(options);

        return options;
    }

    private void createDb() throws RocksDBException {
        // Metastorage recovery is based on the snapshot & external log. WAL is never used for recovery, and can be safely disabled.
        writeOptions = new WriteOptions().setDisableWAL(true);
        rocksResources.add(writeOptions);

        List<ColumnFamilyDescriptor> descriptors = cfDescriptors();

        assert descriptors.size() == 5 : descriptors.size();

        var handles = new ArrayList<ColumnFamilyHandle>(descriptors.size());

        flusher = new RocksDbFlusher(
                "rocksdb metastorage kv storage",
                nodeName,
                busyLock,
                scheduledExecutor,
                executor,
                () -> KV_STORAGE_FLUSH_DELAY,
                // It is expected that the metastorage command raft log works with fsync=true.
                new NoOpLogSyncer(),
                failureProcessor,
                () -> {}
        );

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
                executor
        );

        flusher.init(db, handles);

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

    private long checksumByRevisionOrZero(long revision) throws RocksDBException {
        byte[] bytes = revisionToChecksum.get(longToBytes(revision));

        if (bytes == null) {
            return 0;
        }

        return bytesToLong(bytes);
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
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        stopCompaction();

        busyLock.block();

        watchProcessor.close();
        flusher.stop();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        closeRocksResources();
    }

    private void closeRocksResources() {
        Collections.reverse(rocksResources);
        RocksUtils.closeAll(rocksResources);
        this.rocksResources = new ArrayList<>();
    }

    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        return snapshotManager
                .createSnapshot(snapshotPath)
                .thenCompose(unused -> flush());
    }

    @Override
    public void restoreSnapshot(Path path) {
        try {
            // TODO IGNITE-25213 Ensure that this doesn't happen on a running node.
            clear();

            snapshotManager.restoreSnapshot(path);

            rev = bytesToLong(data.get(REVISION_KEY));

            byte[] compactionRevisionBytes = data.get(COMPACTION_REVISION_KEY);

            if (compactionRevisionBytes != null) {
                compactionRevision = bytesToLong(compactionRevisionBytes);
            }

            notifyRevisionsUpdate();
        } catch (MetaStorageException e) {
            throw e;
        } catch (Exception e) {
            throw new MetaStorageException(RESTORING_STORAGE_ERR, "Failed to restore snapshot", e);
        }
    }

    @Override
    public Entry get(byte[] key) {
        try (TrackingToken unused = readOperationForCompactionTracker.track(LATEST_REVISION, revSupplier, compactedRevSupplier)) {
            return super.get(key);
        }
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys) {
        try (TrackingToken unused = readOperationForCompactionTracker.track(LATEST_REVISION, revSupplier, compactedRevSupplier)) {
            return super.getAll(keys);
        }
    }

    @Override
    public void put(byte[] key, byte[] value, KeyValueUpdateContext context) {
        try (WriteBatch batch = new WriteBatch()) {
            long newChecksum = checksum.wholePut(key, value);

            long curRev = rev + 1;

            addDataToBatch(batch, key, value, curRev, context.timestamp);

            updateKeysIndex(batch, key, curRev);

            completeAndWriteBatch(batch, curRev, context, newChecksum);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    @Override
    public void setIndexAndTerm(long index, long term) {
        try {
            db.put(data.handle(), writeOptions, INDEX_AND_TERM_KEY, longsToBytes(0, index, term));
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    @Override
    public @Nullable IndexWithTerm getIndexWithTerm() {
        try {
            byte[] bytes = data.get(INDEX_AND_TERM_KEY);

            if (bytes == null) {
                return null;
            }

            return new IndexWithTerm(bytesToLong(bytes, 0), bytesToLong(bytes, Long.BYTES));
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    @Override
    public void saveConfiguration(byte[] configuration, long lastAppliedIndex, long lastAppliedTerm) {
        try (WriteBatch batch = new WriteBatch()) {
            data.put(batch, INDEX_AND_TERM_KEY, longsToBytes(0, lastAppliedIndex, lastAppliedTerm));
            data.put(batch, CONFIGURATION_KEY, configuration);

            writeBatch(batch);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    @Override
    public byte @Nullable [] getConfiguration() {
        try {
            return data.get(CONFIGURATION_KEY);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
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
     * @param context Operation's context.
     * @param newChecksum Checksum corresponding to the revision.
     * @throws RocksDBException If failed.
     */
    private void completeAndWriteBatch(
            WriteBatch batch, long newRev, KeyValueUpdateContext context, long newChecksum
    ) throws RocksDBException {
        byte[] revisionBytes = longToBytes(newRev);

        boolean sameChecksumAlreadyExists = validateNoChecksumConflict(newRev, newChecksum);
        if (!sameChecksumAlreadyExists) {
            revisionToChecksum.put(batch, revisionBytes, longToBytes(newChecksum));
        }

        HybridTimestamp ts = context.timestamp;

        data.put(batch, REVISION_KEY, revisionBytes);

        byte[] tsBytes = hybridTsToArray(ts);

        tsToRevision.put(batch, tsBytes, revisionBytes);
        revisionToTs.put(batch, revisionBytes, tsBytes);

        addIndexAndTermToWriteBatch(batch, context);

        writeBatch(batch);
        rev = newRev; // Done.

        checksum.commitRound(newChecksum);
        updatedEntries.ts = ts;

        queueWatchEvent();

        notifyRevisionsUpdate();
    }

    private boolean validateNoChecksumConflict(long newRev, long newChecksum) throws RocksDBException {
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

        return existingChecksumBytes != null;
    }

    private static byte[] hybridTsToArray(HybridTimestamp ts) {
        return longToBytes(ts.longValue());
    }

    @Override
    public void putAll(List<byte[]> keys, List<byte[]> values, KeyValueUpdateContext context) {
        try (WriteBatch batch = new WriteBatch()) {
            long newChecksum = checksum.wholePutAll(keys, values);

            long curRev = rev + 1;

            addAllToBatch(batch, keys, values, curRev, context.timestamp);

            for (byte[] key : keys) {
                updateKeysIndex(batch, key, curRev);
            }

            completeAndWriteBatch(batch, curRev, context, newChecksum);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    @Override
    public void remove(byte[] key, KeyValueUpdateContext context) {
        try (WriteBatch batch = new WriteBatch()) {
            long newChecksum = checksum.wholeRemove(key);

            long curRev = rev + 1;

            if (addToBatchForRemoval(batch, key, curRev, context.timestamp)) {
                updateKeysIndex(batch, key, curRev);
            }

            completeAndWriteBatch(batch, curRev, context, newChecksum);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    @Override
    public void removeAll(List<byte[]> keys, KeyValueUpdateContext context) {
        try (WriteBatch batch = new WriteBatch()) {
            long newChecksum = checksum.wholeRemoveAll(keys);

            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                if (addToBatchForRemoval(batch, key, curRev, context.timestamp)) {
                    existingKeys.add(key);
                }
            }

            for (byte[] key : existingKeys) {
                updateKeysIndex(batch, key, curRev);
            }

            completeAndWriteBatch(batch, curRev, context, newChecksum);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    @Override
    public void removeByPrefix(byte[] prefix, KeyValueUpdateContext context) {
        try (
                WriteBatch batch = new WriteBatch();
                Cursor<Entry> entryCursor = range(prefix, nextKey(prefix))
        ) {
            long curRev = rev + 1;

            for (Entry entry : entryCursor) {
                byte[] key = entry.key();

                if (addToBatchForRemoval(batch, key, curRev, context.timestamp)) {
                    updateKeysIndex(batch, key, curRev);
                }
            }

            completeAndWriteBatch(batch, curRev, context, checksum.wholeRemoveByPrefix(prefix));
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    @Override
    public boolean invoke(
            Condition condition,
            List<Operation> success,
            List<Operation> failure,
            KeyValueUpdateContext context,
            CommandId commandId
    ) {
        try {
            Entry[] entries = getAll(Arrays.asList(condition.keys())).toArray(new Entry[]{});

            boolean branch = condition.test(entries);
            ByteBuffer updateResult = ByteBuffer.wrap(branch ? INVOKE_RESULT_TRUE_BYTES : INVOKE_RESULT_FALSE_BYTES);

            List<Operation> ops = new ArrayList<>(branch ? success : failure);

            ops.add(Operations.put(toIdempotentCommandKey(commandId), updateResult));

            applyOperations(ops, context, false, updateResult);

            return branch;
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    @Override
    public StatementResult invoke(If iif, KeyValueUpdateContext context, CommandId commandId) {
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

                    ops.add(Operations.put(toIdempotentCommandKey(commandId), updateResult));

                    applyOperations(ops, context, true, updateResult);

                    return update.result();
                } else {
                    currIf = branch.iif();
                }
            }
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
        }
    }

    private void applyOperations(List<Operation> ops, KeyValueUpdateContext context, boolean multiInvoke, ByteBuffer updateResult)
            throws RocksDBException {
        HybridTimestamp opTs = context.timestamp;

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

            completeAndWriteBatch(batch, curRev, context, checksum.roundValue());
        }
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo) {
        return doRange(keyFrom, keyTo, rev);
    }

    @Override
    public Cursor<Entry> range(byte[] keyFrom, byte @Nullable [] keyTo, long revUpperBound) {
        return doRange(keyFrom, keyTo, revUpperBound);
    }

    @Override
    public void startWatches(long startRevision, WatchEventHandlingCallback callback) {
        assert startRevision > 0 : startRevision;

        long currentRevision;

        synchronized (watchProcessorMutex) {
            watchProcessor.setWatchEventHandlingCallback(callback);

            currentRevision = rev;

            // We update the recovery status under the read lock in order to avoid races between starting watches and applying a snapshot
            // or concurrent writes. Replay of events can be done outside of the read lock relying on RocksDB snapshot isolation.
            if (currentRevision == 0) {
                recoveryStatus.set(RecoveryStatus.DONE);
            } else {
                // If revision is not 0, we need to replay updates that match the existing data.
                recoveryStatus.set(RecoveryStatus.IN_PROGRESS);
            }
        }

        if (currentRevision != 0) {
            Set<UpdateEntriesEvent> updateEntriesEvents = collectUpdateEntriesEventsFromStorage(startRevision, currentRevision);
            Set<UpdateOnlyRevisionEvent> updateOnlyRevisionEvents = collectUpdateRevisionEventsFromStorage(startRevision, currentRevision);

            synchronized (watchProcessorMutex) {
                notifyWatchProcessorEventsBeforeStartingWatches.addAll(updateEntriesEvents);
                // Adds events for which there were no entries updates but the revision was updated.
                notifyWatchProcessorEventsBeforeStartingWatches.addAll(updateOnlyRevisionEvents);

                drainNotifyWatchProcessorEventsBeforeStartingWatches();

                recoveryStatus.set(RecoveryStatus.DONE);
            }
        }
    }

    @Override
    public void compact(long revision) {
        assert revision >= 0 : revision;

        LOG.info("Metastore compaction has started. [revision={}]", revision);

        CompactionStatisticsHolder statHolder = new CompactionStatisticsHolder(revision);
        try {
            compactKeys(revision, statHolder);

            compactAuxiliaryMappings(revision, statHolder);

            statHolder.onFinished();

            LOG.info("Metastore compaction completed successfully. [" + statHolder.info() + "]");
        } catch (Throwable t) {
            throw new MetaStorageException(COMPACTION_ERR, "Error during compaction: " + revision, t);
        }
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
     * @param batchKeys A list of keys, in which this method will add keys that it compacted.
     * @param key Target key.
     * @param revs Key revisions.
     * @param compactionRevision Revision up to which (inclusively) the key will be compacted.
     * @param statHolder Compaction statistics holder.
     * @throws MetaStorageException If failed.
     */
    private void compactForKey(
            WriteBatch batch,
            List<byte[]> batchKeys,
            byte[] key,
            long[] revs,
            long compactionRevision,
            CompactionStatisticsHolder statHolder
    ) {
        try {
            statHolder.onKeyEncountered();

            int indexToCompact = indexToCompact(revs, compactionRevision, revision -> isTombstoneForCompaction(key, revision));

            if (NOT_FOUND == indexToCompact) {
                return;
            }

            batchKeys.add(key);

            for (int revisionIndex = 0; revisionIndex <= indexToCompact; revisionIndex++) {
                statHolder.onKeyRevisionCompacted();

                // This revision is not needed anymore, remove data.
                data.delete(batch, keyToRocksKey(revs[revisionIndex], key));
            }

            if (indexToCompact == revs.length - 1) {
                statHolder.onTombstoneCompacted();

                index.delete(batch, key);
            } else {
                statHolder.onKeyCompacted();

                index.put(batch, key, longsToBytes(indexToCompact + 1, revs));
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

        updatedEntries.add(EntryImpl.toEntry(key, curRev, new Value(value, opTs)));
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
        synchronized (watchProcessorMutex) {
            if (recoveryStatus.get() == RecoveryStatus.INITIAL) {
                // Watches haven't been enabled yet, no need to queue any events, they will be replayed upon recovery.
                updatedEntries.clear();
            } else {
                notifyWatchProcessor(updatedEntries.toNotifyWatchProcessorEvent(rev));
            }
        }
    }

    private Set<UpdateEntriesEvent> collectUpdateEntriesEventsFromStorage(long lowerRevision, long upperRevision) {
        long minWatchRevision = Math.max(lowerRevision, watchProcessor.minWatchRevision().orElse(-1));

        if (minWatchRevision > upperRevision) {
            return Set.of();
        }

        var updatedEntries = new ArrayList<Entry>();
        HybridTimestamp ts = null;

        var events = new TreeSet<UpdateEntriesEvent>();

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

                        var event = new UpdateEntriesEvent(updatedEntriesCopy, ts);

                        boolean added = events.add(event);

                        assert added : event;

                        updatedEntries.clear();

                        ts = hybridTimestamp(timestampFromRocksValue(rocksValue));
                    }

                    lastSeenRevision = revision;
                }

                if (ts == null) {
                    // This will only execute on first iteration.
                    ts = hybridTimestamp(timestampFromRocksValue(rocksValue));
                }

                updatedEntries.add(EntryImpl.toEntry(rocksKeyToBytes(rocksKey), revision, bytesToValue(rocksValue)));
            }

            try {
                it.status();
            } catch (RocksDBException e) {
                throw new MetaStorageException(OP_EXECUTION_ERR, e);
            }

            // Adds event left after finishing the loop above.
            if (!updatedEntries.isEmpty()) {
                assert ts != null;

                var event = new UpdateEntriesEvent(updatedEntries, ts);

                boolean added = events.add(event);

                assert added : event;
            }
        }

        return events;
    }

    private Set<UpdateOnlyRevisionEvent> collectUpdateRevisionEventsFromStorage(long lowerRevision, long upperRevision) {
        var events = new TreeSet<UpdateOnlyRevisionEvent>();

        try (
                var upperBound = new Slice(longToBytes(upperRevision + 1));
                var options = new ReadOptions().setIterateUpperBound(upperBound);
                RocksIterator it = revisionToTs.newIterator(options)
        ) {
            it.seek(longToBytes(lowerRevision));

            for (; it.isValid(); it.next()) {
                byte[] rocksKey = it.key();
                byte[] rocksValue = it.value();

                long revision = bytesToLong(rocksKey);
                HybridTimestamp time = hybridTimestamp(bytesToLong(rocksValue));

                UpdateOnlyRevisionEvent event = new UpdateOnlyRevisionEvent(revision, time);

                boolean added = events.add(event);

                assert added : event;

                try {
                    it.status();
                } catch (RocksDBException e) {
                    throw new MetaStorageException(OP_EXECUTION_ERR, e);
                }
            }
        }

        return events;
    }

    @Override
    public HybridTimestamp timestampByRevision(long revision) {
        try {
            assertRequestedRevisionLessThanOrEqualToCurrent(revision, rev);

            byte[] tsBytes = revisionToTs.get(longToBytes(revision));

            if (tsBytes == null) {
                throw new CompactedException("Requested revision has already been compacted: " + revision);
            }

            return hybridTimestamp(bytesToLong(tsBytes));
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, "Error reading revision timestamp: " + revision, e);
        }
    }

    @Override
    public long revisionByTimestamp(HybridTimestamp timestamp) {
        // Find a revision with timestamp lesser or equal to the timestamp.
        try (RocksIterator rocksIterator = tsToRevision.newIterator()) {
            rocksIterator.seekForPrev(hybridTsToArray(timestamp));

            rocksIterator.status();

            if (!rocksIterator.isValid()) {
                // No previous value means that it got deleted, or never existed in the first place.
                throw new CompactedException("Revisions less than or equal to the requested one are already compacted: " + timestamp);
            }

            return bytesToLong(rocksIterator.value());
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, e);
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

        NotifyWatchProcessorEvent toNotifyWatchProcessorEvent(long newRevision) {
            UpdatedEntries copy = transfer();

            return copy.updatedEntries.isEmpty() ? new UpdateOnlyRevisionEvent(newRevision, copy.ts)
                    : new UpdateEntriesEvent(copy.updatedEntries, copy.ts);
        }
    }

    @Override
    protected void saveCompactionRevision(long revision, KeyValueUpdateContext context, boolean advanceSafeTime) {
        try (WriteBatch batch = new WriteBatch()) {
            data.put(batch, COMPACTION_REVISION_KEY, longToBytes(revision));

            addIndexAndTermToWriteBatch(batch, context);

            writeBatch(batch);

            if (advanceSafeTime && areWatchesStarted()) {
                watchProcessor.advanceSafeTime(() -> {}, context.timestamp);
            }
        } catch (Throwable t) {
            throw new MetaStorageException(COMPACTION_ERR, "Error saving compaction revision: " + revision, t);
        }
    }

    @Override
    public long checksum(long revision) {
        try {
            assertRequestedRevisionLessThanOrEqualToCurrent(revision, rev);

            return checksumByRevision(revision);
        } catch (RocksDBException e) {
            throw new MetaStorageException(INTERNAL_ERR, "Cannot get checksum by revision: " + revision, e);
        }
    }

    @Override
    public ChecksumAndRevisions checksumAndRevisions(long revision) {
        try {
            // "rev" is the last thing that gets updated, so it has to be read first.
            long currentRevision = rev;

            return new ChecksumAndRevisions(
                    checksumByRevisionOrZero(revision),
                    minChecksummedRevisionOrZero(),
                    currentRevision
            );
        } catch (RocksDBException e) {
            throw new MetaStorageException(INTERNAL_ERR, "Cannot get checksum by revision: " + revision, e);
        }
    }

    private long minChecksummedRevisionOrZero() throws RocksDBException {
        try (
                var options = new ReadOptions().setTailing(true);
                RocksIterator it = revisionToChecksum.newIterator(options)
        ) {
            it.seekToFirst();

            if (it.isValid()) {
                return bytesToLong(it.key());
            } else {
                it.status();

                return 0;
            }
        }
    }

    @Override
    public void clear() {
        try {
            // There's no way to easily remove all data from RocksDB, so we need to re-create it from scratch.
            closeRocksResources();

            destroyRocksDb();

            this.rev = 0;
            this.compactionRevision = -1;

            this.updatedEntries.clear();

            createDb();
        } catch (Exception e) {
            throw new MetaStorageException(RESTORING_STORAGE_ERR, "Failed to restore snapshot", e);
        }
    }

    private void compactKeys(long compactionRevision, CompactionStatisticsHolder statHolder) throws RocksDBException {
        assertCompactionRevisionLessThanCurrent(this.compactionRevision, rev);

        // Clear bloom filter before opening iterator, so that we don't have collisions right from the start.
        synchronized (writeBatchProtector) {
            writeBatchProtector.clear();
        }

        if (!busyLock.enterBusy()) {
            statHolder.onCancelled();

            return;
        }

        try (RocksIterator iterator = index.newIterator()) {
            byte[] key = null;
            List<byte[]> batchKeys = new ArrayList<>(COMPACT_BATCH_SIZE);

            iterator.seekToFirst();
            while (iterator.isValid()) {
                try (WriteBatch batch = new WriteBatch()) {
                    byte[] retryPositionKey = key;

                    batchKeys.clear();
                    for (int i = 0; i < COMPACT_BATCH_SIZE && iterator.isValid(); i++, iterator.next()) {
                        if (stopCompaction.get()) {
                            statHolder.onCancelled();

                            return;
                        }

                        // Throw an exception if something went wrong.
                        iterator.status();

                        key = iterator.key();

                        compactForKey(batch, batchKeys, key, getAsLongs(iterator.value()), compactionRevision, statHolder);
                    }

                    if (!writeCompactedBatch(batchKeys, batch, statHolder)) {
                        key = retryPositionKey;

                        // Refreshing the iterator is absolutely crucial. We have determined that data has been modified externally,
                        // current snapshot in the iterator is invalid.
                        refreshIterator(iterator, key);
                    }
                }
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void compactAuxiliaryMappings(long compactionRevision, CompactionStatisticsHolder statHolder) throws RocksDBException {
        assertCompactionRevisionLessThanCurrent(compactionRevision, rev);

        if (!busyLock.enterBusy()) {
            statHolder.onCancelled();

            return;
        }

        try (RocksIterator iterator = revisionToTs.newIterator()) {
            boolean continueIterating = true;
            iterator.seekToFirst();

            while (continueIterating && iterator.isValid()) {
                try (WriteBatch batch = new WriteBatch()) {
                    for (int i = 0; i < COMPACT_BATCH_SIZE && iterator.isValid(); i++, iterator.next()) {
                        if (stopCompaction.get()) {
                            statHolder.onCancelled();

                            return;
                        }

                        // Throw an exception if something went wrong.
                        iterator.status();

                        statHolder.onAuxiliaryMappingCompacted();

                        if (!deleteAuxiliaryMapping(compactionRevision, iterator, batch)) {
                            continueIterating = false;

                            break;
                        }
                    }

                    db.write(writeOptions, batch);
                }
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    private boolean deleteAuxiliaryMapping(long compactionRevision, RocksIterator iterator, WriteBatch batch) throws RocksDBException {
        byte[] key = iterator.key();
        long revision = bytesToLong(key);

        if (revision > compactionRevision) {
            return false;
        }

        revisionToTs.delete(batch, key);
        tsToRevision.delete(batch, iterator.value());

        revisionToChecksum.delete(batch, key);

        return true;
    }

    /**
     * Writes the batch to the database in the {@code FSM} thread. By the time this method is called, {@link #updatedEntries} must be in a
     * state that corresponds to the current batch. This collection will be used to mark the entries as updated in
     * {@link #writeBatchProtector}.
     *
     * <p>No other actions besides calling {@link WriteBatchProtector#onUpdate(byte[])} and {@link RocksDB#write(WriteOptions, WriteBatch)}
     * are performed here. They're both executed while holding {@link #writeBatchProtector}'s monitor in order to synchronise
     * {@link #writeBatchProtector} modifications.
     *
     * <p>Since there's a gap between reading the data and writing batch into the storage, it is possible that compaction thread had
     * concurrently updated the data that we're modifying. The only real side effect of such a race will be a presence of already deleted
     * revisions in revisions list, associated with any particular key (see {@link RocksDbKeyValueStorage#keyRevisionsForOperation(byte[])}.
     *
     * <p>We ignore this side effect, because retrying the operation in {@code FSM} thread is too expensive, and because there's really no
     * harm in having some obsolete revisions there. We always keep in mind that a particular revision might already be compacted when we
     * read data.
     *
     * @param batch RockDB's {@link WriteBatch}.
     * @throws RocksDBException If {@link RocksDB#write(WriteOptions, WriteBatch)} threw an exception.
     * @see #writeCompactedBatch(List, WriteBatch, CompactionStatisticsHolder)
     */
    private void writeBatch(WriteBatch batch) throws RocksDBException {
        synchronized (writeBatchProtector) {
            for (Entry updatedEntry : updatedEntries.updatedEntries) {
                writeBatchProtector.onUpdate(updatedEntry.key());
            }

            db.write(writeOptions, batch);
        }
    }

    /**
     * Writes the batch to the database in the compaction thread.
     *
     * <p>No other actions besides accessing {@link #writeBatchProtector} and calling {@link RocksDB#write(WriteOptions, WriteBatch)}
     * are performed here. They're both executed while holding {@link #writeBatchProtector}'s monitor in order to synchronise potential
     * {@link #writeBatchProtector} modifications.
     *
     * <p>Since there's a gap between reading the data and writing batch into the storage, it is possible that {@code FSM} thread had
     * concurrently updated the data that we're compacting. Such a race would mean that there are unaccounted revisions in revisions list,
     * associated with keys from {@code batchKeys} (see {@link RocksDbKeyValueStorage#keyRevisionsForOperation(byte[])}.
     *
     * <p>For that reason, unlike {@link #writeBatch(WriteBatch)}, here we don't have a privilege of just writing the batch in case of race.
     * It would lead to data loss. In order to avoid it, we probabilistically check if data that we're compacting has been modified
     * concurrently. If it certainly wasn't, we proceed and return {@code true}. If we're not sure, we abort this batch and return
     * {@code false}.
     *
     * <p>If we return {@code false}, we also clear the {@link #writeBatchProtector} to avoid blocking the next batch. It is important to
     * remember that we'll get false-negative results sometimes if we have hash collisions. That's fine.
     *
     * @param batchKeys Meta-storage keys that have been compacted in this batch.
     * @param batch RockDB's {@link WriteBatch}.
     * @param statHolder Compaction statistics holder.
     * @return {@code true} if writing succeeded, {@code false} if compaction round must be retried due to concurrent storage update.
     * @throws RocksDBException If {@link RocksDB#write(WriteOptions, WriteBatch)} threw an exception.
     * @see #writeBatch(WriteBatch)
     */
    private boolean writeCompactedBatch(
            List<byte[]> batchKeys,
            WriteBatch batch,
            CompactionStatisticsHolder statHolder
    ) throws RocksDBException {
        statHolder.onBeforeWriteBatchLock();

        synchronized (writeBatchProtector) {
            statHolder.onAfterWriteBatchLock();

            for (byte[] key : batchKeys) {
                if (writeBatchProtector.maybeUpdated(key)) {
                    writeBatchProtector.clear();

                    statHolder.onBatchAborted();

                    return false;
                }
            }

            db.write(writeOptions, batch);

            statHolder.onBatchCommitted();
        }

        return true;
    }

    private static void refreshIterator(RocksIterator iterator, byte @Nullable [] key) throws RocksDBException {
        iterator.refresh();

        if (key == null) {
            // First iteration. Seek to the first key.
            iterator.seekToFirst();
        } else {
            // Searches for either the exact key or its previous value.
            iterator.seekForPrev(key);

            if (iterator.isValid()) {
                iterator.next();
            } else {
                // Key has been concurrently removed and it was the first key in column family. Seek to the first key.
                iterator.seekToFirst();
            }
        }

        // Check for errors.
        iterator.status();
    }

    private boolean isTombstone(byte[] key, long revision) throws RocksDBException {
        byte[] rocksKey = keyToRocksKey(revision, key);

        byte[] valueBytes = data.get(rocksKey);

        // "null" means that the value has been deleted in one of previous compaction rounds. Treat it like a tombstone then.
        return valueBytes == null || bytesToValue(valueBytes).tombstone();
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

    @Override
    protected long[] keyRevisionsForOperation(byte[] key) {
        try {
            return getRevisions(key);
        } catch (RocksDBException e) {
            throw new MetaStorageException(OP_EXECUTION_ERR, "Failed to get revisions for the key: " + toUtf8String(key), e);
        }
    }

    @Override
    protected @Nullable Value valueForOperation(byte[] key, long revision) {
        return getValueForOperation(key, revision);
    }

    @Override
    protected boolean areWatchesStarted() {
        return recoveryStatus.get() == RecoveryStatus.DONE;
    }

    private @Nullable Value getValueForOperation(byte[] key, long revision) {
        try {
            byte[] valueBytes = data.get(keyToRocksKey(revision, key));

            if (valueBytes == null) {
                return null;
            }

            return bytesToValue(valueBytes);
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

        var readOpts = new ReadOptions();

        Slice upperBound = keyTo == null ? null : new Slice(keyTo);

        readOpts.setIterateUpperBound(upperBound);

        RocksIterator iterator = index.newIterator(readOpts);

        iterator.seek(keyFrom);

        TrackingToken token;
        try {
            token = readOperationForCompactionTracker.track(revUpperBound, this::revision, this::getCompactionRevision);
        } catch (Throwable e) {
            // Revision might have been compacted already, in which case we must free the resources.
            RocksUtils.closeAll(iterator, upperBound, readOpts);

            throw e;
        }

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
                Value value = getValueForOperation(key, revision);

                if (value == null || value.tombstone()) {
                    return EntryImpl.empty(key);
                }

                return EntryImpl.toEntry(key, revision, value);
            }

            @Override
            public void close() {
                token.close();

                super.close();

                RocksUtils.closeAll(readOpts, upperBound);
            }
        };
    }

    private void addIndexAndTermToWriteBatch(WriteBatch batch, KeyValueUpdateContext context) throws RocksDBException {
        data.put(batch, INDEX_AND_TERM_KEY, longsToBytes(0, context.index, context.term));
    }

    @Override
    public CompletableFuture<Void> flush() {
        return inBusyLockAsync(busyLock, () -> flusher.awaitFlush(true));
    }
}
