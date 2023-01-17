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

package org.apache.ignite.internal.storage.rocksdb;

import static java.lang.ThreadLocal.withInitial;
import static java.nio.ByteBuffer.allocateDirect;
import static java.util.Arrays.copyOf;
import static java.util.Arrays.copyOfRange;
import static org.apache.ignite.internal.hlc.HybridTimestamp.HYBRID_TIMESTAMP_SIZE;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.bytesToUuid;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.putUuidToBytes;
import static org.rocksdb.ReadTier.PERSISTED_TIER;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.rocksdb.BusyRocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.schema.TableRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;

/**
 * Multi-versioned partition storage implementation based on RocksDB. Stored data has the following format.
 *
 * <p/>Key:
 * <pre><code>
 * For write-intents
 * | partId (2 bytes, BE) | rowId (16 bytes, BE) |
 *
 * For committed rows
 * | partId (2 bytes, BE) | rowId (16 bytes, BE) | timestamp (12 bytes, DESC) |
 * </code></pre>
 * Value:
 * <pre><code>
 * For write-intents
 * | txId (16 bytes) | commitTableId (16 bytes) | commitPartitionId (2 bytes) | Row data |
 *
 * For committed rows
 * | Row data |
 * </code></pre>
 *
 * <p/>Pending transactions (write-intents) data doesn't have a timestamp assigned, but they have transaction
 * state (txId, commitTableId and commitPartitionId).
 *
 * <p/>BE means Big Endian, meaning that lexicographical bytes order matches a natural order of partitions.
 *
 * <p/>DESC means that timestamps are sorted from newest to oldest (N2O). Please refer to {@link #putTimestamp(ByteBuffer, HybridTimestamp)}
 * to see how it's achieved. Missing timestamp could be interpreted as a moment infinitely far away in the future.
 */
public class RocksDbMvPartitionStorage implements MvPartitionStorage {
    /** Position of row id inside the key. */
    private static final int ROW_ID_OFFSET = Short.BYTES;

    /** UUID size in bytes. */
    private static final int ROW_ID_SIZE = 2 * Long.BYTES;

    /** Size of the key without timestamp. */
    private static final int ROW_PREFIX_SIZE = ROW_ID_OFFSET + ROW_ID_SIZE;

    /** Transaction id size (part of the transaction state). */
    private static final int TX_ID_SIZE = 2 * Long.BYTES;

    /** Commit table id size (part of the transaction state). */
    private static final int TABLE_ID_SIZE = 2 * Long.BYTES;

    /** Commit partition id size (part of the transaction state). */
    private static final int PARTITION_ID_SIZE = Short.BYTES;

    /** Size of the value header (transaction state). */
    private static final int VALUE_HEADER_SIZE = TX_ID_SIZE + TABLE_ID_SIZE + PARTITION_ID_SIZE;

    /** Transaction id offset. */
    private static final int TX_ID_OFFSET = 0;

    /** Commit table id offset. */
    private static final int TABLE_ID_OFFSET = TX_ID_SIZE;

    /** Commit partition id offset. */
    private static final int PARTITION_ID_OFFSET = TABLE_ID_OFFSET + TABLE_ID_SIZE;

    /** Value offset (if transaction state is present). */
    private static final int VALUE_OFFSET = VALUE_HEADER_SIZE;

    /** Maximum size of the key. */
    private static final int MAX_KEY_SIZE = ROW_PREFIX_SIZE + HYBRID_TIMESTAMP_SIZE;

    private static final ByteOrder KEY_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    private static final ByteOrder TABLE_ROW_BYTE_ORDER = TableRow.ORDER;

    /** Thread-local direct buffer instance to read keys from RocksDB. */
    private static final ThreadLocal<ByteBuffer> MV_KEY_BUFFER = withInitial(() -> allocateDirect(MAX_KEY_SIZE).order(KEY_BYTE_ORDER));

    /** Thread-local on-heap byte buffer instance to use for key manipulations. */
    private static final ThreadLocal<ByteBuffer> HEAP_KEY_BUFFER = withInitial(
            () -> ByteBuffer.allocate(MAX_KEY_SIZE).order(KEY_BYTE_ORDER)
    );

    /** Thread-local write batch for {@link #runConsistently(WriteClosure)}. */
    private final ThreadLocal<WriteBatchWithIndex> threadLocalWriteBatch = new ThreadLocal<>();

    /** Table storage instance. */
    private final RocksDbTableStorage tableStorage;

    /**
     * Partition ID (should be treated as an unsigned short).
     *
     * <p/>Partition IDs are always stored in the big endian order, since they need to be compared lexicographically.
     */
    private final int partitionId;

    /** RocksDb instance. */
    private final RocksDB db;

    /** Partitions column family. */
    private final ColumnFamilyHandle cf;

    /** Meta column family. */
    private final ColumnFamilyHandle meta;

    /** Write options. */
    private final WriteOptions writeOpts = new WriteOptions().setDisableWAL(true);

    /** Read options for regular reads. */
    private final ReadOptions readOpts = new ReadOptions();

    /** Read options for reading persisted data. */
    private final ReadOptions persistedTierReadOpts = new ReadOptions().setReadTier(PERSISTED_TIER);

    /** Upper bound for scans and reads. */
    private final Slice upperBound;

    private final ReadOptions upperBoundReadOpts;

    /** Read options for scan iterators. */
    private final ReadOptions scanReadOptions;

    /** Key to store applied index value in meta. */
    private final byte[] lastAppliedIndexKey;

    /** Key to store applied term value in meta. */
    private final byte[] lastAppliedTermKey;

    /** Key to store group config in meta. */
    private final byte[] lastGroupConfigKey;

    /** On-heap-cached last applied index value. */
    private volatile long lastAppliedIndex;

    /** On-heap-cached last applied term value. */
    private volatile long lastAppliedTerm;

    /** On-heap-cached last committed group configuration. */
    @Nullable
    private volatile RaftGroupConfiguration lastGroupConfig;

    private volatile long pendingAppliedIndex;

    private volatile long pendingAppliedTerm;

    @Nullable
    private volatile RaftGroupConfiguration pendingGroupConfig;

    /** The value of {@link #lastAppliedIndex} persisted to the device at this moment. */
    private volatile long persistedIndex;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param tableStorage Table storage.
     * @param partitionId Partition id.
     */
    public RocksDbMvPartitionStorage(RocksDbTableStorage tableStorage, int partitionId) {
        this.tableStorage = tableStorage;
        this.partitionId = partitionId;
        db = tableStorage.db();
        cf = tableStorage.partitionCfHandle();
        meta = tableStorage.metaCfHandle();

        upperBound = new Slice(partitionEndPrefix());

        upperBoundReadOpts = new ReadOptions().setIterateUpperBound(upperBound);
        scanReadOptions = new ReadOptions().setIterateUpperBound(upperBound).setTotalOrderSeek(true);

        lastAppliedIndexKey = ("index" + partitionId).getBytes(StandardCharsets.UTF_8);
        lastAppliedTermKey = ("term" + partitionId).getBytes(StandardCharsets.UTF_8);
        lastGroupConfigKey = ("config" + partitionId).getBytes(StandardCharsets.UTF_8);

        lastAppliedIndex = readLastAppliedIndex(readOpts);
        lastAppliedTerm = readLastAppliedTerm(readOpts);
        lastGroupConfig = readLastGroupConfig(readOpts);

        persistedIndex = lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        if (threadLocalWriteBatch.get() != null) {
            return closure.execute();
        } else {
            if (!busyLock.enterBusy()) {
                throw new StorageClosedException();
            }

            try (var writeBatch = new WriteBatchWithIndex()) {
                threadLocalWriteBatch.set(writeBatch);

                pendingAppliedIndex = lastAppliedIndex;
                pendingAppliedTerm = lastAppliedTerm;
                pendingGroupConfig = lastGroupConfig;

                V res = closure.execute();

                try {
                    if (writeBatch.count() > 0) {
                        db.write(writeOpts, writeBatch);
                    }
                } catch (RocksDBException e) {
                    throw new StorageException("Unable to apply a write batch to RocksDB instance.", e);
                }

                lastAppliedIndex = pendingAppliedIndex;
                lastAppliedTerm = pendingAppliedTerm;
                lastGroupConfig = pendingGroupConfig;

                return res;
            } finally {
                threadLocalWriteBatch.set(null);

                busyLock.leaveBusy();
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> flush() {
        return busy(() -> tableStorage.awaitFlush(true));
    }

    /**
     * Returns the partition ID of the partition that this storage is responsible for.
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     * Returns a WriteBatch that can be used by the affiliated storage implementation (like indices) to maintain consistency when run
     * inside the {@link #runConsistently} method.
     */
    public WriteBatchWithIndex currentWriteBatch() {
        return requireWriteBatch();
    }

    @Override
    public long lastAppliedIndex() {
        return busy(() -> threadLocalWriteBatch.get() == null ? lastAppliedIndex : pendingAppliedIndex);
    }

    @Override
    public long lastAppliedTerm() {
        return busy(() -> threadLocalWriteBatch.get() == null ? lastAppliedTerm : pendingAppliedTerm);
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        busy(() -> lastAppliedBusy(lastAppliedIndex, lastAppliedTerm));
    }

    private Void lastAppliedBusy(long lastAppliedIndex, long lastAppliedTerm) throws StorageException {
        WriteBatchWithIndex writeBatch = requireWriteBatch();

        try {
            writeBatch.put(meta, lastAppliedIndexKey, ByteUtils.longToBytes(lastAppliedIndex));
            writeBatch.put(meta, lastAppliedTermKey, ByteUtils.longToBytes(lastAppliedTerm));

            pendingAppliedIndex = lastAppliedIndex;
            pendingAppliedTerm = lastAppliedTerm;

            return null;
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public long persistedIndex() {
        return busy(() -> persistedIndex);
    }

    @Override
    @Nullable
    public RaftGroupConfiguration committedGroupConfiguration() {
        return busy(() -> threadLocalWriteBatch.get() == null ? lastGroupConfig : pendingGroupConfig);
    }

    @Override
    public void committedGroupConfiguration(RaftGroupConfiguration config) {
        busy(() -> committedGroupConfigurationBusy(config));
    }

    private Void committedGroupConfigurationBusy(RaftGroupConfiguration config) {
        WriteBatchWithIndex writeBatch = requireWriteBatch();

        try {
            writeBatch.put(meta, lastGroupConfigKey, ByteUtils.toBytes(config));

            pendingGroupConfig = config;

            return null;
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    /**
     * Reads a value of {@link #lastAppliedIndex()} from the storage, avoiding memtable, and sets it as a new value of
     * {@link #persistedIndex()}.
     *
     * @throws StorageException If failed to read index from the storage.
     */
    public void refreshPersistedIndex() throws StorageException {
        if (!busyLock.enterBusy()) {
            // Don't throw the exception, there's no point in that.
            return;
        }

        try {
            persistedIndex = readLastAppliedIndex(persistedTierReadOpts);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Reads the value of {@link #lastAppliedIndex} from the storage.
     *
     * @param readOptions Read options to be used for reading.
     * @return The value of last applied index.
     */
    private long readLastAppliedIndex(ReadOptions readOptions) {
        return readLongFromMetaCf(lastAppliedIndexKey, readOptions);
    }

    /**
     * Reads the value of {@link #lastAppliedTerm} from the storage.
     *
     * @param readOptions Read options to be used for reading.
     * @return The value of last applied term.
     */
    private long readLastAppliedTerm(ReadOptions readOptions) {
        return readLongFromMetaCf(lastAppliedTermKey, readOptions);
    }

    private long readLongFromMetaCf(byte[] key, ReadOptions readOptions) {
        byte[] bytes;

        try {
            bytes = db.get(meta, readOptions, key);
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }

        return bytes == null ? 0 : bytesToLong(bytes);
    }

    /**
     * Reads the value of {@link #lastGroupConfig} from the storage.
     *
     * @param readOptions Read options to be used for reading.
     * @return Group configuration.
     */
    private RaftGroupConfiguration readLastGroupConfig(ReadOptions readOptions) {
        byte[] bytes;

        try {
            bytes = db.get(meta, readOptions, lastGroupConfigKey);
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }

        return bytes == null ? null : fromBytes(bytes);
    }

    @Override
    public @Nullable TableRow addWrite(RowId rowId, @Nullable TableRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws TxIdMismatchException, StorageException {
        return busy(() -> addWriteBusy(rowId, row, txId, commitTableId, commitPartitionId));
    }

    private @Nullable TableRow addWriteBusy(RowId rowId, @Nullable TableRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws TxIdMismatchException, StorageException {
        @SuppressWarnings("resource") WriteBatchWithIndex writeBatch = requireWriteBatch();

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        TableRow res = null;

        try {
            // Check concurrent transaction data.
            byte[] keyBufArray = keyBuf.array();

            byte[] keyBytes = copyOf(keyBufArray, ROW_PREFIX_SIZE);

            byte[] previousValue = writeBatch.getFromBatchAndDB(db, cf, readOpts, keyBytes);

            // Previous value must belong to the same transaction.
            if (previousValue != null) {
                validateTxId(previousValue, txId);

                res = wrapValueIntoTableRow(previousValue, true);
            }

            if (row == null) {
                // Write empty value as a tombstone.
                if (previousValue != null) {
                    // Reuse old array with transaction id already written to it.
                    writeBatch.put(cf, keyBytes, copyOf(previousValue, VALUE_HEADER_SIZE));
                } else {
                    byte[] valueHeaderBytes = new byte[VALUE_HEADER_SIZE];

                    putUuidToBytes(txId, valueHeaderBytes, TX_ID_OFFSET);
                    putUuidToBytes(commitTableId, valueHeaderBytes, TABLE_ID_OFFSET);
                    putShort(valueHeaderBytes, PARTITION_ID_OFFSET, (short) commitPartitionId);

                    writeBatch.put(cf, keyBytes, valueHeaderBytes);
                }
            } else {
                writeUnversioned(keyBufArray, row, txId, commitTableId, commitPartitionId);
            }
        } catch (RocksDBException e) {
            throw new StorageException("Failed to update a row in storage", e);
        }

        return res;
    }

    /**
     * Writes a tuple of transaction id and a row bytes, using "row prefix" of a key array as a storage key.
     *
     * @param keyArray Array that has partition id and row id in its prefix.
     * @param row Table row, not null.
     * @param txId Transaction id.
     * @throws RocksDBException If write failed.
     */
    private void writeUnversioned(byte[] keyArray, TableRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws RocksDBException {
        @SuppressWarnings("resource") WriteBatchWithIndex writeBatch = requireWriteBatch();

        byte[] rowBytes = rowBytes(row);

        ByteBuffer value = ByteBuffer.allocate(rowBytes.length + VALUE_HEADER_SIZE);
        byte[] array = value.array();

        putUuidToBytes(txId, array, TX_ID_OFFSET);
        putUuidToBytes(commitTableId, array, TABLE_ID_OFFSET);
        putShort(array, PARTITION_ID_OFFSET, (short) commitPartitionId);

        value.position(VALUE_OFFSET).put(rowBytes);

        // Write table row data as a value.
        writeBatch.put(cf, copyOf(keyArray, ROW_PREFIX_SIZE), value.array());
    }

    private static byte[] rowBytes(TableRow row) {
        //TODO IGNITE-16913 Add proper way to write row bytes into array without allocations.
        return row.bytes();
    }

    @Override
    public @Nullable TableRow abortWrite(RowId rowId) throws StorageException {
        return busy(() -> abortWriteBusy(rowId));
    }

    private @Nullable TableRow abortWriteBusy(RowId rowId) throws StorageException {
        WriteBatchWithIndex writeBatch = requireWriteBatch();

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try {
            byte[] keyBytes = copyOf(keyBuf.array(), ROW_PREFIX_SIZE);

            byte[] previousValue = writeBatch.getFromBatchAndDB(db, cf, readOpts, keyBytes);

            if (previousValue == null) {
                //the chain doesn't contain an uncommitted write intent
                return null;
            }

            // Perform unconditional remove for the key without associated timestamp.
            writeBatch.delete(cf, keyBytes);

            return wrapValueIntoTableRow(previousValue, true);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to roll back insert/update", e);
        }
    }

    @Override
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        busy(() -> commitWriteBusy(rowId, timestamp));
    }

    private Void commitWriteBusy(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        WriteBatchWithIndex writeBatch = requireWriteBatch();

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try {
            // Read a value associated with pending write.
            byte[] uncommittedKeyBytes = copyOf(keyBuf.array(), ROW_PREFIX_SIZE);

            byte[] valueBytes = writeBatch.getFromBatchAndDB(db, cf, readOpts, uncommittedKeyBytes);

            if (valueBytes == null) {
                //the chain doesn't contain an uncommitted write intent
                return null;
            }

            // Delete pending write.
            writeBatch.delete(cf, uncommittedKeyBytes);

            // Add timestamp to the key, and put the value back into the storage.
            putTimestamp(keyBuf, timestamp);

            writeBatch.put(cf, copyOf(keyBuf.array(), MAX_KEY_SIZE), copyOfRange(valueBytes, VALUE_HEADER_SIZE, valueBytes.length));

            return null;
        } catch (RocksDBException e) {
            throw new StorageException("Failed to commit row into storage", e);
        }
    }

    @Override
    public void addWriteCommitted(RowId rowId, @Nullable TableRow row, HybridTimestamp commitTimestamp) throws StorageException {
        busy(() -> addWriteCommittedBusy(rowId, row, commitTimestamp));
    }

    private Void addWriteCommittedBusy(RowId rowId, @Nullable TableRow row, HybridTimestamp commitTimestamp) throws StorageException {
        @SuppressWarnings("resource") WriteBatchWithIndex writeBatch = requireWriteBatch();

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);
        putTimestamp(keyBuf, commitTimestamp);

        //TODO IGNITE-16913 Add proper way to write row bytes into array without allocations.
        byte[] rowBytes = rowBytes(row);

        try {
            writeBatch.put(cf, copyOf(keyBuf.array(), MAX_KEY_SIZE), rowBytes);

            return null;
        } catch (RocksDBException e) {
            throw new StorageException("Failed to update a row in storage", e);
        }
    }

    @Override
    public ReadResult read(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        return busy(() -> readBusy(rowId, timestamp));
    }

    private ReadResult readBusy(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        if (rowId.partitionId() != partitionId) {
            throw new IllegalArgumentException(
                    String.format("RowId partition [%d] is not equal to storage partition [%d].", rowId.partitionId(), partitionId));
        }

        // We can read data outside of consistency closure. Batch is not required.
        WriteBatchWithIndex writeBatch = threadLocalWriteBatch.get();

        try (
                // Set next partition as an upper bound.
                RocksIterator baseIterator = db.newIterator(cf, upperBoundReadOpts);
                // "count()" check is mandatory. Write batch iterator without any updates just crashes everything.
                // It's not documented, but this is exactly how it should be used.
                RocksIterator seekIterator = writeBatch != null && writeBatch.count() > 0
                        ? writeBatch.newIteratorWithBase(cf, baseIterator)
                        : baseIterator
        ) {
            if (lookingForLatestVersions(timestamp)) {
                return readLatestVersion(rowId, seekIterator);
            } else {
                return readByTimestamp(seekIterator, rowId, timestamp);
            }
        }
    }

    private static boolean lookingForLatestVersions(HybridTimestamp timestamp) {
        return timestamp == HybridTimestamp.MAX_VALUE;
    }

    private ReadResult readLatestVersion(RowId rowId, RocksIterator seekIterator) {
        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        // Seek to the first appearance of row id if timestamp isn't set.
        // Since timestamps are sorted from newest to oldest, first occurrence will always be the latest version.
        // Unfortunately, copy here is unavoidable with current API.
        assert keyBuf.position() == ROW_PREFIX_SIZE;
        seekIterator.seek(copyOf(keyBuf.array(), ROW_PREFIX_SIZE));

        if (invalid(seekIterator)) {
            // No data at all.
            return ReadResult.empty(rowId);
        }

        ByteBuffer readKeyBuf = MV_KEY_BUFFER.get().position(0).limit(MAX_KEY_SIZE);

        int keyLength = seekIterator.key(readKeyBuf);

        if (!matches(rowId, readKeyBuf)) {
            // It is already a different row, so no version exists for our rowId.
            return ReadResult.empty(rowId);
        }

        boolean isWriteIntent = keyLength == ROW_PREFIX_SIZE;

        byte[] valueBytes = seekIterator.value();

        return readResultFromKeyAndValue(isWriteIntent, readKeyBuf, valueBytes);
    }

    private ReadResult readResultFromKeyAndValue(boolean isWriteIntent, ByteBuffer keyBuf, byte[] valueBytes) {
        assert valueBytes != null;

        RowId rowId = getRowId(keyBuf);

        if (!isWriteIntent) {
            // There is no write-intent, return latest committed row.
            return wrapCommittedValue(rowId, valueBytes, readTimestamp(keyBuf));
        }

        return wrapUncommittedValue(rowId, valueBytes, null);
    }

    /**
     * Finds a row by timestamp. See {@link MvPartitionStorage#read(RowId, HybridTimestamp)} for details.
     *
     * @param seekIterator Newly created seek iterator.
     * @param rowId Row id.
     * @param timestamp Timestamp.
     * @return Read result.
     */
    private ReadResult readByTimestamp(RocksIterator seekIterator, RowId rowId, HybridTimestamp timestamp) {
        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        // Put timestamp restriction according to N2O timestamps order.
        putTimestamp(keyBuf, timestamp);

        // This seek will either find a key with timestamp that's less or equal than required value, or a different key whatsoever.
        // It is guaranteed by descending order of timestamps.
        seekIterator.seek(keyBuf.array());

        return handleReadByTimestampIterator(seekIterator, rowId, timestamp, keyBuf);
    }

    /**
     * Walks "version chain" via the iterator to find a row by timestamp.
     * See {@link MvPartitionStorage#read(RowId, HybridTimestamp)} for details.
     *
     * @param seekIterator Iterator, on which seek operation was already performed.
     * @param rowId Row id.
     * @param timestamp Timestamp.
     * @param keyBuf Buffer with a key in it: partition id + row id + timestamp.
     * @return Read result.
     */
    private static ReadResult handleReadByTimestampIterator(RocksIterator seekIterator, RowId rowId, HybridTimestamp timestamp,
            ByteBuffer keyBuf) {
        // There's no guarantee that required key even exists. If it doesn't, then "seek" will point to a different key.
        // To avoid returning its value, we have to check that actual key matches what we need.
        // Here we prepare direct buffer to read key without timestamp. Shared direct buffer is used to avoid extra memory allocations.
        ByteBuffer foundKeyBuf = MV_KEY_BUFFER.get().position(0).limit(MAX_KEY_SIZE);

        int keyLength = 0;

        if (!invalid(seekIterator)) {
            keyLength = seekIterator.key(foundKeyBuf);
        }

        if (invalid(seekIterator) || !matches(rowId, foundKeyBuf)) {
            // There is no record older than timestamp.
            // There might be a write-intent which we should return.
            // Seek to *just* row id.
            seekIterator.seek(copyOf(keyBuf.array(), ROW_PREFIX_SIZE));

            if (invalid(seekIterator)) {
                // There are no writes with row id.
                return ReadResult.empty(rowId);
            }

            foundKeyBuf.position(0).limit(MAX_KEY_SIZE);
            keyLength = seekIterator.key(foundKeyBuf);

            if (!matches(rowId, foundKeyBuf)) {
                // There are no writes with row id.
                return ReadResult.empty(rowId);
            }

            byte[] valueBytes = seekIterator.value();

            boolean isWriteIntent = keyLength == ROW_PREFIX_SIZE;

            if (isWriteIntent) {
                // Let's check if there is a committed write.
                seekIterator.next();

                if (invalid(seekIterator)) {
                    // There are no committed writes, we can safely return write-intent.
                    return wrapUncommittedValue(rowId, valueBytes, null);
                }

                foundKeyBuf.position(0).limit(MAX_KEY_SIZE);
                seekIterator.key(foundKeyBuf);

                if (!matches(rowId, foundKeyBuf)) {
                    // There are no committed writes, we can safely return write-intent.
                    return wrapUncommittedValue(rowId, valueBytes, null);
                }
            }

            // There is a committed write, but it's more recent than our timestamp (because we didn't find it with first seek).
            return ReadResult.empty(rowId);
        } else {
            // Should not be write-intent, as we were seeking with the timestamp.
            assert keyLength == MAX_KEY_SIZE;

            HybridTimestamp rowTimestamp = readTimestamp(foundKeyBuf);

            byte[] valueBytes = seekIterator.value();

            if (rowTimestamp.equals(timestamp)) {
                // This is exactly the row we are looking for.
                return wrapCommittedValue(rowId, valueBytes, rowTimestamp);
            }

            // Let's check if there is more recent write. If it is a write-intent, then return write-intent.
            // If it is a committed write, then we are already in a right range.
            seekIterator.prev();

            if (invalid(seekIterator)) {
                // There is no more recent commits or write-intents.
                return wrapCommittedValue(rowId, valueBytes, rowTimestamp);
            }

            foundKeyBuf.position(0).limit(MAX_KEY_SIZE);
            keyLength = seekIterator.key(foundKeyBuf);

            if (!matches(rowId, foundKeyBuf)) {
                // There is no more recent commits or write-intents under this row id.
                return wrapCommittedValue(rowId, valueBytes, rowTimestamp);
            }

            boolean isWriteIntent = keyLength == ROW_PREFIX_SIZE;

            if (isWriteIntent) {
                return wrapUncommittedValue(rowId, seekIterator.value(), rowTimestamp);
            }

            return wrapCommittedValue(rowId, valueBytes, readTimestamp(foundKeyBuf));
        }
    }

    /**
     * Checks if row id matches the one written in the key buffer. Note: this operation changes the position in the buffer.
     *
     * @param rowId Row id.
     * @param keyBuf Key buffer.
     * @return {@code true} if row id matches the key buffer, {@code false} otherwise.
     */
    private static boolean matches(RowId rowId, ByteBuffer keyBuf) {
        // Comparison starts from the position of the row id.
        keyBuf.position(ROW_ID_OFFSET);

        return rowId.mostSignificantBits() == normalize(keyBuf.getLong()) && rowId.leastSignificantBits() == normalize(keyBuf.getLong());
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        return busy(() -> scanVersionsBusy(rowId));
    }

    private Cursor<ReadResult> scanVersionsBusy(RowId rowId) throws StorageException {
        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        byte[] lowerBound = copyOf(keyBuf.array(), ROW_PREFIX_SIZE);

        incrementRowId(keyBuf);

        Slice upperBound = new Slice(copyOf(keyBuf.array(), ROW_PREFIX_SIZE));

        var options = new ReadOptions().setIterateUpperBound(upperBound).setTotalOrderSeek(true);

        RocksIterator it = db.newIterator(cf, options);

        WriteBatchWithIndex writeBatch = threadLocalWriteBatch.get();

        if (writeBatch != null && writeBatch.count() > 0) {
            it = writeBatch.newIteratorWithBase(cf, it);
        }

        it.seek(lowerBound);

        return new BusyRocksIteratorAdapter<>(busyLock, it) {
            @Override
            protected ReadResult decodeEntry(byte[] key, byte[] value) {
                int keyLength = key.length;

                boolean isWriteIntent = keyLength == ROW_PREFIX_SIZE;

                return readResultFromKeyAndValue(isWriteIntent, ByteBuffer.wrap(key).order(KEY_BYTE_ORDER), value);
            }

            @Override
            protected void handleBusyFail() {
                throw new StorageClosedException();
            }

            @Override
            public void close() {
                super.close();

                RocksUtils.closeAll(options, upperBound);
            }
        };
    }

    // TODO: IGNITE-16914 Play with prefix settings and benchmark results.
    /** {@inheritDoc} */
    @Override
    public PartitionTimestampCursor scan(HybridTimestamp timestamp) throws StorageException {
        Objects.requireNonNull(timestamp, "timestamp is null");

        return busy(() -> {
            if (lookingForLatestVersions(timestamp)) {
                return new ScanLatestVersionsCursor();
            } else {
                return new ScanByTimestampCursor(timestamp);
            }
        });
    }

    private void setKeyBuffer(ByteBuffer keyBuf, RowId rowId, @Nullable HybridTimestamp timestamp) {
        keyBuf.putLong(ROW_ID_OFFSET, normalize(rowId.mostSignificantBits()));
        keyBuf.putLong(ROW_ID_OFFSET + Long.BYTES, normalize(rowId.leastSignificantBits()));

        if (timestamp != null) {
            putTimestamp(keyBuf.position(ROW_PREFIX_SIZE), timestamp);
        }

        keyBuf.position(0);
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) throws StorageException {
        return busy(() -> closestRowIdBusy(lowerBound));
    }

    private @Nullable RowId closestRowIdBusy(RowId lowerBound) throws StorageException {
        ByteBuffer keyBuf = prepareHeapKeyBuf(lowerBound).position(0).limit(ROW_PREFIX_SIZE);

        try (RocksIterator it = db.newIterator(cf, scanReadOptions)) {
            it.seek(keyBuf);

            if (!it.isValid()) {
                RocksUtils.checkIterator(it);

                return null;
            }

            ByteBuffer readKeyBuf = MV_KEY_BUFFER.get().position(0).limit(ROW_PREFIX_SIZE);

            it.key(readKeyBuf);

            return getRowId(readKeyBuf);
        } finally {
            keyBuf.limit(MAX_KEY_SIZE);
        }
    }

    private static void incrementRowId(ByteBuffer buf) {
        long lsb = 1 + buf.getLong(ROW_ID_OFFSET + Long.BYTES);

        buf.putLong(ROW_ID_OFFSET + Long.BYTES, lsb);

        if (lsb != 0L) {
            return;
        }

        long msb = 1 + buf.getLong(ROW_ID_OFFSET);

        buf.putLong(ROW_ID_OFFSET, msb);

        if (msb != 0L) {
            return;
        }

        short partitionId = (short) (1 + buf.getShort(0));

        assert partitionId != 0;

        buf.putShort(0, partitionId);

        buf.position(0);
    }

    private RowId getRowId(ByteBuffer keyBuffer) {
        keyBuffer.position(ROW_ID_OFFSET);

        return new RowId(partitionId, normalize(keyBuffer.getLong()), normalize(keyBuffer.getLong()));
    }

    @Override
    public long rowsCount() {
        return busy(this::rowsCountBusy);
    }

    private long rowsCountBusy() {
        try (
                var upperBound = new Slice(partitionEndPrefix());
                var options = new ReadOptions().setIterateUpperBound(upperBound);
                RocksIterator it = db.newIterator(cf, options)
        ) {
            it.seek(partitionStartPrefix());

            long size = 0;

            while (it.isValid()) {
                ++size;
                it.next();
            }

            return size;
        }
    }

    /**
     * Deletes partition data from the storage, using write batch to perform the operation.
     */
    public void destroyData(WriteBatch writeBatch) throws RocksDBException {
        writeBatch.delete(meta, lastAppliedIndexKey);
        writeBatch.delete(meta, lastAppliedTermKey);
        writeBatch.delete(meta, lastGroupConfigKey);

        writeBatch.delete(meta, RocksDbMetaStorage.partitionIdKey(partitionId));

        writeBatch.deleteRange(cf, partitionStartPrefix(), partitionEndPrefix());
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        RocksUtils.closeAll(persistedTierReadOpts, readOpts, writeOpts, scanReadOptions, upperBoundReadOpts, upperBound);
    }

    private WriteBatchWithIndex requireWriteBatch() {
        WriteBatchWithIndex writeBatch = threadLocalWriteBatch.get();

        if (writeBatch == null) {
            throw new StorageException("Attempting to write data outside of data access closure.");
        }

        return writeBatch;
    }

    /**
     * Prepares thread-local on-heap byte buffer. Writes row id in it. Partition id is already there. Timestamp is not cleared.
     */
    private ByteBuffer prepareHeapKeyBuf(RowId rowId) {
        assert rowId.partitionId() == partitionId : rowId;

        ByteBuffer keyBuf = HEAP_KEY_BUFFER.get().position(0);

        keyBuf.putShort((short) rowId.partitionId());
        keyBuf.putLong(normalize(rowId.mostSignificantBits()));
        keyBuf.putLong(normalize(rowId.leastSignificantBits()));

        return keyBuf;
    }

    /**
     * Converts signed long into a new long value, that when written in Big Endian, will preserve the comparison order if compared
     * lexicographically as an array of unsigned bytes. For example, values {@code -1} and {@code 0}, when written in BE, will become
     * {@code 0xFF..F} and {@code 0x00..0}, and lose their ascending order.
     *
     * <p/>Flipping the sign bit will change the situation: {@code -1 -> 0x7F..F} and {@code 0 -> 0x80..0}.
     */
    private static long normalize(long value) {
        return value ^ (1L << 63);
    }

    /**
     * Writes a timestamp into a byte buffer, in descending lexicographical bytes order.
     */
    private static void putTimestamp(ByteBuffer buf, HybridTimestamp ts) {
        assert buf.order() == KEY_BYTE_ORDER;

        // "bitwise negation" turns ascending order into a descending one.
        buf.putLong(~ts.getPhysical());
        buf.putInt(~ts.getLogical());
    }

    private static HybridTimestamp readTimestamp(ByteBuffer keyBuf) {
        assert keyBuf.order() == KEY_BYTE_ORDER;

        long physical = ~keyBuf.getLong(ROW_PREFIX_SIZE);
        int logical = ~keyBuf.getInt(ROW_PREFIX_SIZE + Long.BYTES);

        return new HybridTimestamp(physical, logical);
    }

    private static void putShort(byte[] array, int off, short value) {
        GridUnsafe.putShort(array, GridUnsafe.BYTE_ARR_OFF + off, value);
    }

    private static void validateTxId(byte[] valueBytes, UUID txId) {
        long msb = bytesToLong(valueBytes);
        long lsb = bytesToLong(valueBytes, Long.BYTES);

        if (txId.getMostSignificantBits() != msb || txId.getLeastSignificantBits() != lsb) {
            throw new TxIdMismatchException(txId, new UUID(msb, lsb));
        }
    }

    /**
     * Checks iterator validity, including both finished iteration and occurred exception.
     */
    private static boolean invalid(RocksIterator it) {
        boolean invalid = !it.isValid();

        if (invalid) {
            // Check the status first. This operation is guaranteed to throw if an internal error has occurred during
            // the iteration. Otherwise, we've exhausted the data range.
            try {
                it.status();
            } catch (RocksDBException e) {
                throw new StorageException("Failed to read data from storage", e);
            }
        }

        return invalid;
    }

    /**
     * Converts raw byte array representation of the value into a table row.
     *
     * @param valueBytes Value bytes as read from the storage.
     * @param valueHasTxData Whether the value has a transaction id prefix in it.
     * @return Table row instance or {@code null} if value is a tombstone.
     */
    private static @Nullable TableRow wrapValueIntoTableRow(byte[] valueBytes, boolean valueHasTxData) {
        if (isTombstone(valueBytes, valueHasTxData)) {
            return null;
        }

        return valueHasTxData
                ? new TableRow(ByteBuffer.wrap(valueBytes).position(VALUE_OFFSET).slice().order(TABLE_ROW_BYTE_ORDER))
                : new TableRow(valueBytes);
    }

    /**
     * Converts raw byte array representation of the write-intent value into a read result adding newest commit timestamp if
     * it is not {@code null}.
     *
     * @param rowId ID of the corresponding row.
     * @param valueBytes Value bytes as read from the storage.
     * @param newestCommitTs Commit timestamp of the most recent committed write of this value.
     * @return Read result instance.
     */
    private static ReadResult wrapUncommittedValue(RowId rowId, byte[] valueBytes, @Nullable HybridTimestamp newestCommitTs) {
        UUID txId = bytesToUuid(valueBytes, TX_ID_OFFSET);
        UUID commitTableId = bytesToUuid(valueBytes, TABLE_ID_OFFSET);
        int commitPartitionId = GridUnsafe.getShort(valueBytes, GridUnsafe.BYTE_ARR_OFF + PARTITION_ID_OFFSET) & 0xFFFF;

        TableRow row;

        if (isTombstone(valueBytes, true)) {
            row = null;
        } else {
            row = new TableRow(ByteBuffer.wrap(valueBytes).position(VALUE_OFFSET).slice().order(TABLE_ROW_BYTE_ORDER));
        }

        return ReadResult.createFromWriteIntent(rowId, row, txId, commitTableId, commitPartitionId, newestCommitTs);
    }

    /**
     * Converts raw byte array representation of the value into a read result.
     *
     * @param rowId ID of the corresponding row.
     * @param valueBytes Value bytes as read from the storage.
     * @param rowCommitTimestamp Timestamp with which the row was committed.
     * @return Read result instance or {@code null} if value is a tombstone.
     */
    private static ReadResult wrapCommittedValue(RowId rowId, byte[] valueBytes, HybridTimestamp rowCommitTimestamp) {
        if (isTombstone(valueBytes, false)) {
            return ReadResult.empty(rowId);
        }

        return ReadResult.createFromCommitted(rowId, new TableRow(valueBytes), rowCommitTimestamp);
    }

    /**
     * Creates a prefix of all keys in the given partition.
     */
    public byte[] partitionStartPrefix() {
        return unsignedShortAsBytes(partitionId);
    }

    /**
     * Creates a prefix of all keys in the next partition, used as an exclusive bound.
     */
    public byte[] partitionEndPrefix() {
        return unsignedShortAsBytes(partitionId + 1);
    }

    private static byte[] unsignedShortAsBytes(int value) {
        return new byte[] {(byte) (value >>> 8), (byte) value};
    }

    /**
     * Returns {@code true} if value payload represents a tombstone.
     */
    private static boolean isTombstone(byte[] valueBytes, boolean hasTxId) {
        return valueBytes.length == (hasTxId ? VALUE_HEADER_SIZE : 0);
    }

    private abstract class BasePartitionTimestampCursor implements PartitionTimestampCursor {
        protected final RocksIterator it = db.newIterator(cf, scanReadOptions);

        // Here's seek buffer itself. Originally it contains a valid partition id, row id payload that's filled with zeroes, and maybe
        // a timestamp value. Zero row id guarantees that it's lexicographically less than or equal to any other row id stored in the
        // partition.
        // Byte buffer from a thread-local field can't be used here, because of two reasons:
        //  - no one guarantees that there will only be a single cursor;
        //  - no one guarantees that returned cursor will not be used by other threads.
        // The thing is, we need this buffer to preserve its content between invocations of "hasNext" method.
        protected final ByteBuffer seekKeyBuf = ByteBuffer.allocate(MAX_KEY_SIZE).order(KEY_BYTE_ORDER).putShort((short) partitionId);

        protected RowId currentRowId;

        /** Cached value for {@link #next()} method. Also optimizes the code of {@link #hasNext()}. */
        protected ReadResult next;

        protected abstract boolean hasNextBusy();

        @Override
        public boolean hasNext() {
            return busy(this::hasNextBusy);
        }

        @Override
        public @Nullable TableRow committed(HybridTimestamp timestamp) {
            return busy(() -> committedBusy(timestamp));
        }

        private @Nullable TableRow committedBusy(HybridTimestamp timestamp) {
            Objects.requireNonNull(timestamp, "timestamp is null");

            if (currentRowId == null) {
                throw new IllegalStateException("currentRowId is null");
            }

            setKeyBuffer(seekKeyBuf, currentRowId, timestamp);

            it.seek(seekKeyBuf.array());

            ReadResult readResult = handleReadByTimestampIterator(it, currentRowId, timestamp, seekKeyBuf);

            if (readResult.isEmpty()) {
                return null;
            }

            return readResult.tableRow();
        }

        @Override
        public final ReadResult next() {
            return busy(this::nextBusy);
        }

        private ReadResult nextBusy() {
            if (!hasNextBusy()) {
                throw new NoSuchElementException();
            }

            ReadResult res = next;

            next = null;

            return res;
        }

        @Override
        public final void close() {
            it.close();
        }
    }

    private final class ScanLatestVersionsCursor extends BasePartitionTimestampCursor {
        @Override
        public boolean hasNextBusy() {
            // Fast-path for consecutive invocations.
            if (next != null) {
                return true;
            }

            if (currentRowId != null) {
                setKeyBuffer(seekKeyBuf, currentRowId, null);
                incrementRowId(seekKeyBuf);
            }

            currentRowId = null;

            // Prepare direct buffer slice to read keys from the iterator.
            ByteBuffer currentKeyBuffer = MV_KEY_BUFFER.get().position(0);

            while (true) {
                currentKeyBuffer.position(0);

                // At this point, seekKeyBuf should contain row id that's above the one we already scanned, but not greater than any
                // other row id in partition. When we start, row id is filled with zeroes. Value during the iteration is described later
                // in this code. Now let's describe what we'll find, assuming that iterator found something:
                //  - if timestamp is null:
                //      - this seek will find the newest version of the next row in iterator. Exactly what we need.
                //  - if timestamp is not null:
                //      - suppose that seek key buffer has the following value: "| P0 | R0 | T0 |" (partition, row id, timestamp)
                //        and iterator finds something, let's denote it as "| P0 | R1 | T1 |" (partition must match). Again, there are
                //        few possibilities here:
                //          - R1 == R0, this means a match. By the rules of ordering we derive that T1 >= T0. Timestamps are stored in
                //            descending order, this means that we found exactly what's needed.
                //          - R1 > R0, this means that we found next row and T1 is either missing (pending row) or represents the latest
                //            version of the row. It doesn't matter in this case, because this row id will be reused to find its value
                //            at time T0. Additional "seek" will be required to do it.
                //TODO IGNITE-18201 Remove copying.
                it.seek(copyOf(seekKeyBuf.array(), ROW_PREFIX_SIZE));

                // Finish scan if nothing was found.
                if (invalid(it)) {
                    return false;
                }

                // Read the actual key into a direct buffer.
                int keyLength = it.key(currentKeyBuffer.limit(MAX_KEY_SIZE));

                boolean isWriteIntent = keyLength == ROW_PREFIX_SIZE;

                currentKeyBuffer.limit(ROW_PREFIX_SIZE);

                RowId rowId = getRowId(currentKeyBuffer);

                // Copy actual row id into a "seekKeyBuf" buffer.
                seekKeyBuf.putLong(ROW_ID_OFFSET, normalize(rowId.mostSignificantBits()));
                seekKeyBuf.putLong(ROW_ID_OFFSET + Long.BYTES, normalize(rowId.leastSignificantBits()));

                // This one might look tricky. We finished processing next row. There are three options:
                //  - "found" flag is false - there's no fitting version of the row. We'll continue to next iteration;
                //  - value is empty, we found a tombstone. We'll continue to next iteration as well;
                //  - value is not empty and everything's good. We'll cache it and return from method.
                // In all three cases we need to prepare the value of "seekKeyBuf" so that it has not-yet-scanned row id in it.
                // the only valid way to do so is to treat row id payload as one big unsigned integer in Big Endian and increment it.
                // It's important to note that increment may overflow. In this case "carry flag" will go into incrementing partition id.
                // This is fine for three reasons:
                //  - iterator has an upper bound, following "seek" will result in invalid iterator state.
                //  - partition id itself cannot be overflown, because it's limited with a constant less than 0xFFFF.
                // It's something like 65500, I think.
                //  - "seekKeyBuf" buffer value will not be used after that, so it's ok if we corrupt its data (in every other instance,
                //    buffer starts with a valid partition id, which is set during buffer's initialization).
                incrementRowId(seekKeyBuf);

                // Cache row and return "true" if it's found and not a tombstone.
                byte[] valueBytes = it.value();

                HybridTimestamp nextCommitTimestamp = null;

                if (isWriteIntent) {
                    it.next();

                    if (!invalid(it)) {
                        ByteBuffer key = ByteBuffer.wrap(it.key()).order(KEY_BYTE_ORDER);

                        if (matches(rowId, key)) {
                            // This is a next version of current row.
                            nextCommitTimestamp = readTimestamp(key);
                        }
                    }
                }

                currentKeyBuffer.limit(keyLength);

                assert valueBytes != null;

                ReadResult readResult;

                if (!isWriteIntent) {
                    // There is no write-intent, return latest committed row.
                    readResult = wrapCommittedValue(rowId, valueBytes, readTimestamp(currentKeyBuffer));
                } else {
                    readResult = wrapUncommittedValue(rowId, valueBytes, nextCommitTimestamp);
                }

                if (!readResult.isEmpty() || readResult.isWriteIntent()) {
                    next = readResult;
                    currentRowId = rowId;

                    return true;
                }
            }
        }
    }

    private final class ScanByTimestampCursor extends BasePartitionTimestampCursor {
        private final HybridTimestamp timestamp;

        public ScanByTimestampCursor(HybridTimestamp timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public boolean hasNextBusy() {
            // Fast-path for consecutive invocations.
            if (next != null) {
                return true;
            }

            if (currentRowId != null) {
                setKeyBuffer(seekKeyBuf, currentRowId, timestamp);
                incrementRowId(seekKeyBuf);
            }

            currentRowId = null;

            // Prepare direct buffer slice to read keys from the iterator.
            ByteBuffer directBuffer = MV_KEY_BUFFER.get().position(0);

            while (true) {
                //TODO IGNITE-18201 Remove copying.
                it.seek(copyOf(seekKeyBuf.array(), ROW_PREFIX_SIZE));

                if (invalid(it)) {
                    return false;
                }

                // We need to figure out what current row id is.
                it.key(directBuffer.position(0));

                RowId rowId = getRowId(directBuffer);

                setKeyBuffer(seekKeyBuf, rowId, timestamp);

                // Seek to current row id + timestamp.
                it.seek(seekKeyBuf.array());

                ReadResult readResult = handleReadByTimestampIterator(it, rowId, timestamp, seekKeyBuf);

                if (readResult.isEmpty() && !readResult.isWriteIntent()) {
                    // Seek to next row id as we found nothing that matches.
                    incrementRowId(seekKeyBuf);

                    continue;
                }

                next = readResult;
                currentRowId = rowId;

                return true;
            }
        }
    }

    private <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throw new StorageClosedException();
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }
}
