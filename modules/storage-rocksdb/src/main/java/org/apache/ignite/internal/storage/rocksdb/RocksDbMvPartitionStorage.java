/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.rocksdb;

import static java.lang.ThreadLocal.withInitial;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.copyOf;
import static java.util.Arrays.copyOfRange;
import static org.rocksdb.ReadTier.PERSISTED_TIER;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
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
 * Multi-versioned partition storage implementation based on RocksDB. Stored data has the following format:
 * <pre><code>
 * | partId (2 bytes, BE) | rowId (16 bytes, BE) |</code></pre>
 * or
 * <pre><code>
 * | partId (2 bytes, BE) | rowId (16 bytes, BE) | timestamp (16 bytes, DESC) |</code></pre>
 * depending on transaction status. Pending transactions data doesn't have a timestamp assigned.
 *
 * <p/>BE means Big Endian, meaning that lexicographical bytes order matches a natural order of partitions.
 *
 * <p/>DESC means that timestamps are sorted from newest to oldest (N2O). Please refer to {@link #putTimestamp(ByteBuffer, Timestamp)} to
 * see how it's achieved. Missing timestamp could be interpreted as a moment infinitely far away in the future.
 */
public class RocksDbMvPartitionStorage implements MvPartitionStorage {
    /** Position of row id inside of the key. */
    private static final int ROW_ID_OFFSET = Short.BYTES;

    /** UUID size in bytes. */
    private static final int ROW_ID_SIZE = 2 * Long.BYTES;

    /** Size of the key without timestamp. */
    private static final int ROW_PREFIX_SIZE = ROW_ID_OFFSET + ROW_ID_SIZE;

    /** Timestamp size in bytes. */
    private static final int TIMESTAMP_SIZE = 2 * Long.BYTES;

    /** Transaction id size. Matches timestamp size. */
    private static final int TX_ID_SIZE = TIMESTAMP_SIZE;

    /** Maximum size of the key. */
    private static final int MAX_KEY_SIZE = ROW_PREFIX_SIZE + TIMESTAMP_SIZE;

    /** Thread-local direct buffer instance to read keys from RocksDB. */
    private static final ThreadLocal<ByteBuffer> MV_KEY_BUFFER = withInitial(() -> allocateDirect(MAX_KEY_SIZE).order(BIG_ENDIAN));

    /** Thread-local write batch for {@link #runConsistently(WriteClosure)}. */
    private static final ThreadLocal<WriteBatchWithIndex> WRITE_BATCH = new ThreadLocal<>();

    /** Thread-local on-heap byte buffer instance to use for key manipulations. */
    private static final ThreadLocal<ByteBuffer> HEAP_KEY_BUFFER = withInitial(
            () -> ByteBuffer.allocate(MAX_KEY_SIZE).order(BIG_ENDIAN)
    );

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

    /** Key to store applied index value in meta. */
    private final byte[] lastAppliedIndexKey;

    /** On-heap-cached last applied index value. */
    private volatile long lastAppliedIndex;

    private volatile long pendingAppliedIndex;

    /** The value of {@link #lastAppliedIndex} persisted to the device at this moment. */
    private volatile long persistedIndex;

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

        lastAppliedIndexKey = ("index" + partitionId).getBytes(StandardCharsets.UTF_8);

        lastAppliedIndex = readLastAppliedIndex(readOpts);

        persistedIndex = lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public <V> V runConsistently(WriteClosure<V> closure) throws StorageException {
        if (WRITE_BATCH.get() != null) {
            return closure.execute();
        } else {
            try (var writeBatch = new WriteBatchWithIndex()) {
                WRITE_BATCH.set(writeBatch);

                pendingAppliedIndex = lastAppliedIndex;

                V res = closure.execute();

                try {
                    db.write(writeOpts, writeBatch);
                } catch (RocksDBException e) {
                    throw new StorageException("Unable to apply a write batch to RocksDB instance.", e);
                }

                lastAppliedIndex = pendingAppliedIndex;

                return res;
            } finally {
                WRITE_BATCH.set(null);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> flush() {
        return tableStorage.awaitFlush(true);
    }

    /** {@inheritDoc} */
    @Override
    public long lastAppliedIndex() {
        return WRITE_BATCH.get() == null ? lastAppliedIndex : pendingAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public void lastAppliedIndex(long lastAppliedIndex) throws StorageException {
        WriteBatchWithIndex writeBatch = requireWriteBatch();

        try {
            writeBatch.put(meta, lastAppliedIndexKey, ByteUtils.longToBytes(lastAppliedIndex));

            pendingAppliedIndex = lastAppliedIndex;
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long persistedIndex() {
        return persistedIndex;
    }

    /**
     * Reads a value of {@link #lastAppliedIndex()} from the storage, avoiding memtable, and sets it as a new value of
     * {@link #persistedIndex()}.
     *
     * @throws StorageException If failed to read index from the storage.
     */
    public void refreshPersistedIndex() throws StorageException {
        persistedIndex = readLastAppliedIndex(persistedTierReadOpts);
    }

    /**
     * Reads the value of {@link #lastAppliedIndex} from the storage.
     *
     * @param readOptions Read options to be used for reading.
     * @return The value of last applied index.
     */
    private long readLastAppliedIndex(ReadOptions readOptions) {
        byte[] appliedIndexBytes;

        try {
            appliedIndexBytes = db.get(meta, readOptions, lastAppliedIndexKey);
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }

        return appliedIndexBytes == null ? 0 : ByteUtils.bytesToLong(appliedIndexBytes);
    }

    /** {@inheritDoc} */
    @Override
    public RowId insert(BinaryRow row, UUID txId) throws StorageException {
        RowId rowId = new RowId(partitionId);

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try {
            writeUnversioned(keyBuf.array(), row, txId);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to insert new row into storage", e);
        }

        return rowId;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId)
            throws TxIdMismatchException, StorageException {
        WriteBatchWithIndex writeBatch = requireWriteBatch();

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        BinaryRow res = null;

        try {
            // Check concurrent transaction data.
            byte[] keyBufArray = keyBuf.array();

            byte[] previousValue = writeBatch.getFromBatchAndDB(db, cf, readOpts, copyOf(keyBufArray, ROW_PREFIX_SIZE));

            // Previous value must belong to the same transaction.
            if (previousValue != null) {
                validateTxId(previousValue, txId);

                res = wrapValueIntoBinaryRow(previousValue, true);
            }

            if (row == null) {
                // Write empty value as a tombstone.
                if (previousValue != null) {
                    // Reuse old array with transaction id already written to it.
                    writeBatch.put(cf, copyOf(keyBufArray, ROW_PREFIX_SIZE), copyOf(previousValue, TX_ID_SIZE));
                } else {
                    // Use tail of the key buffer to save on array allocations.
                    putTransactionId(keyBufArray, ROW_PREFIX_SIZE, txId);

                    writeBatch.put(cf, copyOf(keyBufArray, ROW_PREFIX_SIZE), copyOfRange(keyBufArray, ROW_PREFIX_SIZE, MAX_KEY_SIZE));
                }
            } else {
                writeUnversioned(keyBufArray, row, txId);
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
     * @param row Binary row, not null.
     * @param txId Transaction id.
     * @throws RocksDBException If write failed.
     */
    private void writeUnversioned(byte[] keyArray, BinaryRow row, UUID txId) throws RocksDBException {
        WriteBatchWithIndex writeBatch = requireWriteBatch();

        //TODO IGNITE-16913 Add proper way to write row bytes into array without allocations.
        byte[] rowBytes = row.bytes();

        ByteBuffer value = ByteBuffer.allocate(rowBytes.length + TX_ID_SIZE).order(LITTLE_ENDIAN);

        putTransactionId(value.array(), 0, txId);

        value.position(TX_ID_SIZE).put(rowBytes);

        // Write binary row data as a value.
        writeBatch.put(cf, copyOf(keyArray, ROW_PREFIX_SIZE), copyOf(value.array(), value.capacity()));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        WriteBatchWithIndex writeBatch = requireWriteBatch();

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try {
            byte[] previousValue = writeBatch.getFromBatchAndDB(db, cf, readOpts, copyOf(keyBuf.array(), ROW_PREFIX_SIZE));

            if (previousValue == null) {
                //the chain doesn't contain an uncommitted write intent
                return null;
            }

            // Perform unconditional remove for the key without associated timestamp.
            writeBatch.delete(cf, copyOf(keyBuf.array(), ROW_PREFIX_SIZE));

            return wrapValueIntoBinaryRow(previousValue, true);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to roll back insert/update", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void commitWrite(RowId rowId, Timestamp timestamp) throws StorageException {
        WriteBatchWithIndex writeBatch = requireWriteBatch();

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try {
            // Read a value associated with pending write.
            byte[] valueBytes = writeBatch.getFromBatchAndDB(db, cf, readOpts, copyOf(keyBuf.array(), ROW_PREFIX_SIZE));

            if (valueBytes == null) {
                //the chain doesn't contain an uncommitted write intent
                return;
            }

            // Delete pending write.
            writeBatch.delete(cf, copyOf(keyBuf.array(), ROW_PREFIX_SIZE));

            // Add timestamp to the key, and put the value back into the storage.
            putTimestamp(keyBuf, timestamp);

            writeBatch.put(cf, copyOf(keyBuf.array(), MAX_KEY_SIZE), copyOfRange(valueBytes, TX_ID_SIZE, valueBytes.length));
        } catch (RocksDBException e) {
            throw new StorageException("Failed to commit row into storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow read(RowId rowId, UUID txId) throws TxIdMismatchException, StorageException {
        return read(rowId, null, txId);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow read(RowId rowId, Timestamp timestamp) throws StorageException {
        return read(rowId, timestamp, null);
    }

    private @Nullable BinaryRow read(RowId rowId, @Nullable Timestamp timestamp, @Nullable UUID txId)
            throws TxIdMismatchException, StorageException {
        assert timestamp == null ^ txId == null;

        if (rowId.partitionId() != partitionId) {
            return null;
        }

        // We can read data outside of consistency closure. Batch is not required.
        WriteBatchWithIndex writeBatch = WRITE_BATCH.get();

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try (
                // Set next partition as an upper bound.
                var readOpts = new ReadOptions().setIterateUpperBound(upperBound);
                RocksIterator baseIterator = db.newIterator(cf, readOpts);
                // "count()" check is mandatory. Write batch iterator without any updates just crashes everything.
                // It's not documented, but this is exactly how it should be used.
                RocksIterator seekIterator = writeBatch != null && writeBatch.count() > 0
                        ? writeBatch.newIteratorWithBase(cf, baseIterator)
                        : baseIterator
        ) {
            if (timestamp == null) {
                // Seek to the first appearance of row id if timestamp isn't set.
                // Since timestamps are sorted from newest to oldest, first occurance will always be the latest version.
                // Unfortunately, copy here is unavoidable with current API.
                seekIterator.seek(copyOf(keyBuf.array(), keyBuf.position()));
            } else {
                // Put timestamp restriction according to N2O timestamps order.
                putTimestamp(keyBuf, timestamp);

                // This seek will either find a key with timestamp that's less or equal than required value, or a different key whatsoever.
                // It is guaranteed by descending order of timestamps.
                seekIterator.seek(keyBuf.array());
            }

            // Return null if nothing was found.
            if (invalid(seekIterator)) {
                return null;
            }

            // There's no guarantee that required key even exists. If it doesn't, then "seek" will point to a different key, obviously.
            // To avoid returning its value, we have to check that actual key matches what we need.
            // Here we prepare direct buffer to read key without timestamp. Shared direct buffer is used to avoid extra memory allocations.
            ByteBuffer directBuffer = MV_KEY_BUFFER.get().position(0).limit(MAX_KEY_SIZE);

            int keyLength = seekIterator.key(directBuffer);

            boolean valueHasTxId = keyLength == ROW_PREFIX_SIZE;

            // Comparison starts from the position of the row id.
            directBuffer.position(ROW_ID_OFFSET);

            // Return null if seek found a wrong key.
            if (!matches(rowId, directBuffer)) {
                return null;
            }

            // Get binary row from the iterator. It has the exact payload that we need.
            byte[] valueBytes = seekIterator.value();

            assert valueBytes != null;

            if (txId != null && valueHasTxId) {
                validateTxId(valueBytes, txId);
            }

            return wrapValueIntoBinaryRow(valueBytes, valueHasTxId);
        }
    }

    private static boolean matches(RowId rowId, ByteBuffer buf) {
        return rowId.mostSignificantBits() == buf.getLong() && rowId.leastSignificantBits() == buf.getLong();
    }

    //TODO IGNITE-16914 Play with prefix settings and benchmark results.
    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, UUID txId) throws TxIdMismatchException, StorageException {
        return scan(keyFilter, null, txId);
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, Timestamp timestamp) throws StorageException {
        return scan(keyFilter, timestamp, null);
    }

    private Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, @Nullable Timestamp timestamp, @Nullable UUID txId)
            throws TxIdMismatchException, StorageException {
        assert timestamp == null ^ txId == null;

        // Set next partition as an upper bound.
        var options = new ReadOptions().setIterateUpperBound(upperBound).setTotalOrderSeek(true);

        RocksIterator it = db.newIterator(cf, options);

        // Seek iterator to the beginning of the partition.
        it.seek(partitionStartPrefix());

        // Size of the buffer to seek values. Should fit partition id, row id and maybe timestamp, if it's not null.
        int seekKeyBufSize = ROW_PREFIX_SIZE + (timestamp == null ? 0 : TIMESTAMP_SIZE);

        // Here's seek buffer itself. Originally it contains a valid partition id, row id payload that's filled with zeroes, and maybe
        // a timestamp value. Zero row id guarantees that it's lexicographically less then or equal to any other row id stored in the
        // partition.
        // Byte buffer from a thread-local field can't be used here, because of two reasons:
        //  - no one guarantees that there will only be a single cursor;
        //  - no one guarantees that returned cursor will not be used by other threads.
        // The thing is, we need this buffer to preserve its content between invocactions of "hasNext" method.
        ByteBuffer seekKeyBuf = ByteBuffer.allocate(seekKeyBufSize).order(BIG_ENDIAN).putShort((short) partitionId);

        if (timestamp != null) {
            putTimestamp(seekKeyBuf.position(ROW_PREFIX_SIZE), timestamp);
        }

        // Version without timestamp to compare row ids.
        ByteBuffer seekKeyBufSliceWithoutTimestamp = timestamp == null
                ? seekKeyBuf
                : seekKeyBuf.position(0).limit(ROW_PREFIX_SIZE).slice().order(BIG_ENDIAN);

        return new Cursor<>() {
            /** Cached value for {@link #next()} method. Also optimizes the code of {@link #hasNext()}. */
            private BinaryRow next;

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                // Fastpath for consecutive invocations.
                if (next != null) {
                    return true;
                }

                // Prepare direct buffer slice to read keys from the iterator.
                ByteBuffer directBuffer = MV_KEY_BUFFER.get().position(0);

                while (true) {
                    // This flag is used to skip row ids that were created after required timestamp.
                    boolean found = true;

                    // We should do after each seek. Here in particular it means one of two things:
                    //  - partition is empty;
                    //  - iterator exhausted all the data in partition.
                    if (invalid(it)) {
                        return false;
                    }

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
                    it.seek(seekKeyBuf.array());

                    // Finish scan if nothing was found.
                    if (invalid(it)) {
                        return false;
                    }

                    // Read the actual key into a direct buffer.
                    int keyLength = it.key(directBuffer.limit(MAX_KEY_SIZE));

                    boolean valueHasTxId = keyLength == ROW_PREFIX_SIZE;

                    directBuffer.limit(ROW_PREFIX_SIZE);

                    boolean wrongRowIdWasFound = !directBuffer.equals(seekKeyBufSliceWithoutTimestamp);

                    // To understand this condition please read that huge comment above.
                    if (timestamp == null || wrongRowIdWasFound) {
                        // Copy actual row id into a "seekKeyBuf" buffer.
                        GridUnsafe.copyMemory(
                                null, GridUnsafe.bufferAddress(directBuffer) + ROW_ID_OFFSET,
                                seekKeyBuf.array(), GridUnsafe.BYTE_ARR_OFF + ROW_ID_OFFSET,
                                ROW_ID_SIZE
                        );
                    }

                    // Perform additional "seek" if timestamp is not null. Motivation for it is described in comments above.
                    if (timestamp != null && wrongRowIdWasFound) {
                        // At this point, "seekKeyBuf" has row id that exists in partition.
                        it.seek(seekKeyBuf.array());

                        // Iterator may not be valid if that row was created after required timestamp.
                        if (invalid(it)) {
                            return false;
                        }

                        // Or iterator may still be valid even if there's no version for required timestamp. In this case row id
                        // itself will be different and we must check it.
                        keyLength = it.key(directBuffer.limit(MAX_KEY_SIZE));

                        valueHasTxId = keyLength == ROW_PREFIX_SIZE;

                        directBuffer.limit(ROW_PREFIX_SIZE);

                        if (!directBuffer.equals(seekKeyBufSliceWithoutTimestamp)) {
                            found = false;
                        }
                    }

                    // This one might look tricky. We finished processing next row. There are three options:
                    //  - "found" flag is false - there's no fitting version of the row. We'll continue to next iteration;
                    //  - value is empty, we found a tombstone. We'll continue to next iteration as well;
                    //  - value is not empty and everything's good. We'll cache it and return from method.
                    // In all three cases we need to prepare the value of "seekKeyBuf" so that it has not-yet-scanned row id in it.
                    // the only valid way to do so is to treat row id payload as one big unsigned integer in Big Endian and increment it.
                    // It's important to note that increment may overflow. In this case "carry flag" will go into incrementing partition id.
                    // This is fine for three reasons:
                    //  - iterator has an upper bound, following "seek" will result in invalid iterator state.
                    //  - partition id iself cannot be overflown, because it's limited with a constant less than 0xFFFF. It's something like
                    //    65500, I think.
                    //  - "seekKeyBuf" buffer value will not be used after that, so it's ok if we corrupt its data (in every other instance,
                    //    buffer starts with a valid partition id, which is set during buffer's initialization).
                    incrementRowId(seekKeyBuf);

                    // Cache row and return "true" if it's found and not a tombstone.
                    if (found) {
                        byte[] valueBytes = it.value();

                        BinaryRow binaryRow = wrapValueIntoBinaryRow(valueBytes, valueHasTxId);

                        if (binaryRow != null && keyFilter.test(binaryRow)) {
                            if (txId != null && valueHasTxId) {
                                validateTxId(valueBytes, txId);
                            }

                            next = binaryRow;

                            return true;
                        }
                    }
                }
            }

            /** {@inheritDoc} */
            @Override
            public BinaryRow next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                BinaryRow res = next;

                next = null;

                return res;
            }

            /** {@inheritDoc} */
            @Override
            public void close() throws Exception {
                IgniteUtils.closeAll(options, it);
            }

            private void incrementRowId(ByteBuffer buf) {
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

                buf.putShort(0, partitionId);
            }
        };
    }

    @Override
    public long rowsCount() {
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

    /** {@inheritDoc} */
    @Override
    public void forEach(BiConsumer<RowId, BinaryRow> consumer) {
        try (
                var upperBound = new Slice(partitionEndPrefix());
                var options = new ReadOptions().setIterateUpperBound(upperBound);
                RocksIterator it = db.newIterator(cf, options)
        ) {
            it.seek(partitionStartPrefix());

            while (it.isValid()) {
                byte[] keyBytes = it.key();
                byte[] valueBytes = it.value();

                boolean valueHasTxId = keyBytes.length == ROW_PREFIX_SIZE;

                if (!isTombstone(valueBytes, valueHasTxId)) {
                    ByteBuffer keyBuf = ByteBuffer.wrap(keyBytes).order(BIG_ENDIAN).position(ROW_ID_OFFSET);

                    RowId rowId = new RowId(partitionId, keyBuf.getLong(), keyBuf.getLong());

                    BinaryRow binaryRow = wrapValueIntoBinaryRow(valueBytes, valueHasTxId);

                    consumer.accept(rowId, binaryRow);
                }

                it.next();
            }
        }
    }

    /**
     * Deletes partition data from the storage.
     */
    public void destroy() {
        try (WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.delete(meta, lastAppliedIndexKey);

            writeBatch.delete(meta, RocksDbMetaStorage.partitionIdKey(partitionId));

            writeBatch.deleteRange(cf, partitionStartPrefix(), partitionEndPrefix());

            db.write(writeOpts, writeBatch);
        } catch (RocksDBException e) {
            TableConfiguration tableCfg = tableStorage.configuration();

            throw new StorageException("Failed to destroy partition " + partitionId + " of table " + tableCfg.name(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        IgniteUtils.closeAll(persistedTierReadOpts, readOpts, writeOpts, upperBound);
    }

    private static WriteBatchWithIndex requireWriteBatch() {
        WriteBatchWithIndex writeBatch = WRITE_BATCH.get();

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
        keyBuf.putLong(rowId.mostSignificantBits());
        keyBuf.putLong(rowId.leastSignificantBits());

        return keyBuf;
    }

    /**
     * Writes a timestamp into a byte buffer, in descending lexicographical bytes order.
     */
    private static void putTimestamp(ByteBuffer buf, Timestamp ts) {
        assert buf.order() == BIG_ENDIAN;

        // Two things to note here:
        //  - "xor" with a single sign bit makes long values comparable according to RocksDB convention, where bytes are unsigned.
        //  - "bitwise negation" turns ascending order into a descending one.
        buf.putLong(~ts.getTimestamp() ^ (1L << 63));
        buf.putLong(~ts.getNodeId() ^ (1L << 63));
    }

    private static void putTransactionId(byte[] array, int off, UUID txId) {
        GridUnsafe.putLong(array, GridUnsafe.BYTE_ARR_OFF + off, txId.getMostSignificantBits());
        GridUnsafe.putLong(array, GridUnsafe.BYTE_ARR_OFF + off + Long.BYTES, txId.getLeastSignificantBits());
    }

    private static void validateTxId(byte[] valueBytes, UUID txId) {
        long msb = GridUnsafe.getLong(valueBytes, GridUnsafe.BYTE_ARR_OFF);
        long lsb = GridUnsafe.getLong(valueBytes, GridUnsafe.BYTE_ARR_OFF + Long.BYTES);

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
            // the iteration. Otherwise we've exhausted the data range.
            try {
                it.status();
            } catch (RocksDBException e) {
                throw new StorageException("Failed to read data from storage", e);
            }
        }

        return invalid;
    }

    /**
     * Converts raw byte array representation of the value into a binary row.
     *
     * @param valueBytes Value bytes as read from the storage.
     * @param valueHasTxId Whether the value has a transaction id prefix in it.
     * @return Binary row instance or {@code null} if value is a tombstone.
     */
    private static @Nullable BinaryRow wrapValueIntoBinaryRow(byte[] valueBytes, boolean valueHasTxId) {
        if (isTombstone(valueBytes, valueHasTxId)) {
            return null;
        }

        return valueHasTxId
                ? new ByteBufferRow(ByteBuffer.wrap(valueBytes).position(TX_ID_SIZE).slice().order(LITTLE_ENDIAN))
                : new ByteBufferRow(valueBytes);
    }

    /**
     * Creates a prefix of all keys in the given partition.
     */
    private byte[] partitionStartPrefix() {
        return unsignedShortAsBytes(partitionId);
    }

    /**
     * Creates a prefix of all keys in the next partition, used as an exclusive bound.
     */
    private byte[] partitionEndPrefix() {
        return unsignedShortAsBytes(partitionId + 1);
    }

    private static byte[] unsignedShortAsBytes(int value) {
        return new byte[] {(byte) (value >>> 8), (byte) value};
    }

    /**
     * Returns {@code true} if value payload represents a tombstone.
     */
    private static boolean isTombstone(byte[] valueBytes, boolean hasTxId) {
        return valueBytes.length == (hasTxId ? TX_ID_SIZE : 0);
    }
}
