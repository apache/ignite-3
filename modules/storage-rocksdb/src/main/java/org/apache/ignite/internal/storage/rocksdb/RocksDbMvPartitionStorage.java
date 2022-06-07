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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.NoUncommittedVersionException;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.tx.Timestamp;
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
import org.rocksdb.WriteOptions;

/**
 * Multi-versioned partition storage implementation based on RocksDB. Stored data has the following format:
 * <pre><code>
 * | rowId (16 bytes, BE) |</code></pre>
 * or
 * <pre><code>
 * | rowId (16 bytes, BE) | timestamp (16 bytes, DESC) |</code></pre>
 * depending on transaction status. Pending transactions data doesn't have a timestamp assigned.
 *
 * <p/>BE means Big Endian, meaning that lexicographical bytes order matches a natural order of partitions.
 *
 * <p/>DESC means that timestamps are sorted from newest to oldest (N2O). Please refer to {@link #putTimestamp(ByteBuffer, Timestamp)} to
 * see how it's achieved. Missing timestamp could be interpreted as a moment infinitely far away in the future.
 */
public class RocksDbMvPartitionStorage implements MvPartitionStorage {
    /** UUID size in bytes. */
    private static final int ROW_ID_SIZE = 2 * Long.BYTES;

    /** Size of the key without timestamp. */
    private static final int ROW_PREFIX_SIZE = ROW_ID_SIZE;

    /** Timestamp size in bytes. */
    private static final int TIMESTAMP_SIZE = 2 * Long.BYTES;

    /** Transaction id size. Matches timestamp size. */
    private static final int TX_ID_SIZE = TIMESTAMP_SIZE;

    /** Maximum size of the key. */
    private static final int MAX_KEY_SIZE = ROW_PREFIX_SIZE + TIMESTAMP_SIZE;

    /** Threadlocal direct buffer instance to read keys from RocksDB. */
    private static final ThreadLocal<ByteBuffer> MV_KEY_BUFFER = withInitial(() -> allocateDirect(MAX_KEY_SIZE).order(BIG_ENDIAN));

    /** Threadlocal on-heap byte buffer instance to use for key manipulations. */
    private final ThreadLocal<ByteBuffer> heapKeyBuffer;

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

    /** Write options. */
    private final WriteOptions writeOpts = new WriteOptions();

    /** Upper bound for scans and reads. */
    private final Slice upperBound;

    /**
     * Constructor.
     *
     * @param partitionId Partition id.
     * @param db RocksDB instance.
     * @param cf Column family handle to store partition data.
     */
    public RocksDbMvPartitionStorage(int partitionId, RocksDB db, ColumnFamilyHandle cf) {
        this.partitionId = partitionId;
        this.db = db;
        this.cf = cf;

        heapKeyBuffer = withInitial(() ->
                ByteBuffer.allocate(MAX_KEY_SIZE)
                        .order(BIG_ENDIAN)
        );

        upperBound = new Slice(partitionEndPrefix());
    }

    /** {@inheritDoc} */
    @Override
    public RowId insert(BinaryRow row, UUID txId) throws StorageException {
        RowId rowId = UuidRowId.randomRowId(partitionId);

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
        assert rowId.partitionId() == partitionId : rowId;

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        BinaryRow res = null;

        try {
            // Check concurrent transaction data.
            byte[] keyBufArray = keyBuf.array();

            byte[] previousValue = db.get(cf, keyBufArray, 0, ROW_PREFIX_SIZE);

            // Previous value must belong to the same transaction.
            if (previousValue != null) {
                validateTxId(previousValue, txId);

                res = wrapValueIntoBinaryRow(previousValue, true);
            }

            if (row == null) {
                // Write empty value as a tombstone.
                if (previousValue != null) {
                    // Reuse old array with transaction id already written to it.
                    db.put(cf, writeOpts, keyBufArray, 0, ROW_PREFIX_SIZE, previousValue, 0, TX_ID_SIZE);
                } else {
                    // Use tail of the key buffer to save on array allocations.
                    putTransactionId(keyBufArray, ROW_PREFIX_SIZE, txId);

                    db.put(cf, writeOpts, keyBufArray, 0, ROW_PREFIX_SIZE, keyBufArray, ROW_PREFIX_SIZE, TX_ID_SIZE);
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
        //TODO IGNITE-16913 Add proper way to write row bytes into array without allocations.
        byte[] rowBytes = row.bytes();

        ByteBuffer value = ByteBuffer.allocate(rowBytes.length + TX_ID_SIZE).order(LITTLE_ENDIAN);

        putTransactionId(value.array(), 0, txId);

        value.position(TX_ID_SIZE).put(rowBytes);

        // Write binary row data as a value.
        db.put(cf, writeOpts, keyArray, 0, ROW_PREFIX_SIZE, value.array(), 0, value.capacity());
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        assert rowId.partitionId() == partitionId : rowId;

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try {
            byte[] previousValue = db.get(cf, keyBuf.array(), 0, ROW_PREFIX_SIZE);
            if (previousValue == null) {
                throw new NoUncommittedVersionException();
            }

            // Perform unconditional remove for the key without associated timestamp.
            db.delete(cf, writeOpts, keyBuf.array(), 0, ROW_PREFIX_SIZE);

            return wrapValueIntoBinaryRow(previousValue, true);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to roll back insert/update", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void commitWrite(RowId rowId, Timestamp timestamp) throws StorageException {
        assert rowId.partitionId() == partitionId : rowId;

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try {
            // Read a value associated with pending write.
            byte[] valueBytes = db.get(cf, keyBuf.array(), 0, ROW_PREFIX_SIZE);

            assert valueBytes != null : "Failed to commit row " + rowId + ", value is missing";

            // Delete pending write.
            db.delete(cf, writeOpts, keyBuf.array(), 0, ROW_PREFIX_SIZE);

            // Add timestamp to the key, and put the value back into the storage.
            putTimestamp(keyBuf, timestamp);

            db.put(cf, writeOpts, keyBuf.array(), 0, MAX_KEY_SIZE, valueBytes, TX_ID_SIZE, valueBytes.length - TX_ID_SIZE);
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
        assert rowId.partitionId() == partitionId : rowId;
        assert timestamp == null ^ txId == null;

        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try (
                // Set next partition as an upper bound.
                var readOpts = new ReadOptions().setIterateUpperBound(upperBound);
                RocksIterator it = db.newIterator(cf, readOpts)
        ) {
            if (timestamp == null) {
                // Seek to the first appearance of row id if timestamp isn't set.
                // Since timestamps are sorted from newest to oldest, first occurance will always be the latest version.
                // Unfortunately, copy here is unavoidable with current API.
                it.seek(Arrays.copyOf(keyBuf.array(), keyBuf.position()));
            } else {
                // Put timestamp restriction according to N2O timestamps order.
                putTimestamp(keyBuf, timestamp);

                // This seek will either find a key with timestamp that's less or equal than required value, or a different key whatsoever.
                // It is guaranteed by descending order of timestamps.
                it.seek(keyBuf.array());
            }

            // Return null if nothing was found.
            if (invalid(it)) {
                return null;
            }

            // There's no guarantee that required key even exists. If it doesn't, then "seek" will point to a different key, obviously.
            // To avoid returning its value, we have to check that actual key matches what we need.
            // Here we prepare direct buffer to read key without timestamp. Shared direct buffer is used to avoid extra memory allocations.
            ByteBuffer directBuffer = MV_KEY_BUFFER.get().position(0).limit(MAX_KEY_SIZE);

            int keyLength = it.key(directBuffer);

            boolean valueHasTxId = keyLength == ROW_PREFIX_SIZE;

            // Return null if seek found a wrong key.
            if (!((UuidRowId) rowId).matches(directBuffer)) {
                return null;
            }

            // Get binary row from the iterator. It has the exact payload that we need.
            byte[] valueBytes = it.value();

            assert valueBytes != null;

            if (txId != null && valueHasTxId) {
                validateTxId(valueBytes, txId);
            }

            return wrapValueIntoBinaryRow(valueBytes, valueHasTxId);
        }
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
        ByteBuffer seekKeyBuf = ByteBuffer.allocate(seekKeyBufSize).order(BIG_ENDIAN);

        if (timestamp != null) {
            putTimestamp(seekKeyBuf.position(ROW_PREFIX_SIZE), timestamp);
        }

        // Version without timestamp to compare row ids.
        ByteBuffer seekKeyBufSliceWithoutTimestamp = timestamp == null
                ? seekKeyBuf
                : seekKeyBuf.position(0).limit(ROW_PREFIX_SIZE).slice().order(BIG_ENDIAN);

        return new Cursor<BinaryRow>() {
            /** Cached value for {@link #next()} method. Also optimizes the code of {@link #hasNext()}. */
            private BinaryRow next = null;

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
                                null, GridUnsafe.bufferAddress(directBuffer),
                                seekKeyBuf.array(), GridUnsafe.BYTE_ARR_OFF,
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
                    // This is fine for two reasons:
                    //  - iterator has an upper bound, following "seek" will result in invalid iterator state.
                    //  - partition id iself cannot be overflown, because it's limited with a constant less than 0xFFFF. It's something like
                    //    65500, I think.
                    incrementRowId(seekKeyBuf);

                    // Cache row and return "true" if it's found and not a tombstone.
                    if (found) {
                        byte[] valueBytes = it.value();

                        BinaryRow binaryRow = wrapValueIntoBinaryRow(valueBytes, valueHasTxId);

                        if (binaryRow != null && keyFilter.test(binaryRow)) {
                            if (txId != null && valueHasTxId) {
                                validateTxId(valueBytes, txId);
                            }

                            this.next = binaryRow;

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
                long lsb = 1 + buf.getLong(Long.BYTES);

                buf.putLong(Long.BYTES, lsb);

                if (lsb == 0L) {
                    long msb = 1 + buf.getLong(0);

                    assert msb != 0 : "partitionId overflow, must never happen";

                    buf.putLong(0, msb);
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        IgniteUtils.closeAll(writeOpts, upperBound);
    }

    /**
     * Prepares thread-local on-heap byte buffer. Writes row id in it. Partition id is already there. Timestamp is not cleared.
     */
    private ByteBuffer prepareHeapKeyBuf(RowId rowId) {
        assert rowId instanceof UuidRowId : rowId;

        ByteBuffer keyBuf = heapKeyBuffer.get().position(0);

        ((UuidRowId) rowId).writeTo(keyBuf);

        assert (keyBuf.getShort(0) & 0xFFFF) == partitionId;

        return keyBuf;
    }

    /**
     * Writes a timestamp into a byte buffer, in descending lexicographical bytes order.
     */
    private void putTimestamp(ByteBuffer buf, Timestamp ts) {
        assert buf.order() == BIG_ENDIAN;

        // Two things to note here:
        //  - "xor" with a single sign bit makes long values comparable according to RocksDB convention, where bytes are unsigned.
        //  - "bitwise negation" turns ascending order into a descending one.
        buf.putLong(~ts.getTimestamp() ^ (1L << 63));
        buf.putLong(~ts.getNodeId() ^ (1L << 63));
    }

    private void putTransactionId(byte[] array, int off, UUID txId) {
        GridUnsafe.putLong(array, GridUnsafe.BYTE_ARR_OFF + off, txId.getMostSignificantBits());
        GridUnsafe.putLong(array, GridUnsafe.BYTE_ARR_OFF + off + Long.BYTES, txId.getLeastSignificantBits());
    }

    private void validateTxId(byte[] valueBytes, UUID txId) {
        if (txId.getMostSignificantBits() != GridUnsafe.getLong(valueBytes, GridUnsafe.BYTE_ARR_OFF)
                || txId.getLeastSignificantBits() != GridUnsafe.getLong(valueBytes, GridUnsafe.BYTE_ARR_OFF + Long.BYTES)) {
            throw new TxIdMismatchException();
        }
    }

    /**
     * Checks iterator validity, including both finished iteration and occurred exception.
     */
    private boolean invalid(RocksIterator it) {
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
    private @Nullable BinaryRow wrapValueIntoBinaryRow(byte[] valueBytes, boolean valueHasTxId) {
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
    private boolean isTombstone(byte[] valueBytes, boolean hasTxId) {
        return valueBytes.length == (hasTxId ? TX_ID_SIZE : 0);
    }
}
