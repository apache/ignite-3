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
import static org.apache.ignite.internal.storage.IgniteRowId.MAX_ROW_ID_SIZE;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.IgniteRowId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteOptions;

/**
 * Mult-versioned partition storage implementation based on RocksDB. Stored data has the following format:
 * <pre><code>
 * | partId (2 bytes, BE) | rowId ({@link #igniteRowIdSize} bytes) |</code></pre>
 * or
 * <pre><code>
 * | partId (2 bytes, BE) | rowId ({@link #igniteRowIdSize} bytes) | timestamp (16 bytes, DESC) |</code></pre>
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

    /** Timestamp size in bytes. */
    private static final int TIMESTAMP_SIZE = 2 * Long.BYTES;

    /** Maximum possible size of the key. */
    private static final int MAX_KEY_SIZE = /* partId */ ROW_ID_OFFSET + /* rowId */ MAX_ROW_ID_SIZE + /* timestamp */ TIMESTAMP_SIZE;

    /** Threadlocal direct buffer instance to read keys from RocksDB. */
    private static final ThreadLocal<ByteBuffer> MV_KEY_BUFFER = withInitial(() -> allocateDirect(MAX_KEY_SIZE).order(BIG_ENDIAN));

    /** Threadlocal ob-heap byte buffer instance to use for key manipulations. */
    private final ThreadLocal<ByteBuffer> heapKeyBuffer;

    /** Size of the {@link IgniteRowId} identifiers for this particular partition storage. */
    private final int igniteRowIdSize;

    /**
     * Partition ID (should be treated as an unsigned short).
     *
     * <p/>Partition IDs are always stored in the big endian order, since they need to be compared lexicographically.
     */
    private final int partId;

    /** RocksDb instance. */
    private final RocksDB db;

    /** Partitions column family. */
    private final ColumnFamilyHandle cf;

    /** Write options. */
    private final WriteOptions writeOpts = new WriteOptions().setDisableWAL(true);

    /** Upper bound for scans and reads. */
    private final Slice upperBound;

    /**
     * Constructor.
     *
     * @param igniteRowIdSize Size of row id.
     * @param partId Partition id.
     * @param db RocksDB instance.
     * @param cf Column family handle to store partition data.
     */
    public RocksDbMvPartitionStorage(int igniteRowIdSize, int partId, RocksDB db, ColumnFamilyHandle cf) {
        this.igniteRowIdSize = igniteRowIdSize;
        this.partId = partId;
        this.db = db;
        this.cf = cf;

        heapKeyBuffer = withInitial(() ->
                ByteBuffer.allocate(Short.BYTES + igniteRowIdSize + ROW_ID_OFFSET * Long.BYTES)
                        .order(BIG_ENDIAN)
                        .putShort((short) partId)
        );

        upperBound = new Slice(partitionEndPrefix());
    }

    /** {@inheritDoc} */
    @Override
    public void addWrite(IgniteRowId rowId, @Nullable BinaryRow row, UUID txId) throws TxIdMismatchException, StorageException {
        // Prepare a buffer with partition id and row id.
        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try {
            // Check concurrent transaction data.
            if (RocksDB.NOT_FOUND != db.get(cf, keyBuf.array(), 0, keyBuf.position(), BYTE_EMPTY_ARRAY, 0, 0)) {
                throw new TxIdMismatchException();
            }

            if (row == null) {
                // Write empty array as a tombstone.
                db.put(cf, writeOpts, keyBuf.array(), 0, keyBuf.position(), BYTE_EMPTY_ARRAY, 0, 0);
            } else {
                byte[] valueBytes = row.bytes();

                // Write binary row data as a value.
                db.put(cf, writeOpts, keyBuf.array(), 0, keyBuf.position(), valueBytes, 0, valueBytes.length);
            }
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void abortWrite(IgniteRowId rowId) throws StorageException {
        // Prepare a buffer with partition id and row id.
        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try {
            // Perform unconditional remove for the key without associated timestamp.
            db.delete(cf, writeOpts, keyBuf.array(), 0, keyBuf.position());
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void commitWrite(IgniteRowId rowId, Timestamp timestamp) throws StorageException {
        // Prepare a buffer with partition id and row id.
        ByteBuffer keyBuf = prepareHeapKeyBuf(rowId);

        try {
            // Read a value associated with pending write.
            byte[] valueBytes = db.get(cf, keyBuf.array(), 0, keyBuf.position());

            // Delete pending write.
            db.delete(cf, writeOpts, keyBuf.array(), 0, keyBuf.position());

            // Add timestamp to the key put the value back into the storage.
            putTimestamp(keyBuf, timestamp);

            db.put(cf, writeOpts, keyBuf.array(), 0, keyBuf.position(), valueBytes, 0, valueBytes.length);
        } catch (RocksDBException e) {
            throw new StorageException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow read(IgniteRowId rowId, @Nullable Timestamp timestamp) {
        // Prepare a buffer with partition id and row id.
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
            ByteBuffer directBuffer = MV_KEY_BUFFER.get().position(0).limit(ROW_ID_OFFSET + igniteRowIdSize);

            it.key(directBuffer);

            // Comparison starts from the position of the row id.
            directBuffer.position(ROW_ID_OFFSET);

            // Return null if seek found a wrong key.
            if (rowId.compare(directBuffer, false) != 0) {
                return null;
            }

            // Get binary row from the iterator. It has the exact payload that we need.
            byte[] valueBytes = it.value();

            assert valueBytes != null;

            // Empty array means a tombstone.
            if (valueBytes.length == 0) {
                return null;
            }

            return new ByteBufferRow(valueBytes);
        }
    }

    //TODO Play with prefix settings and benchmark results.
    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, @Nullable Timestamp timestamp) throws StorageException {
        // Set next partition as an upper bound.
        var options = new ReadOptions().setIterateUpperBound(upperBound).setTotalOrderSeek(true);

        RocksIterator it = db.newIterator(cf, options);

        // Seek iterator to the beginning of the partition.
        it.seek(partitionStartPrefix());

        // Size of the buffer to seek values. Should fit partition id, row id and maybe timestamp, if it's not null.
        int seekKeyBufSize = ROW_ID_OFFSET + igniteRowIdSize + (timestamp == null ? 0 : TIMESTAMP_SIZE);

        // Here's seek buffer itself. Originally it contains a valid partition id, row id payload that's filled with zeroes, and maybe
        // a timestamp value. Zero row id guarantees that it's lexicographically less then or equal to any other row id stored in the
        // partition.
        // Byte buffer from a thread-local field can't be used here, because of two reasons:
        //  - no one guarantees that there will only be a single cursor;
        //  - no one guarantees that returned cursor will not be used other threads.
        // The thing is, we need this buffer to preserve its content between invocactions of "hasNext" method.
        ByteBuffer seekKeyBuf = ByteBuffer.allocate(seekKeyBufSize).order(BIG_ENDIAN).putShort((short) partId);

        if (timestamp != null) {
            putTimestamp(seekKeyBuf.position(ROW_ID_OFFSET + igniteRowIdSize), timestamp);
        }

        // Version without timestamp to compare row ids.
        ByteBuffer seekKeyBufSliceWithoutTimestamp = timestamp == null
                ? seekKeyBuf
                : seekKeyBuf.position(0).limit(ROW_ID_OFFSET + igniteRowIdSize).slice().order(BIG_ENDIAN);

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

                // Prepare direct buffer slice to read keys from the iterator. It's goind to be used to read row ids without timestamps.
                // To enforce this, we limit it to a proper capacity.
                ByteBuffer directBuffer = MV_KEY_BUFFER.get()
                        .position(0).limit(ROW_ID_OFFSET + igniteRowIdSize).slice().order(BIG_ENDIAN);

                while (true) {
                    // This flag is used to skip row ids that were created after required timestamp.
                    boolean found = true;

                    // We should do it all the time. Here in particular it means one of two things:
                    //  - partition is empty;
                    //  - iterator exhausted all the data in partition.
                    if (invalid(it)) {
                        return false;
                    }

                    // At this point, seekKeyBuf should contain row id that's above the one we already scanned, but not greater than any
                    // other row id in partition. When we start, row id is filled with zeroes. Value during the iteration is described later
                    // in this code. Now let's descrie what we'll find, assuming that iterator found something:
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
                    it.key(directBuffer);

                    // To understand this condition please read that huge comment above.
                    if (timestamp == null || !directBuffer.equals(seekKeyBufSliceWithoutTimestamp)) {
                        // Copy actual row id into a "seekKeyBuf" buffer.
                        GridUnsafe.copyMemory(
                                null, GridUnsafe.bufferAddress(directBuffer) + ROW_ID_OFFSET,
                                seekKeyBuf.array(), GridUnsafe.BYTE_ARR_OFF + ROW_ID_OFFSET,
                                igniteRowIdSize
                        );

                        // Perform additional "seek" if timestamp is not null. Motivation for it is described in comments above.
                        if (timestamp != null) {
                            // At this point, "seekKeyBuf" has row id that exists in partition.
                            it.seek(seekKeyBuf.array());

                            // Iterator may not be valid if that row was created after required timestamp.
                            if (invalid(it)) {
                                return false;
                            }

                            // Or iterator may still be valid even if there's no version for required timestamp. In this case row id
                            // itself will be different and must check it.
                            it.key(directBuffer);

                            if (!directBuffer.equals(seekKeyBufSliceWithoutTimestamp)) {
                                found = false;
                            }
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
                    for (int i = ROW_ID_OFFSET + igniteRowIdSize - 1;; i--) {
                        byte b = (byte) (seekKeyBuf.get(i) + 1);

                        seekKeyBuf.put(i, b);

                        if (b != 0) {
                            break;
                        }
                    }

                    // Cache row and return "true" if it's found and not a tombstone.
                    if (found) {
                        byte[] value = it.value();

                        if (value.length != 0) {
                            ByteBufferRow binaryRow = new ByteBufferRow(value);

                            if (!keyFilter.test(binaryRow)) {
                                continue;
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
    private ByteBuffer prepareHeapKeyBuf(IgniteRowId rowId) {
        ByteBuffer keyBuf = heapKeyBuffer.get().position(ROW_ID_OFFSET);

        rowId.writeTo(keyBuf, false);

        assert keyBuf.position() == ROW_ID_OFFSET + igniteRowIdSize;

        return keyBuf;
    }

    /**
     * Writes a timestamp into a byte buffer, in descending lexicographical bytes order.
     */
    private void putTimestamp(ByteBuffer keyBuf, Timestamp ts) {
        // Two things to note here:
        //  - "xor" with a single sign bit makes long values comparable according to RocksDB convention, where bytes are unsigned.
        //  - "bitwise negation" turns ascending order into a descending one.
        keyBuf.putLong(~ts.getTimestamp() ^ (1L << 63));
        keyBuf.putLong(~ts.getNodeId() ^ (1L << 63));
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
                throw new IgniteInternalException(e);
            }
        }

        return invalid;
    }

    /**
     * Creates a prefix of all keys in the given partition.
     */
    private byte[] partitionStartPrefix() {
        return unsignedShortAsBytes(partId);
    }

    /**
     * Creates a prefix of all keys in the next partition, used as an exclusive bound.
     */
    private byte[] partitionEndPrefix() {
        return unsignedShortAsBytes(partId + 1);
    }

    private static byte[] unsignedShortAsBytes(int value) {
        byte[] result = new byte[Short.BYTES];

        result[0] = (byte) (value >>> 8);
        result[1] = (byte) value;

        return result;
    }
}
