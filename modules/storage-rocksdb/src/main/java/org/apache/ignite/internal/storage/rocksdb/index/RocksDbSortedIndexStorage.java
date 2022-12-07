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

package org.apache.ignite.internal.storage.rocksdb.index;

import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CursorUtils.map;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMvPartitionStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatchWithIndex;

/**
 * {@link SortedIndexStorage} implementation based on RocksDB.
 *
 * <p>This storage uses the following format for keys:
 * <pre>
 * Partition ID - 2 bytes
 * Tuple value - variable length
 * Row ID (UUID) - 16 bytes
 * </pre>
 *
 * <p>We use an empty array as values, because all required information can be extracted from the key.
 */
public class RocksDbSortedIndexStorage implements SortedIndexStorage {
    private static final int ROW_ID_SIZE = Long.BYTES * 2;

    private static final ByteOrder ORDER = ByteOrder.BIG_ENDIAN;

    private final SortedIndexDescriptor descriptor;

    private final ColumnFamily indexCf;

    private final RocksDbMvPartitionStorage partitionStorage;

    /**
     * Creates a storage.
     *
     * @param descriptor Sorted Index descriptor.
     * @param indexCf Column family that stores the index data.
     * @param partitionStorage Partition storage of the corresponding index.
     */
    public RocksDbSortedIndexStorage(
            SortedIndexDescriptor descriptor,
            ColumnFamily indexCf,
            RocksDbMvPartitionStorage partitionStorage
    ) {
        this.descriptor = descriptor;
        this.indexCf = indexCf;
        this.partitionStorage = partitionStorage;
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        BinaryTuplePrefix keyPrefix = BinaryTuplePrefix.fromBinaryTuple(key);

        return map(scan(keyPrefix, keyPrefix, true, true), this::decodeRowId);
    }

    @Override
    public void put(IndexRow row) {
        WriteBatchWithIndex writeBatch = partitionStorage.currentWriteBatch();

        try {
            writeBatch.put(indexCf.handle(), rocksKey(row), BYTE_EMPTY_ARRAY);
        } catch (RocksDBException e) {
            throw new StorageException("Unable to insert data into sorted index. Index ID: " + descriptor.id(), e);
        }
    }

    @Override
    public void remove(IndexRow row) {
        WriteBatchWithIndex writeBatch = partitionStorage.currentWriteBatch();

        try {
            writeBatch.delete(indexCf.handle(), rocksKey(row));
        } catch (RocksDBException e) {
            throw new StorageException("Unable to remove data from sorted index. Index ID: " + descriptor.id(), e);
        }
    }

    @Override
    public Cursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
        boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

        return map(scan(lowerBound, upperBound, includeLower, includeUpper), this::decodeRow);
    }

    private Cursor<ByteBuffer> scan(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            boolean includeLower,
            boolean includeUpper
    ) {
        byte[] lowerBoundBytes;

        if (lowerBound == null) {
            lowerBoundBytes = null;
        } else {
            lowerBoundBytes = rocksPrefix(lowerBound);

            // Skip the lower bound, if needed (RocksDB includes the lower bound by default).
            if (!includeLower) {
                setEqualityFlag(lowerBoundBytes);
            }
        }

        byte[] upperBoundBytes;

        if (upperBound == null) {
            upperBoundBytes = null;
        } else {
            upperBoundBytes = rocksPrefix(upperBound);

            // Include the upper bound, if needed (RocksDB excludes the upper bound by default).
            if (includeUpper) {
                setEqualityFlag(upperBoundBytes);
            }
        }

        return createScanCursor(lowerBoundBytes, upperBoundBytes);
    }

    private Cursor<ByteBuffer> createScanCursor(byte @Nullable [] lowerBound, byte @Nullable [] upperBound) {
        Slice upperBoundSlice = upperBound == null ? new Slice(partitionStorage.partitionEndPrefix()) : new Slice(upperBound);

        ReadOptions options = new ReadOptions().setIterateUpperBound(upperBoundSlice);

        RocksIterator it = indexCf.newIterator(options);

        return new Cursor<>() {
            @Nullable
            private Boolean hasNext;

            @Nullable
            private byte[] key;

            @Override
            public void close() {
                it.close();

                RocksUtils.closeAll(options, upperBoundSlice);
            }

            @Override
            public boolean hasNext() {
                advanceIfNeeded();

                return hasNext;
            }

            @Override
            public ByteBuffer next() {
                advanceIfNeeded();

                boolean hasNext = this.hasNext;

                this.hasNext = null;

                if (!hasNext) {
                    throw new NoSuchElementException();
                }

                return ByteBuffer.wrap(key).order(ORDER);
            }

            private void advanceIfNeeded() throws StorageException {
                if (hasNext == null) {
                    try {
                        it.refresh();
                    } catch (RocksDBException e) {
                        throw new StorageException("Error refreshing an iterator", e);
                    }

                    if (key == null) {
                        it.seek(lowerBound == null ? partitionStorage.partitionStartPrefix() : lowerBound);
                    } else {
                        it.seekForPrev(key);

                        it.next();
                    }

                    if (!it.isValid()) {
                        // check the status first. This operation is guaranteed to throw if an internal error has occurred during
                        // the iteration. Otherwise, we've exhausted the data range.
                        RocksUtils.checkIterator(it);

                        hasNext = false;
                    } else {
                        key = it.key();

                        hasNext = true;
                    }
                }
            }
        };
    }

    private static void setEqualityFlag(byte[] prefix) {
        // Flags start after the partition ID.
        byte flags = prefix[Short.BYTES];

        prefix[Short.BYTES] = (byte) (flags | BinaryTupleCommon.EQUALITY_FLAG);
    }

    private IndexRow decodeRow(ByteBuffer bytes) {
        assert bytes.getShort(0) == partitionStorage.partitionId();

        var tuple = new BinaryTuple(descriptor.binaryTupleSchema(), binaryTupleSlice(bytes));

        return new IndexRowImpl(tuple, decodeRowId(bytes));
    }

    private RowId decodeRowId(ByteBuffer bytes) {
        // RowId UUID is located at the last 16 bytes of the key
        long mostSignificantBits = bytes.getLong(bytes.limit() - Long.BYTES * 2);
        long leastSignificantBits = bytes.getLong(bytes.limit() - Long.BYTES);

        return new RowId(partitionStorage.partitionId(), mostSignificantBits, leastSignificantBits);
    }

    private byte[] rocksPrefix(BinaryTuplePrefix prefix) {
        ByteBuffer bytes = prefix.byteBuffer();

        return ByteBuffer.allocate(Short.BYTES + bytes.remaining())
                .order(ORDER)
                .putShort((short) partitionStorage.partitionId())
                .put(bytes)
                .array();
    }

    private byte[] rocksKey(IndexRow row) {
        ByteBuffer bytes = row.indexColumns().byteBuffer();

        return ByteBuffer.allocate(Short.BYTES + bytes.remaining() + ROW_ID_SIZE)
                .order(ORDER)
                .putShort((short) partitionStorage.partitionId())
                .put(bytes)
                .putLong(row.rowId().mostSignificantBits())
                .putLong(row.rowId().leastSignificantBits())
                .array();
    }

    private static ByteBuffer binaryTupleSlice(ByteBuffer key) {
        return key.duplicate()
                // Discard partition ID.
                .position(Short.BYTES)
                // Discard row ID.
                .limit(key.limit() - ROW_ID_SIZE)
                .slice()
                .order(ByteOrder.LITTLE_ENDIAN);
    }

    private static class ScanCursor implements Cursor<ByteBuffer> {
        private final RocksIterator it;

        private ScanCursor(RocksIterator it) {
            this.it = it;
        }

        @Override
        public void close() {
            it.close();


        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public ByteBuffer next() {
            return null;
        }
    }
}
