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

import static org.apache.ignite.internal.storage.rocksdb.index.CursorUtils.concat;
import static org.apache.ignite.internal.storage.rocksdb.index.CursorUtils.dropWhile;
import static org.apache.ignite.internal.storage.rocksdb.index.CursorUtils.map;
import static org.apache.ignite.internal.storage.rocksdb.index.CursorUtils.takeWhile;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Predicate;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.BinaryTupleComparator;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMvPartitionStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
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

        return scan(lowerBound, upperBound, includeLower, includeUpper);
    }

    private Cursor<IndexRow> scan(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            boolean includeLower,
            boolean includeUpper
    ) {
        byte[] lowerBoundBytes = lowerBound == null ? null : rocksPrefix(lowerBound);
        byte[] upperBoundBytes = upperBound == null ? null : rocksPrefix(upperBound);

        Cursor<ByteBuffer> cursor = createScanCursor(lowerBoundBytes, upperBoundBytes);

        // Skip the lower bound, if needed (RocksDB includes the lower bound by default).
        if (!includeLower && lowerBound != null) {
            cursor = dropWhile(cursor, startsWith(lowerBound));
        }

        // Include the upper bound, if needed (RocksDB excludes the upper bound by default).
        if (includeUpper && upperBound != null) {
            Cursor<ByteBuffer> upperBoundCursor = takeWhile(createScanCursor(upperBoundBytes, null), startsWith(upperBound));

            cursor = concat(cursor, upperBoundCursor);
        }

        return map(cursor, this::decodeRow);
    }

    private Cursor<ByteBuffer> createScanCursor(byte @Nullable [] lowerBound, byte @Nullable [] upperBound) {
        Slice upperBoundSlice = upperBound == null ? null : new Slice(upperBound);

        ReadOptions options = new ReadOptions().setIterateUpperBound(upperBoundSlice);

        RocksIterator it = indexCf.newIterator(options);

        if (lowerBound == null) {
            it.seekToFirst();
        } else {
            it.seek(lowerBound);
        }

        return new RocksIteratorAdapter<>(it) {
            @Override
            protected ByteBuffer decodeEntry(byte[] key, byte[] value) {
                return ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
            }

            @Override
            public void close() throws Exception {
                super.close();

                IgniteUtils.closeAll(options, upperBoundSlice);
            }
        };
    }

    private IndexRow decodeRow(ByteBuffer bytes) {
        assert bytes.getShort(0) == partitionStorage.partitionId();

        var tuple = new BinaryTuple(descriptor.binaryTupleSchema(), binaryTupleSlice(bytes));

        // RowId UUID is located at the last 16 bytes of the key
        long mostSignificantBits = bytes.getLong(bytes.limit() - Long.BYTES * 2);
        long leastSignificantBits = bytes.getLong(bytes.limit() - Long.BYTES);

        var rowId = new RowId(partitionStorage.partitionId(), mostSignificantBits, leastSignificantBits);

        return new IndexRowImpl(tuple, rowId);
    }

    private byte[] rocksPrefix(BinaryTuplePrefix prefix) {
        return rocksPrefix(prefix, 0).array();
    }

    private ByteBuffer rocksPrefix(InternalTuple prefix, int extraLength) {
        ByteBuffer keyBytes = prefix.byteBuffer();

        return ByteBuffer.allocate(Short.BYTES + keyBytes.remaining() + extraLength)
                .order(ByteOrder.BIG_ENDIAN)
                .putShort((short) partitionStorage.partitionId())
                .put(keyBytes);
    }

    private byte[] rocksKey(IndexRow row) {
        RowId rowId = row.rowId();

        // We don't store the Partition ID as it is already a part of the key.
        return rocksPrefix(row.indexColumns(), 2 * Long.BYTES)
                .putLong(rowId.mostSignificantBits())
                .putLong(rowId.leastSignificantBits())
                .array();
    }

    private Predicate<ByteBuffer> startsWith(BinaryTuplePrefix prefix) {
        var comparator = new BinaryTupleComparator(descriptor);

        return key -> {
            // First, compare the partitionIDs.
            boolean partitionIdCompare = key.getShort(0) == partitionStorage.partitionId();

            if (!partitionIdCompare) {
                return false;
            }

            // Finally, compare the remaining parts of the tuples.
            // TODO: This part may be optimized by comparing binary tuple representations directly. However, currently BinaryTuple prefixes
            //  are not binary compatible with regular tuples. See https://issues.apache.org/jira/browse/IGNITE-17711.
            return comparator.compare(prefix.byteBuffer(), binaryTupleSlice(key)) == 0;
        };
    }

    private static ByteBuffer binaryTupleSlice(ByteBuffer key) {
        return key.duplicate()
                // Discard partition ID.
                .position(Short.BYTES)
                // Discard row ID.
                .limit(key.limit() - Long.BYTES * 2)
                .slice()
                .order(ByteOrder.LITTLE_ENDIAN);
    }
}
