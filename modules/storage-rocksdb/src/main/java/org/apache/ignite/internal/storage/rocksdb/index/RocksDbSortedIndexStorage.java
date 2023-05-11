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

import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.PARTITION_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.ROW_ID_SIZE;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
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
public class RocksDbSortedIndexStorage extends AbstractRocksDbIndexStorage implements SortedIndexStorage {
    private final SortedIndexDescriptor descriptor;

    private final ColumnFamily indexCf;

    /**
     * Creates a storage.
     *
     * @param descriptor Sorted Index descriptor.
     * @param indexCf Column family that stores the index data.
     * @param helper Partition data helper.
     * @param indexMetaStorage Index meta storage.
     */
    public RocksDbSortedIndexStorage(
            SortedIndexDescriptor descriptor,
            ColumnFamily indexCf,
            PartitionDataHelper helper,
            RocksDbMetaStorage indexMetaStorage
    ) {
        super(descriptor.id(), helper, indexMetaStorage);

        this.descriptor = descriptor;
        this.indexCf = indexCf;
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            BinaryTuplePrefix keyPrefix = BinaryTuplePrefix.fromBinaryTuple(key);

            return scan(keyPrefix, keyPrefix, true, true, this::decodeRowId);
        });
    }

    @Override
    public void put(IndexRow row) {
        busy(() -> {
            try {
                @SuppressWarnings("resource") WriteBatchWithIndex writeBatch = PartitionDataHelper.requireWriteBatch();

                writeBatch.put(indexCf.handle(), rocksKey(row), BYTE_EMPTY_ARRAY);

                return null;
            } catch (RocksDBException e) {
                throw new StorageException("Unable to insert data into sorted index. Index ID: " + descriptor.id(), e);
            }
        });
    }

    @Override
    public void remove(IndexRow row) {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                @SuppressWarnings("resource") WriteBatchWithIndex writeBatch = PartitionDataHelper.requireWriteBatch();

                writeBatch.delete(indexCf.handle(), rocksKey(row));

                return null;
            } catch (RocksDBException e) {
                throw new StorageException("Unable to remove data from sorted index. Index ID: " + descriptor.id(), e);
            }
        });
    }

    @Override
    public PeekCursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
            boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

            return scan(lowerBound, upperBound, includeLower, includeUpper, this::decodeRow);
        });
    }

    private <T> PeekCursor<T> scan(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            boolean includeLower,
            boolean includeUpper,
            Function<ByteBuffer, T> mapper
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

        return createScanCursor(lowerBoundBytes, upperBoundBytes, mapper);
    }

    private <T> PeekCursor<T> createScanCursor(
            byte @Nullable [] lowerBound,
            byte @Nullable [] upperBound,
            Function<ByteBuffer, T> mapper
    ) {
        Slice upperBoundSlice = upperBound == null ? new Slice(helper.partitionEndPrefix()) : new Slice(upperBound);

        ReadOptions options = new ReadOptions().setIterateUpperBound(upperBoundSlice);

        RocksIterator it = indexCf.newIterator(options);

        return new PeekCursor<>() {
            @Nullable
            private Boolean hasNext;

            private byte @Nullable [] key;

            private byte @Nullable [] peekedKey = BYTE_EMPTY_ARRAY;

            @Override
            public void close() {
                try {
                    closeAll(it, options, upperBoundSlice);
                } catch (Exception e) {
                    throw new StorageException("Error closing cursor", e);
                }
            }

            @Override
            public boolean hasNext() {
                return busy(this::advanceIfNeeded);
            }

            @Override
            public T next() {
                return busy(() -> {
                    if (!advanceIfNeeded()) {
                        throw new NoSuchElementException();
                    }

                    this.hasNext = null;

                    return mapper.apply(ByteBuffer.wrap(key).order(KEY_BYTE_ORDER));
                });
            }

            @Override
            public @Nullable T peek() {
                return busy(() -> {
                    throwExceptionIfStorageInProgressOfRebalance(state.get(), RocksDbSortedIndexStorage.this::createStorageInfo);

                    byte[] res = peek0();

                    if (res == null) {
                        return null;
                    } else {
                        return mapper.apply(ByteBuffer.wrap(res).order(KEY_BYTE_ORDER));
                    }
                });
            }

            private byte @Nullable [] peek0() {
                if (hasNext != null) {
                    return key;
                }

                refreshAndPrepareRocksIterator();

                if (!it.isValid()) {
                    RocksUtils.checkIterator(it);

                    peekedKey = null;
                } else {
                    peekedKey = it.key();
                }

                return peekedKey;
            }

            private boolean advanceIfNeeded() throws StorageException {
                throwExceptionIfStorageInProgressOfRebalance(state.get(), RocksDbSortedIndexStorage.this::createStorageInfo);

                //noinspection ArrayEquality
                key = (peekedKey == BYTE_EMPTY_ARRAY) ? peek0() : peekedKey;
                peekedKey = BYTE_EMPTY_ARRAY;

                hasNext = key != null;
                return hasNext;
            }

            private void refreshAndPrepareRocksIterator() {
                try {
                    it.refresh();
                } catch (RocksDBException e) {
                    throw new StorageException("Error refreshing an iterator", e);
                }

                if (key == null) {
                    it.seek(lowerBound == null ? helper.partitionStartPrefix() : lowerBound);
                } else {
                    it.seekForPrev(key);

                    if (it.isValid()) {
                        it.next();
                    } else {
                        RocksUtils.checkIterator(it);

                        it.seek(lowerBound == null ? helper.partitionStartPrefix() : lowerBound);
                    }
                }
            }
        };
    }

    private static void setEqualityFlag(byte[] prefix) {
        // Flags start after the partition ID.
        byte flags = prefix[PARTITION_ID_SIZE];

        prefix[PARTITION_ID_SIZE] = (byte) (flags | BinaryTupleCommon.EQUALITY_FLAG);
    }

    private IndexRow decodeRow(ByteBuffer bytes) {
        assert bytes.getShort(0) == helper.partitionId();

        var tuple = new BinaryTuple(descriptor.binaryTupleSchema(), binaryTupleSlice(bytes));

        return new IndexRowImpl(tuple, decodeRowId(bytes));
    }

    private RowId decodeRowId(ByteBuffer bytes) {
        // RowId UUID is located at the last 16 bytes of the key
        long mostSignificantBits = bytes.getLong(bytes.limit() - Long.BYTES * 2);
        long leastSignificantBits = bytes.getLong(bytes.limit() - Long.BYTES);

        return new RowId(helper.partitionId(), mostSignificantBits, leastSignificantBits);
    }

    private byte[] rocksPrefix(BinaryTuplePrefix prefix) {
        ByteBuffer bytes = prefix.byteBuffer();

        return ByteBuffer.allocate(PARTITION_ID_SIZE + bytes.remaining())
                .order(KEY_BYTE_ORDER)
                .putShort((short) helper.partitionId())
                .put(bytes)
                .array();
    }

    private byte[] rocksKey(IndexRow row) {
        ByteBuffer bytes = row.indexColumns().byteBuffer();

        return ByteBuffer.allocate(PARTITION_ID_SIZE + bytes.remaining() + ROW_ID_SIZE)
                .order(KEY_BYTE_ORDER)
                .putShort((short) helper.partitionId())
                .put(bytes)
                .putLong(row.rowId().mostSignificantBits())
                .putLong(row.rowId().leastSignificantBits())
                .array();
    }

    private static ByteBuffer binaryTupleSlice(ByteBuffer key) {
        return key.duplicate()
                // Discard partition ID.
                .position(PARTITION_ID_SIZE)
                // Discard row ID.
                .limit(key.limit() - ROW_ID_SIZE)
                .slice()
                .order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public void destroyData(WriteBatch writeBatch) throws RocksDBException {
        byte[] constantPrefix = ByteBuffer.allocate(PARTITION_ID_SIZE)
                .order(KEY_BYTE_ORDER)
                .putShort((short) helper.partitionId())
                .array();

        byte[] rangeEnd = incrementPrefix(constantPrefix);

        assert rangeEnd != null;

        writeBatch.deleteRange(indexCf.handle(), constantPrefix, rangeEnd);
    }
}
