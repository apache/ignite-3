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
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.PeekCursor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage;
import org.apache.ignite.internal.storage.util.StorageUtils;
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
 * Table ID - 4 bytes
 * Index ID - 4 bytes
 * Partition ID - 2 bytes
 * Tuple value - variable length
 * Row ID (UUID) - 16 bytes
 * </pre>
 *
 * <p>We use an empty array as values, because all required information can be extracted from the key.
 */
public class RocksDbSortedIndexStorage extends AbstractRocksDbIndexStorage implements SortedIndexStorage {
    private final StorageSortedIndexDescriptor descriptor;

    private final ColumnFamily indexCf;
    private final byte[] partitionStartPrefix;
    private final byte[] partitionEndPrefix;

    private final CatalogIndexStatusSupplier indexStatusSupplier;

    /**
     * Creates a storage.
     *
     * @param descriptor Sorted Index descriptor.
     * @param tableId Table ID.
     * @param partitionId Partition ID.
     * @param indexCf Column family that stores the index data.
     * @param indexMetaStorage Index meta storage.
     * @param indexStatusSupplier Catalog index status supplier.
     */
    public RocksDbSortedIndexStorage(
            StorageSortedIndexDescriptor descriptor,
            int tableId,
            int partitionId,
            ColumnFamily indexCf,
            RocksDbMetaStorage indexMetaStorage,
            CatalogIndexStatusSupplier indexStatusSupplier
    ) {
        super(tableId, descriptor.id(), partitionId, indexMetaStorage, descriptor.isPk());

        this.descriptor = descriptor;
        this.indexCf = indexCf;
        this.indexStatusSupplier = indexStatusSupplier;

        this.partitionStartPrefix = ByteBuffer.allocate(PREFIX_WITH_IDS_LENGTH)
                .order(KEY_BYTE_ORDER)
                .putInt(tableId)
                .putInt(indexId)
                .putShort((short) partitionId)
                .array();

        this.partitionEndPrefix = incrementPrefix(partitionStartPrefix);
    }

    @Override
    public StorageSortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        return busyDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            throwExceptionIfIndexNotBuilt();

            BinaryTuplePrefix keyPrefix = BinaryTuplePrefix.fromBinaryTuple(key);

            return scan(keyPrefix, keyPrefix, true, true, this::decodeRowId);
        });
    }

    @Override
    public void put(IndexRow row) {
        busyNonDataRead(() -> {
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
        busyNonDataRead(() -> {
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
        return scanInternal(lowerBound, upperBound, flags, true);
    }

    protected <T> PeekCursor<T> scan(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            boolean includeLower,
            boolean includeUpper,
            Function<ByteBuffer, T> mapper
    ) {
        byte[] lowerBoundBytes = getBound(lowerBound, partitionStartPrefix, !includeLower);

        byte[] upperBoundBytes = getBound(upperBound, partitionEndPrefix, includeUpper);

        return new UpToDatePeekCursor<>(upperBoundBytes, indexCf, lowerBoundBytes) {
            @Override
            protected T map(ByteBuffer byteBuffer) {
                return mapper.apply(byteBuffer);
            }
        };
    }

    @Override
    public Cursor<IndexRow> readOnlyScan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return busyDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            throwExceptionIfIndexNotBuilt();

            boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
            boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

            byte[] lowerBoundBytes = getBound(lowerBound, partitionStartPrefix, !includeLower);
            byte[] upperBoundBytes = getBound(upperBound, partitionEndPrefix, includeUpper);

            Slice upperBoundSlice = new Slice(upperBoundBytes);

            ReadOptions readOptions = new ReadOptions()
                    .setIterateUpperBound(upperBoundSlice);

            RocksIterator iterator = indexCf.newIterator(readOptions);
            iterator.seek(lowerBoundBytes);

            return new Cursor<IndexRow>() {
                private final RocksIterator it = iterator;

                private byte[] key;

                private boolean advance;

                @Override
                public void close() {
                    try {
                        closeAll(it, readOptions, upperBoundSlice);
                    } catch (Exception e) {
                        throw new StorageException("Error closing RocksDB RO cursor", e);
                    }
                }

                @Override
                public boolean hasNext() {
                    return busyDataRead(this::advanceIfNeededBusy);
                }

                @Override
                public IndexRow next() {
                    return busyDataRead(() -> {
                        if (!advanceIfNeededBusy()) {
                            throw new NoSuchElementException();
                        }

                        advance = true;

                        return decodeRow((ByteBuffer.wrap(key).order(KEY_BYTE_ORDER)));
                    });
                }

                private boolean advanceIfNeededBusy() throws StorageException {
                    throwExceptionIfStorageInProgressOfRebalance(state.get(), () -> createStorageInfo());

                    if (advance) {
                        it.next();
                        advance = false;
                    }

                    if (!it.isValid()) {
                        return false;
                    }

                    key = it.key();

                    return true;
                }
            };
        });
    }

    @Override
    public PeekCursor<IndexRow> tolerantScan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return scanInternal(lowerBound, upperBound, flags, false);
    }

    private byte[] getBound(@Nullable BinaryTuplePrefix bound, byte[] partitionPrefix, boolean changeBoundIncluded) {
        byte[] boundBytes;

        if (bound == null) {
            boundBytes = partitionPrefix;
        } else {
            boundBytes = rocksPrefix(bound);

            // RocksDB excludes upper and includes lower by default), set flag to change.
            if (changeBoundIncluded) {
                setEqualityFlag(boundBytes);
            }
        }

        return boundBytes;
    }

    private static void setEqualityFlag(byte[] prefix) {
        //noinspection ImplicitNumericConversion
        prefix[PREFIX_WITH_IDS_LENGTH] |= BinaryTupleCommon.EQUALITY_FLAG;
    }

    private IndexRow decodeRow(ByteBuffer bytes) {
        assert bytes.getShort(PREFIX_WITH_IDS_LENGTH - PARTITION_ID_SIZE) == partitionId;

        var tuple = new BinaryTuple(descriptor.binaryTupleSchema().elementCount(), binaryTupleSlice(bytes));

        return new IndexRowImpl(tuple, decodeRowId(bytes));
    }

    private RowId decodeRowId(ByteBuffer bytes) {
        // RowId UUID is located at the last 16 bytes of the key
        long mostSignificantBits = bytes.getLong(bytes.limit() - Long.BYTES * 2);
        long leastSignificantBits = bytes.getLong(bytes.limit() - Long.BYTES);

        return new RowId(partitionId, mostSignificantBits, leastSignificantBits);
    }

    private byte[] rocksPrefix(BinaryTuplePrefix prefix) {
        ByteBuffer bytes = prefix.byteBuffer();

        return ByteBuffer.allocate(PREFIX_WITH_IDS_LENGTH + bytes.remaining())
                .order(KEY_BYTE_ORDER)
                .put(partitionStartPrefix)
                .put(bytes)
                .array();
    }

    private byte[] rocksKey(IndexRow row) {
        ByteBuffer bytes = row.indexColumns().byteBuffer();

        return ByteBuffer.allocate(PREFIX_WITH_IDS_LENGTH + bytes.remaining() + ROW_ID_SIZE)
                .order(KEY_BYTE_ORDER)
                .put(partitionStartPrefix)
                .put(bytes)
                .putLong(row.rowId().mostSignificantBits())
                .putLong(row.rowId().leastSignificantBits())
                .array();
    }

    private static ByteBuffer binaryTupleSlice(ByteBuffer key) {
        return key.duplicate()
                // Discard partition ID.
                .position(PREFIX_WITH_IDS_LENGTH)
                // Discard row ID.
                .limit(key.limit() - ROW_ID_SIZE)
                .slice()
                .order(BinaryTuple.ORDER);
    }

    @Override
    public void clearIndex(WriteBatch writeBatch) throws RocksDBException {
        writeBatch.deleteRange(indexCf.handle(), partitionStartPrefix, partitionEndPrefix);
    }

    private PeekCursor<IndexRow> scanInternal(
            @Nullable BinaryTuplePrefix lowerBound,
            @Nullable BinaryTuplePrefix upperBound,
            int flags,
            boolean onlyBuiltIndex
    ) {
        return busyDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            if (onlyBuiltIndex) {
                throwExceptionIfIndexNotBuilt();
            } else {
                throwExceptionIfIndexIsNotBuiltInReadableStatus();
            }

            boolean includeLower = (flags & GREATER_OR_EQUAL) != 0;
            boolean includeUpper = (flags & LESS_OR_EQUAL) != 0;

            return scan(lowerBound, upperBound, includeLower, includeUpper, this::decodeRow);
        });
    }

    private void throwExceptionIfIndexIsNotBuiltInReadableStatus() {
        StorageUtils.throwExceptionIfIndexIsNotBuiltInReadableStatus(
                nextRowIdToBuild,
                () -> indexStatusSupplier.get(indexId),
                this::createStorageInfo
        );
    }
}
