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
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.ROW_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance.deleteByPrefix;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.HashUtils;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;

/**
 * {@link HashIndexStorage} implementation based on RocksDB.
 *
 * <p>This storage uses the following format for keys:
 * <pre>
 * Table ID - 4 bytes
 * Index ID - 4 bytes
 * Partition ID - 2 bytes
 * Tuple hash - 4 bytes
 * Tuple value - variable length
 * Row ID (UUID) - 16 bytes
 * </pre>
 *
 * <p>We use an empty array as values, because all required information can be extracted from the key.
 */
public class RocksDbHashIndexStorage extends AbstractRocksDbIndexStorage implements HashIndexStorage {
    /** Length of the fixed part of the key: Table ID + Index ID + Partition ID + Hash. */
    public static final int FIXED_PREFIX_LENGTH = PREFIX_WITH_IDS_LENGTH + Integer.BYTES;

    private final StorageHashIndexDescriptor descriptor;

    private final ColumnFamily indexCf;

    /** Constant prefix of every index key. */
    private final byte[] constantPrefix;

    /**
     * Creates a new Hash Index storage.
     *
     * @param descriptor Index descriptor.
     * @param tableId Table ID.
     * @param partitionId Partition ID.
     * @param indexCf Column family that stores the index data.
     * @param indexMetaStorage Index meta storage.
     */
    public RocksDbHashIndexStorage(
            StorageHashIndexDescriptor descriptor,
            int tableId,
            int partitionId,
            ColumnFamily indexCf,
            RocksDbMetaStorage indexMetaStorage
    ) {
        super(tableId, descriptor.id(), partitionId, indexMetaStorage, descriptor.isPk());

        this.descriptor = descriptor;
        this.indexCf = indexCf;

        this.constantPrefix = ByteBuffer.allocate(PREFIX_WITH_IDS_LENGTH)
                .order(KEY_BYTE_ORDER)
                .putInt(tableId)
                .putInt(descriptor.id())
                .putShort((short) partitionId)
                .array();
    }

    @Override
    public StorageHashIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) {
        return busyDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            throwExceptionIfIndexNotBuilt();

            byte[] rangeStart = rocksPrefix(key);

            byte[] rangeEnd = incrementPrefix(rangeStart);

            return new UpToDatePeekCursor<RowId>(rangeEnd, indexCf, rangeStart) {
                @Override
                protected RowId map(ByteBuffer byteBuffer) {
                    // RowId UUID is located at the last 16 bytes of the key
                    long mostSignificantBits = byteBuffer.getLong(rangeStart.length);
                    long leastSignificantBits = byteBuffer.getLong(rangeStart.length + Long.BYTES);

                    return new RowId(partitionId, mostSignificantBits, leastSignificantBits);
                }
            };
        });
    }

    @Override
    public void put(IndexRow row) {
        busyNonDataRead(() -> {
            try {
                WriteBatchWithIndex writeBatch = PartitionDataHelper.requireWriteBatch();

                writeBatch.put(indexCf.handle(), rocksKey(row), BYTE_EMPTY_ARRAY);

                return null;
            } catch (RocksDBException e) {
                throw new StorageException("Unable to insert data into hash index. Index ID: " + descriptor.id(), e);
            }
        });
    }

    @Override
    public void remove(IndexRow row) {
        busyNonDataRead(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                WriteBatchWithIndex writeBatch = PartitionDataHelper.requireWriteBatch();

                writeBatch.delete(indexCf.handle(), rocksKey(row));

                return null;
            } catch (RocksDBException e) {
                throw new StorageException("Unable to remove data from hash index. Index ID: " + descriptor.id(), e);
            }
        });
    }

    private byte[] rocksPrefix(BinaryTuple prefix) {
        return rocksPrefix(prefix, 0).array();
    }

    private ByteBuffer rocksPrefix(BinaryTuple prefix, int extraLength) {
        ByteBuffer keyBytes = prefix.byteBuffer();

        return ByteBuffer.allocate(FIXED_PREFIX_LENGTH + keyBytes.remaining() + extraLength)
                .order(KEY_BYTE_ORDER)
                .put(constantPrefix)
                .putInt(HashUtils.hash32(keyBytes))
                .put(keyBytes);
    }

    private byte[] rocksKey(IndexRow row) {
        RowId rowId = row.rowId();

        // We don't store the Partition ID as it is already a part of the key.
        return rocksPrefix(row.indexColumns(), ROW_ID_SIZE)
                .putLong(rowId.mostSignificantBits())
                .putLong(rowId.leastSignificantBits())
                .array();
    }

    @Override
    public void clearIndex(WriteBatch writeBatch) throws RocksDBException {
        deleteByPrefix(writeBatch, indexCf, constantPrefix);
    }
}
