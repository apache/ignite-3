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
import static org.apache.ignite.internal.storage.rocksdb.RocksDbUtils.ORDER;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbUtils.PARTITION_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbUtils.ROW_ID_SIZE;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbUtils.UUID_SIZE;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageInProgressOfRebalance;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMvPartitionStorage;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.HashUtils;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;

/**
 * {@link HashIndexStorage} implementation based on RocksDB.
 *
 * <p>This storage uses the following format for keys:
 * <pre>
 * Index ID (UUID) - 16 bytes
 * Partition ID - 2 bytes
 * Tuple hash - 4 bytes
 * Tuple value - variable length
 * Row ID (UUID) - 16 bytes
 * </pre>
 *
 * <p>We use an empty array as values, because all required information can be extracted from the key.
 */
public class RocksDbHashIndexStorage extends AbstractRocksDbIndexStorage implements HashIndexStorage {
    /** Length of the fixed part of the key: Index ID + Partition ID + Hash. */
    public static final int FIXED_PREFIX_LENGTH = UUID_SIZE + PARTITION_ID_SIZE + Integer.BYTES;

    private final HashIndexDescriptor descriptor;

    private final ColumnFamily indexCf;

    /** Constant prefix of every index key. */
    private final byte[] constantPrefix;

    /**
     * Creates a new Hash Index storage.
     *
     * @param descriptor Index descriptor.
     * @param indexCf Column family that stores the index data.
     * @param partitionStorage Partition storage of the partition that is being indexed (needed for consistency guarantees).
     */
    public RocksDbHashIndexStorage(
            HashIndexDescriptor descriptor,
            ColumnFamily indexCf,
            RocksDbMvPartitionStorage partitionStorage
    ) {
        super(descriptor.id(), partitionStorage);

        this.descriptor = descriptor;
        this.indexCf = indexCf;

        UUID indexId = descriptor.id();

        this.constantPrefix = ByteBuffer.allocate(UUID_SIZE + PARTITION_ID_SIZE)
                .order(ORDER)
                .putLong(indexId.getMostSignificantBits())
                .putLong(indexId.getLeastSignificantBits())
                .putShort((short) partitionStorage.partitionId())
                .array();
    }

    @Override
    public HashIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            byte[] rangeStart = rocksPrefix(key);

            byte[] rangeEnd = incrementPrefix(rangeStart);

            Slice upperBound = rangeEnd == null ? null : new Slice(rangeEnd);

            ReadOptions options = new ReadOptions().setIterateUpperBound(upperBound);

            RocksIterator it = indexCf.newIterator(options);

            it.seek(rangeStart);

            return new RocksIteratorAdapter<RowId>(it) {
                @Override
                protected RowId decodeEntry(byte[] key, byte[] value) {
                    // RowId UUID is located at the last 16 bytes of the key
                    long mostSignificantBits = bytesToLong(key, key.length - Long.BYTES * 2);
                    long leastSignificantBits = bytesToLong(key, key.length - Long.BYTES);

                    return new RowId(partitionStorage.partitionId(), mostSignificantBits, leastSignificantBits);
                }

                @Override
                public boolean hasNext() {
                    return busy(() -> {
                        throwExceptionIfStorageInProgressOfRebalance(state.get(), RocksDbHashIndexStorage.this::createStorageInfo);

                        return super.hasNext();
                    });
                }

                @Override
                public RowId next() {
                    return busy(() -> {
                        throwExceptionIfStorageInProgressOfRebalance(state.get(), RocksDbHashIndexStorage.this::createStorageInfo);

                        return super.next();
                    });
                }

                @Override
                public void close() {
                    super.close();

                    RocksUtils.closeAll(options, upperBound);
                }
            };
        });
    }

    @Override
    public void put(IndexRow row) {
        busy(() -> {
            try {
                WriteBatchWithIndex writeBatch = partitionStorage.currentWriteBatch();

                writeBatch.put(indexCf.handle(), rocksKey(row), BYTE_EMPTY_ARRAY);

                return null;
            } catch (RocksDBException e) {
                throw new StorageException("Unable to insert data into hash index. Index ID: " + descriptor.id(), e);
            }
        });
    }

    @Override
    public void remove(IndexRow row) {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            try {
                WriteBatchWithIndex writeBatch = partitionStorage.currentWriteBatch();

                writeBatch.delete(indexCf.handle(), rocksKey(row));

                return null;
            } catch (RocksDBException e) {
                throw new StorageException("Unable to remove data from hash index. Index ID: " + descriptor.id(), e);
            }
        });
    }

    @Override
    public void destroy() {
        busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state.get(), this::createStorageInfo);

            byte[] rangeEnd = incrementPrefix(constantPrefix);

            assert rangeEnd != null;

            try (WriteOptions writeOptions = new WriteOptions().setDisableWAL(true)) {
                indexCf.db().deleteRange(indexCf.handle(), writeOptions, constantPrefix, rangeEnd);

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
                .order(ORDER)
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
    public void destroyData(WriteBatch writeBatch) throws RocksDBException {
        byte[] rangeEnd = incrementPrefix(constantPrefix);

        assert rangeEnd != null;

        writeBatch.deleteRange(indexCf.handle(), constantPrefix, rangeEnd);
    }
}
