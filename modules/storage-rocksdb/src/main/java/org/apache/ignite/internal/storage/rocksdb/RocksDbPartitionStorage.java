/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

import static java.util.Collections.nCopies;
import static org.apache.ignite.internal.rocksdb.snapshot.ColumnFamilyRange.range;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.rocksdb.snapshot.ColumnFamilyRange;
import org.apache.ignite.internal.rocksdb.snapshot.RocksSnapshotManager;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageUtils;
import org.apache.ignite.internal.storage.basic.DelegatingDataRow;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Storage implementation based on a single RocksDB instance.
 */
class RocksDbPartitionStorage implements PartitionStorage {
    /**
     * Size of the overhead for all keys in the storage: partition ID (unsigned {@code short}) + key hash ({@code int}).
     */
    private static final int PARTITION_KEY_PREFIX_SIZE = Short.BYTES + Integer.BYTES;

    /**
     * Partition ID (should be treated as an unsigned short).
     *
     * <p>Partition IDs are always stored in the big endian order, since they need to be compared lexicographically.
     */
    private final int partId;

    /** RocksDb instance. */
    private final RocksDB db;

    /** Data column family. */
    private final ColumnFamily data;

    /** Snapshot manager. */
    private final RocksSnapshotManager snapshotManager;

    /**
     * Lock used to insure thread-safety of the {@link #restoreSnapshot} method.
     */
    private final Object snapshotRestoreLock = new Object();

    /**
     * Constructor.
     *
     * @param partId       Partition id.
     * @param db           Rocks DB instance.
     * @param columnFamily Column family to be used for all storage operations. This class does not own the column family handler
     *                     as it is shared between multiple storages and will not close it.
     * @param threadPool   Thread pool for async operations.
     * @throws StorageException If failed to create RocksDB instance.
     */
    RocksDbPartitionStorage(RocksDB db, ColumnFamily columnFamily, int partId, Executor threadPool) throws StorageException {
        assert partId >= 0 && partId < 0xFFFF : partId;

        this.partId = partId;
        this.db = db;
        this.data = columnFamily;

        ColumnFamilyRange snapshotRange = range(columnFamily, partitionStartPrefix(), partitionEndPrefix());

        this.snapshotManager = new RocksSnapshotManager(db, List.of(snapshotRange), threadPool);
    }

    /** {@inheritDoc} */
    @Override
    public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public DataRow read(SearchRow key) throws StorageException {
        try {
            byte[] valueBytes = data.get(partitionKey(key));

            return valueBytes == null ? null : new DelegatingDataRow(key, valueBytes);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to read data from the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> readAll(List<? extends SearchRow> keys) throws StorageException {
        int resultSize = keys.size();

        List<byte[]> values;

        try {
            values = db.multiGetAsList(nCopies(resultSize, data.handle()), getKeys(keys));
        } catch (RocksDBException e) {
            throw new StorageException("Failed to read data from the storage", e);
        }

        assert resultSize == values.size();

        List<DataRow> res = new ArrayList<>(resultSize);

        for (int i = 0; i < resultSize; i++) {
            byte[] value = values.get(i);

            if (value != null) {
                res.add(new DelegatingDataRow(keys.get(i), value));
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public void write(DataRow row) throws StorageException {
        try {
            byte[] value = row.valueBytes();

            assert value != null;

            data.put(partitionKey(row), value);
        } catch (RocksDBException e) {
            throw new StorageException("Filed to write data to the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeAll(List<? extends DataRow> rows) throws StorageException {
        try (WriteBatch batch = new WriteBatch();
                WriteOptions opts = new WriteOptions()) {
            for (DataRow row : rows) {
                byte[] value = row.valueBytes();

                assert value != null;

                data.put(batch, partitionKey(row), value);
            }

            db.write(opts, batch);
        } catch (RocksDBException e) {
            throw new StorageException("Filed to write data to the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> insertAll(List<? extends DataRow> rows) throws StorageException {
        List<DataRow> cantInsert = new ArrayList<>();

        try (var batch = new WriteBatch();
                var opts = new WriteOptions()) {

            for (DataRow row : rows) {
                byte[] partitionKey = partitionKey(row);

                if (data.get(partitionKey) == null) {
                    byte[] value = row.valueBytes();

                    assert value != null;

                    data.put(batch, partitionKey, value);
                } else {
                    cantInsert.add(row);
                }
            }

            db.write(opts, batch);
        } catch (RocksDBException e) {
            throw new StorageException("Filed to write data to the storage", e);
        }

        return cantInsert;
    }

    /** {@inheritDoc} */
    @Override
    public void remove(SearchRow key) throws StorageException {
        try {
            data.delete(partitionKey(key));
        } catch (RocksDBException e) {
            throw new StorageException("Failed to remove data from the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Collection<SearchRow> removeAll(List<? extends SearchRow> keys) {
        List<SearchRow> skippedRows = new ArrayList<>();

        try (var batch = new WriteBatch();
                var opts = new WriteOptions()) {

            for (SearchRow key : keys) {
                byte[] partitionKey = partitionKey(key);

                byte[] value = data.get(partitionKey);

                if (value != null) {
                    data.delete(batch, partitionKey);
                } else {
                    skippedRows.add(key);
                }
            }

            db.write(opts, batch);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to remove data from the storage", e);
        }

        return skippedRows;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> removeAllExact(List<? extends DataRow> keyValues) {
        List<DataRow> skippedRows = new ArrayList<>();

        try (WriteBatch batch = new WriteBatch();
                WriteOptions opts = new WriteOptions()) {

            List<byte[]> keys = getKeys(keyValues);
            List<byte[]> values = db.multiGetAsList(nCopies(keys.size(), data.handle()), keys);

            assert values.size() == keys.size();

            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);
                byte[] expectedValue = keyValues.get(i).valueBytes();
                byte[] value = values.get(i);

                if (Arrays.equals(value, expectedValue)) {
                    data.delete(batch, key);
                } else {
                    skippedRows.add(keyValues.get(i));
                }
            }

            db.write(opts, batch);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to remove data from the storage", e);
        }

        return skippedRows;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public <T> T invoke(SearchRow key, InvokeClosure<T> clo) throws StorageException {
        try {
            byte[] partitionKey = partitionKey(key);

            byte[] existingDataBytes = data.get(partitionKey);

            clo.call(existingDataBytes == null ? null : new DelegatingDataRow(key, existingDataBytes));

            switch (clo.operationType()) {
                case WRITE:
                    DataRow newRow = clo.newRow();

                    assert newRow != null;

                    byte[] value = newRow.valueBytes();

                    assert value != null;

                    data.put(partitionKey, value);

                    break;

                case REMOVE:
                    data.delete(partitionKey);

                    break;

                case NOOP:
                    break;

                default:
                    throw new UnsupportedOperationException(String.valueOf(clo.operationType()));
            }

            return clo.result();
        } catch (RocksDBException e) {
            throw new StorageException("Failed to access data in the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {
        var upperBound = new Slice(partitionEndPrefix());

        var options = new ReadOptions().setIterateUpperBound(upperBound);

        RocksIterator it = data.newIterator(options);

        it.seek(partitionStartPrefix());

        return new ScanCursor(it, filter) {
            @Override
            public void close() throws Exception {
                super.close();

                IgniteUtils.closeAll(options, upperBound);
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> snapshot(Path snapshotPath) {
        return snapshotManager.createSnapshot(snapshotPath);
    }

    /** {@inheritDoc} */
    @Override
    public void restoreSnapshot(Path path) {
        synchronized (snapshotRestoreLock) {
            destroy();

            snapshotManager.restoreSnapshot(path);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        // nothing to do
    }

    @Override
    public void destroy() {
        try {
            data.deleteRange(partitionStartPrefix(), partitionEndPrefix());
        } catch (RocksDBException e) {
            throw new StorageException("Unable to delete partition " + partId, e);
        }
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

    /** Cursor wrapper over the RocksIterator object with custom filter. */
    private static class ScanCursor extends RocksIteratorAdapter<DataRow> {
        /** Custom filter predicate. */
        private final Predicate<SearchRow> filter;

        /**
         * Constructor.
         *
         * @param iter   Iterator.
         * @param filter Filter.
         */
        private ScanCursor(RocksIterator iter, Predicate<SearchRow> filter) {
            super(iter);

            this.filter = filter;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            while (super.hasNext() && !filter.test(decodeEntry(it.key(), it.value()))) {
                it.next();
            }

            return super.hasNext();
        }

        @Override
        protected DataRow decodeEntry(byte[] key, byte[] value) {
            byte[] rowKey = Arrays.copyOfRange(key, PARTITION_KEY_PREFIX_SIZE, key.length);

            return new SimpleDataRow(rowKey, value);
        }
    }

    /**
     * Creates a key used in this partition storage by prepending a partition ID (to distinguish between different partition data)
     * and the key's hash (an optimisation).
     */
    private byte[] partitionKey(SearchRow key) {
        ByteBuffer keyBuffer = key.key();

        return ByteBuffer.allocate(PARTITION_KEY_PREFIX_SIZE + keyBuffer.limit())
                .order(ByteOrder.BIG_ENDIAN)
                .putShort((short) partId)
                // TODO: use precomputed hash, see https://issues.apache.org/jira/browse/IGNITE-16370
                .putInt(StorageUtils.hashCode(keyBuffer))
                .put(keyBuffer)
                .array();
    }

    /**
     * Gets a list of key byte arrays.
     *
     * @param keyValues Key rows.
     * @return List of keys as byte arrays.
     */
    private List<byte[]> getKeys(List<? extends SearchRow> keyValues) {
        List<byte[]> keys = new ArrayList<>(keyValues.size());

        for (SearchRow keyValue : keyValues) {
            keys.add(partitionKey(keyValue));
        }

        return keys;
    }
}
