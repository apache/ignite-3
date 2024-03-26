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

package org.apache.ignite.internal.storage.rocksdb.instance;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.toStringName;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstanceCreator.sortedIndexCfOptions;
import static org.apache.ignite.internal.util.ByteUtils.intToBytes;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.rocksdb.IndexIdCursor;
import org.apache.ignite.internal.storage.rocksdb.IndexIdCursor.TableAndIndexId;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Shared RocksDB instance for multiple tables. Managed directly by the engine.
 */
public final class SharedRocksDbInstance {
    /** Write options. */
    public static final WriteOptions DFLT_WRITE_OPTS = new WriteOptions().setDisableWAL(true);

    /**
     * Class that represents a Column Family for sorted indexes and all index IDs that map to this Column Family.
     *
     * <p>Sorted indexes that have the same order and type of indexed columns get mapped to the same Column Family in order to share
     * the same comparator.
     */
    private static class SortedIndexColumnFamily implements AutoCloseable {
        final ColumnFamily columnFamily;

        final Map<Integer, Integer> indexIdToTableId;

        SortedIndexColumnFamily(ColumnFamily columnFamily, int indexId, int tableId) {
            this.columnFamily = columnFamily;
            this.indexIdToTableId = new HashMap<>();

            indexIdToTableId.put(indexId, tableId);
        }

        SortedIndexColumnFamily(ColumnFamily columnFamily, Map<Integer, Integer> indexIdToTableId) {
            this.columnFamily = columnFamily;
            this.indexIdToTableId = indexIdToTableId;
        }

        @Override
        public void close() {
            columnFamily.handle().close();
        }
    }

    /** RocksDB storage engine instance. */
    public final RocksDbStorageEngine engine;

    /** Path for the directory that stores the data. */
    public final Path path;

    /** RocksDB flusher instance. */
    public final RocksDbFlusher flusher;

    /** Rocks DB instance. */
    public final RocksDB db;

    /** Meta information instance that wraps {@link ColumnFamily} instance for meta column family. */
    public final RocksDbMetaStorage meta;

    /** Column Family for partition data. */
    public final ColumnFamily partitionCf;

    /** Column Family for GC queue. */
    public final ColumnFamily gcQueueCf;

    /** Column Family for Hash Index data. */
    private final ColumnFamily hashIndexCf;

    /** Column Family instances for different types of sorted indexes, identified by the column family name. */
    private final ConcurrentMap<ByteArray, SortedIndexColumnFamily> sortedIndexCfsByName = new ConcurrentHashMap<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    SharedRocksDbInstance(
            RocksDbStorageEngine engine,
            Path path,
            IgniteSpinBusyLock busyLock,
            RocksDbFlusher flusher,
            RocksDB db,
            RocksDbMetaStorage meta,
            ColumnFamily partitionCf,
            ColumnFamily gcQueueCf,
            ColumnFamily hashIndexCf,
            List<ColumnFamily> sortedIndexCfs
    ) {
        this.engine = engine;
        this.path = path;
        this.busyLock = busyLock;

        this.flusher = flusher;
        this.db = db;

        this.meta = meta;
        this.partitionCf = partitionCf;
        this.gcQueueCf = gcQueueCf;
        this.hashIndexCf = hashIndexCf;

        recoverExistingSortedIndexes(sortedIndexCfs);
    }

    private void recoverExistingSortedIndexes(List<ColumnFamily> sortedIndexCfs) {
        for (ColumnFamily sortedIndexCf : sortedIndexCfs) {
            var indexIdToTableId = new HashMap<Integer, Integer>();

            try (var sortedIndexIdCursor = new IndexIdCursor(sortedIndexCf.newIterator(), null)) {
                for (TableAndIndexId tableAndIndexId : sortedIndexIdCursor) {
                    indexIdToTableId.put(tableAndIndexId.indexId(), tableAndIndexId.tableId());
                }
            }

            if (indexIdToTableId.isEmpty()) {
                destroyColumnFamily(sortedIndexCf);
            } else {
                this.sortedIndexCfsByName.put(
                        new ByteArray(sortedIndexCf.nameBytes()),
                        new SortedIndexColumnFamily(sortedIndexCf, indexIdToTableId)
                );
            }
        }
    }

    /**
     * Utility method that performs range-deletion in the column family.
     */
    public static void deleteByPrefix(WriteBatch writeBatch, ColumnFamily columnFamily, byte[] prefix) throws RocksDBException {
        byte[] upperBound = incrementPrefix(prefix);

        writeBatch.deleteRange(columnFamily.handle(), prefix, upperBound);
    }

    /**
     * Stops the instance, freeing all allocated resources.
     */
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        List<AutoCloseable> resources = new ArrayList<>();

        resources.add(meta.columnFamily().handle());
        resources.add(partitionCf.handle());
        resources.add(gcQueueCf.handle());
        resources.add(hashIndexCf.handle());
        resources.addAll(sortedIndexCfsByName.values());

        resources.add(db);
        resources.add(flusher::stop);

        try {
            Collections.reverse(resources);

            IgniteUtils.closeAll(resources);
        } catch (Exception e) {
            throw new StorageException("Failed to stop RocksDB storage: " + path, e);
        }
    }

    /**
     * Returns the Column Family containing all hash indexes.
     */
    public ColumnFamily hashIndexCf() {
        return hashIndexCf;
    }

    /**
     * Returns a collection of all hash index IDs that currently exist in the storage.
     */
    public Collection<Integer> hashIndexIds(int tableId) {
        try (
                var readOptions = new ReadOptions();
                var upperBound = tableId == -1 ? null : new Slice(intToBytes(tableId + 1))
        ) {
            // Using total order seek, because the cursor only uses table ID + index ID as the prefix.
            readOptions
                    .setTotalOrderSeek(true)
                    .setIterateUpperBound(upperBound);

            RocksIterator it = hashIndexCf.newIterator(readOptions);

            try (var hashIndexIdCursor = new IndexIdCursor(it, tableId)) {
                return hashIndexIdCursor.stream().map(TableAndIndexId::indexId).collect(toList());
            }
        }
    }

    /**
     * Returns an "index ID - Column Family" mapping for all sorted indexes that currently exist in the storage.
     */
    public Map<Integer, ColumnFamily> sortedIndexes(int targetTableId) {
        var result = new HashMap<Integer, ColumnFamily>();

        for (SortedIndexColumnFamily indexCf : sortedIndexCfsByName.values()) {
            indexCf.indexIdToTableId.forEach((indexId, tableId) -> {
                if (tableId == targetTableId) {
                    result.put(indexId, indexCf.columnFamily);
                }
            });
        }

        return result;
    }

    /**
     * Returns Column Family instance with the desired name. Creates it if it doesn't exist. Tracks every created index by its
     * {@code indexId}.
     */
    public ColumnFamily getOrCreateSortedIndexCf(byte[] cfName, int indexId, int tableId) {
        if (!busyLock.enterBusy()) {
            throw new StorageClosedException();
        }

        try {
            SortedIndexColumnFamily result = sortedIndexCfsByName.compute(new ByteArray(cfName), (unused, sortedIndexCf) -> {
                if (sortedIndexCf == null) {
                    return new SortedIndexColumnFamily(createSortedIndexCf(cfName), indexId, tableId);
                } else {
                    sortedIndexCf.indexIdToTableId.put(indexId, tableId);

                    return sortedIndexCf;
                }
            });

            return result.columnFamily;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Possibly drops the column family after destroying the index.
     */
    public void destroySortedIndexCfIfNeeded(byte[] cfName, int indexId) {
        if (!busyLock.enterBusy()) {
            throw new StorageClosedException();
        }

        try {
            sortedIndexCfsByName.computeIfPresent(new ByteArray(cfName), (unused, indexCf) -> {
                indexCf.indexIdToTableId.remove(indexId);

                if (!indexCf.indexIdToTableId.isEmpty()) {
                    return indexCf;
                }

                destroyColumnFamily(indexCf.columnFamily);

                return null;
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private ColumnFamily createSortedIndexCf(byte[] cfName) {
        ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName, sortedIndexCfOptions(cfName));

        ColumnFamily columnFamily;
        try {
            columnFamily = ColumnFamily.create(db, cfDescriptor);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to create new RocksDB column family: " + toStringName(cfDescriptor.getName()), e);
        }

        flusher.addColumnFamily(columnFamily.handle());

        return columnFamily;
    }

    private void destroyColumnFamily(ColumnFamily columnFamily) {
        ColumnFamilyHandle columnFamilyHandle = columnFamily.handle();

        flusher.removeColumnFamily(columnFamilyHandle);

        try {
            columnFamily.destroy();
        } catch (RocksDBException e) {
            throw new StorageException(
                    "Failed to destroy RocksDB Column Family. [cfName={}, path={}]",
                    e, columnFamily.name(), path
            );
        }
    }
}
