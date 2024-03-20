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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
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

        final Set<Integer> indexIds;

        SortedIndexColumnFamily(ColumnFamily columnFamily, int indexId) {
            this.columnFamily = columnFamily;
            this.indexIds = new HashSet<>();

            indexIds.add(indexId);
        }

        SortedIndexColumnFamily(ColumnFamily columnFamily, Set<Integer> indexIds) {
            this.columnFamily = columnFamily;
            this.indexIds = indexIds;
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
            var indexIds = new HashSet<Integer>();

            try (Cursor<Integer> sortedIndexIdCursor = indexIdsCursor(sortedIndexCf)) {
                for (Integer indexId : sortedIndexIdCursor) {
                    indexIds.add(indexId);
                }
            }

            if (indexIds.isEmpty()) {
                destroyColumnFamily(sortedIndexCf);
            } else {
                this.sortedIndexCfsByName.put(
                        new ByteArray(sortedIndexCf.nameBytes()),
                        new SortedIndexColumnFamily(sortedIndexCf, indexIds)
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
    public Collection<Integer> hashIndexIds() {
        try (Cursor<Integer> hashIndexIdCursor = indexIdsCursor(hashIndexCf)) {
            return hashIndexIdCursor.stream().collect(toList());
        }
    }

    /**
     * Returns an "index ID - Column Family" mapping for all sorted indexes that currently exist in the storage.
     */
    public Map<Integer, ColumnFamily> sortedIndexes() {
        var result = new HashMap<Integer, ColumnFamily>();

        for (SortedIndexColumnFamily indexCf : sortedIndexCfsByName.values()) {
            for (Integer indexId : indexCf.indexIds) {
                result.put(indexId, indexCf.columnFamily);
            }
        }

        return result;
    }

    /**
     * Returns Column Family instance with the desired name. Creates it if it doesn't exist. Tracks every created index by its
     * {@code indexId}.
     */
    public ColumnFamily getOrCreateSortedIndexCf(byte[] cfName, int indexId) {
        if (!busyLock.enterBusy()) {
            throw new StorageClosedException();
        }

        try {
            SortedIndexColumnFamily result = sortedIndexCfsByName.compute(new ByteArray(cfName), (unused, sortedIndexCf) -> {
                if (sortedIndexCf == null) {
                    return new SortedIndexColumnFamily(createColumnFamily(cfName), indexId);
                } else {
                    sortedIndexCf.indexIds.add(indexId);

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
                indexCf.indexIds.remove(indexId);

                if (!indexCf.indexIds.isEmpty()) {
                    return indexCf;
                }

                destroyColumnFamily(indexCf.columnFamily);

                return null;
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static Cursor<Integer> indexIdsCursor(ColumnFamily cf) {
        return new IndexIdCursor(cf.newIterator());
    }

    private ColumnFamily createColumnFamily(byte[] cfName) {
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
