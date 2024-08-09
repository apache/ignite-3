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
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.ESTIMATED_SIZE_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.INDEX_ROW_ID_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.LEASE_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.PARTITION_CONF_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage.PARTITION_META_PREFIX;
import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageUtils.KEY_BYTE_ORDER;
import static org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstanceCreator.sortedIndexCfOptions;
import static org.apache.ignite.internal.util.ByteUtils.intToBytes;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
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

        final Map<Integer, Integer> indexIdToTableId = new ConcurrentHashMap<>();

        SortedIndexColumnFamily(ColumnFamily columnFamily) {
            this.columnFamily = columnFamily;
        }

        SortedIndexColumnFamily(ColumnFamily columnFamily, Map<Integer, Integer> indexIdToTableId) {
            this.columnFamily = columnFamily;

            this.indexIdToTableId.putAll(indexIdToTableId);
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

    public final ColumnFamily dataCf;

    /** Column Family for Hash Index data. */
    private final ColumnFamily hashIndexCf;

    /** Column Family instances for different types of sorted indexes, identified by the column family name. */
    private final ConcurrentMap<ByteArray, SortedIndexColumnFamily> sortedIndexCfsByName = new ConcurrentHashMap<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Tracks external resources that need to be closed. */
    private final List<AutoCloseable> resources;

    SharedRocksDbInstance(
            RocksDbStorageEngine engine,
            Path path,
            IgniteSpinBusyLock busyLock,
            RocksDbFlusher flusher,
            RocksDB db,
            RocksDbMetaStorage meta,
            ColumnFamily partitionCf,
            ColumnFamily gcQueueCf,
            ColumnFamily dataCf,
            ColumnFamily hashIndexCf,
            List<ColumnFamily> sortedIndexCfs,
            List<AutoCloseable> resources
    ) {
        this.engine = engine;
        this.path = path;
        this.busyLock = busyLock;

        this.flusher = flusher;
        this.db = db;

        this.meta = meta;
        this.partitionCf = partitionCf;
        this.gcQueueCf = gcQueueCf;
        this.dataCf = dataCf;
        this.hashIndexCf = hashIndexCf;

        this.resources = new ArrayList<>(resources);

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

        // Add resources from sorted indexes.
        {
            int expectedSize = sortedIndexCfsByName.size();
            List<AutoCloseable> sortedIndexOptions = new ArrayList<>(expectedSize);
            List<AutoCloseable> sortedIndexHandles = new ArrayList<>(expectedSize);
            for (SortedIndexColumnFamily sortedIndexCf : sortedIndexCfsByName.values()) {
                ColumnFamily cf = sortedIndexCf.columnFamily;
                sortedIndexHandles.add(cf.handle());
                @Nullable ColumnFamilyOptions options = cf.privateOptions();
                if (options != null) {
                    sortedIndexOptions.add(cf.privateOptions());
                }
            }

            // Some of the CF handles/options might be repeated, it should not be a critical but it is not ideal.
            resources.addAll(0, sortedIndexOptions);
            resources.addAll(sortedIndexHandles);
        }

        resources.add(flusher::stop);

        try {
            Collections.reverse(resources);

            closeAll(resources);
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
    public List<IndexColumnFamily> sortedIndexes(int targetTableId) {
        var result = new ArrayList<IndexColumnFamily>();

        for (SortedIndexColumnFamily indexCf : sortedIndexCfsByName.values()) {
            indexCf.indexIdToTableId.forEach((indexId, tableId) -> {
                if (tableId == targetTableId) {
                    result.add(new IndexColumnFamily(indexId, indexCf.columnFamily));
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
                    sortedIndexCf = new SortedIndexColumnFamily(createSortedIndexCf(cfName));
                }

                sortedIndexCf.indexIdToTableId.put(indexId, tableId);

                return sortedIndexCf;
            });

            return result.columnFamily;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Removes the given sorted index from this instance. This prevents this index to be returned by {@link #sortedIndexes} call.
     */
    public void removeSortedIndex(int indexId, ColumnFamily cf) {
        var cfNameBytes = new ByteArray(cf.nameBytes());

        sortedIndexCfsByName.computeIfPresent(cfNameBytes, (unused, indexCf) -> {
            indexCf.indexIdToTableId.remove(indexId);

            return indexCf;
        });
    }

    /**
     * Schedules a drop of a column family after destroying an index, if it was the last index managed by that CF.
     */
    public CompletableFuture<Void> scheduleIndexCfsDestroyIfNeeded(List<ColumnFamily> columnFamilies) {
        assert !columnFamilies.isEmpty();

        return flusher.awaitFlush(false)
                .thenRunAsync(() -> {
                    if (!busyLock.enterBusy()) {
                        throw new StorageClosedException();
                    }

                    try {
                        columnFamilies.forEach(this::destroySortedIndexCfIfNeeded);
                    } finally {
                        busyLock.leaveBusy();
                    }
                }, engine.threadPool());
    }

    void destroySortedIndexCfIfNeeded(ColumnFamily columnFamily) {
        var cfNameBytes = new ByteArray(columnFamily.nameBytes());

        sortedIndexCfsByName.computeIfPresent(cfNameBytes, (unused, indexCf) -> {
            if (!indexCf.indexIdToTableId.isEmpty()) {
                return indexCf;
            }

            destroyColumnFamily(indexCf.columnFamily);

            return null;
        });
    }

    /**
     * Removes all data associated with the given table ID in this storage.
     */
    public void destroyTable(int targetTableId) {
        try (WriteBatch writeBatch = new WriteBatch()) {
            byte[] tableIdBytes = ByteBuffer.allocate(Integer.BYTES)
                    .order(KEY_BYTE_ORDER)
                    .putInt(targetTableId)
                    .array();

            deleteByPrefix(writeBatch, partitionCf, tableIdBytes);
            deleteByPrefix(writeBatch, dataCf, tableIdBytes);
            deleteByPrefix(writeBatch, gcQueueCf, tableIdBytes);
            deleteByPrefix(writeBatch, hashIndexCf, tableIdBytes);

            deleteByPrefix(writeBatch, meta.columnFamily(), metaPrefix(PARTITION_META_PREFIX, tableIdBytes));
            deleteByPrefix(writeBatch, meta.columnFamily(), metaPrefix(PARTITION_CONF_PREFIX, tableIdBytes));
            deleteByPrefix(writeBatch, meta.columnFamily(), metaPrefix(INDEX_ROW_ID_PREFIX, tableIdBytes));
            deleteByPrefix(writeBatch, meta.columnFamily(), metaPrefix(LEASE_PREFIX, tableIdBytes));
            deleteByPrefix(writeBatch, meta.columnFamily(), metaPrefix(ESTIMATED_SIZE_PREFIX, tableIdBytes));

            var cfsToRemove = new ArrayList<ColumnFamily>();

            for (SortedIndexColumnFamily indexCf : sortedIndexCfsByName.values()) {
                Iterator<Integer> it = indexCf.indexIdToTableId.values().iterator();

                while (it.hasNext()) {
                    int tableId = it.next();

                    if (targetTableId == tableId) {
                        it.remove();

                        deleteByPrefix(writeBatch, indexCf.columnFamily, tableIdBytes);

                        cfsToRemove.add(indexCf.columnFamily);
                    }
                }
            }

            db.write(DFLT_WRITE_OPTS, writeBatch);

            if (!cfsToRemove.isEmpty()) {
                scheduleIndexCfsDestroyIfNeeded(cfsToRemove);
            }
        } catch (RocksDBException e) {
            throw new StorageException("Failed to destroy table data. [tableId={}]", e, targetTableId);
        }
    }

    private static byte[] metaPrefix(byte[] metaPrefix, byte[] tableIdBytes) {
        return ByteBuffer.allocate(metaPrefix.length + tableIdBytes.length)
                .order(KEY_BYTE_ORDER)
                .put(metaPrefix)
                .put(tableIdBytes)
                .array();
    }

    private ColumnFamily createSortedIndexCf(byte[] cfName) {
        ColumnFamilyOptions cfOptions = sortedIndexCfOptions(cfName);
        this.resources.add(0, cfOptions); // Added to the first position of the resources.
        ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName, cfOptions);

        ColumnFamily columnFamily;
        try {
            columnFamily = ColumnFamily.withPrivateOptions(db, cfDescriptor);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to create new RocksDB column family: " + toStringName(cfName), e);
        }

        flusher.addColumnFamily(columnFamily.handle());

        return columnFamily;
    }

    private void destroyColumnFamily(ColumnFamily columnFamily) {
        flusher.removeColumnFamily(columnFamily.handle());

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
