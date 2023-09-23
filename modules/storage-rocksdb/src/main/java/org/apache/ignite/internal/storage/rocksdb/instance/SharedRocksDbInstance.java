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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
    public final ColumnFamily hashIndexCf;

    /** Column Family instances for different types of sorted indexes, identified by the column family name. */
    private final ConcurrentMap<ByteArray, ColumnFamily> sortedIndexCfs;

    /** Column family names mapped to sets of index IDs, that use that CF. */
    private final ConcurrentMap<ByteArray, Set<Integer>> sortedIndexIdsByCfName = new ConcurrentHashMap<>();

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
            ConcurrentMap<ByteArray, ColumnFamily> sortedIndexCfs
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
        this.sortedIndexCfs = sortedIndexCfs;
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
        resources.addAll(sortedIndexCfs.values().stream()
                .map(ColumnFamily::handle)
                .collect(toList())
        );

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
     * Returns Column Family instance with the desired name. Creates it it it doesn't exist.
     * Tracks every created index by its {@code indexId}.
     */
    public ColumnFamily getSortedIndexCfOnIndexCreate(byte[] cfName, int indexId) {
        if (!busyLock.enterBusy()) {
            throw new StorageClosedException();
        }

        try {
            ColumnFamily[] result = {null};

            sortedIndexIdsByCfName.compute(new ByteArray(cfName), (name, indexIds) -> {
                ColumnFamily columnFamily = getOrCreateColumnFamily(cfName, name);

                result[0] = columnFamily;

                if (indexIds == null) {
                    indexIds = new HashSet<>();
                }

                indexIds.add(indexId);

                return indexIds;
            });

            return result[0];
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Possibly drops the column family after destroying the index.
     */
    public void dropCfOnIndexDestroy(byte[] cfName, int indexId) {
        if (!busyLock.enterBusy()) {
            throw new StorageClosedException();
        }

        try {
            sortedIndexIdsByCfName.compute(new ByteArray(cfName), (name, indexIds) -> {
                if (indexIds == null) {
                    return null;
                }

                indexIds.remove(indexId);

                if (indexIds.isEmpty()) {
                    indexIds = null;

                    destroyColumnFamily(name);
                }

                return indexIds;
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private ColumnFamily getOrCreateColumnFamily(byte[] cfName, ByteArray name) {
        return sortedIndexCfs.computeIfAbsent(name, unused -> {
            ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName, sortedIndexCfOptions(cfName));

            ColumnFamily columnFamily;
            try {
                columnFamily = ColumnFamily.create(db, cfDescriptor);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to create new RocksDB column family: " + toStringName(cfDescriptor.getName()), e);
            }

            flusher.addColumnFamily(columnFamily.handle());

            return columnFamily;
        });
    }

    private void destroyColumnFamily(ByteArray cfName) {
        sortedIndexCfs.computeIfPresent(cfName, (unused, columnFamily) -> {
            ColumnFamilyHandle columnFamilyHandle = columnFamily.handle();

            flusher.removeColumnFamily(columnFamilyHandle);

            try {
                db.dropColumnFamily(columnFamilyHandle);

                db.destroyColumnFamilyHandle(columnFamilyHandle);
            } catch (RocksDBException e) {
                throw new StorageException(
                        "Failed to destroy RocksDB Column Family. [cfName={}, path={}]",
                        e, toStringName(cfName.bytes()), path
                );
            }

            return null;
        });
    }
}
