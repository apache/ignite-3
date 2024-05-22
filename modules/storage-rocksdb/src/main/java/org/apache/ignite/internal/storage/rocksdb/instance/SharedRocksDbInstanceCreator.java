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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.toStringName;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils;
import org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.ColumnFamilyType;
import org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper;
import org.apache.ignite.internal.storage.rocksdb.RocksDbMetaStorage;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageProfile;
import org.apache.ignite.internal.storage.rocksdb.index.AbstractRocksDbIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbHashIndexStorage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Single-use class to create {@link SharedRocksDbInstance} fully initialized instances.
 * Contains a boilerplate code for reading/creating the DB.
 */
public class SharedRocksDbInstanceCreator {
    /** List of resources that must be closed if DB creation failed in the process. */
    private final List<AutoCloseable> resources = new ArrayList<>();

    /**
     * Creates an instance of {@link SharedRocksDbInstance}.
     */
    public SharedRocksDbInstance create(
            RocksDbStorageEngine engine,
            RocksDbStorageProfile profile,
            Path path
    ) throws RocksDBException, IOException {
        var busyLock = new IgniteSpinBusyLock();

        try {
            Files.createDirectories(path);

            var flusher = new RocksDbFlusher(
                    busyLock,
                    engine.scheduledPool(),
                    engine.threadPool(),
                    engine.configuration().flushDelayMillis()::value,
                    engine.logSyncer(),
                    () -> {} // No-op.
            );

            List<ColumnFamilyDescriptor> cfDescriptors = getExistingCfDescriptors(path);

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());

            DBOptions dbOptions = add(new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    // Atomic flush must be enabled to guarantee consistency between different column families when WAL is disabled.
                    .setAtomicFlush(true)
                    .setListeners(List.of(flusher.listener()))
                    .setWriteBufferManager(profile.writeBufferManager())
            );

            RocksDB db = add(RocksDB.open(dbOptions, path.toAbsolutePath().toString(), cfDescriptors, cfHandles));

            RocksDbMetaStorage meta = null;
            ColumnFamily partitionCf = null;
            ColumnFamily gcQueueCf = null;
            ColumnFamily hashIndexCf = null;
            var sortedIndexCfs = new ArrayList<ColumnFamily>();

            // Read all existing Column Families from the db and parse them according to type: meta, partition data or index.
            for (ColumnFamilyHandle cfHandle : cfHandles) {
                ColumnFamily cf = ColumnFamily.wrap(db, cfHandle);

                switch (ColumnFamilyType.fromCfName(cf.name())) {
                    case META:
                        meta = new RocksDbMetaStorage(cf);

                        break;

                    case PARTITION:
                        partitionCf = cf;

                        break;

                    case GC_QUEUE:
                        gcQueueCf = cf;

                        break;

                    case HASH_INDEX:
                        hashIndexCf = cf;

                        break;

                    case SORTED_INDEX:
                        sortedIndexCfs.add(cf);

                        break;

                    default:
                        throw new StorageException("Unidentified column family: [name={}, path={}]", cf.name(), path);
                }
            }

            flusher.init(db, cfHandles);

            return new SharedRocksDbInstance(
                    engine,
                    path,
                    busyLock,
                    flusher,
                    dbOptions,
                    db,
                    requireNonNull(meta, "meta"),
                    requireNonNull(partitionCf, "partitionCf"),
                    requireNonNull(gcQueueCf, "gcQueueCf"),
                    requireNonNull(hashIndexCf, "hashIndexCf"),
                    sortedIndexCfs
            );
        } catch (Throwable t) {
            Collections.reverse(resources);

            try {
                closeAll(resources);
            } catch (Exception e) {
                t.addSuppressed(e);
            }

            throw t;
        } finally {
            resources.clear();
        }
    }

    /**
     * Returns a list of CF descriptors present in the RocksDB instance.
     */
    private List<ColumnFamilyDescriptor> getExistingCfDescriptors(Path path) throws RocksDBException {
        String absolutePathStr = path.toAbsolutePath().toString();

        List<byte[]> existingNames;

        try (Options opts = new Options()) {
            existingNames = RocksDB.listColumnFamilies(opts, absolutePathStr);

            // Even if the database is new (no existing Column Families), we return the names of mandatory column families, that
            // will be created automatically.
            if (existingNames.isEmpty()) {
                existingNames = ColumnFamilyUtils.DEFAULT_CF_NAMES;
            }
        }

        return existingNames.stream()
                .map(cfName -> new ColumnFamilyDescriptor(cfName, createCfOptions(cfName, path)))
                .collect(toList());
    }

    @SuppressWarnings("resource")
    private ColumnFamilyOptions createCfOptions(byte[] cfName, Path path) {
        String utf8cfName = toStringName(cfName);

        switch (ColumnFamilyType.fromCfName(utf8cfName)) {
            case META:
            case GC_QUEUE:
                return add(new ColumnFamilyOptions());

            case PARTITION:
                return add(defaultCfOptions().useCappedPrefixExtractor(PartitionDataHelper.ROW_PREFIX_SIZE));

            case HASH_INDEX:
                return add(defaultCfOptions().useCappedPrefixExtractor(RocksDbHashIndexStorage.FIXED_PREFIX_LENGTH));

            case SORTED_INDEX:
                return add(sortedIndexCfOptions(cfName));
            default:
                throw new StorageException("Unidentified column family: [name={}, path={}]", cfName, path);
        }
    }

    @SuppressWarnings("resource")
    private static ColumnFamilyOptions defaultCfOptions() {
        return new ColumnFamilyOptions()
                .setMemtablePrefixBloomSizeRatio(0.125)
                .setTableFormatConfig(new BlockBasedTableConfig().setFilterPolicy(new BloomFilter()));
    }

    @SuppressWarnings("resource")
    static ColumnFamilyOptions sortedIndexCfOptions(byte[] cfName) {
        return new ColumnFamilyOptions()
                .setComparator(ColumnFamilyUtils.comparatorFromCfName(cfName))
                .useCappedPrefixExtractor(AbstractRocksDbIndexStorage.PREFIX_WITH_IDS_LENGTH);
    }

    private <T extends AutoCloseable> T add(T value) {
        resources.add(value);

        return value;
    }
}
