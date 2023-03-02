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

package org.apache.ignite.internal.storage.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.GC_QUEUE_CF_NAME;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.HASH_INDEX_CF_NAME;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.META_CF_NAME;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.PARTITION_CF_NAME;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexCfName;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexId;
import static org.apache.ignite.internal.storage.util.StorageUtils.createMissingMvPartitionErrorMessage;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.ColumnFamilyType;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbBinaryTupleComparator;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbHashIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbSortedIndexStorage;
import org.apache.ignite.internal.storage.util.MvPartitionStorages;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Table storage implementation based on {@link RocksDB} instance.
 */
public class RocksDbTableStorage implements MvTableStorage {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RocksDbTableStorage.class);

    /** RocksDB storage engine instance. */
    private final RocksDbStorageEngine engine;

    /** Path for the directory that stores table data. */
    private final Path tablePath;

    /** Table configuration. */
    private final TableConfiguration tableCfg;

    /** Indexes configuration. */
    private final TablesConfiguration tablesCfg;

    /** Data region for the table. */
    private final RocksDbDataRegion dataRegion;

    /** RocksDB flusher instance. */
    private volatile RocksDbFlusher flusher;

    /** Rocks DB instance. */
    private volatile RocksDB db;

    /** Write options for write operations. */
    private final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

    /** Meta information. */
    private volatile RocksDbMetaStorage meta;

    /** Column Family handle for partition data. */
    private volatile ColumnFamily partitionCf;

    /** Column Family handle for GC queue. */
    private volatile ColumnFamily gcQueueCf;

    /** Column Family handle for Hash Index data. */
    private volatile ColumnFamily hashIndexCf;

    /** Partition storages. */
    private volatile MvPartitionStorages<RocksDbMvPartitionStorage> mvPartitionStorages;

    /** Hash Index storages by Index IDs. */
    private final ConcurrentMap<UUID, HashIndex> hashIndices = new ConcurrentHashMap<>();

    /** Sorted Index storages by Index IDs. */
    private final ConcurrentMap<UUID, SortedIndex> sortedIndices = new ConcurrentHashMap<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param engine RocksDB storage engine instance.
     * @param tablePath Path for the directory that stores table data.
     * @param tableCfg Table configuration.
     * @param dataRegion Data region for the table.
     */
    RocksDbTableStorage(
            RocksDbStorageEngine engine,
            Path tablePath,
            RocksDbDataRegion dataRegion,
            TableConfiguration tableCfg,
            TablesConfiguration tablesCfg
    ) {
        this.engine = engine;
        this.tablePath = tablePath;
        this.tableCfg = tableCfg;
        this.dataRegion = dataRegion;
        this.tablesCfg = tablesCfg;
    }

    /**
     * Returns a storage engine instance.
     */
    public RocksDbStorageEngine engine() {
        return engine;
    }

    /**
     * Returns a {@link RocksDB} instance.
     */
    public RocksDB db() {
        return db;
    }

    /**
     * Returns a column family handle for partitions column family.
     */
    public ColumnFamilyHandle partitionCfHandle() {
        return partitionCf.handle();
    }

    /**
     * Returns a column family handle for meta column family.
     */
    public ColumnFamilyHandle metaCfHandle() {
        return meta.columnFamily().handle();
    }

    /**
     * Returns a column family handle for GC queue.
     */
    public ColumnFamilyHandle gcQueueHandle() {
        return gcQueueCf.handle();
    }

    @Override
    public TableConfiguration configuration() {
        return tableCfg;
    }

    @Override
    public TablesConfiguration tablesConfiguration() {
        return tablesCfg;
    }

    @Override
    public void start() throws StorageException {
        inBusyLock(busyLock, () -> {
            flusher = new RocksDbFlusher(
                    busyLock,
                    engine.scheduledPool(),
                    engine().threadPool(),
                    engine.configuration().flushDelayMillis()::value,
                    this::refreshPersistedIndexes
            );

            try {
                Files.createDirectories(tablePath);
            } catch (IOException e) {
                throw new StorageException("Failed to create a directory for the table storage", e);
            }

            List<ColumnFamilyDescriptor> cfDescriptors = getExistingCfDescriptors();

            List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());

            DBOptions dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    // Atomic flush must be enabled to guarantee consistency between different column families when WAL is disabled.
                    .setAtomicFlush(true)
                    .setListeners(List.of(flusher.listener()))
                    .setWriteBufferManager(dataRegion.writeBufferManager());

            try {
                db = RocksDB.open(dbOptions, tablePath.toAbsolutePath().toString(), cfDescriptors, cfHandles);

                // read all existing Column Families from the db and parse them according to type: meta, partition data or index.
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
                            UUID indexId = sortedIndexId(cf.name());

                            var indexDescriptor = new SortedIndexDescriptor(indexId, tablesCfg.value());

                            sortedIndices.put(indexId, new SortedIndex(cf, indexDescriptor));

                            break;

                        default:
                            throw new StorageException("Unidentified column family [name=" + cf.name() + ", table="
                                    + getTableName() + ']');
                    }
                }

                assert meta != null;
                assert partitionCf != null;
                assert hashIndexCf != null;

                flusher.init(db, cfHandles);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to initialize RocksDB instance", e);
            }

            MvPartitionStorages<RocksDbMvPartitionStorage> mvPartitionStorages = new MvPartitionStorages<>(tableCfg.value());

            for (int partitionId : meta.getPartitionIds()) {
                // There is no need to wait for futures, since there will be no parallel operations yet.
                mvPartitionStorages.create(partitionId, partId -> new RocksDbMvPartitionStorage(this, partitionId));
            }

            this.mvPartitionStorages = mvPartitionStorages;
        });
    }

    /**
     * Returns a future to wait next flush operation from the current point in time. Uses {@link RocksDB#getLatestSequenceNumber()} to
     * achieve this.
     *
     * @param schedule {@code true} if {@link RocksDB#flush(FlushOptions)} should be explicitly triggerred in the near future.
     */
    public CompletableFuture<Void> awaitFlush(boolean schedule) {
        return inBusyLock(busyLock, () -> flusher.awaitFlush(schedule));
    }

    private void refreshPersistedIndexes() {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            TableView tableCfgView = configuration().value();

            for (int partitionId = 0; partitionId < tableCfgView.partitions(); partitionId++) {
                RocksDbMvPartitionStorage partition = mvPartitionStorages.get(partitionId);

                if (partition != null) {
                    try {
                        partition.refreshPersistedIndex();
                    } catch (StorageException storageException) {
                        LOG.error(
                                "Filed to refresh persisted applied index value for table {} partition {}",
                                storageException,
                                configuration().name().value(),
                                partitionId
                        );
                    }
                }
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void stop() throws StorageException {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        List<AutoCloseable> resources = new ArrayList<>();

        resources.add(flusher::stop);

        resources.add(meta.columnFamily().handle());
        resources.add(partitionCf.handle());
        resources.add(gcQueueCf.handle());
        resources.add(hashIndexCf.handle());
        resources.addAll(
                sortedIndices.values().stream()
                        .map(index -> (AutoCloseable) index::close)
                        .collect(toList())
        );

        resources.add(db);

        resources.add(writeOptions);

        try {
            mvPartitionStorages
                    .getAllForCloseOrDestroy()
                    .get(10, TimeUnit.SECONDS)
                    .forEach(mvPartitionStorage -> resources.add(mvPartitionStorage::close));

            for (HashIndex index : hashIndices.values()) {
                resources.add(index::close);
            }

            for (SortedIndex index : sortedIndices.values()) {
                resources.add(index::close);
            }

            Collections.reverse(resources);

            IgniteUtils.closeAll(resources);
        } catch (Exception e) {
            throw new StorageException("Failed to stop RocksDB table storage: " + getTableName(), e);
        }
    }

    @Override
    public void close() throws StorageException {
        stop();
    }

    @Override
    public CompletableFuture<Void> destroy() {
        try {
            stop();

            IgniteUtils.deleteIfExists(tablePath);

            return completedFuture(null);
        } catch (Throwable t) {
            return failedFuture(new StorageException("Failed to destroy RocksDB table storage: " + getTableName(), t));
        }
    }

    @Override
    public CompletableFuture<MvPartitionStorage> createMvPartition(int partitionId) throws StorageException {
        return inBusyLock(busyLock, () -> mvPartitionStorages.create(partitionId, partId -> {
            RocksDbMvPartitionStorage partition = new RocksDbMvPartitionStorage(this, partitionId);

            meta.putPartitionId(partitionId);

            return partition;
        }));
    }

    @Override
    public @Nullable RocksDbMvPartitionStorage getMvPartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.get(partitionId));
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.destroy(partitionId, mvPartitionStorage -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.close();

                // Operation to delete partition data should be fast, since we will write only the range of keys for deletion, and the
                // RocksDB itself will then destroy the data on flash.
                mvPartitionStorage.destroyData(writeBatch);

                for (HashIndex hashIndex : hashIndices.values()) {
                    hashIndex.destroy(partitionId, writeBatch);
                }

                for (SortedIndex sortedIndex : sortedIndices.values()) {
                    sortedIndex.destroy(partitionId, writeBatch);
                }

                db.write(writeOptions, writeBatch);

                return awaitFlush(true);
            } catch (RocksDBException e) {
                throw new StorageException("Error when destroying storage: [{}]", e, mvPartitionStorages.createStorageInfo(partitionId));
            }
        }));
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) {
        return inBusyLock(busyLock, () -> {
            SortedIndex storages = sortedIndices.computeIfAbsent(indexId, this::createSortedIndex);

            RocksDbMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

            if (partitionStorage == null) {
                throw new StorageException(createMissingMvPartitionErrorMessage(partitionId));
            }

            return storages.getOrCreateStorage(partitionStorage);
        });
    }

    private SortedIndex createSortedIndex(UUID indexId) {
        var indexDescriptor = new SortedIndexDescriptor(indexId, tablesCfg.value());

        ColumnFamilyDescriptor cfDescriptor = sortedIndexCfDescriptor(sortedIndexCfName(indexId), indexDescriptor);

        ColumnFamily columnFamily;
        try {
            columnFamily = ColumnFamily.create(db, cfDescriptor);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to create new RocksDB column family: " + new String(cfDescriptor.getName(), UTF_8), e);
        }

        flusher.addColumnFamily(columnFamily.handle());

        return new SortedIndex(columnFamily, indexDescriptor);
    }

    @Override
    public HashIndexStorage getOrCreateHashIndex(int partitionId, UUID indexId) {
        return inBusyLock(busyLock, () -> {
            HashIndex storages = hashIndices.computeIfAbsent(indexId, id -> {
                var indexDescriptor = new HashIndexDescriptor(indexId, tablesCfg.value());

                return new HashIndex(hashIndexCf, indexDescriptor);
            });

            RocksDbMvPartitionStorage partitionStorage = mvPartitionStorages.get(partitionId);

            if (partitionStorage == null) {
                throw new StorageException(String.format("Partition ID %d does not exist", partitionId));
            }

            return storages.getOrCreateStorage(partitionStorage);
        });
    }

    @Override
    public CompletableFuture<Void> destroyIndex(UUID indexId) {
        return inBusyLock(busyLock, () -> {
            HashIndex hashIdx = hashIndices.remove(indexId);

            if (hashIdx != null) {
                hashIdx.destroy();
            }

            // Sorted Indexes have a separate Column Family per index, so we simply destroy it immediately after a flush completes
            // in order to avoid concurrent access to the CF.
            SortedIndex sortedIdx = sortedIndices.remove(indexId);

            if (sortedIdx != null) {
                // Remove the to-be destroyed CF from the flusher
                flusher.removeColumnFamily(sortedIdx.indexCf().handle());

                sortedIdx.destroy();
            }

            if (hashIdx == null) {
                return completedFuture(null);
            } else {
                return awaitFlush(false);
            }
        });
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    /**
     * Returns a list of Column Families' names that belong to a RocksDB instance in the given path.
     *
     * @return List with column families names.
     * @throws StorageException If something went wrong.
     */
    private List<String> getExistingCfNames() {
        String absolutePathStr = tablePath.toAbsolutePath().toString();

        try (Options opts = new Options()) {
            List<String> existingNames = RocksDB.listColumnFamilies(opts, absolutePathStr)
                    .stream()
                    .map(cfNameBytes -> new String(cfNameBytes, UTF_8))
                    .collect(toList());

            // even if the database is new (no existing Column Families), we return the names of mandatory column families, that
            // will be created automatically.
            return existingNames.isEmpty() ? List.of(META_CF_NAME, PARTITION_CF_NAME, GC_QUEUE_CF_NAME, HASH_INDEX_CF_NAME) : existingNames;
        } catch (RocksDBException e) {
            throw new StorageException(
                    "Failed to read list of column families names for the RocksDB instance located at path " + absolutePathStr, e
            );
        }
    }

    /**
     * Returns a list of CF descriptors present in the RocksDB instance.
     *
     * @return List of CF descriptors.
     */
    private List<ColumnFamilyDescriptor> getExistingCfDescriptors() {
        return getExistingCfNames().stream()
                .map(this::cfDescriptorFromName)
                .collect(toList());
    }

    /**
     * Creates a Column Family descriptor for the given Family type (encoded in its name).
     */
    private ColumnFamilyDescriptor cfDescriptorFromName(String cfName) {
        switch (ColumnFamilyType.fromCfName(cfName)) {
            case META:
            case GC_QUEUE:
                return new ColumnFamilyDescriptor(
                        cfName.getBytes(UTF_8),
                        new ColumnFamilyOptions()
                );

            case PARTITION:
                return new ColumnFamilyDescriptor(
                        cfName.getBytes(UTF_8),
                        new ColumnFamilyOptions().useFixedLengthPrefixExtractor(PartitionDataHelper.ROW_PREFIX_SIZE)
                );

            case HASH_INDEX:
                return new ColumnFamilyDescriptor(
                        cfName.getBytes(UTF_8),
                        new ColumnFamilyOptions().useFixedLengthPrefixExtractor(RocksDbHashIndexStorage.FIXED_PREFIX_LENGTH)
                );

            case SORTED_INDEX:
                var indexDescriptor = new SortedIndexDescriptor(sortedIndexId(cfName), tablesCfg.value());

                return sortedIndexCfDescriptor(cfName, indexDescriptor);

            default:
                throw new StorageException("Unidentified column family [name=" + cfName + ", table=" + getTableName() + ']');
        }
    }

    /**
     * Creates a Column Family descriptor for a Sorted Index.
     */
    private static ColumnFamilyDescriptor sortedIndexCfDescriptor(String cfName, SortedIndexDescriptor descriptor) {
        var comparator = new RocksDbBinaryTupleComparator(descriptor);

        ColumnFamilyOptions options = new ColumnFamilyOptions().setComparator(comparator);

        return new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), options);
    }

    @Override
    public CompletableFuture<Void> startRebalancePartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.startRebalace(partitionId, mvPartitionStorage -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.startRebalance(writeBatch);

                getHashIndexStorages(partitionId).forEach(index -> index.startRebalance(writeBatch));
                getSortedIndexStorages(partitionId).forEach(index -> index.startRebalance(writeBatch));

                db.write(writeOptions, writeBatch);

                return completedFuture(null);
            } catch (RocksDBException e) {
                throw new StorageRebalanceException(
                        "Error when trying to start rebalancing storage: [{}]",
                        e,
                        mvPartitionStorage.createStorageInfo()
                );
            }
        }));
    }

    @Override
    public CompletableFuture<Void> abortRebalancePartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.abortRebalance(partitionId, mvPartitionStorage -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.abortRebalance(writeBatch);

                getHashIndexStorages(partitionId).forEach(index -> index.abortReblance(writeBatch));
                getSortedIndexStorages(partitionId).forEach(index -> index.abortReblance(writeBatch));

                db.write(writeOptions, writeBatch);

                return completedFuture(null);
            } catch (RocksDBException e) {
                throw new StorageRebalanceException("Error when trying to abort rebalancing storage: [{}]",
                        e,
                        mvPartitionStorage.createStorageInfo()
                );
            }
        }));
    }

    @Override
    public CompletableFuture<Void> finishRebalancePartition(
            int partitionId,
            long lastAppliedIndex,
            long lastAppliedTerm,
            byte[] groupConfig
    ) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.finishRebalance(partitionId, mvPartitionStorage -> {
            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.finishRebalance(writeBatch, lastAppliedIndex, lastAppliedTerm, groupConfig);

                getHashIndexStorages(partitionId).forEach(RocksDbHashIndexStorage::finishRebalance);
                getSortedIndexStorages(partitionId).forEach(RocksDbSortedIndexStorage::finishRebalance);

                db.write(writeOptions, writeBatch);

                return completedFuture(null);
            } catch (RocksDBException e) {
                throw new StorageRebalanceException("Error when trying to finish rebalancing storage: [{}]",
                        e,
                        mvPartitionStorage.createStorageInfo()
                );
            }
        }));
    }

    @Override
    public CompletableFuture<Void> clearPartition(int partitionId) {
        return inBusyLock(busyLock, () -> mvPartitionStorages.clear(partitionId, mvPartitionStorage -> {
            List<RocksDbHashIndexStorage> hashIndexStorages = getHashIndexStorages(partitionId);
            List<RocksDbSortedIndexStorage> sortedIndexStorages = getSortedIndexStorages(partitionId);

            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartitionStorage.startCleanup(writeBatch);

                for (RocksDbHashIndexStorage hashIndexStorage : hashIndexStorages) {
                    hashIndexStorage.startCleanup(writeBatch);
                }

                for (RocksDbSortedIndexStorage sortedIndexStorage : sortedIndexStorages) {
                    sortedIndexStorage.startCleanup(writeBatch);
                }

                db.write(writeOptions, writeBatch);

                return completedFuture(null);
            } catch (RocksDBException e) {
                throw new StorageException("Error when trying to cleanup storage: [{}]", e, mvPartitionStorage.createStorageInfo());
            } finally {
                mvPartitionStorage.finishCleanup();

                hashIndexStorages.forEach(RocksDbHashIndexStorage::finishCleanup);
                sortedIndexStorages.forEach(RocksDbSortedIndexStorage::finishCleanup);
            }
        }));
    }

    /**
     * Returns table name.
     */
    String getTableName() {
        return tableCfg.name().value();
    }

    private List<RocksDbHashIndexStorage> getHashIndexStorages(int partitionId) {
        return hashIndices.values().stream().map(indexes -> indexes.get(partitionId)).filter(Objects::nonNull).collect(toList());
    }

    private List<RocksDbSortedIndexStorage> getSortedIndexStorages(int partitionId) {
        return sortedIndices.values().stream().map(indexes -> indexes.get(partitionId)).filter(Objects::nonNull).collect(toList());
    }
}
