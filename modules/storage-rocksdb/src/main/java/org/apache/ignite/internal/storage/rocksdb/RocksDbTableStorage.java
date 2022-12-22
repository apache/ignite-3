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
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.HASH_INDEX_CF_NAME;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.META_CF_NAME;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.PARTITION_CF_NAME;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexCfName;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.flush.RocksDbFlusher;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.ColumnFamilyType;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbBinaryTupleComparator;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbHashIndexStorage;
import org.apache.ignite.internal.tostring.S;
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

    /** Column Family handle for Hash Index data. */
    private volatile ColumnFamily hashIndexCf;

    /** Partition storages. */
    private volatile AtomicReferenceArray<RocksDbMvPartitionStorage> partitions;

    /** Hash Index storages by Index IDs. */
    private final ConcurrentMap<UUID, HashIndex> hashIndices = new ConcurrentHashMap<>();

    /** Sorted Index storages by Index IDs. */
    private final ConcurrentMap<UUID, SortedIndex> sortedIndices = new ConcurrentHashMap<>();

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final ConcurrentMap<Integer, CompletableFuture<Void>> partitionIdDestroyFutureMap = new ConcurrentHashMap<>();

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
                                + tableCfg.value().name() + ']');
                }
            }

            assert meta != null;
            assert partitionCf != null;
            assert hashIndexCf != null;

            flusher.init(db, cfHandles);
        } catch (RocksDBException e) {
            throw new StorageException("Failed to initialize RocksDB instance", e);
        }

        partitions = new AtomicReferenceArray<>(tableCfg.value().partitions());

        for (int partId : meta.getPartitionIds()) {
            partitions.set(partId, new RocksDbMvPartitionStorage(this, partId));
        }
    }

    /**
     * Returns a future to wait next flush operation from the current point in time. Uses {@link RocksDB#getLatestSequenceNumber()} to
     * achieve this.
     *
     * @param schedule {@code true} if {@link RocksDB#flush(FlushOptions)} should be explicitly triggerred in the near future.
     */
    public CompletableFuture<Void> awaitFlush(boolean schedule) {
        return flusher.awaitFlush(schedule);
    }

    private void refreshPersistedIndexes() {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            TableView tableCfgView = configuration().value();

            for (int partitionId = 0; partitionId < tableCfgView.partitions(); partitionId++) {
                RocksDbMvPartitionStorage partition = getMvPartition(partitionId);

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
        resources.add(hashIndexCf.handle());
        resources.addAll(
                sortedIndices.values().stream()
                        .map(index -> (AutoCloseable) index::close)
                        .collect(toList())
        );

        resources.add(db);

        resources.add(writeOptions);

        for (int i = 0; i < partitions.length(); i++) {
            MvPartitionStorage partition = partitions.get(i);

            if (partition != null) {
                resources.add(partition::close);
            }
        }

        Collections.reverse(resources);

        try {
            IgniteUtils.closeAll(resources);
        } catch (Exception e) {
            throw new StorageException("Failed to stop RocksDB table storage.", e);
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
        } catch (Throwable throwable) {
            return failedFuture(throwable);
        }
    }

    @Override
    public RocksDbMvPartitionStorage getOrCreateMvPartition(int partitionId) throws StorageException {
        RocksDbMvPartitionStorage partition = getMvPartition(partitionId);

        if (partition != null) {
            return partition;
        }

        partition = new RocksDbMvPartitionStorage(this, partitionId);

        partitions.set(partitionId, partition);

        meta.putPartitionId(partitionId);

        return partition;
    }

    @Override
    public @Nullable RocksDbMvPartitionStorage getMvPartition(int partitionId) {
        checkPartitionId(partitionId);

        return partitions.get(partitionId);
    }

    @Override
    public CompletableFuture<Void> destroyPartition(int partitionId) {
        checkPartitionId(partitionId);

        CompletableFuture<Void> destroyPartitionFuture = new CompletableFuture<>();

        CompletableFuture<Void> previousDestroyPartitionFuture = partitionIdDestroyFutureMap.putIfAbsent(
                partitionId,
                destroyPartitionFuture
        );

        if (previousDestroyPartitionFuture != null) {
            return previousDestroyPartitionFuture;
        }

        RocksDbMvPartitionStorage mvPartition = partitions.getAndSet(partitionId, null);

        if (mvPartition != null) {
            try {
                //TODO IGNITE-17626 Destroy indexes as well...

                // Operation to delete partition data should be fast, since we will write only the range of keys for deletion, and the
                // RocksDB itself will then destroy the data on flash.
                mvPartition.destroy();

                mvPartition.close();

                partitionIdDestroyFutureMap.remove(partitionId).complete(null);
            } catch (Throwable throwable) {
                partitionIdDestroyFutureMap.remove(partitionId).completeExceptionally(throwable);
            }
        } else {
            partitionIdDestroyFutureMap.remove(partitionId).complete(null);
        }

        return destroyPartitionFuture;
    }

    @Override
    public SortedIndexStorage getOrCreateSortedIndex(int partitionId, UUID indexId) {
        SortedIndex storages = sortedIndices.computeIfAbsent(indexId, this::createSortedIndex);

        RocksDbMvPartitionStorage partitionStorage = getMvPartition(partitionId);

        if (partitionStorage == null) {
            throw new StorageException(String.format("Partition ID %d does not exist", partitionId));
        }

        return storages.getOrCreateStorage(partitionStorage);
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
        HashIndex storages = hashIndices.computeIfAbsent(indexId, id -> {
            var indexDescriptor = new HashIndexDescriptor(indexId, tablesCfg.value());

            return new HashIndex(hashIndexCf, indexDescriptor);
        });

        RocksDbMvPartitionStorage partitionStorage = getMvPartition(partitionId);

        if (partitionStorage == null) {
            throw new StorageException(String.format("Partition ID %d does not exist", partitionId));
        }

        return storages.getOrCreateStorage(partitionStorage);
    }

    @Override
    public CompletableFuture<Void> destroyIndex(UUID indexId) {
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
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    /**
     * Checks that a passed partition id is within the proper bounds.
     *
     * @param partId Partition id.
     */
    private void checkPartitionId(int partId) {
        if (partId < 0 || partId >= partitions.length()) {
            throw new IllegalArgumentException(S.toString(
                    "Unable to access partition with id outside of configured range",
                    "table", tableCfg.value().name(), false,
                    "partitionId", partId, false,
                    "partitions", partitions.length(), false
            ));
        }
    }

    /**
     * Returns a list of Column Families' names that belong to a RocksDB instance in the given path.
     *
     * @return Map with column families names.
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
            return existingNames.isEmpty() ? List.of(META_CF_NAME, PARTITION_CF_NAME, HASH_INDEX_CF_NAME) : existingNames;
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
            case PARTITION:
                return new ColumnFamilyDescriptor(
                        cfName.getBytes(UTF_8),
                        new ColumnFamilyOptions()
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
                throw new StorageException("Unidentified column family [name=" + cfName + ", table=" + tableCfg.value().name() + ']');
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
    public CompletableFuture<Void> startFullRebalancePartition(int partitionId) {
        // TODO: IGNITE-18027 Implement
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> abortFullRebalancePartition(int partitionId) {
        // TODO: IGNITE-18027 Implement
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> finishFullRebalancePartition(int partitionId) {
        // TODO: IGNITE-18027 Implement
        throw new UnsupportedOperationException();
    }
}
