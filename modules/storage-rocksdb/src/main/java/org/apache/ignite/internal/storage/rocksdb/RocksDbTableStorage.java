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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.META_CF_NAME;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.PARTITION_CF_NAME;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.columnFamilyType;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexCfName;
import static org.apache.ignite.internal.storage.rocksdb.ColumnFamilyUtils.sortedIndexName;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_FLUSH_BEGIN;
import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_FLUSH_COMPLETED;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.rocksdb.index.BinaryRowComparator;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbSortedIndexStorage;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Table storage implementation based on {@link RocksDB} instance.
 */
class RocksDbTableStorage implements TableStorage, MvTableStorage {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RocksDbTableStorage.class);

    /** RocksDB storage engine instance. */
    private final RocksDbStorageEngine engine;

    /** Path for the directory that stores table data. */
    private final Path tablePath;

    /** Table configuration. */
    private final TableConfiguration tableCfg;

    /** Data region for the table. */
    private final RocksDbDataRegion dataRegion;

    /** Rocks DB instance. */
    private volatile RocksDB db;

    /** Write options for write operations. */
    private final WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);

    /** Meta information. */
    private volatile RocksDbMetaStorage meta;

    /** Column Family handle for partition data. */
    private volatile ColumnFamily partitionCf;

    /** Partition storages. */
    private volatile AtomicReferenceArray<RocksDbMvPartitionStorage> partitions;

    /** Column families for indexes by their names. */
    private final Map<String, RocksDbSortedIndexStorage> sortedIndices = new ConcurrentHashMap<>();

    /**
     * Instance of the latest scheduled flush closure.
     *
     * @see #scheduleFlush()
     */
    private volatile Runnable latestFlushClosure;

    /** Flag indicating if the storage has been stopped. */
    @Deprecated
    private volatile boolean stopped = false;

    //TODO Use it instead of the "stopped" flag.
    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
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
            TableConfiguration tableCfg,
            RocksDbDataRegion dataRegion
    ) {
        this.engine = engine;
        this.tablePath = tablePath;
        this.tableCfg = tableCfg;
        this.dataRegion = dataRegion;
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

    /** {@inheritDoc} */
    @Override
    public TableConfiguration configuration() {
        return tableCfg;
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
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
                .setListeners(List.of(flushListener()))
                .setWriteBufferManager(dataRegion.writeBufferManager());

        try {
            db = RocksDB.open(dbOptions, tablePath.toAbsolutePath().toString(), cfDescriptors, cfHandles);

            // read all existing Column Families from the db and parse them according to type: meta, partition data or index.
            for (ColumnFamilyHandle cfHandle : cfHandles) {
                ColumnFamily cf = ColumnFamily.wrap(db, cfHandle);

                switch (columnFamilyType(cf.name())) {
                    case META:
                        meta = new RocksDbMetaStorage(cf);

                        break;

                    case PARTITION:
                        partitionCf = cf;

                        break;

                    case SORTED_INDEX:
                        String indexName = sortedIndexName(cf.name());

                        var indexDescriptor = new SortedIndexDescriptor(indexName, tableCfg.value());

                        sortedIndices.put(indexName, new RocksDbSortedIndexStorage(cf, indexDescriptor));

                        break;

                    default:
                        throw new StorageException("Unidentified column family [name=" + cf.name() + ", table=" + tableCfg.name() + ']');
                }
            }
        } catch (RocksDBException e) {
            throw new StorageException("Failed to initialize RocksDB instance", e);
        }

        partitions = new AtomicReferenceArray<>(tableCfg.value().partitions());

        for (int partId : meta.getPartitionIds()) {
            partitions.set(partId, new RocksDbMvPartitionStorage(this, partId));
        }
    }

    /**
     * Schedules a flush of the table. If run several times within a small amount of time, only the last scheduled flush will be executed.
     */
    public void scheduleFlush() {
        Runnable newClosure = new Runnable() {
            @Override
            public void run() {
                if (latestFlushClosure != this) {
                    return;
                }

                try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(false)) {
                    db.flush(flushOptions);
                } catch (RocksDBException e) {
                    LOG.error("Error occurred during the explicit flush for table '{}'", e, tableCfg.name());
                }
            }
        };

        latestFlushClosure = newClosure;

        int delay = engine.engineConfiguration().flushDelay().value();

        engine.scheduledPool().schedule(newClosure, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns a listener of RocksDB flush events. This listener is responsible for updating persisted index of partitions.
     *
     * @see RocksDbMvPartitionStorage#persistedIndex()
     * @see RocksDbMvPartitionStorage#refreshPersistedIndex()
     */
    private AbstractEventListener flushListener() {
        return new AbstractEventListener(ON_FLUSH_BEGIN, ON_FLUSH_COMPLETED) {
            /**
             * Type of last processed event. Real amount of events doesn't matter in atomic flush mode. All "completed" events go after all
             * "begin" events, and vice versa.
             */
            private final AtomicReference<EnabledEventCallback> lastEventType = new AtomicReference<>(ON_FLUSH_COMPLETED);

            /**
             * Future that guarantees that last flush was fully processed and the new flush can safely begin.
             */
            private volatile CompletableFuture<?> lastFlushProcessed = CompletableFuture.completedFuture(null);

            /** {@inheritDoc} */
            @Override
            public void onFlushBegin(RocksDB db, FlushJobInfo flushJobInfo) {
                if (lastEventType.compareAndSet(ON_FLUSH_COMPLETED, ON_FLUSH_BEGIN)) {
                    lastFlushProcessed.join();
                }
            }

            /** {@inheritDoc} */
            @Override
            public void onFlushCompleted(RocksDB db, FlushJobInfo flushJobInfo) {
                if (lastEventType.compareAndSet(ON_FLUSH_BEGIN, ON_FLUSH_COMPLETED)) {
                    lastFlushProcessed = CompletableFuture.runAsync(this::refreshPersistedIndexes, engine.threadPool());
                }
            }

            private void refreshPersistedIndexes() {
                if (!busyLock.enterBusy()) {
                    return;
                }

                try {
                    for (int partitionId = 0; partitionId < partitions.length(); partitionId++) {
                        RocksDbMvPartitionStorage partition = partitions.get(partitionId);

                        if (partition != null) {
                            try {
                                partition.refreshPersistedIndex();
                            } catch (StorageException storageException) {
                                LOG.error(
                                        "Filed to refresh persisted applied index value for table {} partition {}",
                                        storageException,
                                        tableCfg.name().value(),
                                        partitionId
                                );
                            }
                        }
                    }
                } finally {
                    busyLock.leaveBusy();
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        stopped = true;

        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        List<AutoCloseable> resources = new ArrayList<>();

        resources.add(db);

        resources.add(writeOptions);

        resources.addAll(sortedIndices.values());

        for (int i = 0; i < partitions.length(); i++) {
            MvPartitionStorage partition = partitions.get(i);

            if (partition != null) {
                resources.add(partition);
            }
        }

        Collections.reverse(resources);

        try {
            IgniteUtils.closeAll(resources);
        } catch (Exception e) {
            throw new StorageException("Failed to stop RocksDB table storage.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() throws StorageException {
        stop();

        IgniteUtils.deleteIfExists(tablePath);
    }

    /** {@inheritDoc} */
    @Override
    public PartitionStorage getOrCreatePartition(int partId) throws StorageException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public PartitionStorage getPartition(int partId) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void dropPartition(int partId) throws StorageException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public @Nullable RocksDbMvPartitionStorage getMvPartition(int partitionId) {
        checkPartitionId(partitionId);

        return partitions.get(partitionId);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<?> destroyPartition(int partitionId) throws StorageException {
        RocksDbMvPartitionStorage mvPartition = getMvPartition(partitionId);

        if (mvPartition != null) {
            partitions.set(partitionId, null);

            try (WriteBatch writeBatch = new WriteBatch()) {
                mvPartition.destroy(writeBatch);

                meta.removePartitionId(partitionId, writeBatch);

                db.write(writeOptions, writeBatch);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to destroy partition " + partitionId + " of table " + tableCfg.name(), e);
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexStorage getOrCreateSortedIndex(String indexName) {
        assert !stopped : "Storage has been stopped";

        return sortedIndices.computeIfAbsent(indexName, name -> {
            var indexDescriptor = new SortedIndexDescriptor(name, tableCfg.value());

            ColumnFamilyDescriptor cfDescriptor = sortedIndexCfDescriptor(indexDescriptor);

            ColumnFamily cf;
            try {
                cf = ColumnFamily.create(db, cfDescriptor);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to create new RocksDB column family: " + new String(cfDescriptor.getName(), UTF_8), e);
            }

            return new RocksDbSortedIndexStorage(cf, indexDescriptor);
        });
    }

    /** {@inheritDoc} */
    @Override
    public void dropIndex(String indexName) {
        assert !stopped : "Storage has been stopped";

        sortedIndices.computeIfPresent(indexName, (name, indexStorage) -> {
            indexStorage.destroy();

            return null;
        });
    }

    /** {@inheritDoc} */
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
                    "table", tableCfg.name().value(), false,
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
                    .collect(Collectors.toList());

            // even if the database is new (no existing Column Families), we return the names of mandatory column families, that
            // will be created automatically.
            return existingNames.isEmpty() ? List.of(META_CF_NAME, PARTITION_CF_NAME) : existingNames;
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
                .collect(Collectors.toList());
    }

    /**
     * Creates a Column Family descriptor for the given Family type (encoded in its name).
     */
    private ColumnFamilyDescriptor cfDescriptorFromName(String cfName) {
        switch (columnFamilyType(cfName)) {
            case META:
            case PARTITION:
                return new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), new ColumnFamilyOptions());

            case SORTED_INDEX:
                var indexDescriptor = new SortedIndexDescriptor(sortedIndexName(cfName), tableCfg.value());

                return sortedIndexCfDescriptor(indexDescriptor);

            default:
                throw new StorageException("Unidentified column family [name=" + cfName + ", table=" + tableCfg.name() + ']');
        }
    }

    /**
     * Creates a Column Family descriptor for a Sorted Index.
     */
    private static ColumnFamilyDescriptor sortedIndexCfDescriptor(SortedIndexDescriptor descriptor) {
        String cfName = sortedIndexCfName(descriptor.name());

        ColumnFamilyOptions options = new ColumnFamilyOptions().setComparator(new BinaryRowComparator(descriptor));

        return new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), options);
    }
}
