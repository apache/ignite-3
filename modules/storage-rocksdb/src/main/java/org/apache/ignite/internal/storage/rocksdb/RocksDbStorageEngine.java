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

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileView;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstance;
import org.apache.ignite.internal.storage.rocksdb.instance.SharedRocksDbInstanceCreator;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.rocksdb.RocksDB;

/**
 * Storage engine implementation based on RocksDB.
 */
public class RocksDbStorageEngine implements StorageEngine {
    /** Engine name. */
    // TODO: KKK db vs Db
    public static final String ENGINE_NAME = "rocksDb";

    private static final IgniteLogger LOG = Loggers.forClass(RocksDbStorageEngine.class);

    private static class RocksDbStorage implements ManuallyCloseable {
        final RocksDbDataRegion dataRegion;

        final SharedRocksDbInstance rocksDbInstance;

        RocksDbStorage(RocksDbDataRegion dataRegion, SharedRocksDbInstance rocksDbInstance) {
            this.dataRegion = dataRegion;
            this.rocksDbInstance = rocksDbInstance;
        }

        @Override
        public void close() throws Exception {
            IgniteUtils.closeAllManually(rocksDbInstance::stop, dataRegion::stop);
        }
    }

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDbStorageEngineConfiguration engineConfig;

    private final StorageConfiguration storageConfig;

    private final Path storagePath;

    private final ExecutorService threadPool;

    private final ScheduledExecutorService scheduledPool;

    /**
     * Mapping from the data region name to the shared RocksDB instance. Most likely, the association of shared
     * instances with regions will be removed/revisited in the future.
     */
    // TODO IGNITE-19762 Think of proper way to use regions and storages.
    private final Map<String, RocksDbStorage> storageByRegionName = new ConcurrentHashMap<>();

    private final LogSyncer logSyncer;

    private final CatalogIndexStatusSupplier indexStatusSupplier;

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param engineConfig RocksDB storage engine configuration.
     * @param storageConfig Root configuration of storage engines and profiles.
     * @param storagePath Storage path.
     * @param logSyncer Write-ahead log synchronizer.
     * @param indexStatusSupplier Catalog index status supplier.
     */
    public RocksDbStorageEngine(
            String nodeName,
            RocksDbStorageEngineConfiguration engineConfig,
            StorageConfiguration storageConfig,
            Path storagePath,
            LogSyncer logSyncer,
            CatalogIndexStatusSupplier indexStatusSupplier
    ) {
        this.engineConfig = engineConfig;
        this.storageConfig = storageConfig;
        this.storagePath = storagePath;
        this.logSyncer = logSyncer;
        this.indexStatusSupplier = indexStatusSupplier;

        threadPool = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                NamedThreadFactory.create(nodeName, "rocksdb-storage-engine-pool", LOG)
        );

        scheduledPool = Executors.newSingleThreadScheduledExecutor(
                NamedThreadFactory.create(nodeName, "rocksdb-storage-engine-scheduled-pool", LOG)
        );
    }

    /**
     * Returns a RocksDB storage engine configuration.
     */
    public RocksDbStorageEngineConfiguration configuration() {
        return engineConfig;
    }

    /**
     * Returns a common thread pool for async operations.
     */
    public ExecutorService threadPool() {
        return threadPool;
    }

    /**
     * Returns a scheduled thread pool for async operations.
     */
    public ScheduledExecutorService scheduledPool() {
        return scheduledPool;
    }

    public LogSyncer logSyncer() {
        return logSyncer;
    }

    @Override
    public String name() {
        return ENGINE_NAME;
    }

    @Override
    public void start() throws StorageException {
        // TODO: IGNITE-17066 Add handling deleting/updating data regions configuration
        storageConfig.profiles().value().stream().forEach(p -> {
            if (p instanceof RocksDbProfileView) {
                registerDataRegion((RocksDbProfileView) p);
            }
        });
    }

    private void registerDataRegion(RocksDbProfileView dataRegionView) {
        String regionName = dataRegionView.name();

        var region = new RocksDbDataRegion(dataRegionView);

        region.start();

        SharedRocksDbInstance rocksDbInstance = newRocksDbInstance(regionName, region);

        RocksDbStorage previousStorage = storageByRegionName.put(regionName, new RocksDbStorage(region, rocksDbInstance));

        assert previousStorage == null : regionName;
    }

    private SharedRocksDbInstance newRocksDbInstance(String regionName, RocksDbDataRegion region) {
        try {
            return new SharedRocksDbInstanceCreator().create(this, region, storagePath.resolve("rocksdb-" + regionName));
        } catch (Exception e) {
            throw new StorageException("Failed to create new RocksDB instance", e);
        }
    }

    @Override
    public void stop() throws StorageException {
        try {
            IgniteUtils.closeAllManually(storageByRegionName.values());
        } catch (Exception e) {
            throw new StorageException("Error when stopping the rocksdb engine", e);
        }

        IgniteUtils.shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(scheduledPool, 10, TimeUnit.SECONDS);
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    @Override
    public RocksDbTableStorage createMvTable(
            StorageTableDescriptor tableDescriptor,
            StorageIndexDescriptorSupplier indexDescriptorSupplier
    ) throws StorageException {
        String regionName = tableDescriptor.getStorageProfile();

        RocksDbStorage storage = storageByRegionName.get(regionName);

        assert storage != null :
                String.format("RocksDB instance has not yet been created for [tableId=%d, region=%s]", tableDescriptor.getId(), regionName);

        var tableStorage = new RocksDbTableStorage(storage.rocksDbInstance, tableDescriptor, indexDescriptorSupplier);

        tableStorage.start();

        return tableStorage;
    }

    @Override
    public void dropMvTable(int tableId) {
        for (RocksDbStorage rocksDbStorage : storageByRegionName.values()) {
            rocksDbStorage.rocksDbInstance.destroyTable(tableId);
        }
    }

    /** Returns catalog index status supplier. */
    public CatalogIndexStatusSupplier indexStatusSupplier() {
        return indexStatusSupplier;
    }
}
