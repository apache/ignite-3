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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.configurations.StoragesConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileConfiguration;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileStorageEngineConfiguration;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileView;
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
    public static final String ENGINE_NAME = "rocksdb";

    private static final IgniteLogger LOG = Loggers.forClass(RocksDbStorageEngine.class);

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDbProfileStorageEngineConfiguration engineConfig;

    private final StoragesConfiguration storagesConfiguration;

    private final Path storagePath;

    private final ExecutorService threadPool;

    private final ScheduledExecutorService scheduledPool;

    private final Map<String, RocksDbDataRegion> regions = new ConcurrentHashMap<>();

    /**
     * Mapping from the data region name to the shared RocksDB instance. Map is filled lazily.
     * Most likely, the association of shared instances with regions will be removed/revisited in the future.
     */
    //TODO IGNITE-19762 Think of proper way to use regions and storages.
    private final Map<String, SharedRocksDbInstance> sharedInstances = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param engineConfig RocksDB storage engine configuration.
     * @param storagePath Storage path.
     */
    public RocksDbStorageEngine(String nodeName, RocksDbProfileStorageEngineConfiguration engineConfig,
            StoragesConfiguration storagesConfiguration, Path storagePath) {
        this.engineConfig = engineConfig;
        this.storagesConfiguration = storagesConfiguration;
        this.storagePath = storagePath;

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
    public RocksDbProfileStorageEngineConfiguration configuration() {
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

    @Override
    public String name() {
        return ENGINE_NAME;
    }

    @Override
    public void testStart() throws StorageException {
        storagesConfiguration.profiles().value().stream().forEach(p -> {
            if (p instanceof RocksDbProfileView) {
                registerDataRegion(p.name());
            }
        });
    }

    @Override
    public void start() throws StorageException {
        // TODO: IGNITE-17066 Add handling deleting/updating data regions configuration
        storagesConfiguration.profiles().value().stream().forEach(p -> {
            if (p instanceof RocksDbProfileView) {
                registerDataRegion(p.name());
            }
        });
    }

    private void registerDataRegion(String name) {
        // TODO: why this cast is needed?
        RocksDbProfileConfiguration storageProfileConfiguration =
                (RocksDbProfileConfiguration) storagesConfiguration.profiles().get(name);

        var region = new RocksDbDataRegion(storageProfileConfiguration);

        region.start();

        RocksDbDataRegion previousRegion = regions.put(storageProfileConfiguration.name().value(), region);

        assert previousRegion == null : storageProfileConfiguration.name().value();
    }

    @Override
    public void stop() throws StorageException {
        try {
            IgniteUtils.closeAll(Stream.concat(
                    regions.values().stream().map(region -> region::stop),
                    sharedInstances.values().stream().map(instance -> instance::stop)
            ));
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
        RocksDbDataRegion dataRegion = regions.get(tableDescriptor.getDataRegion());

        int tableId = tableDescriptor.getId();

        assert dataRegion != null : "tableId=" + tableId + ", dataRegion=" + tableDescriptor.getDataRegion();

        SharedRocksDbInstance sharedInstance = sharedInstances.computeIfAbsent(tableDescriptor.getDataRegion(), name -> {
            try {
                return new SharedRocksDbInstanceCreator().create(
                        this,
                        dataRegion,
                        storagePath.resolve("rocksdb-" + name)
                );
            } catch (Exception e) {
                throw new StorageException("Failed to create new RocksDB data region", e);
            }
        });

        return new RocksDbTableStorage(sharedInstance, tableDescriptor);
    }
}
