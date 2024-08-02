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
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
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
    public static final String ENGINE_NAME = "rocksdb";

    private static final IgniteLogger LOG = Loggers.forClass(RocksDbStorageEngine.class);

    private static class RocksDbStorage implements ManuallyCloseable {
        final RocksDbStorageProfile profile;

        final SharedRocksDbInstance rocksDbInstance;

        RocksDbStorage(RocksDbStorageProfile profile, SharedRocksDbInstance rocksDbInstance) {
            this.profile = profile;
            this.rocksDbInstance = rocksDbInstance;
        }

        @Override
        public void close() throws Exception {
            IgniteUtils.closeAllManually(rocksDbInstance::stop, profile::stop);
        }
    }

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDbStorageEngineConfiguration engineConfig;

    private final StorageConfiguration storageConfiguration;

    private final Path storagePath;

    private final ExecutorService threadPool;

    private final ScheduledExecutorService scheduledPool;

    /**
     * Mapping from the storage profile name to the shared RocksDB instance.
     */
    private final Map<String, RocksDbStorage> storageByProfileName = new ConcurrentHashMap<>();

    private final LogSyncer logSyncer;

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param engineConfig RocksDB storage engine configuration.
     * @param storageConfiguration Storage configuration.
     * @param storagePath Storage path.
     * @param logSyncer Write-ahead log synchronizer.
     */
    public RocksDbStorageEngine(
            String nodeName,
            RocksDbStorageEngineConfiguration engineConfig,
            StorageConfiguration storageConfiguration,
            Path storagePath,
            LogSyncer logSyncer
    ) {
        this.engineConfig = engineConfig;
        this.storageConfiguration = storageConfiguration;
        this.storagePath = storagePath;
        this.logSyncer = logSyncer;

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
        // TODO: IGNITE-17066 Add handling deleting/updating storage profiles configuration
        storageConfiguration.profiles().value();
        for (StorageProfileView profile : storageConfiguration.profiles().value()) {
            if (profile instanceof RocksDbProfileView) {
                registerProfile((RocksDbProfileView) profile);
            }
        }
    }

    private void registerProfile(RocksDbProfileView profileConfig) {
        String profileName = profileConfig.name();

        var profile = new RocksDbStorageProfile(profileConfig);

        profile.start();

        SharedRocksDbInstance rocksDbInstance = newRocksDbInstance(profileName, profile);

        RocksDbStorage previousStorage = storageByProfileName.put(profileName, new RocksDbStorage(profile, rocksDbInstance));

        assert previousStorage == null : "Storage already exists for profile: " + profileName;
    }

    private SharedRocksDbInstance newRocksDbInstance(String profileName, RocksDbStorageProfile profile) {
        Path dbPath = storagePath.resolve("rocksdb-" + profileName);

        try {
            return new SharedRocksDbInstanceCreator().create(this, profile, dbPath);
        } catch (Exception e) {
            throw new StorageException("Failed to create new RocksDB instance", e);
        }
    }

    @Override
    public void stop() throws StorageException {
        try {
            IgniteUtils.closeAllManually(storageByProfileName.values());
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
        String profileName = tableDescriptor.getStorageProfile();

        RocksDbStorage storage = storageByProfileName.get(profileName);

        assert storage != null :
                String.format(
                        "RocksDB instance has not yet been created for [tableId=%d, profile=%s]", tableDescriptor.getId(), profileName
                );

        var tableStorage = new RocksDbTableStorage(storage.rocksDbInstance, tableDescriptor, indexDescriptorSupplier);

        tableStorage.start();

        return tableStorage;
    }

    @Override
    public void dropMvTable(int tableId) {
        for (RocksDbStorage rocksDbStorage : storageByProfileName.values()) {
            rocksDbStorage.rocksDbInstance.destroyTable(tableId);
        }
    }
}
