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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataRegionConfiguration;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataRegionView;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageView;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.rocksdb.RocksDB;

/**
 * Storage engine implementation based on RocksDB.
 */
public class RocksDbStorageEngine implements StorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "rocksdb";

    /** Prefix for table directories. */
    private static final String TABLE_DIR_PREFIX = "table-";

    private static final IgniteLogger LOG = Loggers.forClass(RocksDbStorageEngine.class);

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDbStorageEngineConfiguration engineConfig;

    private final Path storagePath;

    private final ExecutorService threadPool = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("rocksdb-storage-engine-pool", LOG)
    );

    private final ScheduledExecutorService scheduledPool = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("rocksdb-storage-engine-scheduled-pool", LOG)
    );

    private final Map<String, RocksDbDataRegion> regions = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param engineConfig RocksDB storage engine configuration.
     * @param storagePath Storage path.
     */
    public RocksDbStorageEngine(RocksDbStorageEngineConfiguration engineConfig, Path storagePath) {
        this.engineConfig = engineConfig;
        this.storagePath = storagePath;
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

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        registerDataRegion(engineConfig.defaultRegion());

        // TODO: IGNITE-17066 Add handling deleting/updating data regions configuration
        engineConfig.regions().listenElements(new ConfigurationNamedListListener<>() {
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<RocksDbDataRegionView> ctx) {
                registerDataRegion(ctx.config(RocksDbDataRegionConfiguration.class));

                return CompletableFuture.completedFuture(null);
            }
        });
    }

    private void registerDataRegion(RocksDbDataRegionConfiguration dataRegionConfig) {
        var region = new RocksDbDataRegion(dataRegionConfig);

        region.start();

        RocksDbDataRegion previousRegion = regions.put(dataRegionConfig.name().value(), region);

        assert previousRegion == null : dataRegionConfig.name().value();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        IgniteUtils.shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);

        try {
            IgniteUtils.closeAll(regions.values().stream().map(region -> region::stop));
        } catch (Exception e) {
            throw new StorageException("Error when stopping regions", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public RocksDbTableStorage createMvTable(TableConfiguration tableCfg) throws StorageException {
        TableView tableView = tableCfg.value();

        assert tableView.dataStorage().name().equals(ENGINE_NAME) : tableView.dataStorage().name();

        RocksDbDataStorageView dataStorageView = (RocksDbDataStorageView) tableView.dataStorage();

        RocksDbDataRegion dataRegion = regions.get(dataStorageView.dataRegion());

        Path tablePath = storagePath.resolve(TABLE_DIR_PREFIX + tableView.tableId());

        try {
            Files.createDirectories(tablePath);
        } catch (IOException e) {
            throw new StorageException("Failed to create table store directory for " + tableView.name() + ": " + e.getMessage(), e);
        }

        return new RocksDbTableStorage(this, tablePath, tableCfg, dataRegion);
    }
}
