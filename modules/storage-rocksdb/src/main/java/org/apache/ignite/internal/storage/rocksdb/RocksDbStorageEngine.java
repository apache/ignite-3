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

import static org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;
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

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDbStorageEngineConfiguration engineConfig;

    private final Path storagePath;

    private final ExecutorService threadPool = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("rocksdb-storage-engine-pool")
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

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        RocksDbDataRegion defaultRegion = new RocksDbDataRegion(engineConfig.defaultRegion());

        defaultRegion.start();

        regions.put(DEFAULT_DATA_REGION_NAME, defaultRegion);

        for (String regionName : engineConfig.regions().value().namedListKeys()) {
            RocksDbDataRegion region = new RocksDbDataRegion(engineConfig.regions().get(regionName));

            region.start();

            regions.put(regionName, region);
        }
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
    public TableStorage createTable(TableConfiguration tableCfg) throws StorageException {
        TableView tableView = tableCfg.value();

        assert tableView.dataStorage().name().equals(ENGINE_NAME) : tableView.dataStorage().name();

        RocksDbDataStorageView dataStorageView = (RocksDbDataStorageView) tableView.dataStorage();

        RocksDbDataRegion dataRegion = regions.get(dataStorageView.dataRegion());

        Path tablePath = storagePath.resolve(tableView.name());

        try {
            Files.createDirectories(tablePath);
        } catch (IOException e) {
            throw new StorageException("Failed to create table store directory for " + tableView.name() + ": " + e.getMessage(), e);
        }

        return new RocksDbTableStorage(tablePath, tableCfg, threadPool, dataRegion);
    }
}
