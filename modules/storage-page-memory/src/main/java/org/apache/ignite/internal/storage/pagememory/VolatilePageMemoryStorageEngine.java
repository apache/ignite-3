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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileView;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/**
 * Storage engine implementation based on {@link PageMemory} for in-memory case.
 */
public class VolatilePageMemoryStorageEngine implements StorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "aimem";

    /**
     * Maximum "work units" that are allowed to be used during {@link BplusTree} destruction.
     *
     * @see BplusTree#startGradualDestruction
     */
    public static final int MAX_DESTRUCTION_WORK_UNITS = 1_000;

    private static final IgniteLogger LOG = Loggers.forClass(VolatilePageMemoryStorageEngine.class);

    private final String igniteInstanceName;

    private final StorageConfiguration storageConfig;

    private final VolatilePageMemoryStorageEngineConfiguration engineConfig;

    private final PageIoRegistry ioRegistry;

    private final PageEvictionTracker pageEvictionTracker;

    private final Map<String, VolatilePageMemoryDataRegion> regions = new ConcurrentHashMap<>();

    private volatile ExecutorService destructionExecutor;

    private final CatalogIndexStatusSupplier indexStatusSupplier;

    /**
     * Constructor.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param engineConfig PageMemory storage engine configuration.
     * @param storageConfig Root configuration of storage engines and profiles.
     * @param ioRegistry IO registry.
     * @param pageEvictionTracker Eviction tracker to use.
     * @param indexStatusSupplier Catalog index status supplier.
     */
    public VolatilePageMemoryStorageEngine(
            String igniteInstanceName,
            VolatilePageMemoryStorageEngineConfiguration engineConfig,
            StorageConfiguration storageConfig,
            PageIoRegistry ioRegistry,
            PageEvictionTracker pageEvictionTracker,
            CatalogIndexStatusSupplier indexStatusSupplier
    ) {
        this.igniteInstanceName = igniteInstanceName;
        this.engineConfig = engineConfig;
        this.storageConfig = storageConfig;
        this.ioRegistry = ioRegistry;
        this.pageEvictionTracker = pageEvictionTracker;
        this.indexStatusSupplier = indexStatusSupplier;
    }

    @Override
    public String name() {
        return ENGINE_NAME;
    }

    @Override
    public void start() throws StorageException {
        storageConfig.profiles().value().stream().forEach(p -> {
            if (p instanceof VolatilePageMemoryProfileView) {
                addDataRegion(p.name());
            }
        });

        // TODO: remove this executor, see https://issues.apache.org/jira/browse/IGNITE-21683
        destructionExecutor = new ThreadPoolExecutor(
                0,
                Runtime.getRuntime().availableProcessors(),
                100,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                NamedThreadFactory.create(igniteInstanceName, "volatile-mv-partition-destruction", LOG)
        );
    }

    @Override
    public void stop() throws StorageException {
        try {
            Stream<AutoCloseable> closeRegions = regions.values().stream().map(region -> region::stop);

            ExecutorService destructionExecutor = this.destructionExecutor;

            Stream<AutoCloseable> shutdownExecutor = Stream.of(
                    destructionExecutor == null
                            ? null
                            : (AutoCloseable) () -> shutdownAndAwaitTermination(destructionExecutor, 30, TimeUnit.SECONDS)
            );

            closeAll(Stream.concat(shutdownExecutor, closeRegions));
        } catch (Exception e) {
            throw new StorageException("Error when stopping components", e);
        }
    }

    @Override
    public boolean isVolatile() {
        return true;
    }

    @Override
    public VolatilePageMemoryTableStorage createMvTable(
            StorageTableDescriptor tableDescriptor,
            StorageIndexDescriptorSupplier indexDescriptorSupplier
    ) throws StorageException {
        VolatilePageMemoryDataRegion dataRegion = regions.get(tableDescriptor.getStorageProfile());

        assert dataRegion != null : tableDescriptor;

        return new VolatilePageMemoryTableStorage(
                tableDescriptor,
                indexDescriptorSupplier,
                indexStatusSupplier,
                dataRegion,
                destructionExecutor
        );
    }

    @Override
    public void dropMvTable(int tableId) {
        // No-op.
    }

    /**
     * Creates, starts and adds a new data region to the engine.
     *
     * @param name Data region name.
     */
    private void addDataRegion(String name) {
        VolatilePageMemoryProfileConfiguration storageProfileConfiguration =
                (VolatilePageMemoryProfileConfiguration) storageConfig.profiles().get(name);

        int pageSize = engineConfig.pageSize().value();

        VolatilePageMemoryDataRegion dataRegion = new VolatilePageMemoryDataRegion(
                storageProfileConfiguration,
                ioRegistry,
                pageSize,
                pageEvictionTracker
        );

        dataRegion.start();

        regions.put(name, dataRegion);
    }
}
