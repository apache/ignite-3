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

package org.apache.ignite.internal.raft.storage.impl;

import static org.apache.ignite.internal.raft.storage.impl.RocksDbSharedLogStorageUtils.raftNodeStorageEndPrefix;
import static org.apache.ignite.internal.raft.storage.impl.RocksDbSharedLogStorageUtils.raftNodeStorageStartPrefix;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.configuration.LogStorageBudgetView;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.raft.jraft.core.LogStorageBudgetFactory;
import org.apache.ignite.raft.jraft.core.LogStorageBudgetsModule;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Log storage factory based on {@link VolatileLogStorage}.
 */
public class VolatileLogStorageFactory implements LogStorageFactory {
    private final LogStorageBudgetView logStorageBudgetConfig;

    /** Shared db instance. */
    private final RocksDB db;

    /** Shared data column family handle. */
    private final ColumnFamilyHandle columnFamily;

    /** Executor for spill-out RocksDB tasks. */
    private final Executor executor;

    private final Map<String, LogStorageBudgetFactory> budgetFactories;

    /**
     * Creates a new instance.
     *
     * @param logStorageBudgetConfig Budget config.
     */
    public VolatileLogStorageFactory(
            LogStorageBudgetView logStorageBudgetConfig,
            RocksDB db,
            ColumnFamilyHandle columnFamily,
            Executor executor
    ) {
        this.logStorageBudgetConfig = logStorageBudgetConfig;
        this.db = db;
        this.columnFamily = columnFamily;
        this.executor = executor;

        Map<String, LogStorageBudgetFactory> factories = new HashMap<>();

        ClassLoader serviceClassLoader = Thread.currentThread().getContextClassLoader();

        for (LogStorageBudgetsModule module : ServiceLoader.load(LogStorageBudgetsModule.class, serviceClassLoader)) {
            Map<String, LogStorageBudgetFactory> factoriesFromModule = module.budgetFactories();

            checkForBudgetNameClashes(factories.keySet(), factoriesFromModule.keySet());

            factories.putAll(factoriesFromModule);
        }

        budgetFactories = Map.copyOf(factories);
    }

    private void checkForBudgetNameClashes(Set<String> names1, Set<String> names2) {
        Set<String> intersection = new HashSet<>(names1);
        intersection.retainAll(names2);

        if (!intersection.isEmpty()) {
            // TODO: IGNITE-17540 - add error code
            throw new IgniteInternalException(
                    String.format("Storage budget '%s' is provided by more than one module", intersection.iterator().next())
            );
        }
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public LogStorage createLogStorage(String raftNodeStorageId, RaftOptions raftOptions) {
        RocksDbSpillout spiltOnDisk = new RocksDbSpillout(db, columnFamily, raftNodeStorageId, executor);
        return new VolatileLogStorage(createLogStorageBudget(), new OnHeapLogs(), spiltOnDisk);
    }

    @Override
    public void destroyLogStorage(String uri) {
        try {
            RocksDbSpillout.deleteAllEntriesBetween(db, columnFamily, raftNodeStorageStartPrefix(uri), raftNodeStorageEndPrefix(uri));
        } catch (RocksDBException e) {
            throw new LogStorageException("Fail to destroy the log storage spillout for " + uri, e);
        }
    }

    @Override
    public Set<String> raftNodeStorageIdsOnDisk() {
        // This is a volatile storage; the storage is destroyed as a whole on startup, so nothing can remain on disk to the moment
        // when this method is called.
        return Set.of();
    }

    private LogStorageBudget createLogStorageBudget() {
        return newBudget(logStorageBudgetConfig);
    }

    private LogStorageBudget newBudget(LogStorageBudgetView logStorageBudgetConfig) {
        LogStorageBudgetFactory factory = budgetFactories.get(logStorageBudgetConfig.name());

        if (factory == null) {
            // TODO: IGNITE-17540 - add error code
            throw new IgniteInternalException("Cannot find a log storage budget by name '" + logStorageBudgetConfig.name() + "'");
        }

        return factory.create(logStorageBudgetConfig);
    }
}
