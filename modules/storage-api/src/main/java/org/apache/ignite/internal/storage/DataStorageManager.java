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

package org.apache.ignite.internal.storage;

import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema.UNKNOWN_DATA_STORAGE;
import static org.apache.ignite.internal.util.CollectionUtils.first;

import java.nio.file.Path;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import org.apache.ignite.configuration.schemas.store.DataStorageChange;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageView;
import org.apache.ignite.configuration.schemas.table.TableConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TablesConfigurationSchema;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageEngineFactory;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Data storage manager.
 */
public class DataStorageManager implements IgniteComponent {
    /** Mapping: {@link StorageEngineFactory#name} -> {@link StorageEngine}. */
    private final Map<String, StorageEngine> engines;

    /**
     * Constructor.
     *
     * @param clusterConfigRegistry Register of the (distributed) cluster configuration.
     * @param storagePath Storage path.
     * @param engineFactories Storage engine factories.
     * @throws IllegalStateException If there are duplicates of the data storage engine.
     */
    public DataStorageManager(
            ConfigurationRegistry clusterConfigRegistry,
            Path storagePath,
            Iterable<StorageEngineFactory> engineFactories
    ) {
        engines = StreamSupport.stream(engineFactories.spliterator(), false)
                .collect(toUnmodifiableMap(
                        StorageEngineFactory::name,
                        engineFactory -> engineFactory.createEngine(clusterConfigRegistry, storagePath)
                ));
    }

    /**
     * Constructor, overloads {@link DataStorageManager#DataStorageManager(ConfigurationRegistry, Path, Iterable)} with loading factories
     * through a {@link ServiceLoader}.
     */
    public DataStorageManager(
            ConfigurationRegistry clusterConfigRegistry,
            Path storagePath
    ) {
        this(clusterConfigRegistry, storagePath, ServiceLoader.load(StorageEngineFactory.class));
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        engines.values().forEach(StorageEngine::start);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {
        engines.values().forEach(StorageEngine::stop);
    }

    /**
     * Returns the data storage engine by data storage configuration.
     *
     * @param config Data storage configuration.
     */
    public @Nullable StorageEngine engine(DataStorageConfiguration config) {
        return engine(config.value());
    }

    private @Nullable StorageEngine engine(DataStorageView view) {
        return engines.get(view.name());
    }

    /**
     * Returns a consumer that will set the default {@link TableConfigurationSchema#dataStorage table data storage} depending on the {@link
     * StorageEngine engine}.
     *
     * @param defaultDataStorageView View of {@link TablesConfigurationSchema#defaultDataStorage}.
     */
    // TODO: IGNITE-16792 исправить это
    public Consumer<DataStorageChange> defaultTableDataStorageConsumer(String defaultDataStorageView) {
        if (!defaultDataStorageView.equals(UNKNOWN_DATA_STORAGE)) {
            return engines.get(defaultDataStorageView).defaultTableDataStorageConsumer(defaultDataStorageView);
        }

        if (engines.size() == 1) {
            return first(engines.values()).defaultTableDataStorageConsumer(defaultDataStorageView);
        }

        return change -> {
        };
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(DataStorageManager.class, this);
    }
}
