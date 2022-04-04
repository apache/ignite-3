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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageEngineFactory;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Data storage manager.
 */
public class DataStorageManager implements IgniteComponent {
    /** Mapping: {@link StorageEngine#name} -> {@link StorageEngine}. */
    private final Map<String, StorageEngine> engines;

    /**
     * Constructor.
     *
     * @param clusterConfigRegistry Register of the (distributed) cluster configuration.
     * @param storagePath Storage path.
     * @throws StorageException If there are duplicates of the data storage engine.
     */
    public DataStorageManager(
            ConfigurationRegistry clusterConfigRegistry,
            Path storagePath
    ) {
        this.engines = StreamSupport.stream(engineFactories().spliterator(), false)
                .map(engineFactory -> engineFactory.createEngine(clusterConfigRegistry, storagePath))
                .collect(toUnmodifiableMap(
                        StorageEngine::name,
                        identity(),
                        (storageEngine1, storageEngine2) -> {
                            throw new StorageException(String.format(
                                    "Duplicate key [key=%s, engines=%s]",
                                    storageEngine1.name(),
                                    List.of(storageEngine1, storageEngine2)
                            ));
                        }
                ));
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        engines.values().forEach(StorageEngine::start);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        IgniteUtils.closeAll(engines.values().stream().map(engine -> engine::stop));
    }

    /**
     * Returns the storage engine factories.
     *
     * <p>NOTE: It is recommended to override only in tests.
     */
    protected Iterable<StorageEngineFactory> engineFactories() {
        return ServiceLoader.load(StorageEngineFactory.class);
    }

    /**
     * Returns the data storage engine by data storage configuration.
     *
     * @param config Data storage configuration.
     */
    public @Nullable StorageEngine engine(DataStorageConfiguration config) {
        return engines.get(config.value().name());
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(DataStorageManager.class, this);
    }
}
