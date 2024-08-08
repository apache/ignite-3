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

package org.apache.ignite.internal.storage;

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.jetbrains.annotations.Nullable;

/**
 * Auxiliary class for working with {@link DataStorageModule}.
 */
public class DataStorageModules {
    /** Mapping: {@link DataStorageModule#name} -> DataStorageModule. */
    private final Map<String, DataStorageModule> modules;

    /**
     * Constructor.
     *
     * <p>Modules are expected to have a unique {@link DataStorageModule#name}.
     *
     * @param dataStorageModules Data storage modules.
     * @throws IllegalStateException If the module is not correct.
     */
    public DataStorageModules(Iterable<DataStorageModule> dataStorageModules) {
        Map<String, DataStorageModule> modules = new HashMap<>();

        for (DataStorageModule module : dataStorageModules) {
            String name = module.name();

            if (modules.containsKey(name)) {
                throw new IllegalStateException(String.format(
                        "Duplicate name [name=%s, factories=%s]",
                        name,
                        List.of(modules.get(name), module)
                ));
            }

            modules.put(name, module);
        }

        assert !modules.isEmpty();

        this.modules = modules;
    }

    /**
     * Creates new storage engines unique by {@link DataStorageModule#name name}.
     *
     * @param igniteInstanceName String igniteInstanceName
     * @param configRegistry Configuration register.
     * @param storagePath Storage path.
     * @param longJvmPauseDetector Long JVM pause detector.
     * @param failureProcessor Failure processor that is used to handle critical errors.
     * @param logSyncer Write-ahead log synchronizer.
     * @param clock Hybrid Logical Clock.
     * @throws StorageException If there is an error when creating the storage engines.
     */
    public Map<String, StorageEngine> createStorageEngines(
            String igniteInstanceName,
            ConfigurationRegistry configRegistry,
            Path storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            FailureProcessor failureProcessor,
            LogSyncer logSyncer,
            HybridClock clock
    ) {
        return modules.entrySet().stream().collect(toUnmodifiableMap(
                Entry::getKey,
                e -> e.getValue().createEngine(
                        igniteInstanceName,
                        configRegistry,
                        storagePath,
                        longJvmPauseDetector,
                        failureProcessor,
                        logSyncer,
                        clock
                )
        ));
    }
}
