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

import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.ENGINE_NAME;

import com.google.auto.service.AutoService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineExtensionConfiguration;
import org.apache.ignite.internal.util.LazyPath;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation for creating {@link PersistentPageMemoryStorageEngine}.
 */
@AutoService(DataStorageModule.class)
public class PersistentPageMemoryDataStorageModule implements DataStorageModule {
    @Override
    public String name() {
        return ENGINE_NAME;
    }

    @Override
    public StorageEngine createEngine(
            String igniteInstanceName,
            ConfigurationRegistry configRegistry,
            LazyPath storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            FailureProcessor failureProcessor,
            LogSyncer logSyncer,
            HybridClock clock
    ) throws StorageException {
        StorageConfiguration storageConfig = configRegistry.getConfiguration(StorageConfiguration.KEY);

        PersistentPageMemoryStorageEngineConfiguration engineConfig =
                ((PersistentPageMemoryStorageEngineExtensionConfiguration) storageConfig.engines()).aipersist();

        assert engineConfig != null;

        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PersistentPageMemoryStorageEngine(
                igniteInstanceName,
                engineConfig,
                storageConfig,
                ioRegistry,
                storagePath,
                longJvmPauseDetector,
                failureProcessor,
                logSyncer,
                clock
        );
    }
}
