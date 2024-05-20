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

import static org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine.ENGINE_NAME;

import com.google.auto.service.AutoService;
import java.nio.file.Path;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineExtensionConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation for creating {@link VolatilePageMemoryStorageEngine}.
 */
@AutoService(DataStorageModule.class)
public class VolatilePageMemoryDataStorageModule implements DataStorageModule {
    /** {@inheritDoc} */
    @Override
    public String name() {
        return ENGINE_NAME;
    }

    /** {@inheritDoc} */
    @Override
    public StorageEngine createEngine(
            String igniteInstanceName,
            ConfigurationRegistry configRegistry,
            Path storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            FailureProcessor failureProcessor,
            LogSyncer logSyncer
    ) throws StorageException {
        VolatilePageMemoryStorageEngineConfiguration engineConfig =
                ((VolatilePageMemoryStorageEngineExtensionConfiguration) configRegistry
                        .getConfiguration(StorageConfiguration.KEY).engines()).aimem();

        assert engineConfig != null;

        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new VolatilePageMemoryStorageEngine(igniteInstanceName, engineConfig,
                configRegistry.getConfiguration(StorageConfiguration.KEY), ioRegistry);
    }
}
