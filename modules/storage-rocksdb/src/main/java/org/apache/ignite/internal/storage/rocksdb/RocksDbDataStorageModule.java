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

import static org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine.ENGINE_NAME;

import com.google.auto.service.AutoService;
import java.nio.file.Path;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileStorageEngineConfiguration;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineExtensionConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation for creating {@link RocksDbStorageEngine}s.
 */
@AutoService(DataStorageModule.class)
public class RocksDbDataStorageModule implements DataStorageModule {
    @Override
    public String name() {
        return ENGINE_NAME;
    }

    @Override
    public StorageEngine createEngine(
            String igniteInstanceName,
            ConfigurationRegistry configRegistry,
            Path storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector
    ) throws StorageException {
        RocksDbProfileStorageEngineConfiguration engineConfig =
                ((RocksDbStorageEngineExtensionConfiguration) configRegistry
                        .getConfiguration(StorageConfiguration.KEY).engines()).rocksdb();

        StorageConfiguration storageConfig = configRegistry.getConfiguration(StorageConfiguration.KEY);

        assert engineConfig != null;

        return new RocksDbStorageEngine(igniteInstanceName, engineConfig, storageConfig, storagePath);
    }
}
