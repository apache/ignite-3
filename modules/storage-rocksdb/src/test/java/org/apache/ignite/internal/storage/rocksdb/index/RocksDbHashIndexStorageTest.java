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

package org.apache.ignite.internal.storage.rocksdb.index;

import java.nio.file.Path;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.EntryCountBudgetConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.schemas.table.UnlimitedBudgetConfigurationSchema;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.index.AbstractHashIndexStorageTest;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.RocksDbTableStorage;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the {@link RocksDbHashIndexStorage} class.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbHashIndexStorageTest extends AbstractHashIndexStorageTest {
    private RocksDbStorageEngine engine;

    private RocksDbTableStorage tableStorage;

    @BeforeEach
    void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration("mock {flushDelayMillis = 0, defaultRegion {size = 16536, writeBufferSize = 16536}}")
            RocksDbStorageEngineConfiguration rocksDbEngineConfig,
            @InjectConfiguration(
                    polymorphicExtensions = {
                            RocksDbDataStorageConfigurationSchema.class,
                            HashIndexConfigurationSchema.class,
                            NullValueDefaultConfigurationSchema.class,
                            UnlimitedBudgetConfigurationSchema.class
                    },
                    value = "mock.dataStorage.name = " + RocksDbStorageEngine.ENGINE_NAME
            )
            TableConfiguration tableCfg,

            @InjectConfiguration(polymorphicExtensions = {
                    HashIndexConfigurationSchema.class,
                    UnknownDataStorageConfigurationSchema.class,
                    ConstantValueDefaultConfigurationSchema.class,
                    FunctionCallDefaultConfigurationSchema.class,
                    NullValueDefaultConfigurationSchema.class,
                    UnlimitedBudgetConfigurationSchema.class,
                    EntryCountBudgetConfigurationSchema.class
            })
            TablesConfiguration tablesConfig
    ) {
        engine = new RocksDbStorageEngine(rocksDbEngineConfig, workDir);

        engine.start();

        tableStorage = engine.createMvTable(tableCfg, tablesConfig);

        tableStorage.start();

        initialize(tableStorage, tablesConfig);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                tableStorage == null ? null : tableStorage::stop,
                engine == null ? null : engine::stop
        );
    }
}
