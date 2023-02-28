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

import static org.apache.ignite.internal.storage.BaseMvStoragesTest.getOrCreateMvPartition;
import static org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageConfiguration;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link RocksDbStorageEngine}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbStorageEngineTest {
    private RocksDbStorageEngine engine;

    @InjectConfiguration
    private RocksDbStorageEngineConfiguration engineConfig;

    @BeforeEach
    void setUp(@WorkDirectory Path workDir) {
        engine = new RocksDbStorageEngine(engineConfig, workDir);

        engine.start();
    }

    @AfterEach
    void tearDown() {
        engine.stop();
    }

    @Test
    void testCreateTableWithDefaultDataRegion(
            @InjectConfiguration(
                    value = "mock.tables.foo.dataStorage.name=" + RocksDbStorageEngine.ENGINE_NAME
            )
            TablesConfiguration tablesConfig
    ) {
        MvTableStorage table = engine.createMvTable(tablesConfig.tables().get("foo"), tablesConfig);

        table.start();

        try {
            RocksDbDataStorageConfiguration dataStorageConfig = (RocksDbDataStorageConfiguration) table.configuration().dataStorage();

            assertThat(dataStorageConfig.dataRegion().value(), is(DEFAULT_DATA_REGION_NAME));

            getOrCreateMvPartition(table, 1);
        } finally {
            table.stop();
        }
    }

    @Test
    void testCreateTableWithDynamicCustomDataRegion(
            @InjectConfiguration(
                    value = "mock.tables.foo.dataStorage{name=" + RocksDbStorageEngine.ENGINE_NAME + ", dataRegion=foobar}"
            )
            TablesConfiguration tablesConfig
    ) {
        String customRegionName = "foobar";

        CompletableFuture<Void> engineConfigChangeFuture = engineConfig.regions()
                .change(c -> c.create(customRegionName, rocksDbDataRegionChange -> {}));

        assertThat(engineConfigChangeFuture, willCompleteSuccessfully());

        MvTableStorage table = engine.createMvTable(tablesConfig.tables().get("foo"), tablesConfig);

        table.start();

        try {
            RocksDbDataStorageConfiguration dataStorageConfig = (RocksDbDataStorageConfiguration) table.configuration().dataStorage();

            assertThat(dataStorageConfig.dataRegion().value(), is(customRegionName));

            getOrCreateMvPartition(table, 1);
        } finally {
            table.stop();
        }
    }
}
