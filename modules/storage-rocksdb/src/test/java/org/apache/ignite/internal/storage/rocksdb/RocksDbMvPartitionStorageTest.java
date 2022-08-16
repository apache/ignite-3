/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.rocksdb;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.AbstractMvPartitionStorageTest;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageChange;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbDataStorageView;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Storage test implementation for {@link RocksDbMvPartitionStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class RocksDbMvPartitionStorageTest extends AbstractMvPartitionStorageTest {
    private RocksDbStorageEngine engine;

    private RocksDbTableStorage table;

    @BeforeEach
    public void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration RocksDbStorageEngineConfiguration engineConfig,
            @InjectConfiguration(
                    name = "table",
                    polymorphicExtensions = {
                            HashIndexConfigurationSchema.class,
                            UnknownDataStorageConfigurationSchema.class,
                            RocksDbDataStorageConfigurationSchema.class,
                            ConstantValueDefaultConfigurationSchema.class,
                            FunctionCallDefaultConfigurationSchema.class,
                            NullValueDefaultConfigurationSchema.class,
                    }
            ) TableConfiguration tableCfg
    ) throws Exception {
        tableCfg.dataStorage().change(c -> c.convert(RocksDbDataStorageChange.class)).get(1, SECONDS);

        assertThat(((RocksDbDataStorageView) tableCfg.dataStorage().value()).dataRegion(), equalTo(DEFAULT_DATA_REGION_NAME));

        engineConfig.change(cfg -> cfg
                .changeFlushDelayMillis(0)
                .changeDefaultRegion(c -> c.changeSize(16 * 1024).changeWriteBufferSize(16 * 1024))
        ).get(1, SECONDS);

        engine = new RocksDbStorageEngine(engineConfig, workDir);

        engine.start();

        table = (RocksDbTableStorage) engine.createMvTable(tableCfg);

        table.start();

        storage = table.getOrCreateMvPartition(0);
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                storage,
                table == null ? null : table::stop,
                engine == null ? null : engine::stop
        );
    }
}
