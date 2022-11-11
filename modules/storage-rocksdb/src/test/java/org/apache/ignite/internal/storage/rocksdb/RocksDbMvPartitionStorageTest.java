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

import static org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.AbstractMvPartitionStorageTest;
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
            @InjectConfiguration("mock {flushDelayMillis = 0, defaultRegion {size = 16536, writeBufferSize = 16536}}")
            RocksDbStorageEngineConfiguration engineConfig,
            @InjectConfiguration(
                    value = "mock.tables.foo.dataStorage.name = " + RocksDbStorageEngine.ENGINE_NAME
            ) TablesConfiguration tablesCfg
    ) throws Exception {
        TableConfiguration tableCfg = tablesCfg.tables().get("foo");

        assertThat(((RocksDbDataStorageView) tableCfg.dataStorage().value()).dataRegion(), equalTo(DEFAULT_DATA_REGION_NAME));

        engine = new RocksDbStorageEngine(engineConfig, workDir);

        engine.start();

        table = engine.createMvTable(tableCfg, tablesCfg);

        table.start();

        storage = table.getOrCreateMvPartition(PARTITION_ID);
    }

    @AfterEach
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                storage::close,
                table == null ? null : table::stop,
                engine == null ? null : engine::stop
        );
    }

    @Override
    public void addWriteCommittedThrowsIfUncommittedVersionExists() {
        // Disable this test because RocksDbMvPartitionStorage does not throw. It does not throw because this
        // exception is thrown only to ease debugging as the caller must make sure that no write intent exists
        // before calling addWriteCommitted(). For RocksDbMvPartitionStorage, it is not that cheap to check whether
        // there is a write intent in the storage, so we do not require it to throw this optional exception.
    }
}
