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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageChange;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfigurationSchema;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

class VolatilePageMemoryMvPartitionStorageTest extends AbstractPageMemoryMvPartitionStorageTest {
    @InjectConfiguration(polymorphicExtensions = UnsafeMemoryAllocatorConfigurationSchema.class)
    private VolatilePageMemoryStorageEngineConfiguration engineConfig;

    @InjectConfiguration(
            name = "table",
            polymorphicExtensions = {
                    HashIndexConfigurationSchema.class,
                    UnknownDataStorageConfigurationSchema.class,
                    VolatilePageMemoryDataStorageConfigurationSchema.class,
                    ConstantValueDefaultConfigurationSchema.class,
                    FunctionCallDefaultConfigurationSchema.class,
                    NullValueDefaultConfigurationSchema.class,
            }
    )
    private TableConfiguration tableCfg;

    private VolatilePageMemoryStorageEngine engine;

    private VolatilePageMemoryTableStorage table;

    @WorkDirectory
    private Path workDir;

    @BeforeEach
    void setUp() throws Exception {
        engine = new VolatilePageMemoryStorageEngine(engineConfig, ioRegistry);

        engine.start();

        tableCfg
                .change(c -> c.changeDataStorage(dsc -> dsc.convert(VolatilePageMemoryDataStorageChange.class)))
                .get(1, TimeUnit.SECONDS);

        assertEquals(
                VolatilePageMemoryStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME,
                ((VolatilePageMemoryDataStorageView) tableCfg.dataStorage().value()).dataRegion()
        );

        table = engine.createMvTable(tableCfg);
        table.start();

        storage = table.createMvPartitionStorage(PARTITION_ID);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                storage,
                table == null ? null : table::stop,
                engine == null ? null : engine::stop
        );
    }

    /** {@inheritDoc} */
    @Override
    int pageSize() {
        return engineConfig.pageSize().value();
    }
}
