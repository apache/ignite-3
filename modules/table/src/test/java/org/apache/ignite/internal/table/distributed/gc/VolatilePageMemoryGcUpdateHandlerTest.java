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

package org.apache.ignite.internal.table.distributed.gc;

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.storage.pagememory.configuration.schema.BasePageMemoryStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTrackerNoOp;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class VolatilePageMemoryGcUpdateHandlerTest extends AbstractGcUpdateHandlerTest {
    private VolatilePageMemoryStorageEngine engine;

    private VolatilePageMemoryTableStorage table;

    @BeforeEach
    void setUp(
            @InjectConfiguration VolatilePageMemoryStorageEngineConfiguration engineConfig,
            @InjectConfiguration("mock.tables.foo{}") TablesConfiguration tablesConfig
    ) {
        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new VolatilePageMemoryStorageEngine("test", engineConfig, ioRegistry, PageEvictionTrackerNoOp.INSTANCE);

        engine.start();

        table = engine.createMvTable(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, DEFAULT_DATA_REGION_NAME),
                new StorageIndexDescriptorSupplier(tablesConfig)
        );

        table.start();

        initialize(table);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAllManually(
                table,
                engine == null ? null : engine::stop
        );
    }
}
