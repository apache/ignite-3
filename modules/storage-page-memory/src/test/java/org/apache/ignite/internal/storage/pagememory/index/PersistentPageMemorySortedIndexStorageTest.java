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

package org.apache.ignite.internal.storage.pagememory.index;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.storage.pagememory.configuration.schema.BasePageMemoryStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME;

import java.nio.file.Path;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Sorted index test implementation for persistent page memory storage.
 */
@ExtendWith({ConfigurationExtension.class, WorkDirectoryExtension.class})
class PersistentPageMemorySortedIndexStorageTest extends AbstractPageMemorySortedIndexStorageTest {
    private PersistentPageMemoryStorageEngine engine;

    @BeforeEach
    void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration
            PersistentPageMemoryProfileStorageEngineConfiguration engineConfig,
            @InjectConfiguration("mock.profiles.default = {engine = \"aipersist\"}")
            StorageConfiguration storageConfiguration
    ) {
        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new PersistentPageMemoryStorageEngine("test", engineConfig, storageConfiguration, ioRegistry, workDir, null);

        engine.start();

        tableStorage = engine.createMvTable(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, DEFAULT_DATA_REGION_NAME),
                new StorageIndexDescriptorSupplier(catalogService)
        );

        tableStorage.start();

        initialize(tableStorage, engineConfig.pageSize().value());
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                tableStorage == null ? null : tableStorage::stop,
                engine == null ? null : engine::stop
        );
    }
}
