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

import java.nio.file.Path;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Hash index test implementation for persistent page memory storage.
 */
@ExtendWith({ConfigurationExtension.class, WorkDirectoryExtension.class})
class PersistentPageMemoryHashIndexStorageTest extends AbstractPageMemoryHashIndexStorageTest {
    private PersistentPageMemoryStorageEngine engine;

    private PersistentPageMemoryTableStorage table;

    @BeforeEach
    void setUp(
            @WorkDirectory
            Path workDir,
            @InjectConfiguration
            PersistentPageMemoryStorageEngineConfiguration engineConfig,
            @InjectConfiguration(
                    value = "mock.tables.foo.dataStorage.name = " + PersistentPageMemoryStorageEngine.ENGINE_NAME
            )
            TablesConfiguration tablesConfig,
            @InjectConfiguration
            DistributionZonesConfiguration distributionZonesConfiguration
    ) {
        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new PersistentPageMemoryStorageEngine("test", engineConfig, ioRegistry, workDir, null);

        engine.start();

        table = engine.createMvTable(tablesConfig.tables().get("foo"), tablesConfig, distributionZonesConfiguration.defaultDistributionZone().partitions().value());

        table.start();

        initialize(table, tablesConfig, engineConfig);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                table == null ? null : table::stop,
                engine == null ? null : engine::stop
        );
    }
}
