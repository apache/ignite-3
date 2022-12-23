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

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link VolatilePageMemoryTableStorage}.
 */
@ExtendWith(ConfigurationExtension.class)
public class VolatilePageMemoryMvTableStorageTest extends AbstractMvTableStorageTest {
    private VolatilePageMemoryStorageEngine engine;

    private MvTableStorage tableStorage;

    @BeforeEach
    void setUp(
            @InjectConfiguration
            VolatilePageMemoryStorageEngineConfiguration engineConfig,
            @InjectConfiguration(
                    value = "mock.tables.foo{ partitions = 512, dataStorage.name = " + VolatilePageMemoryStorageEngine.ENGINE_NAME + "}"
            )
            TablesConfiguration tablesConfig
    ) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new VolatilePageMemoryStorageEngine(engineConfig, ioRegistry);

        engine.start();

        tableStorage = engine.createMvTable(tablesConfig.tables().get("foo"), tablesConfig);

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

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17833")
    @Override
    public void testDestroyPartition() throws Exception {
        super.testDestroyPartition();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17833")
    @Override
    public void testReCreatePartition() throws Exception {
        super.testReCreatePartition();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18028")
    @Override
    public void testSuccessFullRebalance() throws Exception {
        super.testSuccessFullRebalance();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18028")
    @Override
    public void testFailFullRebalance() throws Exception {
        super.testFailFullRebalance();
    }
}
