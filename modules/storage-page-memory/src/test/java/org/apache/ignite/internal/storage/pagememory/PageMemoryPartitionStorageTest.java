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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Path;
import org.apache.ignite.configuration.schemas.store.DataRegionConfiguration;
import org.apache.ignite.configuration.schemas.store.PageMemoryDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.AbstractPartitionStorageTest;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Storage test implementation for {@link PageMemoryPartitionStorage}.
 */
// TODO: IGNITE-16641 Add test for persistent case.
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
public class PageMemoryPartitionStorageTest extends AbstractPartitionStorageTest {
    private static PageIoRegistry ioRegistry;

    @InjectConfiguration(
            value = "mock.type = pagemem",
            polymorphicExtensions = {
                    PageMemoryDataRegionConfigurationSchema.class,
                    UnsafeMemoryAllocatorConfigurationSchema.class
            })
    private DataRegionConfiguration dataRegionCfg;

    @InjectConfiguration(
            value = "mock.name = default",
            polymorphicExtensions = HashIndexConfigurationSchema.class
    )
    private TableConfiguration tableCfg;

    @WorkDirectory
    private Path workDir;

    private StorageEngine engine;

    private TableStorage table;

    private DataRegion dataRegion;

    @BeforeAll
    static void beforeAll() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    @BeforeEach
    void setUp() {
        engine = new PageMemoryStorageEngine(ioRegistry);

        engine.start();

        dataRegion = engine.createDataRegion(fixConfiguration(dataRegionCfg));

        assertThat(dataRegion, is(instanceOf(PageMemoryDataRegion.class)));

        dataRegion.start();

        table = engine.createTable(workDir, tableCfg, dataRegion);

        assertThat(table, is(instanceOf(PageMemoryTableStorage.class)));

        table.start();

        storage = table.getOrCreatePartition(0);

        assertThat(storage, is(instanceOf(PageMemoryPartitionStorage.class)));
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                storage,
                table == null ? null : table::stop,
                dataRegion == null ? null : dataRegion::stop,
                engine == null ? null : engine::stop
        );
    }

    @AfterAll
    static void afterAll() {
        ioRegistry = null;
    }

    /** {@inheritDoc} */
    @Test
    @Override
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-16644")
    public void testSnapshot(@WorkDirectory Path workDir) throws Exception {
        super.testSnapshot(workDir);
    }
}
