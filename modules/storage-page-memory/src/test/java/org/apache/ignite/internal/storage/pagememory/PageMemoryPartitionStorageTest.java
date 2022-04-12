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

import static java.util.stream.Collectors.joining;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.AbstractPartitionStorageTest;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryDataStorageChange;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryDataStorageView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PageMemoryStorageEngineConfigurationSchema;
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

    @InjectConfiguration(polymorphicExtensions = UnsafeMemoryAllocatorConfigurationSchema.class)
    PageMemoryStorageEngineConfiguration engineConfig;

    @InjectConfiguration(
            name = "table",
            polymorphicExtensions = {
                    HashIndexConfigurationSchema.class,
                    UnknownDataStorageConfigurationSchema.class,
                    PageMemoryDataStorageConfigurationSchema.class
            }
    )
    private TableConfiguration tableCfg;

    private StorageEngine engine;

    private TableStorage table;

    @BeforeAll
    static void beforeAll() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    @BeforeEach
    void setUp() throws Exception {
        engine = new PageMemoryStorageEngine(engineConfig, ioRegistry);

        engine.start();

        tableCfg.change(c -> c.changeDataStorage(dsc -> dsc.convert(PageMemoryDataStorageChange.class)))
                .get(1, TimeUnit.SECONDS);

        assertEquals(
                PageMemoryStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME,
                ((PageMemoryDataStorageView) tableCfg.dataStorage().value()).dataRegion()
        );

        table = engine.createTable(tableCfg);

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

    /**
     * Checks that fragments are written and read correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    void testFragments() {
        int pageSize = engineConfig.defaultRegion().value().pageSize();

        DataRow dataRow = dataRow(createRandomString(pageSize), createRandomString(pageSize));

        storage.write(dataRow);

        DataRow read = storage.read(dataRow);

        assertArrayEquals(dataRow.valueBytes(), read.valueBytes());
    }

    private String createRandomString(int len) {
        return ThreadLocalRandom.current().ints(len).mapToObj(i -> String.valueOf(Math.abs(i % 10))).collect(joining(""));
    }
}
