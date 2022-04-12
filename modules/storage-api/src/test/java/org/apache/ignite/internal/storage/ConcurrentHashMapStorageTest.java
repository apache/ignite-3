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

package org.apache.ignite.internal.storage;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.chm.TestConcurrentHashMapPartitionStorage;
import org.apache.ignite.internal.storage.chm.TestConcurrentHashMapStorageEngine;
import org.apache.ignite.internal.storage.chm.schema.TestConcurrentHashMapDataStorageChange;
import org.apache.ignite.internal.storage.chm.schema.TestConcurrentHashMapDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Storage test implementation for {@link TestConcurrentHashMapPartitionStorage}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ConcurrentHashMapStorageTest extends AbstractPartitionStorageTest {
    private StorageEngine engine;

    private TableStorage table;

    @BeforeEach
    public void setUp(
            @InjectConfiguration(
                    polymorphicExtensions = {
                            HashIndexConfigurationSchema.class,
                            UnknownDataStorageConfigurationSchema.class,
                            TestConcurrentHashMapDataStorageConfigurationSchema.class
                    }
            ) TableConfiguration tableCfg
    ) throws Exception {
        engine = new TestConcurrentHashMapStorageEngine();

        engine.start();

        tableCfg.dataStorage().change(c -> c.convert(TestConcurrentHashMapDataStorageChange.class)).get(1, TimeUnit.SECONDS);

        table = engine.createTable(tableCfg);

        table.start();

        storage = new TestConcurrentHashMapPartitionStorage(0);
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
