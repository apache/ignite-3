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

package org.apache.ignite.internal.storage.index;

import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.schemas.table.UnlimitedBudgetConfigurationSchema;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.storage.impl.TestStorageEngine;
import org.apache.ignite.internal.storage.impl.schema.TestDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.index.impl.TestSortedIndexStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Class for testing the {@link TestSortedIndexStorage}.
 */
@ExtendWith(ConfigurationExtension.class)
public class TestSortedIndexStorageTest extends AbstractSortedIndexStorageTest {
    @BeforeEach
    void setUp(
            @InjectConfiguration(
                    polymorphicExtensions = {
                            TestDataStorageConfigurationSchema.class,
                            UnknownDataStorageConfigurationSchema.class,
                            SortedIndexConfigurationSchema.class,
                            NullValueDefaultConfigurationSchema.class,
                            UnlimitedBudgetConfigurationSchema.class
                    },
                    value = "mock.tables.foo.dataStorage.name = " + TestStorageEngine.ENGINE_NAME
            )
            TablesConfiguration tablesConfig
    ) {
        TableConfiguration tableConfig = tablesConfig.tables().get("foo");

        var storage = new TestMvTableStorage(tableConfig, tablesConfig);

        initialize(storage, tablesConfig);
    }
}
