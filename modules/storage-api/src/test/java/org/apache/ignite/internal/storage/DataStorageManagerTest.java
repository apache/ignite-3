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

import static org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema.UNKNOWN_DATA_STORAGE;
import static org.apache.ignite.internal.storage.DataStorageModulesTest.FirstDataStorageConfigurationSchema.FIRST;
import static org.apache.ignite.internal.storage.DataStorageModulesTest.SecondDataStorageConfigurationSchema.SECOND;
import static org.apache.ignite.internal.storage.DataStorageModulesTest.createMockedDataStorageModule;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.ConfigurationWrongPolymorphicTypeIdException;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageView;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageChange;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageView;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.storage.DataStorageModulesTest.FirstDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.DataStorageModulesTest.SecondDataStorageConfigurationSchema;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link DataStorageManager} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class DataStorageManagerTest {
    @WorkDirectory
    private Path workDir;

    @InjectConfiguration(
            polymorphicExtensions = {
                    UnknownDataStorageConfigurationSchema.class,
                    FirstDataStorageConfigurationSchema.class,
                    SecondDataStorageConfigurationSchema.class
            }
    )
    private DataStorageConfiguration dataStorageConfig;

    @InjectConfiguration(polymorphicExtensions = {
            HashIndexConfigurationSchema.class,
            UnknownDataStorageConfigurationSchema.class,
            FirstDataStorageConfigurationSchema.class,
            SecondDataStorageConfigurationSchema.class
    })
    private TablesConfiguration tablesConfig;

    @Test
    void testDefaultDataStorageSingleStorage() {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(createMockedDataStorageModule(FIRST)));

        DataStorageManager dataStorageManager = new DataStorageManager(
                tablesConfig,
                dataStorageModules.createStorageEngines(mock(ConfigurationRegistry.class), workDir)
        );

        // Checks that the current default is "pagememory" even if we have one engine and it's not "pagememory".
        assertThat("pagememory", equalTo(dataStorageManager.defaultDataStorage()));
    }

    @Test
    void testDefaultDataStorageMultipleStorages() throws Exception {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                createMockedDataStorageModule(FIRST),
                createMockedDataStorageModule(SECOND)
        ));

        DataStorageManager dataStorageManager = new DataStorageManager(
                tablesConfig,
                dataStorageModules.createStorageEngines(mock(ConfigurationRegistry.class), workDir)
        );

        assertThat("pagememory", equalTo(dataStorageManager.defaultDataStorage()));

        tablesConfig.defaultDataStorage().update(FIRST).get(1, TimeUnit.SECONDS);
        assertThat(FIRST, equalTo(dataStorageManager.defaultDataStorage()));

        tablesConfig.defaultDataStorage().update(SECOND).get(1, TimeUnit.SECONDS);
        assertThat(SECOND, equalTo(dataStorageManager.defaultDataStorage()));

        tablesConfig.defaultDataStorage().update(UNKNOWN_DATA_STORAGE).get(1, TimeUnit.SECONDS);
        assertThat(UNKNOWN_DATA_STORAGE, equalTo(dataStorageManager.defaultDataStorage()));
    }

    @Test
    void testTableDataStorageConsumerError() {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                createMockedDataStorageModule(FIRST),
                createMockedDataStorageModule(SECOND)
        ));

        DataStorageManager dataStorageManager = new DataStorageManager(
                tablesConfig,
                dataStorageModules.createStorageEngines(mock(ConfigurationRegistry.class), workDir)
        );

        // Check random polymorphicTypeId.
        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> dataStorageConfig
                        .change(dataStorageManager.tableDataStorageConsumer(UUID.randomUUID().toString(), Map.of()))
                        .get(1, TimeUnit.SECONDS)
        );

        assertThat(exception.getCause(), instanceOf(ConfigurationWrongPolymorphicTypeIdException.class));

        // Check random field name.
        exception = assertThrows(
                ExecutionException.class,
                () -> dataStorageConfig
                        .change(dataStorageManager.tableDataStorageConsumer(FIRST, Map.of(UUID.randomUUID().toString(), 1)))
                        .get(1, TimeUnit.SECONDS)
        );

        assertThat(exception.getCause(), instanceOf(NoSuchElementException.class));

        // Check other field type.
        exception = assertThrows(
                ExecutionException.class,
                () -> dataStorageConfig
                        .change(dataStorageManager.tableDataStorageConsumer(FIRST, Map.of("strVal", 1)))
                        .get(1, TimeUnit.SECONDS)
        );

        assertThat(exception.getCause(), instanceOf(ClassCastException.class));
    }

    @Test
    void testTableDataStorageConsumerSuccess() throws Exception {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                createMockedDataStorageModule(FIRST),
                createMockedDataStorageModule(SECOND)
        ));

        DataStorageManager dataStorageManager = new DataStorageManager(
                tablesConfig,
                dataStorageModules.createStorageEngines(mock(ConfigurationRegistry.class), workDir)
        );

        DataStorageView dataStorageView = dataStorageConfig.value();

        // Just change type and check defaults.
        dataStorageConfig
                .change(dataStorageManager.tableDataStorageConsumer(FIRST, Map.of()))
                .get(1, TimeUnit.SECONDS);

        dataStorageView = dataStorageConfig.value();

        assertThat(dataStorageView, instanceOf(FirstDataStorageView.class));

        assertThat(((FirstDataStorageView) dataStorageView).strVal(), equalTo("foo"));
        assertThat(((FirstDataStorageView) dataStorageView).intVal(), equalTo(100));

        // Change type and check values.
        dataStorageConfig
                .change(dataStorageManager.tableDataStorageConsumer(SECOND, Map.of("strVal", "foobar", "longVal", 666L)))
                .get(1, TimeUnit.SECONDS);

        dataStorageView = dataStorageConfig.value();

        assertThat(dataStorageView, instanceOf(SecondDataStorageView.class));

        assertThat(((SecondDataStorageView) dataStorageView).strVal(), equalTo("foobar"));
        assertThat(((SecondDataStorageView) dataStorageView).longVal(), equalTo(666L));
    }

    @Test
    void testDefaultTableDataStorageConsumerSingleEngine() throws Exception {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(createMockedDataStorageModule(FIRST)));

        DataStorageManager dataStorageManager = new DataStorageManager(
                tablesConfig,
                dataStorageModules.createStorageEngines(mock(ConfigurationRegistry.class), workDir)
        );

        dataStorageConfig.change(dataStorageManager.defaultTableDataStorageConsumer(FIRST)).get(1, TimeUnit.SECONDS);
        assertThat(dataStorageConfig.value(), instanceOf(FirstDataStorageView.class));

        dataStorageConfig.change(c -> c.convert(UnknownDataStorageChange.class)).get(1, TimeUnit.SECONDS);

        dataStorageConfig.change(dataStorageManager.defaultTableDataStorageConsumer(UNKNOWN_DATA_STORAGE)).get(1, TimeUnit.SECONDS);
        assertThat(dataStorageConfig.value(), instanceOf(FirstDataStorageView.class));
    }

    @Test
    void testDefaultTableDataStorageConsumerMultipleEngines() throws Exception {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                createMockedDataStorageModule(FIRST),
                createMockedDataStorageModule(SECOND)
        ));

        DataStorageManager dataStorageManager = new DataStorageManager(
                tablesConfig,
                dataStorageModules.createStorageEngines(mock(ConfigurationRegistry.class), workDir)
        );

        dataStorageConfig.change(dataStorageManager.defaultTableDataStorageConsumer(FIRST)).get(1, TimeUnit.SECONDS);
        assertThat(dataStorageConfig.value(), instanceOf(FirstDataStorageView.class));

        dataStorageConfig.change(dataStorageManager.defaultTableDataStorageConsumer(SECOND)).get(1, TimeUnit.SECONDS);
        assertThat(dataStorageConfig.value(), instanceOf(SecondDataStorageView.class));

        dataStorageConfig.change(c -> c.convert(UnknownDataStorageChange.class)).get(1, TimeUnit.SECONDS);

        dataStorageConfig.change(dataStorageManager.defaultTableDataStorageConsumer(UNKNOWN_DATA_STORAGE)).get(1, TimeUnit.SECONDS);
        assertThat(dataStorageConfig.value(), instanceOf(UnknownDataStorageView.class));
    }
}
