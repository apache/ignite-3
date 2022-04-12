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
import static org.apache.ignite.internal.storage.DataStorageModulesTest.createMockedStorageEngineFactory;
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
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageView;
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

    @Test
    void testDataStorageSingleStorage() {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                createMockedStorageEngineFactory(FIRST, FirstDataStorageConfigurationSchema.class)
        ));

        DataStorageManager dataStorageManager = new DataStorageManager(
                dataStorageModules.createStorageEngines(mock(ConfigurationRegistry.class), workDir)
        );

        assertThat(SECOND, equalTo(dataStorageManager.dataStorage(SECOND)));

        assertThat(FIRST, equalTo(dataStorageManager.dataStorage(UNKNOWN_DATA_STORAGE)));
    }

    @Test
    void testDataStorageMultipleStorages() {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                createMockedStorageEngineFactory(FIRST, FirstDataStorageConfigurationSchema.class),
                createMockedStorageEngineFactory(SECOND, SecondDataStorageConfigurationSchema.class)
        ));

        DataStorageManager dataStorageManager = new DataStorageManager(
                dataStorageModules.createStorageEngines(mock(ConfigurationRegistry.class), workDir)
        );

        assertThat(FIRST, equalTo(dataStorageManager.dataStorage(FIRST)));

        assertThat(SECOND, equalTo(dataStorageManager.dataStorage(SECOND)));

        assertThat(UNKNOWN_DATA_STORAGE, equalTo(dataStorageManager.dataStorage(UNKNOWN_DATA_STORAGE)));
    }

    @Test
    void testTableDataStorageConsumerError() {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                createMockedStorageEngineFactory(FIRST, FirstDataStorageConfigurationSchema.class),
                createMockedStorageEngineFactory(SECOND, SecondDataStorageConfigurationSchema.class)
        ));

        DataStorageManager dataStorageManager = new DataStorageManager(
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
                createMockedStorageEngineFactory(FIRST, FirstDataStorageConfigurationSchema.class),
                createMockedStorageEngineFactory(SECOND, SecondDataStorageConfigurationSchema.class)
        ));

        DataStorageManager dataStorageManager = new DataStorageManager(
                dataStorageModules.createStorageEngines(mock(ConfigurationRegistry.class), workDir)
        );

        DataStorageView dataStorageView = dataStorageConfig.value();

        assertThat(dataStorageView, instanceOf(UnknownDataStorageView.class));

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
}
