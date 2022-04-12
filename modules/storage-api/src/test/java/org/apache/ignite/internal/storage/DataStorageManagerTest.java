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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.configuration.schemas.store.DataStorageChange;
import org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TablesConfigurationSchema;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageEngineFactory;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * For {@link DataStorageManager} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class DataStorageManagerTest {
    @WorkDirectory
    private Path workDir;

    @Test
    void testStorageEngineDuplicate() {
        String sameName = UUID.randomUUID().toString();

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> new DataStorageManager(
                        mock(ConfigurationRegistry.class),
                        workDir,
                        List.of(
                                createMockedStorageEngineFactory(sameName),
                                createMockedStorageEngineFactory(sameName)
                        )
                ));

        assertThat(exception.getMessage(), Matchers.startsWith("Duplicate key"));
    }

    @Test
    void testDefaultTableDataStorageConsumerSingleStorageEngine() {
        String engineName = UUID.randomUUID().toString();

        DataStorageManager dataStorageManager = new DataStorageManager(
                mock(ConfigurationRegistry.class),
                workDir,
                List.of(createMockedStorageEngineFactory(engineName))
        );

        checkDefaultTableDataStorageConsumer(dataStorageManager, engineName, engineName);

        checkDefaultTableDataStorageConsumer(dataStorageManager, UNKNOWN_DATA_STORAGE, engineName);
    }

    @Test
    void testDefaultTableDataStorageConsumerMultipleStorageEngines() {
        String engineName1 = UUID.randomUUID().toString();
        String engineName2 = UUID.randomUUID().toString();

        DataStorageManager dataStorageManager = new DataStorageManager(
                mock(ConfigurationRegistry.class),
                workDir,
                List.of(
                        createMockedStorageEngineFactory(engineName1),
                        createMockedStorageEngineFactory(engineName2)
                )
        );

        checkDefaultTableDataStorageConsumer(dataStorageManager, engineName1, engineName1);

        checkDefaultTableDataStorageConsumer(dataStorageManager, engineName2, engineName2);

        checkDefaultTableDataStorageConsumer(dataStorageManager, UNKNOWN_DATA_STORAGE, null);
    }

    /**
     * Checks that the consumer from {@link DataStorageManager#defaultTableDataStorageConsumer} will correctly set the {@link
     * DataStorageConfigurationSchema data storage} type via {@link DataStorageChange#convert(String)}.
     *
     * @param dataStorageManager Data storage manager.
     * @param defaultDataStorageView One of the options for the value of the {@link TablesConfigurationSchema#defaultDataStorage}.
     * @param expPolymorphicTypeId Expected value that should get into the {@link DataStorageChange#convert(String)}. {@code null} if
     *      nothing should go into the method.
     */
    private static void checkDefaultTableDataStorageConsumer(
            DataStorageManager dataStorageManager,
            String defaultDataStorageView,
            @Nullable String expPolymorphicTypeId
    ) {
        DataStorageChange mock = mock(DataStorageChange.class);

        ArgumentCaptor<String> stringArgumentCaptor = ArgumentCaptor.forClass(String.class);

        when(mock.convert(stringArgumentCaptor.capture())).thenReturn(mock);

        dataStorageManager.defaultTableDataStorageConsumer(defaultDataStorageView).accept(mock);

        if (expPolymorphicTypeId != null) {
            assertThat(stringArgumentCaptor.getAllValues(), hasSize(1));

            assertThat(stringArgumentCaptor.getValue(), equalTo(expPolymorphicTypeId));
        } else {
            assertThat(stringArgumentCaptor.getAllValues(), empty());
        }
    }

    private static StorageEngineFactory createMockedStorageEngineFactory(String name) {
        StorageEngineFactory mock = mock(StorageEngineFactory.class);

        when(mock.name()).thenReturn(name);

        when(mock.createEngine(any(), any())).thenReturn(mock(StorageEngine.class));

        return mock;
    }
}
