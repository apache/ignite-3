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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link DataStorageModules} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class DataStorageModulesTest {
    @WorkDirectory
    private Path workDir;

    /**
     * Test extension of {@link DataStorageConfigurationSchema}.
     */
    @PolymorphicConfigInstance(FIRST)
    public static class FirstDataStorageConfigurationSchema extends DataStorageConfigurationSchema {
        static final String FIRST = "first";

        @Value(hasDefault = true)
        public String strVal = "foo";

        @Value(hasDefault = true)
        public int intVal = 100;
    }

    /**
     * Test extension of {@link DataStorageConfigurationSchema}.
     */
    @PolymorphicConfigInstance(SECOND)
    public static class SecondDataStorageConfigurationSchema extends DataStorageConfigurationSchema {
        static final String SECOND = "second";

        @Value(hasDefault = true)
        public String strVal = "bar";

        @Value(hasDefault = true)
        public long longVal = 500L;
    }

    @Test
    void testDuplicateName() {
        String sameName = FIRST;

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> new DataStorageModules(List.of(
                        createMockedStorageEngineFactory(sameName, FirstDataStorageConfigurationSchema.class),
                        createMockedStorageEngineFactory(sameName, SecondDataStorageConfigurationSchema.class)
                ))
        );

        assertThat(exception.getMessage(), Matchers.startsWith("Duplicate name"));
    }

    @Test
    void testInvalidName() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> new DataStorageModules(List.of(
                        createMockedStorageEngineFactory(UNKNOWN_DATA_STORAGE, FirstDataStorageConfigurationSchema.class)
                ))
        );

        assertThat(exception.getMessage(), Matchers.startsWith("Invalid name"));
    }

    @Test
    void testNamesDoNotMatch() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> new DataStorageModules(List.of(
                        createMockedStorageEngineFactory(SECOND, FirstDataStorageConfigurationSchema.class)
                ))
        );

        assertThat(exception.getMessage(), Matchers.startsWith("Names do not match"));
    }

    @Test
    void testCreateDataEngines() {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                createMockedStorageEngineFactory(FIRST, FirstDataStorageConfigurationSchema.class),
                createMockedStorageEngineFactory(SECOND, SecondDataStorageConfigurationSchema.class)
        ));

        Map<String, StorageEngine> engines = dataStorageModules.createStorageEngines(mock(ConfigurationRegistry.class), workDir);

        assertThat(engines, aMapWithSize(2));

        assertTrue(engines.containsKey(FIRST));
        assertTrue(engines.containsKey(SECOND));

        assertNotSame(engines.get(FIRST), engines.get(SECOND));
    }

    @Test
    void testCollectSchemasFields() {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                createMockedStorageEngineFactory(FIRST, FirstDataStorageConfigurationSchema.class),
                createMockedStorageEngineFactory(SECOND, SecondDataStorageConfigurationSchema.class)
        ));

        Map<String, Map<String, Class<?>>> fields = dataStorageModules.collectSchemasFields();

        assertThat(fields, aMapWithSize(2));

        assertThat(fields.get(FIRST), equalTo(Map.of("strVal", String.class, "intVal", int.class)));

        assertThat(fields.get(SECOND), equalTo(Map.of("strVal", String.class, "longVal", long.class)));
    }

    static DataStorageModule createMockedStorageEngineFactory(
            String name,
            Class<? extends DataStorageConfigurationSchema> dataStorageSchema
    ) {
        DataStorageModule mock = mock(DataStorageModule.class);

        when(mock.name()).thenReturn(name);

        when(mock.createEngine(any(), any())).thenReturn(mock(StorageEngine.class));

        when(mock.schema()).then(answer -> dataStorageSchema);

        return mock;
    }
}
