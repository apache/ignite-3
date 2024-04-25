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

package org.apache.ignite.internal.storage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link DataStorageModules} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class DataStorageModulesTest extends BaseIgniteAbstractTest {
    @WorkDirectory
    private Path workDir;

    private static final String FIRST = "first";

    private static final String SECOND = "second";

    @Test
    void testDuplicateName() {
        String sameName = FIRST;

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> new DataStorageModules(List.of(
                        createMockedDataStorageModule(sameName),
                        createMockedDataStorageModule(sameName)
                ))
        );

        assertThat(exception.getMessage(), startsWith("Duplicate name"));
    }

    @Test
    void testCreateDataEngines() {
        DataStorageModules dataStorageModules = new DataStorageModules(List.of(
                createMockedDataStorageModule(FIRST),
                createMockedDataStorageModule(SECOND)
        ));

        Map<String, StorageEngine> engines = dataStorageModules.createStorageEngines(
                "test",
                mock(ConfigurationRegistry.class),
                workDir,
                null,
                mock(FailureProcessor.class),
                mock(LogSyncer.class),
                mock(CatalogIndexStatusSupplier.class)
        );

        assertThat(engines, aMapWithSize(2));

        assertTrue(engines.containsKey(FIRST));
        assertTrue(engines.containsKey(SECOND));

        assertNotSame(engines.get(FIRST), engines.get(SECOND));
    }

    static DataStorageModule createMockedDataStorageModule(String name) {
        DataStorageModule mock = mock(DataStorageModule.class);

        when(mock.name()).thenReturn(name);

        when(mock.createEngine(any(), any(), any(), any(), any(), any(), any())).thenReturn(mock(StorageEngine.class));

        return mock;
    }
}
