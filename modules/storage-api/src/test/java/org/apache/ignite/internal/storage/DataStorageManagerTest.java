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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

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

        StorageException exception = assertThrows(
                StorageException.class,
                () -> new DataStorageManager(
                        Mockito.mock(ConfigurationRegistry.class),
                        workDir,
                        List.of(
                                (configRegistry, storagePath) -> createMockedStorageEngine(sameName),
                                (configRegistry, storagePath) -> createMockedStorageEngine(sameName)
                        )
                ));

        assertThat(exception.getMessage(), Matchers.startsWith("Duplicate key"));
    }

    private StorageEngine createMockedStorageEngine(String name) {
        StorageEngine mocked = Mockito.mock(StorageEngine.class);

        Mockito.when(mocked.name()).thenReturn(name);

        return mocked;
    }
}
