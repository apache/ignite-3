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

package org.apache.ignite.internal.storage.pagememory.engine;

import java.nio.file.Path;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.configurations.StoragesConfiguration;
import org.apache.ignite.internal.storage.engine.AbstractStorageEngineTest;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Implementation of the {@link AbstractStorageEngineTest} for the {@link PersistentPageMemoryStorageEngine#ENGINE_NAME} engine.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
public class PersistentPageMemoryStorageEngineTest extends AbstractStorageEngineTest {
    @InjectConfiguration("mock {checkpoint.checkpointDelayMillis = 0, defaultRegion.size = 1048576}")
    private PersistentPageMemoryProfileStorageEngineConfiguration engineConfiguration;

    @InjectConfiguration
    private StoragesConfiguration storagesConfiguration;

    @WorkDirectory
    private Path workDir;

    @Override
    protected StorageEngine createEngine() {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PersistentPageMemoryStorageEngine(
                "test",
                engineConfiguration,
                storagesConfiguration,
                ioRegistry,
                workDir,
                null
        );
    }
}
