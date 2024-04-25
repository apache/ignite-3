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

import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.AbstractStorageEngineTest;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Implementation of the {@link AbstractStorageEngineTest} for the {@link PersistentPageMemoryStorageEngine#ENGINE_NAME} engine.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class PersistentPageMemoryStorageEngineTest extends AbstractStorageEngineTest {
    @InjectConfiguration("mock {checkpoint.checkpointDelayMillis = 0}")
    private PersistentPageMemoryStorageEngineConfiguration engineConfiguration;

    @InjectConfiguration("mock.profiles.default = {engine = \"aipersist\", size = 1048576}")
    private StorageConfiguration storageConfiguration;

    @WorkDirectory
    private Path workDir;

    @Override
    protected StorageEngine createEngine() {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PersistentPageMemoryStorageEngine(
                "test",
                engineConfiguration,
                storageConfiguration,
                ioRegistry,
                workDir,
                null,
                mock(FailureProcessor.class),
                logSyncer,
                mock(CatalogIndexStatusSupplier.class)
        );
    }
}
