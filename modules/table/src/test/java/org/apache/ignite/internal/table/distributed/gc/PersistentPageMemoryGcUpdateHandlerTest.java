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

package org.apache.ignite.internal.table.distributed.gc;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.storage.pagememory.configuration.PageMemoryStorageEngineLocalConfigurationModule.DEFAULT_PROFILE_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class PersistentPageMemoryGcUpdateHandlerTest extends AbstractGcUpdateHandlerTest {
    @WorkDirectory
    private Path workDir;

    private PersistentPageMemoryStorageEngine engine;

    private MvTableStorage table;

    @BeforeEach
    void setUp(
            TestInfo testInfo,
            @InjectConfiguration PersistentPageMemoryStorageEngineConfiguration engineConfig,
            @InjectConfiguration("mock.profiles.default = {engine = \"aipersist\"}")
            StorageConfiguration storageConfig
    ) {
        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        String nodeName = testNodeName(testInfo, 0);

        engine = new PersistentPageMemoryStorageEngine(
                nodeName,
                engineConfig,
                storageConfig,
                ioRegistry,
                workDir,
                new LongJvmPauseDetector(nodeName),
                mock(FailureManager.class),
                mock(LogSyncer.class),
                clock
        );

        engine.start();

        table = engine.createMvTable(
                new StorageTableDescriptor(TABLE_ID, DEFAULT_PARTITION_COUNT, DEFAULT_PROFILE_NAME),
                mock(StorageIndexDescriptorSupplier.class)
        );

        initialize(table);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAllManually(
                table,
                engine == null ? null : engine::stop
        );
    }
}
