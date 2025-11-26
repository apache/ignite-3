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

package org.apache.ignite.internal.storage.pagememory.index;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Sorted index test implementation for persistent page memory storage.
 */
@ExtendWith({ConfigurationExtension.class, WorkDirectoryExtension.class, ExecutorServiceExtension.class})
class PersistentPageMemorySortedIndexStorageTest extends AbstractPageMemorySortedIndexStorageTest {
    private PersistentPageMemoryStorageEngine engine;

    @InjectExecutorService
    ExecutorService executorService;

    @BeforeEach
    void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration("mock.profiles.default = {engine = aipersist}")
            StorageConfiguration storageConfig
    ) {
        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new PersistentPageMemoryStorageEngine(
                "test",
                mock(MetricManager.class),
                storageConfig,
                null,
                ioRegistry,
                workDir,
                null,
                mock(FailureManager.class),
                mock(LogSyncer.class),
                executorService,
                clock
        );

        engine.start();

        tableStorage = engine.createMvTable(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, DEFAULT_STORAGE_PROFILE),
                mock(StorageIndexDescriptorSupplier.class)
        );

        initialize(tableStorage, engine.configuration().pageSizeBytes().value());
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(
                tableStorage == null ? null : tableStorage::close,
                engine == null ? null : engine::stop
        );
    }
}
