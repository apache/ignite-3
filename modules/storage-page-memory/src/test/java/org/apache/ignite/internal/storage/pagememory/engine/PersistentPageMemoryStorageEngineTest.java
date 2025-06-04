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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.engine.AbstractStorageEngineTest;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Implementation of the {@link AbstractStorageEngineTest} for the {@link PersistentPageMemoryStorageEngine#ENGINE_NAME} engine.
 */
@ExtendWith({WorkDirectoryExtension.class, ExecutorServiceExtension.class})
public class PersistentPageMemoryStorageEngineTest extends AbstractStorageEngineTest {
    @InjectConfiguration("mock.profiles.default.engine = aipersist")
    private StorageConfiguration storageConfig;

    @InjectExecutorService
    ExecutorService executorService;

    @WorkDirectory
    private Path workDir;

    @Override
    protected StorageEngine createEngine() {
        return createEngine(storageConfig);
    }

    private StorageEngine createEngine(StorageConfiguration configuration) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PersistentPageMemoryStorageEngine(
                "test",
                mock(MetricManager.class),
                configuration,
                null,
                ioRegistry,
                workDir,
                null,
                mock(FailureManager.class),
                logSyncer,
                executorService,
                clock
        );
    }

    @Test
    void dataRegionSizeGetsInitialized() {
        for (StorageProfileView view : storageConfig.profiles().value()) {
            assertThat(((PersistentPageMemoryProfileView) view).sizeBytes(), is(StorageEngine.defaultDataRegionSize()));
        }
    }

    @Test
    void dataRegionSizeUsedWhenSet(
            @InjectConfiguration("mock.profiles.default {engine = aipersist, sizeBytes = 12345}")
            StorageConfiguration storageConfig
    ) {
        StorageEngine anotherEngine = createEngine(storageConfig);

        anotherEngine.start();

        for (StorageProfileView view : storageConfig.profiles().value()) {
            assertThat(((PersistentPageMemoryProfileView) view).sizeBytes(), is(12345L));
        }
    }
}
