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

package org.apache.ignite.internal.storage.rocksdb.engine;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.engine.AbstractPersistentStorageEngineTest;
import org.apache.ignite.internal.storage.engine.AbstractStorageEngineTest;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorageEngine;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileView;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Implementation of the {@link AbstractStorageEngineTest} for the {@link RocksDbStorageEngine#ENGINE_NAME} engine.
 */
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
public class RocksDbStorageEngineTest extends AbstractPersistentStorageEngineTest {
    @InjectConfiguration("mock.profiles.default.engine = rocksdb")
    StorageConfiguration storageConfiguration;

    @WorkDirectory
    private Path workDir;

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutor;

    @Override
    protected StorageEngine createEngine() {
        return createEngine(workDir, storageConfiguration);
    }

    private StorageEngine createEngine(Path path, StorageConfiguration configuration) {
        return new RocksDbStorageEngine(
                "test",
                configuration,
                path,
                logSyncer,
                scheduledExecutor,
                mock(FailureProcessor.class)
        );
    }

    @Override
    protected void persistTableDestructionIfNeeded() {
        assertThat(((RocksDbStorageEngine) storageEngine).flush(), willCompleteSuccessfully());
    }

    @Test
    void dataRegionSizeUsedWhenSet(
            @InjectConfiguration("mock.profiles.default {engine = rocksdb, sizeBytes = 12345}")
            StorageConfiguration storageConfig
    ) {
        StorageEngine anotherEngine = createEngine(workDir.resolve("dataRegionSizeUsedWhenSet"), storageConfig);

        anotherEngine.start();

        for (StorageProfileView view : storageConfig.profiles().value()) {
            assertThat(((RocksDbProfileView) view).sizeBytes(), is(12345L));
        }

        assertThat(anotherEngine.requiredOffHeapMemorySize(), is(12345L));
    }
}
