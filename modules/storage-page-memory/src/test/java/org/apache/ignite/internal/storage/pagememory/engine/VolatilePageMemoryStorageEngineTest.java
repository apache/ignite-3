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

import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.engine.AbstractStorageEngineTest;
import org.apache.ignite.internal.storage.engine.AbstractVolatileStorageEngineTest;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.VolatilePageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryProfileView;
import org.junit.jupiter.api.Test;

/**
 * Implementation of the {@link AbstractStorageEngineTest} for the {@link VolatilePageMemoryStorageEngine#ENGINE_NAME} engine.
 */
public class VolatilePageMemoryStorageEngineTest extends AbstractVolatileStorageEngineTest {
    @InjectConfiguration("mock.profiles.default.engine = aimem")
    private StorageConfiguration storageConfig;

    @Override
    protected StorageEngine createEngine() {
        return createEngine(storageConfig);
    }

    private StorageEngine createEngine(StorageConfiguration configuration) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new VolatilePageMemoryStorageEngine(
                "test",
                configuration,
                null,
                ioRegistry,
                mock(FailureManager.class),
                clock
        );
    }

    @Test
    void dataRegionSizeUsedWhenSet(
            @InjectConfiguration("mock.profiles.default {engine = aimem, maxSizeBytes = 12345000, initSizeBytes = 123000}")
            StorageConfiguration storageConfig
    ) {
        StorageEngine anotherEngine = createEngine(storageConfig);

        anotherEngine.start();

        for (StorageProfileView view : storageConfig.profiles().value()) {
            assertThat(((VolatilePageMemoryProfileView) view).initSizeBytes(), is(123000L));
            assertThat(((VolatilePageMemoryProfileView) view).maxSizeBytes(), is(12345000L));
        }

        assertThat(anotherEngine.requiredOffHeapMemorySize(), is(12345000L));
    }
}
