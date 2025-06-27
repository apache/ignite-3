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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.ENGINE_NAME;

import com.google.auto.service.AutoService;
import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalExtensionConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation for creating {@link PersistentPageMemoryStorageEngine}.
 */
@AutoService(DataStorageModule.class)
public class PersistentPageMemoryDataStorageModule implements DataStorageModule {
    @Override
    public String name() {
        return ENGINE_NAME;
    }

    @Override
    public StorageEngine createEngine(
            String igniteInstanceName,
            MetricManager metricManager,
            ConfigurationRegistry configRegistry,
            Path storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            FailureManager failureManager,
            LogSyncer logSyncer,
            HybridClock clock,
            ScheduledExecutorService commonScheduler
    ) throws StorageException {
        StorageConfiguration storageConfig = configRegistry.getConfiguration(StorageExtensionConfiguration.KEY).storage();

        SystemLocalConfiguration systemLocalConfig = configRegistry.getConfiguration(SystemLocalExtensionConfiguration.KEY).system();

        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PersistentPageMemoryStorageEngine(
                igniteInstanceName,
                metricManager,
                storageConfig,
                systemLocalConfig,
                ioRegistry,
                storagePath,
                longJvmPauseDetector,
                failureManager,
                logSyncer,
                // TODO https://issues.apache.org/jira/browse/IGNITE-25563 Don't use common scheduler for throttling logs.
                commonScheduler,
                clock
        );
    }
}
