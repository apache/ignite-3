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

import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.jetbrains.annotations.Nullable;

/**
 * Data storage module.
 */
public interface DataStorageModule {
    /**
     * Returns the unique name of the data storage.
     *
     * <p>Used to map {@link StorageProfileConfiguration#engine()} to {@link StorageEngine}.
     */
    String name();

    /**
     * Creates a new storage engine.
     *
     * @param igniteInstanceName String igniteInstanceName
     * @param metricManager Metric manager.
     * @param configRegistry Configuration register.
     * @param storagePath Storage path.
     * @param longJvmPauseDetector Long JVM pause detector.
     * @param failureManager Failure processor that is used to handle critical errors.
     * @param logSyncer Write-ahead log synchronizer.
     * @param clock Hybrid Logical Clock.
     * @param commonScheduler Common scheduled thread pool. Needed only for asynchronous start of scheduled operations without
     *         performing blocking, long or IO operations.
     * @throws StorageException If there is an error when creating the storage engine.
     */
    StorageEngine createEngine(
            String igniteInstanceName,
            MetricManager metricManager,
            ConfigurationRegistry configRegistry,
            Path storagePath,
            @Nullable LongJvmPauseDetector longJvmPauseDetector,
            FailureManager failureManager,
            LogSyncer logSyncer,
            HybridClock clock,
            ScheduledExecutorService commonScheduler
    ) throws StorageException;
}
