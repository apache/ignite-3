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

package org.apache.ignite.internal.app.di;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.di.IgniteStartupPhase;
import org.apache.ignite.internal.di.StartupPhase;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.VaultService;

/**
 * Micronaut factory for leaf Ignite components that have few or no dependencies on other components.
 */
@Factory
public class CoreComponentsFactory {
    /** Creates the JVM pause detector. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    public LongJvmPauseDetector longJvmPauseDetector(NodeSeedParams seedParams) {
        return new LongJvmPauseDetector(seedParams.nodeName());
    }

    /** Creates the vault manager backed by persistent storage. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    public VaultManager vaultManager(VaultService vaultService) {
        return new VaultManager(vaultService);
    }

    /**
     * Creates the metric manager.
     *
     * @param seedParams Node seed parameters.
     * @param clusterIdSupplier Supplier of the cluster ID; resolved lazily after cluster initialization.
     */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    public MetricManager metricManager(
            NodeSeedParams seedParams,
            @Named("clusterIdSupplier") Supplier<UUID> clusterIdSupplier
    ) {
        return new MetricManagerImpl(seedParams.nodeName(), clusterIdSupplier);
    }

    /** Creates the thread pools manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    public ThreadPoolsManager threadPoolsManager(NodeSeedParams seedParams, MetricManager metricManager) {
        return new ThreadPoolsManager(seedParams.nodeName(), metricManager);
    }

    /** Creates the hybrid logical clock. */
    @Singleton
    public HybridClock hybridClock(FailureProcessor failureProcessor) {
        return new HybridClockImpl(failureProcessor);
    }
}
