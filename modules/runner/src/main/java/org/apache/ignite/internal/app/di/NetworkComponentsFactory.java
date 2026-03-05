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
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalExtensionConfiguration;
import org.apache.ignite.internal.di.IgniteStartupPhase;
import org.apache.ignite.internal.di.StartupPhase;
import org.apache.ignite.internal.disaster.system.ClusterIdService;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;
import org.apache.ignite.internal.failure.configuration.FailureProcessorExtensionConfiguration;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.NettyWorkersRegistrar;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfiguration;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;
import org.apache.ignite.internal.worker.CriticalWorkerWatchdog;

/**
 * Micronaut factory for network and infrastructure components.
 */
@Factory
public class NetworkComponentsFactory {
    /** Creates the network configuration from the node config registry. */
    @Singleton
    public NetworkConfiguration networkConfiguration(@Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry) {
        return nodeConfigRegistry.getConfiguration(NetworkExtensionConfiguration.KEY).network();
    }

    /** Creates the failure processor configuration from the node config registry. */
    @Singleton
    public FailureProcessorConfiguration failureProcessorConfiguration(
            @Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry
    ) {
        return nodeConfigRegistry.getConfiguration(FailureProcessorExtensionConfiguration.KEY).failureHandler();
    }

    /** Creates the system local configuration from the node config registry. */
    @Singleton
    public SystemLocalConfiguration systemLocalConfiguration(@Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry) {
        return nodeConfigRegistry.getConfiguration(SystemLocalExtensionConfiguration.KEY).system();
    }

    /** Creates the failure manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    public FailureManager failureManager(
            NodeSeedParams seedParams,
            FailureProcessorConfiguration failureProcessorConfiguration
    ) {
        return new FailureManager(seedParams.nodeName(), seedParams.node()::shutdown, failureProcessorConfiguration);
    }

    /** Creates the critical worker watchdog. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    public CriticalWorkerWatchdog criticalWorkerWatchdog(
            SystemLocalConfiguration systemConfiguration,
            ThreadPoolsManager threadPoolsManager,
            FailureManager failureManager
    ) {
        return new CriticalWorkerWatchdog(
                systemConfiguration.criticalWorkers(),
                threadPoolsManager.commonScheduler(),
                failureManager
        );
    }

    /** Creates the netty bootstrap factory. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    public NettyBootstrapFactory nettyBootstrapFactory(NodeSeedParams seedParams, NetworkConfiguration networkConfiguration) {
        return new NettyBootstrapFactory(networkConfiguration, seedParams.nodeName());
    }

    /** Creates the netty workers registrar. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    public NettyWorkersRegistrar nettyWorkersRegistrar(
            CriticalWorkerRegistry criticalWorkerRegistry,
            ThreadPoolsManager threadPoolsManager,
            NettyBootstrapFactory nettyBootstrapFactory,
            SystemLocalConfiguration systemConfiguration,
            FailureManager failureManager
    ) {
        return new NettyWorkersRegistrar(
                criticalWorkerRegistry,
                threadPoolsManager.commonScheduler(),
                nettyBootstrapFactory,
                systemConfiguration.criticalWorkers(),
                failureManager
        );
    }

    /** Creates the clock waiter. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    public ClockWaiter clockWaiter(NodeSeedParams seedParams, HybridClock clock, ThreadPoolsManager threadPoolsManager) {
        return new ClockWaiter(seedParams.nodeName(), clock, threadPoolsManager.commonScheduler());
    }

    /** Creates the cluster ID service. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    public ClusterIdService clusterIdService(VaultManager vaultManager) {
        return new ClusterIdService(vaultManager);
    }
}
