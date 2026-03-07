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
import io.micronaut.core.annotation.Order;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.compute.ComputeComponentImpl;
import org.apache.ignite.internal.compute.IgniteComputeImpl;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.configuration.ComputeExtensionConfiguration;
import org.apache.ignite.internal.compute.executor.ComputeExecutorImpl;
import org.apache.ignite.internal.compute.state.InMemoryComputeStateMachine;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.deployunit.DeploymentManagerImpl;
import org.apache.ignite.internal.deployunit.configuration.DeploymentConfiguration;
import org.apache.ignite.internal.deployunit.configuration.DeploymentExtensionConfiguration;
import org.apache.ignite.internal.deployunit.loader.UnitsClassLoaderFactory;
import org.apache.ignite.internal.deployunit.loader.UnitsContextManager;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStoreImpl;
import org.apache.ignite.internal.di.IgniteStartupPhase;
import org.apache.ignite.internal.di.StartupPhase;
import org.apache.ignite.internal.eventlog.impl.EventLogImpl;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriverManager;
import org.apache.ignite.internal.table.distributed.TableManager;

/**
 * Micronaut factory for compute and deployment components.
 */
@Factory
public class ComputeFactory {
    /** Creates the compute configuration from the node config registry. */
    @Singleton
    public ComputeConfiguration computeConfiguration(
            @Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry
    ) {
        return nodeConfigRegistry.getConfiguration(ComputeExtensionConfiguration.KEY).compute();
    }

    /** Creates the deployment configuration from the node config registry. */
    @Singleton
    public DeploymentConfiguration deploymentConfiguration(
            @Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry
    ) {
        return nodeConfigRegistry.getConfiguration(DeploymentExtensionConfiguration.KEY).deployment();
    }

    /** Creates the in-memory compute state machine. */
    @Singleton
    public InMemoryComputeStateMachine inMemoryComputeStateMachine(
            ComputeConfiguration computeConfiguration,
            NodeSeedParams seedParams
    ) {
        return new InMemoryComputeStateMachine(computeConfiguration, seedParams.nodeName());
    }

    /** Creates the compute executor. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1100)
    public ComputeExecutorImpl computeExecutor(
            Provider<Ignite> igniteProvider,
            InMemoryComputeStateMachine stateMachine,
            ComputeConfiguration computeConfiguration,
            ClusterService clusterService,
            ClockServiceImpl clockService,
            EventLogImpl eventLog
    ) {
        return new ComputeExecutorImpl(
                igniteProvider.get(),
                stateMachine,
                computeConfiguration,
                clusterService.topologyService(),
                clockService,
                eventLog
        );
    }

    /** Creates the deployment manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(3000)
    public DeploymentManagerImpl deploymentManager(
            ClusterService clusterService,
            MetaStorageManagerImpl metaStorageManager,
            LogicalTopologyService logicalTopologyService,
            NodeSeedParams seedParams,
            DeploymentConfiguration deploymentConfiguration,
            ClusterManagementGroupManager cmgManager,
            ComputeExecutorImpl computeExecutor
    ) {
        return new DeploymentManagerImpl(
                clusterService,
                new DeploymentUnitStoreImpl(metaStorageManager),
                logicalTopologyService,
                seedParams.workDir(),
                deploymentConfiguration,
                cmgManager,
                seedParams.nodeName(),
                computeExecutor::onUnitRemoving
        );
    }

    /** Creates the compute component. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1200)
    public ComputeComponentImpl computeComponent(
            NodeSeedParams seedParams,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            DeploymentManagerImpl deploymentManager,
            ComputeExecutorImpl computeExecutor,
            ComputeConfiguration computeConfiguration,
            EventLogImpl eventLog,
            HybridTimestampTracker observableTimestampTracker
    ) {
        return new ComputeComponentImpl(
                seedParams.nodeName(),
                clusterService.messagingService(),
                clusterService.topologyService(),
                logicalTopologyService,
                new UnitsContextManager(
                        deploymentManager,
                        deploymentManager.deploymentUnitAccessor(),
                        new UnitsClassLoaderFactory()
                ),
                computeExecutor,
                computeConfiguration,
                eventLog,
                observableTimestampTracker
        );
    }

    /** Creates the internal compute implementation. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1300)
    public IgniteComputeInternal igniteCompute(
            NodeSeedParams seedParams,
            PlacementDriverManager placementDriverManager,
            ClusterService clusterService,
            TableManager tableManager,
            ComputeComponentImpl computeComponent,
            HybridClock clock,
            HybridTimestampTracker observableTimestampTracker
    ) {
        return new IgniteComputeImpl(
                seedParams.nodeName(),
                placementDriverManager.placementDriver(),
                clusterService.topologyService(),
                tableManager,
                computeComponent,
                clock,
                observableTimestampTracker
        );
    }
}
