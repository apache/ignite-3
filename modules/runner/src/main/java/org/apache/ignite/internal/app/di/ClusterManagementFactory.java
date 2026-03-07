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
import jakarta.inject.Singleton;
import org.apache.ignite.configuration.ConfigurationDynamicDefaultsPatcher;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.NodeAttributesCollector;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesExtensionConfiguration;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorageManager;
import org.apache.ignite.internal.cluster.management.raft.RocksDbClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.ValidationManager;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationDynamicDefaultsPatcherImpl;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.di.IgniteStartupPhase;
import org.apache.ignite.internal.di.StartupPhase;
import org.apache.ignite.internal.disaster.system.ClusterIdService;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryStorage;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ChannelTypeRegistryProvider;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.network.scalecube.ScaleCubeClusterService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.SerializationRegistryServiceLoader;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfiguration;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.version.DefaultIgniteProductVersionSource;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;

/**
 * Micronaut factory for cluster management components.
 */
@Factory
public class ClusterManagementFactory {
    /** Creates the message serialization registry, loading all serializers from the classpath. */
    @Singleton
    public MessageSerializationRegistry messageSerializationRegistry(NodeSeedParams seedParams) {
        var serviceLoader = new SerializationRegistryServiceLoader(seedParams.serviceProviderClassLoader());
        var registry = new MessageSerializationRegistryImpl();
        serviceLoader.registerSerializationFactories(registry);
        return registry;
    }

    /** Creates the cluster service. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(1300)
    public ClusterService clusterService(
            NodeSeedParams seedParams,
            NetworkConfiguration networkConfiguration,
            NettyBootstrapFactory nettyBootstrapFactory,
            MessageSerializationRegistry serializationRegistry,
            ClusterIdService clusterIdService,
            CriticalWorkerRegistry criticalWorkerRegistry,
            FailureManager failureManager,
            MetricManager metricManager
    ) {
        return new ScaleCubeClusterService(
                seedParams.nodeName(),
                networkConfiguration,
                nettyBootstrapFactory,
                serializationRegistry,
                new InMemoryStaleIds(),
                clusterIdService,
                criticalWorkerRegistry,
                failureManager,
                ChannelTypeRegistryProvider.loadByServiceLoader(seedParams.serviceProviderClassLoader()),
                new DefaultIgniteProductVersionSource(),
                metricManager
        );
    }

    /** Creates the node attributes configuration from the node config registry. */
    @Singleton
    public NodeAttributesConfiguration nodeAttributesConfiguration(
            @Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry
    ) {
        return nodeConfigRegistry.getConfiguration(NodeAttributesExtensionConfiguration.KEY).nodeAttributes();
    }

    /** Creates the storage profiles configuration from the node config registry. */
    @Singleton
    public StorageConfiguration storageConfiguration(@Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry) {
        return nodeConfigRegistry.getConfiguration(StorageExtensionConfiguration.KEY).storage();
    }

    /** Creates the cluster state storage backed by RocksDB. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(700)
    public RocksDbClusterStateStorage clusterStateStorage(@Named("cmg") ComponentWorkingDir cmgWorkDir, NodeSeedParams seedParams) {
        return new RocksDbClusterStateStorage(cmgWorkDir.dbPath(), seedParams.nodeName());
    }

    /** Creates the cluster state storage manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(710)
    public ClusterStateStorageManager clusterStateStorageManager(RocksDbClusterStateStorage clusterStateStorage) {
        return new ClusterStateStorageManager(clusterStateStorage);
    }

    /** Creates the logical topology implementation. */
    @Singleton
    public LogicalTopologyImpl logicalTopology(RocksDbClusterStateStorage clusterStateStorage, FailureManager failureManager) {
        return new LogicalTopologyImpl(clusterStateStorage, failureManager);
    }

    /** Creates the validation manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(720)
    public ValidationManager validationManager(
            ClusterStateStorageManager clusterStateStorageManager,
            LogicalTopologyImpl logicalTopology
    ) {
        return new ValidationManager(clusterStateStorageManager, logicalTopology);
    }

    /** Creates the distributed configuration tree generator. */
    @Singleton
    @Named("distributed")
    public ConfigurationTreeGenerator distributedConfigurationTreeGenerator(ConfigurationModules modules) {
        return new ConfigurationTreeGenerator(
                modules.distributed().rootKeys(),
                modules.distributed().schemaExtensions(),
                modules.distributed().polymorphicSchemaExtensions()
        );
    }

    /** Creates the distributed configuration validator. */
    @Singleton
    @Named("distributed")
    public ConfigurationValidator distributedConfigurationValidator(
            @Named("distributed") ConfigurationTreeGenerator generator,
            ConfigurationModules modules
    ) {
        return ConfigurationValidatorImpl.withDefaultValidators(generator, modules.distributed().validators());
    }

    /** Creates the configuration dynamic defaults patcher. */
    @Singleton
    public ConfigurationDynamicDefaultsPatcher configurationDynamicDefaultsPatcher(
            ConfigurationModules modules,
            @Named("distributed") ConfigurationTreeGenerator generator
    ) {
        return new ConfigurationDynamicDefaultsPatcherImpl(modules.distributed(), generator);
    }

    /** Creates the cluster initializer. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(1310)
    public ClusterInitializer clusterInitializer(
            ClusterService clusterService,
            ConfigurationDynamicDefaultsPatcher configurationDynamicDefaultsPatcher,
            @Named("distributed") ConfigurationValidator distributedCfgValidator
    ) {
        return new ClusterInitializer(clusterService, configurationDynamicDefaultsPatcher, distributedCfgValidator);
    }

    /** Creates the node attributes collector. */
    @Singleton
    public NodeAttributesCollector nodeAttributesCollector(
            NodeAttributesConfiguration nodeAttributesConfiguration,
            StorageConfiguration storageConfiguration
    ) {
        return new NodeAttributesCollector(nodeAttributesConfiguration, storageConfiguration);
    }

    /** Creates the system disaster recovery storage. */
    @Singleton
    public SystemDisasterRecoveryStorage systemDisasterRecoveryStorage(VaultManager vaultManager) {
        return new SystemDisasterRecoveryStorage(vaultManager);
    }

    /** Creates the cluster management group manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(1900)
    public ClusterManagementGroupManager clusterManagementGroupManager(
            VaultManager vaultManager,
            SystemDisasterRecoveryStorage systemDisasterRecoveryStorage,
            ClusterService clusterService,
            ClusterInitializer clusterInitializer,
            RaftManager raftManager,
            ClusterStateStorageManager clusterStateStorageManager,
            LogicalTopologyImpl logicalTopology,
            ValidationManager validationManager,
            NodeAttributesCollector nodeAttributesCollector,
            FailureManager failureManager,
            ClusterIdService clusterIdService,
            @Named("cmg") RaftGroupOptionsConfigurer cmgRaftConfigurer,
            MetricManager metricManager
    ) {
        return new ClusterManagementGroupManager(
                vaultManager,
                systemDisasterRecoveryStorage,
                clusterService,
                clusterInitializer,
                raftManager,
                clusterStateStorageManager,
                logicalTopology,
                validationManager,
                nodeAttributesCollector,
                failureManager,
                clusterIdService,
                cmgRaftConfigurer,
                metricManager
        );
    }

    /** Creates the logical topology service. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(1910)
    public LogicalTopologyServiceImpl logicalTopologyService(
            LogicalTopologyImpl logicalTopology,
            ClusterManagementGroupManager cmgManager
    ) {
        return new LogicalTopologyServiceImpl(logicalTopology, cmgManager);
    }
}
