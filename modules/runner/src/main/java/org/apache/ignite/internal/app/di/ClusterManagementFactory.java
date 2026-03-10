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
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesExtensionConfiguration;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.NodeIdentity;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.configuration.ConfigurationDynamicDefaultsPatcherImpl;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.disaster.system.ClusterIdService;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ChannelTypeRegistryProvider;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.recovery.InMemoryStaleIds;
import org.apache.ignite.internal.network.scalecube.ScaleCubeClusterService;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.SerializationRegistryServiceLoader;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfiguration;
import org.apache.ignite.internal.version.DefaultIgniteProductVersionSource;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;

/**
 * Micronaut factory for cluster management components.
 */
@Factory
public class ClusterManagementFactory {
    /** Creates the message serialization registry, loading all serializers from the classpath. */
    @Singleton
    public MessageSerializationRegistry messageSerializationRegistry(NodeIdentity nodeIdentity) {
        var serviceLoader = new SerializationRegistryServiceLoader(nodeIdentity.serviceProviderClassLoader());
        var registry = new MessageSerializationRegistryImpl();
        serviceLoader.registerSerializationFactories(registry);
        return registry;
    }

    /** Creates the cluster service. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(1300)
    public ClusterService clusterService(
            NodeIdentity nodeIdentity,
            NetworkConfiguration networkConfiguration,
            NettyBootstrapFactory nettyBootstrapFactory,
            MessageSerializationRegistry serializationRegistry,
            ClusterIdService clusterIdService,
            CriticalWorkerRegistry criticalWorkerRegistry,
            FailureManager failureManager,
            MetricManager metricManager
    ) {
        return new ScaleCubeClusterService(
                nodeIdentity.nodeName(),
                networkConfiguration,
                nettyBootstrapFactory,
                serializationRegistry,
                new InMemoryStaleIds(),
                clusterIdService,
                criticalWorkerRegistry,
                failureManager,
                ChannelTypeRegistryProvider.loadByServiceLoader(nodeIdentity.serviceProviderClassLoader()),
                new DefaultIgniteProductVersionSource(),
                metricManager
        );
    }

    /** Exposes the cluster messaging service as a named bean. */
    @Singleton
    @Named("clusterMessaging")
    public MessagingService clusterMessagingService(ClusterService clusterService) {
        return clusterService.messagingService();
    }

    /** Exposes the cluster topology service as a bean. */
    @Singleton
    public TopologyService clusterTopologyService(ClusterService clusterService) {
        return clusterService.topologyService();
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

}
