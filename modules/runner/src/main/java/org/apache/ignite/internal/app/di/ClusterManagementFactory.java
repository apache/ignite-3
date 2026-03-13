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
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesExtensionConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfiguration;

/**
 * Micronaut factory for cluster management components.
 */
@Factory
public class ClusterManagementFactory {
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

}
