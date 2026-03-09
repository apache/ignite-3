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
import org.apache.ignite.IgniteServer;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.NodeIdentity;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalExtensionConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;
import org.apache.ignite.internal.failure.configuration.FailureProcessorExtensionConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkExtensionConfiguration;
import org.apache.ignite.internal.worker.configuration.CriticalWorkersConfiguration;

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

    /** Creates the critical workers configuration from the system local config. */
    @Singleton
    public CriticalWorkersConfiguration criticalWorkersConfiguration(SystemLocalConfiguration systemConfiguration) {
        return systemConfiguration.criticalWorkers();
    }

    /** Creates the failure manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(600)
    public FailureManager failureManager(
            NodeIdentity nodeIdentity,
            IgniteServer igniteServer,
            FailureProcessorConfiguration failureProcessorConfiguration
    ) {
        return new FailureManager(nodeIdentity.nodeName(), igniteServer::shutdown, failureProcessorConfiguration);
    }

}
