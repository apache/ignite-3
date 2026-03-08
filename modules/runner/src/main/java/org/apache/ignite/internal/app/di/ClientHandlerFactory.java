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
import java.util.concurrent.ExecutorService;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.client.handler.configuration.ClientConnectorExtensionConfiguration;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.SuggestionsClusterExtensionConfiguration;
import org.apache.ignite.internal.configuration.SuggestionsConfiguration;
import org.apache.ignite.internal.eventlog.impl.EventLogImpl;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityExtensionConfiguration;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.tx.TxManager;

/**
 * Micronaut factory for client handler and authentication components.
 */
@Factory
public class ClientHandlerFactory {
    /** Creates the security configuration from the cluster config registry. */
    @Singleton
    public SecurityConfiguration securityConfiguration(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        return clusterConfigRegistry.getConfiguration(SecurityExtensionConfiguration.KEY).security();
    }

    /** Creates the client connector configuration from the node config registry. */
    @Singleton
    public ClientConnectorConfiguration clientConnectorConfiguration(
            @Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry
    ) {
        return nodeConfigRegistry.getConfiguration(ClientConnectorExtensionConfiguration.KEY).clientConnector();
    }

    /** Creates the suggestions configuration from the cluster config registry. */
    @Singleton
    public SuggestionsConfiguration suggestionsConfiguration(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        return clusterConfigRegistry.getConfiguration(SuggestionsClusterExtensionConfiguration.KEY).suggestions();
    }

    /** Creates the client handler module. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(2900)
    public ClientHandlerModule clientHandlerModule(
            SqlQueryProcessor sqlQueryProcessor,
            TableManager tableManager,
            TxManager txManager,
            IgniteComputeInternal compute,
            ClusterService clusterService,
            NettyBootstrapFactory nettyBootstrapFactory,
            IgniteImpl igniteImpl,
            MetricManager metricManager,
            AuthenticationManager authenticationManager,
            ClockServiceImpl clockService,
            SchemaSyncServiceImpl schemaSyncService,
            CatalogManagerImpl catalogManager,
            PlacementDriver placementDriver,
            ClientConnectorConfiguration clientConnectorConfiguration,
            EventLogImpl eventLog,
            LowWatermarkImpl lowWatermark,
            @Named("partitionOperationsExecutor") ExecutorService partitionOperationsExecutor,
            SuggestionsConfiguration suggestionsConfiguration
    ) {
        return new ClientHandlerModule(
                sqlQueryProcessor,
                tableManager,
                txManager,
                compute,
                clusterService,
                nettyBootstrapFactory,
                igniteImpl::clusterInfo,
                metricManager,
                new ClientHandlerMetricSource(),
                authenticationManager,
                clockService,
                schemaSyncService,
                catalogManager,
                placementDriver,
                clientConnectorConfiguration,
                eventLog,
                lowWatermark,
                partitionOperationsExecutor,
                () -> suggestionsConfiguration.sequentialDdlExecution().enabled().value()
        );
    }
}
