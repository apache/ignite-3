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
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.eventlog.impl.EventLogImpl;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.configuration.distributed.SqlClusterExtensionConfiguration;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlNodeExtensionConfiguration;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommandHandler;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TransactionInflights;

/**
 * Micronaut factory for SQL engine components.
 */
@Factory
public class SqlFactory {
    /** Creates the SQL distributed configuration from the cluster config registry. */
    @Singleton
    public SqlDistributedConfiguration sqlDistributedConfiguration(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        return clusterConfigRegistry.getConfiguration(SqlClusterExtensionConfiguration.KEY).sql();
    }

    /** Creates the SQL local configuration from the node config registry. */
    @Singleton
    public SqlLocalConfiguration sqlLocalConfiguration(
            @Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry
    ) {
        return nodeConfigRegistry.getConfiguration(SqlNodeExtensionConfiguration.KEY).sql();
    }

    /** Creates the kill command handler. */
    @Singleton
    public KillCommandHandler killCommandHandler(
            NodeSeedParams seedParams,
            LogicalTopologyService logicalTopologyService,
            @Named("clusterMessaging") MessagingService clusterMessagingService
    ) {
        return new KillCommandHandler(seedParams.nodeName(), logicalTopologyService, clusterMessagingService);
    }

    /** Creates the SQL query processor. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(2800)
    public SqlQueryProcessor sqlQueryProcessor(
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            TableManager tableManager,
            SchemaManager schemaManager,
            ReplicaService replicaService,
            ClockServiceImpl clockService,
            SchemaSyncServiceImpl schemaSyncService,
            CatalogManagerImpl catalogManager,
            MetricManager metricManager,
            SystemViewManagerImpl systemViewManager,
            FailureManager failureManager,
            PlacementDriver placementDriver,
            SqlDistributedConfiguration sqlDistributedConfiguration,
            SqlLocalConfiguration sqlLocalConfiguration,
            TransactionInflights transactionInflights,
            TxManager txManager,
            LowWatermarkImpl lowWatermark,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler,
            KillCommandHandler killCommandHandler,
            EventLogImpl eventLog
    ) {
        return new SqlQueryProcessor(
                clusterService,
                logicalTopologyService,
                tableManager,
                schemaManager,
                replicaService,
                clockService,
                schemaSyncService,
                catalogManager,
                metricManager,
                systemViewManager,
                failureManager,
                placementDriver,
                sqlDistributedConfiguration,
                sqlLocalConfiguration,
                transactionInflights,
                txManager,
                lowWatermark,
                commonScheduler,
                killCommandHandler,
                eventLog
        );
    }

    /** Creates the IgniteSql implementation. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(3100)
    public IgniteSqlImpl igniteSql(
            SqlQueryProcessor sqlQueryProcessor,
            HybridTimestampTracker observableTimestampTracker,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler
    ) {
        return new IgniteSqlImpl(sqlQueryProcessor, observableTimestampTracker, commonScheduler);
    }
}
