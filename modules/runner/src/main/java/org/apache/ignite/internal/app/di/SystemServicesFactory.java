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
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.CatalogCompactionRunner;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.NodeIdentity;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceMinimumRequiredTimeProviderImpl;
import org.apache.ignite.internal.eventlog.config.schema.EventLogExtensionConfiguration;
import org.apache.ignite.internal.eventlog.impl.EventLogImpl;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.index.IndexNodeFinishedRwTransactionsChecker;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;

/**
 * Micronaut factory for system services and miscellaneous components.
 */
@Factory
public class SystemServicesFactory {
    /** Creates the event log. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(3500)
    public EventLogImpl eventLog(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry,
            Provider<ClusterManagementGroupManager> cmgManagerProvider,
            NodeIdentity nodeIdentity
    ) {
        return new EventLogImpl(
                clusterConfigRegistry.getConfiguration(EventLogExtensionConfiguration.KEY).eventlog(),
                () -> cmgManagerProvider.get().clusterState().join().clusterTag().clusterId(),
                nodeIdentity.nodeName()
        );
    }

    /** Creates the catalog compaction runner. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(300)
    public CatalogCompactionRunner catalogCompactionRunner(
            NodeIdentity nodeIdentity,
            CatalogManagerImpl catalogManager,
            @Named("clusterMessaging") MessagingService clusterMessagingService,
            TopologyService topologyService,
            LogicalTopologyService logicalTopologyService,
            PlacementDriver placementDriver,
            ReplicaService replicaService,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            LowWatermark lowWatermark,
            IndexNodeFinishedRwTransactionsChecker indexNodeFinishedRwTransactionsChecker,
            MinimumRequiredTimeCollectorService minTimeCollectorService,
            MetaStorageManager metaStorageManager
    ) {
        return new CatalogCompactionRunner(
                nodeIdentity.nodeName(),
                catalogManager,
                clusterMessagingService,
                logicalTopologyService,
                placementDriver,
                replicaService,
                clockService,
                schemaSyncService,
                topologyService,
                lowWatermark,
                indexNodeFinishedRwTransactionsChecker,
                minTimeCollectorService,
                new RebalanceMinimumRequiredTimeProviderImpl(metaStorageManager, catalogManager)
        );
    }

}
