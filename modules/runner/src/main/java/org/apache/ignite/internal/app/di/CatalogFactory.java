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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.PartitionCountCalculatorWrapper;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.ClockWaiter;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.metastorage.cache.IdempotentCacheVacuumizer;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.sources.ClockServiceMetricSource;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.PlacementDriverManager;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;

/**
 * Micronaut factory for catalog, clock service, and placement driver components.
 */
@Factory
public class CatalogFactory {
    /** Creates the clock service. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(50)
    public ClockServiceImpl clockService(
            HybridClock clock,
            ClockWaiter clockWaiter,
            SchemaSynchronizationConfiguration schemaSyncConfig,
            ClockServiceMetricSource clockServiceMetricSource
    ) {
        return new ClockServiceImpl(
                clock,
                clockWaiter,
                () -> schemaSyncConfig.maxClockSkewMillis().value(),
                clockServiceMetricSource::onMaxClockSkewExceeded
        );
    }

    /** Creates the idempotent cache vacuumizer. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(600)
    public IdempotentCacheVacuumizer idempotentCacheVacuumizer(
            NodeSeedParams seedParams,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler,
            MetaStorageManagerImpl metaStorageManager,
            RaftConfiguration raftConfiguration,
            ClockService clockService,
            FailureManager failureManager
    ) {
        return new IdempotentCacheVacuumizer(
                seedParams.nodeName(),
                commonScheduler,
                metaStorageManager::evictIdempotentCommandsCache,
                raftConfiguration.retryTimeoutMillis(),
                clockService,
                failureManager,
                1,
                1,
                TimeUnit.MINUTES
        );
    }

    /** Creates the catalog manager. UpdateLogImpl is created inline because its lifecycle is managed by CatalogManagerImpl. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(100)
    public CatalogManagerImpl catalogManager(
            MetaStorageManagerImpl metaStorageManager,
            ClockService clockService,
            FailureManager failureManager,
            SchemaSynchronizationConfiguration schemaSyncConfig,
            PartitionCountCalculatorWrapper partitionCountCalculatorWrapper
    ) {
        UpdateLogImpl updateLog = new UpdateLogImpl(metaStorageManager, failureManager);

        return new CatalogManagerImpl(
                updateLog,
                clockService,
                failureManager,
                () -> schemaSyncConfig.delayDurationMillis().value(),
                partitionCountCalculatorWrapper
        );
    }

    /** Creates the placement driver manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(800)
    public PlacementDriverManager placementDriverManager(
            NodeSeedParams seedParams,
            MetaStorageManagerImpl metaStorageManager,
            ClusterService clusterService,
            ClusterManagementGroupManager cmgManager,
            LogicalTopologyService logicalTopologyService,
            RaftManager raftManager,
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            ClockService clockService,
            FailureManager failureManager,
            ReplicationConfiguration replicationConfiguration,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler,
            MetricManager metricManager,
            Provider<DistributionZoneManager> distributionZoneManagerProvider
    ) {
        return new PlacementDriverManager(
                seedParams.nodeName(),
                metaStorageManager,
                MetastorageGroupId.INSTANCE,
                clusterService,
                cmgManager::metaStorageNodes,
                logicalTopologyService,
                raftManager,
                topologyAwareRaftGroupServiceFactory,
                clockService,
                failureManager,
                replicationConfiguration,
                commonScheduler,
                metricManager,
                zoneId -> distributionZoneManagerProvider.get().currentDataNodes(zoneId)
        );
    }

    /** Exposes the placement driver as a bean. */
    @Singleton
    public PlacementDriver placementDriver(PlacementDriverManager placementDriverManager) {
        return placementDriverManager.placementDriver();
    }
}
