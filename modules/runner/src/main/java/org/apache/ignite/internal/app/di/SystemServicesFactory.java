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
import org.apache.ignite.internal.app.LowWatermarkRectifier;
import org.apache.ignite.internal.app.SystemPropertiesComponent;
import org.apache.ignite.internal.app.ThreadPoolsManager;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.CatalogCompactionRunner;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.di.IgniteStartupPhase;
import org.apache.ignite.internal.di.StartupPhase;
import org.apache.ignite.internal.disaster.system.ClusterIdService;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryManagerImpl;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceMinimumRequiredTimeProviderImpl;
import org.apache.ignite.internal.eventlog.config.schema.EventLogExtensionConfiguration;
import org.apache.ignite.internal.eventlog.impl.EventLogImpl;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.index.IndexNodeFinishedRwTransactionsChecker;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageCompactionTrigger;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.logstorage.LogStorageMetrics;
import org.apache.ignite.internal.metrics.messaging.MetricMessaging;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.placementdriver.PlacementDriverManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageManagerCreator;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.system.CpuInformationProvider;
import org.apache.ignite.internal.system.JvmCpuInformationProvider;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorServiceImpl;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.ResourceVacuumManager;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.vault.VaultManager;

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
            NodeSeedParams seedParams
    ) {
        return new EventLogImpl(
                clusterConfigRegistry.getConfiguration(EventLogExtensionConfiguration.KEY).eventlog(),
                () -> cmgManagerProvider.get().clusterState().join().clusterTag().clusterId(),
                seedParams.nodeName()
        );
    }

    /** Creates the system view manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    public SystemViewManagerImpl systemViewManager(
            NodeSeedParams seedParams,
            CatalogManagerImpl catalogManager,
            FailureManager failureManager
    ) {
        return new SystemViewManagerImpl(seedParams.nodeName(), catalogManager, failureManager);
    }

    /** Creates the CPU information provider. */
    @Singleton
    public CpuInformationProvider cpuInformationProvider() {
        return new JvmCpuInformationProvider();
    }


    /** Creates the system disaster recovery manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(900)
    public SystemDisasterRecoveryManagerImpl systemDisasterRecoveryManager(
            NodeSeedParams seedParams,
            ClusterService clusterService,
            VaultManager vaultManager,
            MetaStorageManagerImpl metaStorageManager,
            ClusterManagementGroupManager cmgManager,
            ClusterIdService clusterIdService
    ) {
        return new SystemDisasterRecoveryManagerImpl(
                seedParams.nodeName(),
                clusterService.topologyService(),
                clusterService.messagingService(),
                vaultManager,
                seedParams.restarter(),
                metaStorageManager,
                cmgManager,
                clusterIdService
        );
    }

    /** Creates the MetaStorage compaction trigger. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(3400)
    public MetaStorageCompactionTrigger metaStorageCompactionTrigger(
            NodeSeedParams seedParams,
            RocksDbKeyValueStorage storage,
            MetaStorageManagerImpl metaStorageManager,
            FailureManager failureManager,
            ReadOperationForCompactionTracker readOperationForCompactionTracker,
            SystemDistributedConfiguration systemDistributedConfiguration
    ) {
        return new MetaStorageCompactionTrigger(
                seedParams.nodeName(),
                storage,
                metaStorageManager,
                failureManager,
                readOperationForCompactionTracker,
                systemDistributedConfiguration
        );
    }

    /** Creates the catalog compaction runner. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(300)
    public CatalogCompactionRunner catalogCompactionRunner(
            NodeSeedParams seedParams,
            CatalogManagerImpl catalogManager,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            PlacementDriverManager placementDriverManager,
            ReplicaService replicaService,
            ClockServiceImpl clockService,
            SchemaSyncServiceImpl schemaSyncService,
            LowWatermarkImpl lowWatermark,
            IndexNodeFinishedRwTransactionsChecker indexNodeFinishedRwTransactionsChecker,
            MetaStorageManagerImpl metaStorageManager
    ) {
        MinimumRequiredTimeCollectorServiceImpl minTimeCollectorService = new MinimumRequiredTimeCollectorServiceImpl();

        return new CatalogCompactionRunner(
                seedParams.nodeName(),
                catalogManager,
                clusterService.messagingService(),
                logicalTopologyService,
                placementDriverManager.placementDriver(),
                replicaService,
                clockService,
                schemaSyncService,
                clusterService.topologyService(),
                lowWatermark,
                indexNodeFinishedRwTransactionsChecker,
                minTimeCollectorService,
                new RebalanceMinimumRequiredTimeProviderImpl(metaStorageManager, catalogManager)
        );
    }

    /** Creates the log storage metrics. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1400)
    public LogStorageMetrics logStorageMetrics(
            NodeSeedParams seedParams,
            MetricManager metricManager,
            @Named("cmg") LogStorageManager cmgLogStorageManager,
            @Named("metastorage") LogStorageManager msLogStorageManager,
            @Named("partitions") LogStorageManager partitionsLogStorageManager,
            VolatileLogStorageManagerCreator volatileLogStorageManagerCreator
    ) {
        return new LogStorageMetrics(
                seedParams.nodeName(),
                metricManager,
                cmgLogStorageManager,
                msLogStorageManager,
                partitionsLogStorageManager,
                volatileLogStorageManagerCreator
        );
    }

    /** Creates the metric messaging. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1000)
    public MetricMessaging metricMessaging(
            MetricManager metricManager,
            ClusterService clusterService
    ) {
        return new MetricMessaging(metricManager, clusterService.messagingService(), clusterService.topologyService());
    }

    /** Creates the resource vacuum manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(3300)
    public ResourceVacuumManager resourceVacuumManager(
            NodeSeedParams seedParams,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            ClusterService clusterService,
            @Named("storageOperations") MessagingService messagingService,
            TransactionInflights transactionInflights,
            TxManager txManager,
            LowWatermarkImpl lowWatermark,
            FailureManager failureManager,
            MetricManager metricManager
    ) {
        return new ResourceVacuumManager(
                seedParams.nodeName(),
                resourcesRegistry,
                clusterService.topologyService(),
                messagingService,
                transactionInflights,
                txManager,
                lowWatermark,
                failureManager,
                metricManager
        );
    }

    /** Creates the system properties component. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(3200)
    public SystemPropertiesComponent systemPropertiesComponent(
            SystemDistributedConfiguration systemDistributedConfiguration
    ) {
        return new SystemPropertiesComponent(systemDistributedConfiguration);
    }

    /** Creates the low watermark rectifier. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(200)
    public LowWatermarkRectifier lowWatermarkRectifier(
            LowWatermarkImpl lowWatermark,
            CatalogManagerImpl catalogManager
    ) {
        return new LowWatermarkRectifier(lowWatermark, catalogManager);
    }

    /** Creates the disaster recovery manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(2500)
    public DisasterRecoveryManager disasterRecoveryManager(
            ThreadPoolsManager threadPoolsManager,
            @Named("storageOperations") MessagingService messagingService,
            MetaStorageManagerImpl metaStorageManager,
            CatalogManagerImpl catalogManager,
            DistributionZoneManager distributionZoneManager,
            Loza raftManager,
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            TableManager tableManager,
            MetricManager metricManager,
            FailureManager failureManager,
            PartitionReplicaLifecycleManager partitionReplicaLifecycleManager,
            SystemViewManagerImpl systemViewManager
    ) {
        return new DisasterRecoveryManager(
                threadPoolsManager.tableIoExecutor(),
                messagingService,
                metaStorageManager,
                catalogManager,
                distributionZoneManager,
                raftManager,
                clusterService.topologyService(),
                logicalTopologyService,
                tableManager,
                metricManager,
                failureManager,
                partitionReplicaLifecycleManager,
                systemViewManager
        );
    }
}
