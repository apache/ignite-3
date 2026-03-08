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

import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.pendingPartAssignmentsQueueKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.zonePartitionStableAssignments;

import io.micronaut.context.annotation.Factory;
import io.micronaut.core.annotation.Order;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockServiceImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.index.IndexNodeFinishedRwTransactionsChecker;
import org.apache.ignite.internal.lowwatermark.LowWatermarkImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageRevisionListenerRegistry;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.schema.CatalogValidationSchemasSource;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageManagerCreator;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.VersionedAssignments;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaSafeTimeTrackerImpl;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorServiceImpl;
import org.apache.ignite.internal.table.distributed.raft.PartitionSafeTimeValidator;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncServiceImpl;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.sql.IgniteSql;

/**
 * Micronaut factory for data path components.
 */
@Factory
public class DataPathFactory {
    /** Creates the volatile log storage manager creator. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1300)
    public VolatileLogStorageManagerCreator volatileLogStorageManagerCreator(NodeSeedParams seedParams) {
        return new VolatileLogStorageManagerCreator(
                seedParams.nodeName(),
                seedParams.workDir().resolve("volatile-log-spillout")
        );
    }

    /** Creates the schema safe time tracker. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1900)
    public SchemaSafeTimeTrackerImpl schemaSafeTimeTracker(MetaStorageManagerImpl metaStorageManager) {
        return new SchemaSafeTimeTrackerImpl(metaStorageManager.clusterTime());
    }

    /** Creates the schema sync service. */
    @Singleton
    public SchemaSyncServiceImpl schemaSyncService(
            SchemaSafeTimeTrackerImpl schemaSafeTimeTracker,
            SchemaSynchronizationConfiguration schemaSyncConfig
    ) {
        LongSupplier delayDurationMsSupplier = () -> schemaSyncConfig.delayDurationMillis().value();
        return new SchemaSyncServiceImpl(schemaSafeTimeTracker, delayDurationMsSupplier);
    }

    /** Creates the index node finished rw transactions checker. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1600)
    public IndexNodeFinishedRwTransactionsChecker indexNodeFinishedRwTransactionsChecker(
            CatalogManagerImpl catalogManager,
            @Named("clusterMessaging") MessagingService clusterMessagingService,
            HybridClock clock
    ) {
        return new IndexNodeFinishedRwTransactionsChecker(catalogManager, clusterMessagingService, clock);
    }

    /** Creates the observable hybrid timestamp tracker. */
    @Singleton
    public HybridTimestampTracker hybridTimestampTracker() {
        return HybridTimestampTracker.atomicTracker(null);
    }

    /** Creates the replica manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1500)
    public ReplicaManager replicaManager(
            NodeSeedParams seedParams,
            ClusterService clusterService,
            ClusterManagementGroupManager cmgManager,
            MetaStorageManagerImpl metaStorageManager,
            ClockServiceImpl clockService,
            PlacementDriver placementDriver,
            @Named("partitionOperationsExecutor") ExecutorService partitionOperationsExecutor,
            ReplicationConfiguration replicationConfiguration,
            FailureManager failureManager,
            CatalogValidationSchemasSource validationSchemasSource,
            CatalogManagerImpl catalogManager,
            SchemaSyncServiceImpl schemaSyncService,
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            RaftManager raftManager,
            @Named("partitions") RaftGroupOptionsConfigurer partitionRaftConfigurer,
            VolatileLogStorageManagerCreator volatileLogStorageManagerCreator,
            @Named("tableIoExecutor") ScheduledExecutorService tableIoExecutor,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler
    ) {
        return new ReplicaManager(
                seedParams.nodeName(),
                clusterService,
                cmgManager,
                groupId -> zonePartitionStableAssignments(metaStorageManager, groupId),
                clockService,
                Set.of(
                        PartitionReplicationMessageGroup.class,
                        TxMessageGroup.class
                ),
                placementDriver,
                partitionOperationsExecutor,
                () -> replicationConfiguration.idleSafeTimePropagationDurationMillis().value(),
                failureManager,
                new ThreadLocalPartitionCommandsMarshaller(
                        clusterService.serializationRegistry()
                ),
                new PartitionSafeTimeValidator(validationSchemasSource, catalogManager, schemaSyncService),
                topologyAwareRaftGroupServiceFactory,
                raftManager,
                partitionRaftConfigurer,
                volatileLogStorageManagerCreator,
                tableIoExecutor,
                replicaGrpId -> metaStorageManager.get(
                        pendingPartAssignmentsQueueKey((TablePartitionId) replicaGrpId)
                ).thenApply(entry -> new VersionedAssignments(
                        entry.value(), entry.revision()
                )),
                commonScheduler
        );
    }

    /** Creates the data storage manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1800)
    public DataStorageManager dataStorageManager(
            NodeSeedParams seedParams,
            MetricManager metricManager,
            @Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry,
            @Named("partitions") ComponentWorkingDir partitionsWorkDir,
            @Named("longJvmPauseDetector") Provider<LongJvmPauseDetector> longJvmPauseDetector,
            FailureManager failureManager,
            @Named("partitions") LogStorageManager partitionsLogStorageManager,
            HybridClock clock,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler,
            StorageConfiguration storageConfiguration
    ) {
        DataStorageModules dataStorageModules = new DataStorageModules(
                ServiceLoader.load(DataStorageModule.class, seedParams.serviceProviderClassLoader())
        );

        Path storagePath = partitionsWorkDir.dbPath();

        Map<String, StorageEngine> storageEngines = dataStorageModules.createStorageEngines(
                seedParams.nodeName(),
                metricManager,
                nodeConfigRegistry,
                storagePath,
                longJvmPauseDetector.get(),
                failureManager,
                partitionsLogStorageManager.logSyncer(),
                clock,
                commonScheduler
        );

        return new DataStorageManager(storageEngines, storageConfiguration);
    }

    /** Creates the low watermark. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(2000)
    public LowWatermarkImpl lowWatermark(
            NodeSeedParams seedParams,
            GcConfiguration gcConfiguration,
            ClockServiceImpl clockService,
            VaultManager vaultManager,
            FailureManager failureManager,
            @Named("clusterMessaging") MessagingService clusterMessagingService
    ) {
        return new LowWatermarkImpl(
                seedParams.nodeName(),
                gcConfiguration.lowWatermark(),
                clockService,
                vaultManager,
                failureManager,
                clusterMessagingService
        );
    }

    /** Creates the distribution zone manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(1100)
    public DistributionZoneManager distributionZoneManager(
            NodeSeedParams seedParams,
            TopologyService topologyService,
            MetaStorageManagerImpl metaStorageManager,
            LogicalTopologyService logicalTopologyService,
            FailureManager failureManager,
            CatalogManagerImpl catalogManager,
            SystemDistributedConfiguration systemDistributedConfiguration,
            ClockServiceImpl clockService,
            MetricManager metricManager,
            LowWatermarkImpl lowWatermark
    ) {
        return new DistributionZoneManager(
                seedParams.nodeName(),
                () -> topologyService.localMember().id(),
                metaStorageManager,
                logicalTopologyService,
                failureManager,
                catalogManager,
                systemDistributedConfiguration,
                clockService,
                metricManager,
                lowWatermark
        );
    }

    /** Creates the partition replica lifecycle manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(2300)
    public PartitionReplicaLifecycleManager partitionReplicaLifecycleManager(
            CatalogManagerImpl catalogManager,
            ReplicaManager replicaManager,
            DistributionZoneManager distributionZoneManager,
            MetaStorageManagerImpl metaStorageManager,
            TopologyService topologyService,
            LowWatermarkImpl lowWatermark,
            FailureManager failureManager,
            @Named("tableIoExecutor") ScheduledExecutorService tableIoExecutor,
            @Named("rebalanceScheduler") ScheduledExecutorService rebalanceScheduler,
            @Named("partitionOperationsExecutor") ExecutorService partitionOperationsExecutor,
            ClockServiceImpl clockService,
            PlacementDriver placementDriver,
            SchemaSyncServiceImpl schemaSyncService,
            SystemDistributedConfiguration systemDistributedConfiguration,
            TxStateRocksDbSharedStorage sharedTxStateStorage,
            TxManager txManager,
            SchemaManager schemaManager,
            DataStorageManager dataStorageManager,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            MetricManager metricManager,
            @Named("storageOperations") MessagingService messagingService,
            ReplicaService replicaService
    ) {
        return new PartitionReplicaLifecycleManager(
                catalogManager,
                replicaManager,
                distributionZoneManager,
                metaStorageManager,
                topologyService,
                lowWatermark,
                failureManager,
                tableIoExecutor,
                rebalanceScheduler,
                partitionOperationsExecutor,
                clockService,
                placementDriver,
                schemaSyncService,
                systemDistributedConfiguration,
                sharedTxStateStorage,
                txManager,
                schemaManager,
                dataStorageManager,
                outgoingSnapshotsManager,
                metricManager,
                messagingService,
                replicaService
        );
    }

    /** Creates the shared minimum required time collector service. */
    @Singleton
    public MinimumRequiredTimeCollectorServiceImpl minimumRequiredTimeCollectorService() {
        return new MinimumRequiredTimeCollectorServiceImpl();
    }

    /** Creates the table manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(2400)
    public TableManager tableManager(
            NodeSeedParams seedParams,
            MetaStorageRevisionListenerRegistry revisionListenerRegistry,
            GcConfiguration gcConfiguration,
            ReplicationConfiguration replicationConfiguration,
            @Named("storageOperations") MessagingService messagingService,
            TopologyService topologyService,
            LockManager lockManager,
            ReplicaService replicaService,
            TxManager txManager,
            DataStorageManager dataStorageManager,
            MetaStorageManagerImpl metaStorageManager,
            SchemaManager schemaManager,
            CatalogValidationSchemasSource validationSchemasSource,
            @Named("tableIoExecutor") ScheduledExecutorService tableIoExecutor,
            @Named("partitionOperationsExecutor") ExecutorService partitionOperationsExecutor,
            ClockServiceImpl clockService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            SchemaSyncServiceImpl schemaSyncService,
            CatalogManagerImpl catalogManager,
            FailureManager failureManager,
            HybridTimestampTracker observableTimestampTracker,
            PlacementDriver placementDriver,
            Provider<IgniteSql> sqlProvider,
            RemotelyTriggeredResourceRegistry resourcesRegistry,
            LowWatermarkImpl lowWatermark,
            TransactionInflights transactionInflights,
            IndexMetaStorage indexMetaStorage,
            PartitionReplicaLifecycleManager partitionReplicaLifecycleManager,
            MinimumRequiredTimeCollectorServiceImpl minTimeCollectorService,
            SystemDistributedConfiguration systemDistributedConfiguration,
            MetricManager metricManager,
            PartitionModificationCounterFactory partitionModificationCounterFactory
    ) {
        return new TableManager(
                seedParams.nodeName(),
                revisionListenerRegistry,
                gcConfiguration,
                replicationConfiguration,
                messagingService,
                topologyService,
                lockManager,
                replicaService,
                txManager,
                dataStorageManager,
                metaStorageManager,
                schemaManager,
                validationSchemasSource,
                tableIoExecutor,
                partitionOperationsExecutor,
                clockService,
                outgoingSnapshotsManager,
                schemaSyncService,
                catalogManager,
                failureManager,
                observableTimestampTracker,
                placementDriver,
                sqlProvider::get,
                resourcesRegistry,
                lowWatermark,
                transactionInflights,
                indexMetaStorage,
                partitionReplicaLifecycleManager,
                minTimeCollectorService,
                systemDistributedConfiguration,
                metricManager,
                partitionModificationCounterFactory
        );
    }

    /** Creates the partition modification counter factory. */
    @Singleton
    public PartitionModificationCounterFactory partitionModificationCounterFactory(
            ClockServiceImpl clockService,
            @Named("clusterMessaging") MessagingService clusterMessagingService
    ) {
        return new PartitionModificationCounterFactory(clockService::current, clusterMessagingService);
    }

}

