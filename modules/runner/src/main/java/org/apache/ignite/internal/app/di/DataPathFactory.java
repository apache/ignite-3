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
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.components.NodeIdentity;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.schema.CatalogValidationSchemasSource;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.VolatileLogStorageManagerCreator;
import org.apache.ignite.internal.replicator.ReplicaManager;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.VersionedAssignments;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.storage.DataStorageModule;
import org.apache.ignite.internal.storage.DataStorageModules;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.table.distributed.raft.PartitionSafeTimeValidator;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.internal.tx.message.TxMessageGroup;

/**
 * Micronaut factory for data path components.
 */
@Factory
public class DataPathFactory {
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
            NodeIdentity nodeIdentity,
            ClusterService clusterService,
            ClusterManagementGroupManager cmgManager,
            MetaStorageManager metaStorageManager,
            ClockService clockService,
            PlacementDriver placementDriver,
            @Named("partitionOperationsExecutor") ExecutorService partitionOperationsExecutor,
            ReplicationConfiguration replicationConfiguration,
            FailureManager failureManager,
            CatalogValidationSchemasSource validationSchemasSource,
            CatalogManager catalogManager,
            SchemaSyncService schemaSyncService,
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            RaftManager raftManager,
            @Named("partitions") RaftGroupOptionsConfigurer partitionRaftConfigurer,
            VolatileLogStorageManagerCreator volatileLogStorageManagerCreator,
            @Named("tableIoExecutor") ScheduledExecutorService tableIoExecutor,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler
    ) {
        return new ReplicaManager(
                nodeIdentity.nodeName(),
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
            NodeIdentity nodeIdentity,
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
                ServiceLoader.load(DataStorageModule.class, nodeIdentity.serviceProviderClassLoader())
        );

        Path storagePath = partitionsWorkDir.dbPath();

        Map<String, StorageEngine> storageEngines = dataStorageModules.createStorageEngines(
                nodeIdentity.nodeName(),
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

}
