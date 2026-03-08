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
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationExtensionConfiguration;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.disaster.system.MetastorageRepairImpl;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryStorage;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.configuration.ReplicationExtensionConfiguration;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;

/**
 * Micronaut factory for MetaStorage and cluster configuration components.
 */
@Factory
public class MetaStorageFactory {
    /** Creates the topology-aware Raft group service factory. */
    @Singleton
    public TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory(
            ClusterService clusterService,
            LogicalTopologyService logicalTopologyService,
            RaftGroupEventsClientListener raftGroupEventsClientListener
    ) {
        return new TopologyAwareRaftGroupServiceFactory(
                clusterService,
                logicalTopologyService,
                Loza.FACTORY,
                raftGroupEventsClientListener
        );
    }

    /** Creates the RocksDB key-value storage for metastorage. */
    @Singleton
    public RocksDbKeyValueStorage metastorageKeyValueStorage(
            NodeSeedParams seedParams,
            @Named("metastorage") ComponentWorkingDir metastorageWorkDir,
            FailureManager failureManager,
            ReadOperationForCompactionTracker readOperationForCompactionTracker,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler
    ) {
        return new RocksDbKeyValueStorage(
                seedParams.nodeName(),
                metastorageWorkDir.dbPath(),
                failureManager,
                readOperationForCompactionTracker,
                commonScheduler
        );
    }

    /** Creates the metastorage repair implementation. */
    @Singleton
    public MetastorageRepairImpl metastorageRepair(
            @Named("clusterMessaging") MessagingService clusterMessagingService,
            LogicalTopologyImpl logicalTopology,
            ClusterManagementGroupManager cmgManager
    ) {
        return new MetastorageRepairImpl(
                clusterMessagingService,
                logicalTopology,
                cmgManager
        );
    }

    /** Creates the MetaStorage manager. */
    @Singleton
    @IgniteStartupPhase(StartupPhase.PHASE_2)
    @Order(200)
    public MetaStorageManagerImpl metaStorageManager(
            ClusterService clusterService,
            ClusterManagementGroupManager cmgManager,
            LogicalTopologyService logicalTopologyService,
            RaftManager raftManager,
            RocksDbKeyValueStorage storage,
            HybridClock clock,
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            MetricManager metricManager,
            SystemDisasterRecoveryStorage systemDisasterRecoveryStorage,
            MetastorageRepairImpl metastorageRepair,
            @Named("metastorage") RaftGroupOptionsConfigurer msRaftConfigurer,
            ReadOperationForCompactionTracker readOperationForCompactionTracker,
            @Named("tableIoExecutor") ScheduledExecutorService tableIoExecutor,
            FailureManager failureManager
    ) {
        return new MetaStorageManagerImpl(
                clusterService,
                cmgManager,
                logicalTopologyService,
                raftManager,
                storage,
                clock,
                topologyAwareRaftGroupServiceFactory,
                metricManager,
                systemDisasterRecoveryStorage,
                metastorageRepair,
                msRaftConfigurer,
                readOperationForCompactionTracker,
                tableIoExecutor,
                failureManager
        );
    }

    /** Creates the distributed configuration storage backed by MetaStorage. */
    @Singleton
    public DistributedConfigurationStorage distributedConfigurationStorage(
            NodeSeedParams seedParams,
            MetaStorageManagerImpl metaStorageManager
    ) {
        return new DistributedConfigurationStorage(seedParams.nodeName(), metaStorageManager);
    }

    /** Creates the cluster (distributed) configuration registry. */
    @Singleton
    @Named("clusterConfig")
    public ConfigurationRegistry clusterConfigRegistry(
            ConfigurationModules modules,
            DistributedConfigurationStorage cfgStorage,
            @Named("distributed") ConfigurationTreeGenerator generator,
            @Named("distributed") ConfigurationValidator validator
    ) {
        return ConfigurationRegistry.create(modules.distributed(), cfgStorage, generator, validator);
    }

    /** Creates the schema synchronization configuration from the cluster config registry. */
    @Singleton
    public SchemaSynchronizationConfiguration schemaSynchronizationConfiguration(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        return clusterConfigRegistry
                .getConfiguration(SchemaSynchronizationExtensionConfiguration.KEY).schemaSync();
    }

    /** Creates the replication configuration from the cluster config registry. */
    @Singleton
    public ReplicationConfiguration replicationConfiguration(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        return clusterConfigRegistry
                .getConfiguration(ReplicationExtensionConfiguration.KEY).replication();
    }
}

