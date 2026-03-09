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
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationExtensionConfiguration;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.components.NodeIdentity;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.metastorage.server.ReadOperationForCompactionTracker;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDbKeyValueStorage;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.configuration.ReplicationExtensionConfiguration;
import org.apache.ignite.internal.sql.configuration.distributed.SqlClusterExtensionConfiguration;
import org.apache.ignite.internal.sql.configuration.distributed.SqlDistributedConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlLocalConfiguration;
import org.apache.ignite.internal.sql.configuration.local.SqlNodeExtensionConfiguration;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;

/**
 * Micronaut factory for MetaStorage, cluster configuration, and SQL configuration components.
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
            NodeIdentity nodeIdentity,
            @Named("metastorage") ComponentWorkingDir metastorageWorkDir,
            FailureManager failureManager,
            ReadOperationForCompactionTracker readOperationForCompactionTracker,
            @Named("commonScheduler") ScheduledExecutorService commonScheduler
    ) {
        return new RocksDbKeyValueStorage(
                nodeIdentity.nodeName(),
                metastorageWorkDir.dbPath(),
                failureManager,
                readOperationForCompactionTracker,
                commonScheduler
        );
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
}

