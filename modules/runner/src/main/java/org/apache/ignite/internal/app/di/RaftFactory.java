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
import java.nio.file.Path;
import java.util.Map;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.internal.components.IgniteStartupPhase;
import org.apache.ignite.internal.components.NodeIdentity;
import org.apache.ignite.internal.components.StartupPhase;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.RaftGroupOptionsConfigHelper;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.raft.RaftGroupOptionsConfigurer;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftExtensionConfiguration;
import org.apache.ignite.internal.raft.server.impl.GroupStoragesContextResolver;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.util.SharedLogStorageManagerUtils;
import org.apache.ignite.internal.replicator.PartitionGroupId;

/**
 * Micronaut factory for Raft infrastructure components.
 */
@Factory
public class RaftFactory {
    /** Used for durable destruction purposes. */
    private static final String PARTITION_GROUP_NAME = "partition";

    /** Creates the Raft configuration from the node config registry. */
    @Singleton
    public RaftConfiguration raftConfiguration(@Named("nodeConfig") ConfigurationRegistry nodeConfigRegistry) {
        return nodeConfigRegistry.getConfiguration(RaftExtensionConfiguration.KEY).raft();
    }

    /** Creates the partitions log storage manager. */
    @Singleton
    @Named("partitions")
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(1500)
    public LogStorageManager partitionsLogStorageManager(
            NodeIdentity nodeIdentity,
            RaftConfiguration raftConfiguration,
            @Named("partitions") ComponentWorkingDir partitionsWorkDir
    ) {
        return SharedLogStorageManagerUtils.create(
                "table data log",
                nodeIdentity.nodeName(),
                partitionsWorkDir.raftLogPath(),
                raftConfiguration.fsync().value()
        );
    }

    /** Creates the metastorage log storage manager. */
    @Singleton
    @Named("metastorage")
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(1600)
    public LogStorageManager msLogStorageManager(
            NodeIdentity nodeIdentity,
            @Named("metastorage") ComponentWorkingDir metastorageWorkDir
    ) {
        return SharedLogStorageManagerUtils.create(
                "meta-storage log",
                nodeIdentity.nodeName(),
                metastorageWorkDir.raftLogPath(),
                true
        );
    }

    /** Creates the CMG log storage manager. */
    @Singleton
    @Named("cmg")
    @IgniteStartupPhase(StartupPhase.PHASE_1)
    @Order(1700)
    public LogStorageManager cmgLogStorageManager(
            NodeIdentity nodeIdentity,
            @Named("cmg") ComponentWorkingDir cmgWorkDir
    ) {
        return SharedLogStorageManagerUtils.create(
                "cluster-management-group log",
                nodeIdentity.nodeName(),
                cmgWorkDir.raftLogPath(),
                true
        );
    }

    /** Creates the partitions Raft group options configurer. */
    @Singleton
    @Named("partitions")
    public RaftGroupOptionsConfigurer partitionRaftConfigurer(
            @Named("partitions") LogStorageManager partitionsLogStorageManager,
            @Named("partitions") ComponentWorkingDir partitionsWorkDir
    ) {
        return RaftGroupOptionsConfigHelper.configureProperties(partitionsLogStorageManager, partitionsWorkDir.metaPath());
    }

    /** Creates the metastorage Raft group options configurer. */
    @Singleton
    @Named("metastorage")
    public RaftGroupOptionsConfigurer msRaftConfigurer(
            @Named("metastorage") LogStorageManager msLogStorageManager,
            @Named("metastorage") ComponentWorkingDir metastorageWorkDir
    ) {
        return RaftGroupOptionsConfigHelper.configureProperties(msLogStorageManager, metastorageWorkDir.metaPath());
    }

    /** Creates the CMG Raft group options configurer. */
    @Singleton
    @Named("cmg")
    public RaftGroupOptionsConfigurer cmgRaftConfigurer(
            @Named("cmg") LogStorageManager cmgLogStorageManager,
            @Named("cmg") ComponentWorkingDir cmgWorkDir
    ) {
        return RaftGroupOptionsConfigHelper.configureProperties(cmgLogStorageManager, cmgWorkDir.metaPath());
    }

    /** Creates the group storages context resolver. */
    @Singleton
    public GroupStoragesContextResolver groupStoragesContextResolver(
            @Named("partitions") LogStorageManager partitionsLogStorageManager,
            @Named("metastorage") LogStorageManager msLogStorageManager,
            @Named("cmg") LogStorageManager cmgLogStorageManager,
            @Named("partitions") ComponentWorkingDir partitionsWorkDir,
            @Named("metastorage") ComponentWorkingDir metastorageWorkDir,
            @Named("cmg") ComponentWorkingDir cmgWorkDir
    ) {
        Map<String, LogStorageManager> logStorageManagerByGroupName = Map.of(
                PARTITION_GROUP_NAME, partitionsLogStorageManager,
                MetastorageGroupId.INSTANCE.toString(), msLogStorageManager,
                CmgGroupId.INSTANCE.toString(), cmgLogStorageManager
        );

        Map<String, Path> serverDataPathByGroupName = Map.of(
                PARTITION_GROUP_NAME, partitionsWorkDir.metaPath(),
                MetastorageGroupId.INSTANCE.toString(), metastorageWorkDir.metaPath(),
                CmgGroupId.INSTANCE.toString(), cmgWorkDir.metaPath()
        );

        return new GroupStoragesContextResolver(
                replicationGroupId -> replicationGroupId instanceof PartitionGroupId
                        ? PARTITION_GROUP_NAME : replicationGroupId.toString(),
                serverDataPathByGroupName,
                logStorageManagerByGroupName
        );
    }

}
