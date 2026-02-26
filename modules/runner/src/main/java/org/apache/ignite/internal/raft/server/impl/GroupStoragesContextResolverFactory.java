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

package org.apache.ignite.internal.raft.server.impl;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.Map;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.replicator.PartitionGroupId;

/**
 * Factory class for creating instances of {@link GroupStoragesContextResolver}.
 *
 * <p>This factory provides a concrete implementation of the {@link GroupStoragesContextResolver} class,
 * which facilitates the resolution of {@link LogStorageManager} instances and server data paths for various
 * replication groups. It is designed to handle storage and context resolution needs for specific
 * replication group use cases, including partition groups, metastorage groups, and configuration management groups (CMG).
 *
 * <p>This class supports the dependency injection mechanism and initializes the mappings between group names,
 * storage managers, and metadata paths required for resolving contexts during operation.
 */
@Factory
public class GroupStoragesContextResolverFactory {    /** Used for durable destruction purposes. */
    private static final String PARTITION_GROUP_NAME = "partition";

    /**
     * Creates an instance of {@link GroupStoragesContextResolver} to manage the associations
     * between replication group names, their corresponding {@link LogStorageManager} instances,
     * and server data paths for various group contexts such as partition, metastorage, and CMG.
     *
     * @param partitionsLogStorageManager The {@link LogStorageManager} for partition groups.
     * @param msLogStorageManager The {@link LogStorageManager} for metastorage groups.
     * @param cmgLogStorageManager The {@link LogStorageManager} for configuration management groups (CMG).
     * @param partitionsWorkDir The {@link ComponentWorkingDir} containing metadata paths for partition groups.
     * @param metastorageWorkDir The {@link ComponentWorkingDir} containing metadata paths for metastorage groups.
     * @param cmgWorkDir The {@link ComponentWorkingDir} containing metadata paths for CMG groups.
     * @return A {@link GroupStoragesContextResolver} instance configured to resolve storage contexts for the provided replication groups.
     */
    @Singleton
    @Inject
    public static GroupStoragesContextResolver createGroupStoragesContextResolver(
            LogStorageManager partitionsLogStorageManager,
            LogStorageManager msLogStorageManager,
            LogStorageManager cmgLogStorageManager,
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
                replicationGroupId -> replicationGroupId instanceof PartitionGroupId ? PARTITION_GROUP_NAME : replicationGroupId.toString(),
                serverDataPathByGroupName,
                logStorageManagerByGroupName
        );
    }
}
