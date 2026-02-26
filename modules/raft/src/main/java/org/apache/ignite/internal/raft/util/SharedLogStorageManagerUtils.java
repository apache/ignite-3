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

package org.apache.ignite.internal.raft.util;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import org.apache.ignite.internal.IgniteNodeDetails;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftExtensionConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageManager;
import org.apache.ignite.internal.raft.storage.logit.LogitLogStorageManager;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.jetbrains.annotations.TestOnly;

/** Utility methods for creating {@link LogStorageManager}is for the Shared Log. */
@Factory
public class SharedLogStorageManagerUtils {
    /**
     * Enables logit log storage. {@code false} by default. This is a temporary property, that should only be used for testing and comparing
     * the two storages.
     */
    public static final String LOGIT_STORAGE_ENABLED_PROPERTY = "LOGIT_STORAGE_ENABLED";

    private static final boolean LOGIT_STORAGE_ENABLED_PROPERTY_DEFAULT = false;

    /**
     * Creates a LogStorageManager with {@link DefaultLogStorageManager} or {@link LogitLogStorageManager} implementation depending on
     * LOGIT_STORAGE_ENABLED_PROPERTY and fsync set to true.
     */
    @TestOnly
    public static LogStorageManager create(String nodeName, Path logStoragePath) {
        return create("test", nodeName, logStoragePath, true);
    }

    /**
     * Creates a LogStorageManager with {@link DefaultLogStorageManager} or {@link LogitLogStorageManager} implementation depending on
     * LOGIT_STORAGE_ENABLED_PROPERTY.
     */
    public static LogStorageManager create(String factoryName, String nodeName, Path logStoragePath, boolean fsync) {
        return IgniteSystemProperties.getBoolean(LOGIT_STORAGE_ENABLED_PROPERTY, LOGIT_STORAGE_ENABLED_PROPERTY_DEFAULT)
                ? new LogitLogStorageManager(nodeName, new StoreOptions(), logStoragePath)
                : new DefaultLogStorageManager(factoryName, nodeName, logStoragePath, fsync);
    }

    private static LogStorageManager create(String factoryName, String nodeName, Path raftWorkDir, ConfigurationRegistry config) {
        RaftConfiguration raftConfiguration = config.getConfiguration(RaftExtensionConfiguration.KEY).raft();

        return create(factoryName, nodeName, raftWorkDir, raftConfiguration.fsync().value());
    }

    /**
     * Creates a {@link LogStorageManager} instance for managing log storage specific to table data partitions.
     * This storage manager is responsible for persisting logs related to table partition operations, utilizing
     * the specified configuration registry and working directory.
     *
     * @param nodeDetails Details of the Ignite node, including its name and working directory.
     * @param configRegistry The configuration registry used to configure the log storage manager.
     * @param workingDir The working directory specific to partitions for resolving the path to the log storage.
     * @return A {@link LogStorageManager} configured for managing table partition logs.
     */
    @Singleton
    @Inject
    public static LogStorageManager partitionsLogStorageManager(
            IgniteNodeDetails nodeDetails,
            ConfigurationRegistry configRegistry,
            @Named("partitions") ComponentWorkingDir workingDir
    ) {
        return create("table data log", nodeDetails.nodeName(), workingDir.raftLogPath(), configRegistry);
    }

    /**
     * Creates a {@link LogStorageManager} instance for managing the MetaStorage (MS) log storage.
     * This log storage manages logs critical for the MetaStorage subsystem.
     *
     * @param nodeDetails Details of the Ignite node, including its name and working directory.
     * @param workingDir  The working directory of the component, used to resolve the path for the raft log storage.
     * @return A {@link LogStorageManager} configured to manage the MS log storage.
     */
    @Singleton
    @Inject
    public static LogStorageManager msLogStorageManager(
            IgniteNodeDetails nodeDetails,
            @Named("partitions") ComponentWorkingDir workingDir
    ) {
        return create("meta-storage log", nodeDetails.nodeName(), workingDir.raftLogPath(), true);
    }

    /**
     * Creates a {@link LogStorageManager} instance for managing the log storage of the cluster management group (CMG) log.
     * This log storage is used to persist logs critical for the cluster management group.
     *
     * @param nodeDetails Details of the Ignite node, including its name and working directory.
     * @param workingDir  The working directory of the component, used to resolve the path for the raft log storage.
     * @return A {@link LogStorageManager} configured to manage the CMG log storage.
     */
    @Singleton
    @Inject
    public static LogStorageManager cmgLogStorageManager(
            IgniteNodeDetails nodeDetails,
            @Named("partitions") ComponentWorkingDir workingDir
    ) {
        return create("cluster-management-group log", nodeDetails.nodeName(), workingDir.raftLogPath(), true);
    }
}
