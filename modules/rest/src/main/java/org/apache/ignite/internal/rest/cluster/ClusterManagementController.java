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

package org.apache.ignite.internal.rest.cluster;

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Rest.CLUSTER_NOT_INIT_ERR;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.cluster.ClusterManagementApi;
import org.apache.ignite.internal.rest.api.cluster.ClusterState;
import org.apache.ignite.internal.rest.api.cluster.ClusterTag;
import org.apache.ignite.internal.rest.api.cluster.InitCommand;
import org.apache.ignite.internal.rest.cluster.exception.InvalidArgumentClusterInitializationException;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Cluster management controller implementation.
 */
@Controller("/management/v1/cluster")
public class ClusterManagementController implements ClusterManagementApi, ResourceHolder {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterManagementController.class);

    private ClusterInitializer clusterInitializer;

    private ClusterManagementGroupManager clusterManagementGroupManager;

    private JoinFutureProvider joinFutureProvider;

    /**
     * Cluster management controller constructor.
     *
     * @param clusterInitializer Cluster initializer.
     * @param clusterManagementGroupManager Cluster management group manager.
     * @param joinFutureProvider Node join future provider.
     */
    public ClusterManagementController(
            ClusterInitializer clusterInitializer,
            ClusterManagementGroupManager clusterManagementGroupManager,
            JoinFutureProvider joinFutureProvider
    ) {
        this.clusterInitializer = clusterInitializer;
        this.clusterManagementGroupManager = clusterManagementGroupManager;
        this.joinFutureProvider = joinFutureProvider;
    }

    @Override
    public CompletableFuture<ClusterState> clusterState() {
        return clusterManagementGroupManager.clusterState()
                .thenApply(ClusterManagementController::mapClusterState)
                .exceptionally(ex -> {
                    throw mapException(ex);
                });
    }

    @Override
    public CompletableFuture<Void> init(@Body InitCommand initCommand) {
        LOG.info("Received init command [metaStorageNodes={}, cmgNodes={}]",
                initCommand.metaStorageNodes(), initCommand.cmgNodes()
        );

        return clusterInitializer.initCluster(
                        initCommand.metaStorageNodes(),
                        initCommand.cmgNodes(),
                        initCommand.clusterName(),
                        initCommand.clusterConfiguration()
                )
                .thenCompose(unused -> joinFutureProvider.joinFuture())
                .handle((unused, ex) -> {
                    if (ex != null) {
                        throw mapException(ex);
                    }
                    return null;
                });
    }

    @Override
    public CompletableFuture<ClusterTag> rename(String newName) {
        LOG.info("Received rename command with new name = '{}'", newName);

        return clusterManagementGroupManager.renameCluster(newName)
                .thenCompose(unused -> clusterManagementGroupManager.clusterState())
                .thenApply(ClusterManagementController::mapClusterTag)
                .exceptionally(ex -> {
                    throw mapException(ex);
                });
    }

    private static ClusterTag mapClusterTag(@Nullable org.apache.ignite.internal.cluster.management.ClusterState clusterState) {
        if (clusterState == null) {
            throw new IgniteException(
                    CLUSTER_NOT_INIT_ERR,
                    "Cluster has not yet been initialized or the node is in the process of being stopped."
            );
        }

        var clusterTag = clusterState.clusterTag();

        return new ClusterTag(clusterTag.clusterName(), clusterTag.clusterId());
    }

    private static ClusterState mapClusterState(@Nullable org.apache.ignite.internal.cluster.management.ClusterState clusterState) {
        if (clusterState == null) {
            throw new IgniteException(
                    CLUSTER_NOT_INIT_ERR,
                    "Cluster has not yet been initialized or the node is in the process of being stopped."
            );
        }

        return new ClusterState(
                clusterState.cmgNodes(),
                clusterState.metaStorageNodes(),
                clusterState.igniteVersion().toString(),
                new ClusterTag(clusterState.clusterTag().clusterName(), clusterState.clusterTag().clusterId()),
                clusterState.formerClusterIds()
        );
    }

    private static RuntimeException mapException(Throwable ex) {
        Throwable cause = ExceptionUtils.unwrapCause(ex);

        if (cause instanceof IgniteInternalException) {
            return (IgniteInternalException) cause;
        } else if (cause instanceof IllegalArgumentException || cause instanceof ConfigurationValidationException) {
            return new InvalidArgumentClusterInitializationException(cause);
        } else if (cause instanceof IgniteException) {
            return (RuntimeException) cause;
        } else {
            return new IgniteException(INTERNAL_ERR, cause);
        }
    }

    @Override
    public void cleanResources() {
        clusterInitializer = null;
        clusterManagementGroupManager = null;
        joinFutureProvider = null;
    }
}
