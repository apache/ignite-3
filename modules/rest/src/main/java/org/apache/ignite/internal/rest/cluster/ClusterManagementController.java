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

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.api.cluster.ClusterManagementApi;
import org.apache.ignite.internal.rest.api.cluster.ClusterState;
import org.apache.ignite.internal.rest.api.cluster.ClusterTag;
import org.apache.ignite.internal.rest.api.cluster.InitCommand;
import org.apache.ignite.internal.rest.cluster.exception.InvalidArgumentClusterInitializationException;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteException;

/**
 * Cluster management controller implementation.
 */
@Controller("/management/v1/cluster")
public class ClusterManagementController implements ClusterManagementApi {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterManagementController.class);

    private final ClusterInitializer clusterInitializer;

    private final ClusterManagementGroupManager clusterManagementGroupManager;

    /**
     * Cluster management controller constructor.
     *
     * @param clusterInitializer cluster initializer.
     * @param clusterManagementGroupManager cluster management group manager.
     */
    public ClusterManagementController(
            ClusterInitializer clusterInitializer,
            ClusterManagementGroupManager clusterManagementGroupManager
    ) {
        this.clusterInitializer = clusterInitializer;
        this.clusterManagementGroupManager = clusterManagementGroupManager;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ClusterState> clusterState() {
        return clusterManagementGroupManager.clusterState().thenApply(ClusterManagementController::mapClusterState);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> init(@Body InitCommand initCommand) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Received init command [metaStorageNodes={}, cmgNodes={}]", initCommand.metaStorageNodes(),
                    initCommand.cmgNodes());
        }

        return clusterInitializer.initCluster(
                initCommand.metaStorageNodes(),
                initCommand.cmgNodes(),
                initCommand.clusterName(),
                initCommand.clusterConfiguration()
        ).exceptionally(ex -> {
            throw mapException(ex);
        });
    }

    private static ClusterState mapClusterState(org.apache.ignite.internal.cluster.management.ClusterState clusterState) {
        return new ClusterState(
                clusterState.cmgNodes(),
                clusterState.metaStorageNodes(),
                clusterState.igniteVersion().toString(),
                new ClusterTag(clusterState.clusterTag().clusterName(), clusterState.clusterTag().clusterId())
        );
    }

    private static RuntimeException mapException(Throwable ex) {
        var cause = ExceptionUtils.unwrapCause(ex);
        if (cause instanceof IgniteInternalException) {
            return (IgniteInternalException) cause;
        } else if (cause instanceof IllegalArgumentException || cause instanceof ConfigurationValidationException) {
            return new InvalidArgumentClusterInitializationException(cause);
        } else if (cause instanceof IgniteException) {
            return (RuntimeException) cause;
        } else {
            return new IgniteException(cause);
        }
    }
}
