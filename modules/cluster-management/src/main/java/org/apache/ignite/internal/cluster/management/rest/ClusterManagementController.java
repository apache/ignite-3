/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cluster.management.rest;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.rest.exception.InvalidArgumentClusterInitializationException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Cluster management controller.
 */
@Controller("/management/v1/cluster")
@Tag(name = "clusterManagement")
public class ClusterManagementController {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterManagementController.class);

    private final ClusterInitializer clusterInitializer;

    private final ClusterManagementGroupManager clusterManagementGroupManager;

    /**
     * Cluster management controller constructor.
     *
     * @param clusterInitializer cluster initializer.
     * @param clusterManagementGroupManager cluster management group manager.
     */
    public ClusterManagementController(ClusterInitializer clusterInitializer, ClusterManagementGroupManager clusterManagementGroupManager) {
        this.clusterInitializer = clusterInitializer;
        this.clusterManagementGroupManager = clusterManagementGroupManager;
    }

    @Get("state")
    @Operation(operationId = "clusterState")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Return cluster state",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ClusterState.class))),
            @ApiResponse(responseCode = "404", description = "Cluster state not found, it means that the cluster is not initialized"),
            @ApiResponse(responseCode = "500", description = "Internal error",
                    content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    })
    @Produces({
            MediaType.APPLICATION_JSON,
            MediaType.PROBLEM_JSON
    })
    public CompletableFuture<org.apache.ignite.internal.cluster.management.rest.ClusterState> clusterState()
            throws ExecutionException, InterruptedException {
        return clusterManagementGroupManager.clusterState().thenApply(this::mapClusterState);
    }

    /**
     * Initializes cluster.
     *
     * @return Completable future that will be completed when cluster is initialized.
     */
    @Post("init")
    @Operation(operationId = "init")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Cluster initialized"),
            @ApiResponse(responseCode = "500", description = "Internal error",
                    content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class))),
            @ApiResponse(responseCode = "400", description = "Incorrect configuration",
                    content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))

    })
    @Consumes(MediaType.APPLICATION_JSON)
    public CompletableFuture<Void> init(@Body InitCommand initCommand) throws ExecutionException, InterruptedException {
        if (LOG.isInfoEnabled()) {
            LOG.info("Received init command [metaStorageNodes={}, cmgNodes={}]", initCommand.metaStorageNodes(),
                    initCommand.cmgNodes());
        }

        return clusterInitializer.initCluster(initCommand.metaStorageNodes(), initCommand.cmgNodes(), initCommand.clusterName())
                .exceptionally(ex -> {
                    throw mapException(ex);
                });
    }

    private org.apache.ignite.internal.cluster.management.rest.ClusterState mapClusterState(ClusterState clusterState) {
        if (clusterState == null) {
            return null;
        }

        return new org.apache.ignite.internal.cluster.management.rest.ClusterState(
                clusterState.cmgNodes(),
                clusterState.metaStorageNodes(),
                new IgniteProductVersion(clusterState.igniteVersion().major(), clusterState.igniteVersion().minor(),
                        clusterState.igniteVersion().maintenance(), clusterState.igniteVersion().snapshot(),
                        clusterState.igniteVersion().alphaVersion()),
                new ClusterTag(clusterState.clusterTag().clusterName(), clusterState.clusterTag().clusterId()));
    }

    private RuntimeException mapException(Throwable ex) {
        if (ex instanceof CompletionException) {
            var cause = ex.getCause();
            if (cause instanceof IgniteInternalException) {
                return (IgniteInternalException) cause;
            }
        }

        if (ex instanceof IllegalArgumentException) {
            return new InvalidArgumentClusterInitializationException(ex);
        }

        return new IgniteException(ex);
    }
}
