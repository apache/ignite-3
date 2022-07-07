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
import io.micronaut.http.annotation.Post;
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
@Controller("/management/v1/cluster/init")
@Tag(name = "clusterManagement")
public class ClusterManagementController {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterManagementController.class);

    private final ClusterInitializer clusterInitializer;

    /**
     * Cluster management controller constructor.
     *
     * @param clusterInitializer cluster initializer.
     */
    public ClusterManagementController(ClusterInitializer clusterInitializer) {
        this.clusterInitializer = clusterInitializer;
    }

    /**
     * Initializes cluster.
     *
     * @return Completable future that will be completed when cluster is initialized.
     */
    @Post
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
