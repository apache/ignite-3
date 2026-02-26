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

package org.apache.ignite.internal.rest.api.cluster;

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
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;

/**
 * Cluster management controller.
 */
@Controller("/management/v1/cluster")
@Tag(name = "clusterManagement")
public interface ClusterManagementApi {
    /**
     * Returns cluster state.
     */
    @Get("state")
    @Operation(operationId = "clusterState", summary = "Get cluster state", description = "Returns current cluster status.")
    @ApiResponse(responseCode = "200", description = "Cluster status returned.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ClusterState.class)))
    @ApiResponse(responseCode = "404", description = "Cluster status not found. Most likely, the cluster is not initialized.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    CompletableFuture<ClusterState> clusterState();

    /**
     * Initializes cluster.
     *
     * @return Completable future that will be completed when cluster is initialized.
     */
    @Post("init")
    @Operation(operationId = "init", summary = "Initialize cluster", description = "Initialize a new cluster.")
    @ApiResponse(responseCode = "200", description = "Cluster initialized.")
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Incorrect configuration.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.PROBLEM_JSON)
    CompletableFuture<Void> init(@Body InitCommand initCommand);

    /**
     * Renames the cluster.
     *
     * @param newName the new name of the cluster.
     * @return Completable future that will be completed when cluster is renamed.
     */
    @Post("rename")
    @Operation(operationId = "rename", summary = "Rename cluster", description = "Assigns a new name to the cluster.")
    @ApiResponse(responseCode = "200", description = "Cluster renamed.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ClusterTag.class)))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Invalid name.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces({MediaType.APPLICATION_JSON, MediaType.PROBLEM_JSON})
    CompletableFuture<ClusterTag> rename(@Body String newName);
}
