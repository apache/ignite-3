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

package org.apache.ignite.internal.rest.api.configuration;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Patch;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;

/**
 * Cluster configuration controller.
 */
@Controller("/management/v1/configuration/cluster/")
@Tag(name = "clusterConfiguration")
public interface ClusterConfigurationApi {
    /**
     * Returns cluster configuration in HOCON format. This is represented as a plain text.
     *
     * @return the whole cluster configuration in HOCON format.
     */
    @Operation(
            operationId = "getClusterConfiguration",
            summary = "Get cluster configuration",
            description = "Gets the current configuration of the cluster. The configuration is returned in HOCON format."
    )
    @ApiResponse(
            responseCode = "200", description = "Received cluster configuration.",
            content = @Content(mediaType = MediaType.TEXT_PLAIN, schema = @Schema(type = "string")))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Incorrect configuration.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "404", description = "Configuration not found. Most likely, the cluster is not initialized.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.PROBLEM_JSON)
    @Get
    String getConfiguration();

    /**
     * Updates cluster configuration in HOCON format. This is represented as a plain text.
     *
     * @param updatedConfiguration the cluster configuration to update.
     */
    @Operation(
            operationId = "updateClusterConfiguration",
            summary = "Update cluster configuration",
            description = "Updates cluster configuration. New configuration should be provided in HOCON format."
    )
    @ApiResponse(responseCode = "200", description = "Configuration updated.")
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Incorrect configuration.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "404", description = "Configuration not found. Most likely, the cluster is not initialized.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "422", description = "Configuration parse error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.PROBLEM_JSON)
    @Patch
    CompletableFuture<Void> updateConfiguration(
            @Body @RequestBody(description = "The cluster configuration to update.") String updatedConfiguration
    );

    /**
     * Returns configuration in HOCON format represented by path. This is represented as a plain text.
     *
     * @param path to represent a cluster configuration.
     * @return system configuration in HOCON format represented by given path.
     */
    @Operation(
            operationId = "getClusterConfigurationByPath",
            summary = "Get configuration represented by path",
            description = "Gets the configuration on the specific path. Configuration is in HOCON format"
    )
    @ApiResponse(responseCode = "200", description = "Configuration of the cluster on the specified path.",
            content = @Content(mediaType = MediaType.TEXT_PLAIN, schema = @Schema(type = "string")))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Incorrect configuration.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "404", description = "Configuration not found. Most likely, the cluster is not initialized.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.PROBLEM_JSON)
    @Get("/{path}")
    String getConfigurationByPath(@PathVariable("path") @Parameter(required = true,
            description = "Configuration tree address. For example: `element.subelement`.") String path);
}
