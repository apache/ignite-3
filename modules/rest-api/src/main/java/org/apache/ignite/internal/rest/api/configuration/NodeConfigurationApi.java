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
 * Node configuration controller.
 */
@Controller("/management/v1/configuration/node")
@Tag(name = "nodeConfiguration")
public interface NodeConfigurationApi {
    /**
     * Returns node configuration in HOCON format. This is represented as a plain text.
     *
     * @return the whole node configuration in HOCON format.
     */
    @Operation(operationId = "getNodeConfiguration", description = "Gets node configuration in HOCON format.")
    @ApiResponse(responseCode = "200", description = "Full node configuration.",
            content = @Content(mediaType = MediaType.TEXT_PLAIN, schema = @Schema(type = "string")))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Incorrect configuration.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.PROBLEM_JSON)
    @Get
    String getConfiguration();

    /**
     * Returns configuration in HOCON format represented by path. This is represented as a plain text.
     *
     * @param path to represent a node configuration.
     * @return system configuration in HOCON format represented by given path.
     */
    @Operation(operationId = "getNodeConfigurationByPath",
            description = "Gets a configuration of a specific node, in HOCON format.")
    @ApiResponse(responseCode = "200",
            content = @Content(mediaType = MediaType.TEXT_PLAIN, schema = @Schema(type = "string")),
            description = "Returned node configuration.")
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Incorrect configuration.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.PROBLEM_JSON)
    @Get("/{path}")
    String getConfigurationByPath(@PathVariable("path") @Parameter(required = true,
            description = "Configuration tree address. For example: `element.subelement`.") String path);

    /**
     * Updates node configuration in HOCON format. This is represented as a plain text.
     *
     * @param updatedConfiguration the node configuration to update. This is represented as a plain text.
     */
    @Operation(operationId = "updateNodeConfiguration", description = "Updates node configuration. "
            + "New configuration should be provided in HOCON format.")
    @ApiResponse(responseCode = "200", description = "Configuration successfully updated.")
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Incorrect configuration.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.PROBLEM_JSON)
    @Patch
    CompletableFuture<Void> updateConfiguration(
            @Body @RequestBody(description = "The node configuration to update.") String updatedConfiguration
    );
}
