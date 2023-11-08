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

package org.apache.ignite.internal.rest.api.node;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;

/**
 * REST endpoint allows to read node state.
 */
@Controller("/management/v1/node")
@Tag(name = "nodeManagement")
public interface NodeManagementApi {

    @Get("state")
    @Operation(operationId = "nodeState", description = "Gets current network status.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Current node status.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = NodeState.class))),
            @ApiResponse(responseCode = "500", description = "Internal error.",
                    content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    })
    NodeState state();

    @Get("info")
    @Operation(operationId = "nodeInfo", description = "Gets node info.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Node info.",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = NodeInfo.class))),
            @ApiResponse(responseCode = "500", description = "Internal error.",
                    content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    })
    NodeInfo info();

    @Get("version")
    @Operation(operationId = "nodeVersion", description = "Gets the version of Apache Ignite the node uses.")
    @ApiResponse(responseCode = "200", description = "Node version.",
            content = @Content(mediaType = MediaType.TEXT_PLAIN, schema = @Schema(type = "string")))
    @ApiResponse(responseCode = "500", description = "Internal error",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.PROBLEM_JSON)
    String version();
}
