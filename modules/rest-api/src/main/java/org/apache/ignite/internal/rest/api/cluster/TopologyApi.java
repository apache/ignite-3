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

package org.apache.ignite.internal.rest.api.cluster;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Collection;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;

/**
 * Cluster topology endpoint.
 */
@Controller("/management/v1/cluster/topology")
@Tag(name = "topology")
public interface TopologyApi {
    /**
     * Cluster physical topology.
     */
    @Get("physical")
    @Operation(operationId = "physical")
    @ApiResponse(responseCode = "200", description = "Physical topology returned")
    @ApiResponse(responseCode = "500", description = "Internal error",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.APPLICATION_JSON)
    Collection<ClusterNodeDto> physicalTopology();

    /**
     * Cluster logical topology.
     */
    @Get("logical")
    @Operation(operationId = "logical")
    @ApiResponse(responseCode = "200", description = "Logical topology returned")
    @ApiResponse(responseCode = "404", description = "Logical topology not found, it means that the cluster is not initialized",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "500", description = "Internal error",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.APPLICATION_JSON)
    Collection<ClusterNodeDto> logicalTopology();
}
