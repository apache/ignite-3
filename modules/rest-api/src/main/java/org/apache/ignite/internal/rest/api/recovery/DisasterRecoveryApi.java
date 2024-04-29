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

package org.apache.ignite.internal.rest.api.recovery;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;

/**
 * Disaster recovery controller.
 */
@Controller("/management/v1/recovery")
@Tag(name = "recovery")
public interface DisasterRecoveryApi {
    @Get("state/local")
    @Operation(operationId = "getLocalPartitionStates", description = "Returns local partition states.")
    @ApiResponse(responseCode = "200", description = "Partition states returned.")
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Bad request.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.APPLICATION_JSON)
    CompletableFuture<LocalPartitionStatesResponse> getLocalPartitionStates(
            @QueryValue
            @Schema(description = "Names specifying zones to get partition states from. All zones if empty.")
            Optional<Set<String>> zoneNames,
            @QueryValue
            @Schema(description = "Names specifying nodes to get partition states from. All nodes if empty.")
            Optional<Set<String>> nodeNames,
            @QueryValue
            @Schema(description = "IDs of partitions to get states. All partitions if empty.") Optional<Set<Integer>> partitionIds
    );

    @Get("state/global")
    @Operation(operationId = "getGlobalPartitionStates", description = "Returns global partition states.")
    @ApiResponse(responseCode = "200", description = "Partition states returned.")
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Bad request.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.APPLICATION_JSON)
    CompletableFuture<GlobalPartitionStatesResponse> getGlobalPartitionStates(
            @QueryValue
            @Schema(description = "Names specifying zones to get partition states from. All zones if empty.")
            Optional<Set<String>> zoneNames,
            @QueryValue
            @Schema(description = "IDs of partitions to get states of. All partitions if empty.")
            Optional<Set<Integer>> partitionIds
    );
}
