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

package org.apache.ignite.internal.rest.api.zone;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
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
 * Data nodes of distribution zones controller.
 */
@Controller("/management/v1/zones")
@Tag(name = "dataNodes")
public interface DataNodesApi {
    @Get("{zoneName}/datanodes")
    @Operation(
            operationId = "getDataNodesForZone",
            description = "Returns current data nodes for zone."
    )
    @ApiResponse(
            responseCode = "200",
            description = "Data nodes are returned.",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    array = @ArraySchema(schema = @Schema(implementation = String.class), uniqueItems = true)
            )
    )
    @ApiResponse(responseCode = "400", description = "Bad request.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces({ MediaType.APPLICATION_JSON, MediaType.PROBLEM_JSON })
    CompletableFuture<Set<String>> getDataNodesForZone(
            @PathVariable
            @Schema(description = "Case-sensitive zone name to return datanodes for.")
            String zoneName
    );

    @Post("{zoneName}/datanodes/reset")
    @Operation(
            operationId = "resetDataNodesForZone",
            description = "Recalculates and resets data nodes for zone."
    )
    @ApiResponse(responseCode = "200", description = "Data nodes are recalculated and reset.")
    @ApiResponse(responseCode = "400", description = "Bad request.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.PROBLEM_JSON)
    CompletableFuture<Void> resetDataNodesForZone(
            @PathVariable
            @Schema(description = "Case-sensitive zone name to recalculate datanodes for.")
            String zoneName
    );

    @Post("/datanodes/reset")
    @Operation(
            operationId = "resetDataNodesForZones",
            description = "Recalculates and resets data nodes for given zones."
    )
    @ApiResponse(responseCode = "200", description = "Data nodes are recalculated and reset.")
    @ApiResponse(responseCode = "400", description = "Bad request.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Produces(MediaType.PROBLEM_JSON)
    CompletableFuture<Void> resetDataNodesForZones(
            @Body
            @Schema(description = "Names specifying zones to recalculate data nodes for. Case-sensitive, "
                    + "if empty then all zones' data nodes will be recalculated.")
            Optional<Set<String>> zoneNames
    );
}
