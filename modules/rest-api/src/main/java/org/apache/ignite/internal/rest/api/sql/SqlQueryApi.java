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

package org.apache.ignite.internal.rest.api.sql;

import static io.swagger.v3.oas.annotations.media.Schema.RequiredMode.REQUIRED;
import static org.apache.ignite.internal.rest.constants.MediaType.APPLICATION_JSON;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.Problem;

/**
 * API for managing queries.
 */
@Controller("/management/v1/sql/")
@Tag(name = "sql")
public interface SqlQueryApi {

    /**
     * Retrieves all running sql queries.
     *
     *
     * @return A collection of all running sql queries.
     */
    @Operation(summary = "Retrieve all running sql queries.", description = "Fetches all running sql queries.")
    @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved all running sql queries.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = SqlQueryInfo.class)))
    )
    @Get("queries")
    CompletableFuture<Collection<SqlQueryInfo>> queries();

    /**
     * Retrieves the sql query.
     *
     * @param queryId The unique identifier of the sql query.
     * @return The sql query.
     */
    @Operation(summary = "Retrieve sql query.", description = "Fetches the current state of a specific sql query.")
    @ApiResponse(
            responseCode = "200",
            description = "Successfully retrieved the sql query.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = SqlQueryInfo.class))
    )
    @ApiResponse(
            responseCode = "404",
            description = "Query not found.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Get("queries/{queryId}")
    CompletableFuture<SqlQueryInfo> query(
            @Schema(name = "queryId", description = "The unique identifier of the sql query.", requiredMode = REQUIRED) UUID queryId
    );

    /**
     * Kills a specific sql query.
     *
     * @param queryId The unique identifier of the sql query.
     * @return The result of the kill operation.
     */
    @Operation(summary = "Kill sql query.", description = "Kills a specific sql query identified by query id.")
    @ApiResponse(
            responseCode = "200",
            description = "Successfully killed the sql query.",
            content = @Content(mediaType = APPLICATION_JSON)
    )
    @ApiResponse(
            responseCode = "404",
            description = "Query not found.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(
            responseCode = "409",
            description = "Query is in an illegal state.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Delete("queries/{queryId}")
    CompletableFuture<Void> killQuery(
            @Schema(name = "queryId", description = "The unique identifier of the sql query.", requiredMode = REQUIRED) UUID queryId
    );

    /**
     * Invalidates SQL query planner cache.
     *
     * @return The result of the operation.
     */
    @Operation(
            summary = "Invalidates SQL planner cache.",
            description = "Invalidates SQL planner cache records on node that related to provided table names.")
    @ApiResponse(responseCode = "200", description = "Successfully cleared SQL query plan cache.")
    @Get("plan/clear-cache")
    CompletableFuture<Void> clearCache(
            @QueryValue
            @Schema(description = "SQL query plans, which are related to given tables, will be evicted from cache. Case-sensitive, "
                    + "cache will be reset if empty.")
            Optional<Set<String>> tableNames
    );
}
