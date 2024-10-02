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

package org.apache.ignite.internal.rest.api.recovery.system;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
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
 * Controller for system groups disaster recovery.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-128%3A+CMG+and+Metastorage+Disaster+Recovery">IEP-128</a>
 */
@Controller("/management/v1/recovery/cluster")
@Tag(name = "system")
@Tag(name = "recovery")
public interface SystemDisasterRecoveryApi {
    @Post("reset")
    @Operation(
            operationId = "resetCluster",
            description = "Initiates cluster reset to repair CMG/Metastorage group/both."
    )
    @ApiResponse(responseCode = "200", description = "Cluster reset initiated.")
    // TODO: IGNITE-23274 - introduce a constant for common error descriptions.
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Bad request.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces({MediaType.APPLICATION_JSON, MediaType.PROBLEM_JSON})
    CompletableFuture<Void> reset(@Body ResetClusterRequest command);

    @Post("migrate")
    @Operation(
            operationId = "migrate",
            description = "Migrates nodes from old cluster to new (repaired) cluster."
    )
    @ApiResponse(responseCode = "200", description = "Migration initiated.")
    // TODO: IGNITE-23274 - introduce a constant for common error descriptions.
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "400", description = "Bad request.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces({MediaType.APPLICATION_JSON, MediaType.PROBLEM_JSON})
    CompletableFuture<Void> migrate(@Body MigrateRequest command);
}
