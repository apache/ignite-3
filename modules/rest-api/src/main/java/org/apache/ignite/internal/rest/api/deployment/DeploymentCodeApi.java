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

package org.apache.ignite.internal.rest.api.deployment;

import static org.apache.ignite.internal.rest.constants.MediaType.APPLICATION_JSON;
import static org.apache.ignite.internal.rest.constants.MediaType.FORM_DATA;
import static org.apache.ignite.internal.rest.constants.MediaType.PROBLEM_JSON;
import static org.apache.ignite.internal.rest.constants.MediaType.TEXT_PLAIN;

import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.multipart.CompletedFileUpload;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.Problem;

/**
 * REST endpoint allows to deployment code service.
 */
@Controller("/management/v1/deployment/")
@Tag(name = "deployment")
public interface DeploymentCodeApi {

    /**
     * Deploy unit REST method.
     */
    @Operation(operationId = "deployUnit", description = "Deploys provided unit to the cluster.")
    @ApiResponse(responseCode = "200", description = "Unit deployed successfully.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(type = "boolean"))
    )
    @ApiResponse(responseCode = "409", description = "Unit with same identifier and version is already deployed.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(FORM_DATA)
    @Post("units")
    CompletableFuture<Boolean> deploy(
            @Schema(name = "unitId",
                    requiredMode = RequiredMode.REQUIRED,
                    description = "The ID of the deployment unit.") String unitId,
            @Schema(name = "unitVersion",
                    description = "The version of the deployment unit.") String unitVersion,
            @Schema(name = "unitContent",
                    requiredMode = RequiredMode.REQUIRED,
                    description = "The code to deploy.") CompletedFileUpload unitContent);

    /**
     * Undeploy unit REST method.
     */
    @Operation(operationId = "undeployUnit", description = "Undeploys the unit with provided unitId and unitVersion.")
    @ApiResponse(responseCode = "200", description = "Unit undeployed successfully.")
    @ApiResponse(responseCode = "404",
            description = "Unit with provided identifier and version does not exist.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(APPLICATION_JSON)
    @Delete("units/{unitId}/{unitVersion}")
    CompletableFuture<Void> undeploy(
            @PathVariable("unitId") @Schema(name = "unitId", description = "The ID of the deployment unit.",
                    requiredMode = RequiredMode.REQUIRED) String unitId,
            @PathVariable("unitVersion") @Schema(name = "unitVersion",
                    description = "The version of the deployment unit.", requiredMode = RequiredMode.REQUIRED) String unitVersion);

    /**
     * Undeploy latest unit REST method.
     */
    @Operation(operationId = "undeployLatestUnit", description = "Undeploys the latest unit with the provided unitId.")
    @ApiResponse(responseCode = "200", description = "Unit undeployed successfully.")
    @ApiResponse(responseCode = "404",
            description = "Unit with provided identifier and version does not exist.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(APPLICATION_JSON)
    @Delete("units/{unitId}")
    CompletableFuture<Void> undeploy(
            @PathVariable("unitId") @Schema(name = "unitId", description = "The ID of the deployment unit.",
                    requiredMode = RequiredMode.REQUIRED) String unitId);

    /**
     * All units status REST method.
     */
    @Operation(operationId = "units", description = "All units statuses.")
    @ApiResponse(responseCode = "200",
            description = "All statuses returned successfully.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = UnitStatus.class)))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(TEXT_PLAIN)
    @Get("units")
    CompletableFuture<Collection<UnitStatus>> units();

    /**
     * Versions of unit REST method.
     */
    @Operation(operationId = "versions", description = "Returns all versions of the unit with the provided unit identifier.")
    @ApiResponse(responseCode = "200",
            description = "Versions returned successfully.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = String.class)))
    )
    @ApiResponse(responseCode = "404",
            description = "Unit with the provided identifier does not exist.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Consumes(APPLICATION_JSON)
    @Get("units/{unitId}/versions")
    CompletableFuture<Collection<String>> versions(
            @PathVariable("unitId") @Schema(name = "unitId",
                    description = "The ID of the deployment unit.", requiredMode = RequiredMode.REQUIRED) String unitId);

    /**
     * Unit status REST method.
     */
    @Operation(operationId = "status", description = "Returns the status of the unit with the provided identifier.")
    @ApiResponse(responseCode = "200",
            description = "Status returned successfully.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = UnitStatus.class))
    )
    @ApiResponse(responseCode = "404",
            description = "Unit with provided identifier does not exist.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(APPLICATION_JSON)
    @Get("units/{unitId}/status")
    CompletableFuture<UnitStatus> status(
            @PathVariable("unitId") @Schema(name = "unitId",
                    description = "The ID of the deployment unit.", requiredMode = RequiredMode.REQUIRED) String unitId);

    /**
     * Find unit by node consistent id.
     */
    @Operation(operationId = "byConsistentId", description = "Returns the status of units that are deployed on the node.")
    @ApiResponse(responseCode = "200",
            description = "All statuses returned successfully.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = UnitStatus.class)))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(APPLICATION_JSON)
    @Get("units/consistentId/{consistentId}")
    CompletableFuture<Collection<UnitStatus>> findByConsistentId(
            @PathVariable("consistentId") @Schema(name = "consistentId",
                    description = "The consistent deployment unit identifier.", requiredMode = RequiredMode.REQUIRED) String consistentId);
}
