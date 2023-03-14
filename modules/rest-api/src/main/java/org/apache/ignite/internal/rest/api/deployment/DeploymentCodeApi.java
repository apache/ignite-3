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
import static org.apache.ignite.internal.rest.constants.MediaType.PROBLEM_JSON;

import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.multipart.CompletedFileUpload;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;

/**
 * REST endpoint allows to deployment code service.
 */
@Controller("/management/v1/deployment/")
@Tag(name = "deployment")
public interface DeploymentCodeApi {

    /**
     * Deploy unit REST method.
     */
    @Operation(operationId = "deployUnit", description = "Deploy provided unit to the cluster.")
    @ApiResponse(responseCode = "200", description = "Unit deployed successfully.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(type = "boolean"))
    )
    @ApiResponse(responseCode = "409", description = "Unit with same identifier and version already deployed.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(MediaType.FORM_DATA)
    @Produces({
            APPLICATION_JSON,
            PROBLEM_JSON
    })
    @Post("units")
    CompletableFuture<Boolean> deploy(
            @Schema(name = "unitId", required = true) String unitId,
            @Schema(name = "unitVersion") String unitVersion,
            @Schema(name = "unitContent", required = true) CompletedFileUpload unitContent);

    /**
     * Undeploy unit REST method.
     */
    @Operation(operationId = "undeployUnit", description = "Undeploy unit with provided unitId and unitVersion.")
    @ApiResponse(responseCode = "200",
            description = "Unit undeployed successfully.",
            //DO NOT Remove redundant parameter. It will BREAK generated spec.
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Void.class))
    )
    @ApiResponse(responseCode = "404",
            description = "Unit with provided identifier and version doesn't exist.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(APPLICATION_JSON)
    @Produces({
            APPLICATION_JSON,
            PROBLEM_JSON
    })
    @Delete("units/{unitId}/{unitVersion}")
    CompletableFuture<Void> undeploy(
            @PathVariable("unitId") @Schema(name = "unitId", required = true) String unitId,
            @PathVariable("unitVersion") @Schema(name = "unitVersion", required = true) String unitVersion);

    /**
     * Undeploy latest unit REST method.
     */
    @Operation(operationId = "undeployLatestUnit", description = "Undeploy latest unit with provided unitId.")
    @ApiResponse(responseCode = "200",
            description = "Unit undeployed successfully.",
            //DO NOT Remove redundant parameter. It will BREAK generated spec.
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Void.class))
    )
    @ApiResponse(responseCode = "404",
            description = "Unit with provided identifier and version doesn't exist.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(APPLICATION_JSON)
    @Produces({
            APPLICATION_JSON,
            PROBLEM_JSON
    })
    @Delete("units/{unitId}")
    CompletableFuture<Void> undeploy(
            @PathVariable("unitId") @Schema(name = "unitId", required = true) String unitId);

    /**
     * All units status REST method.
     */
    @Operation(operationId = "units", description = "All units statutes.")
    @ApiResponse(responseCode = "200",
            description = "All statutes returned successful.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = UnitStatusDto.class)))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces({
            APPLICATION_JSON,
            PROBLEM_JSON
    })
    @Get("units")
    CompletableFuture<Collection<UnitStatusDto>> units();

    /**
     * Versions of unit REST method.
     */
    @Operation(operationId = "versions", description = "All versions of unit with provided unit identifier.")
    @ApiResponse(responseCode = "200",
            description = "Versions returned successful.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = String.class)))
    )
    @ApiResponse(responseCode = "404",
            description = "Unit with provided identifier doesn't exist.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Consumes(APPLICATION_JSON)
    @Produces({
            APPLICATION_JSON,
            PROBLEM_JSON
    })
    @Get("units/{unitId}/versions")
    CompletableFuture<Collection<String>> versions(
            @PathVariable("unitId") @Schema(name = "unitId", required = true) String unitId);

    /**
     * Unit status REST method.
     */
    @Operation(operationId = "status", description = "Status of unit with provided identifier.")
    @ApiResponse(responseCode = "200",
            description = "Status returned successful.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = UnitStatusDto.class))
    )
    @ApiResponse(responseCode = "404",
            description = "Unit with provided identifier doesn't exist.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(APPLICATION_JSON)
    @Produces({
            APPLICATION_JSON,
            PROBLEM_JSON
    })
    @Get("units/{unitId}/status")
    CompletableFuture<UnitStatusDto> status(
            @PathVariable("unitId") @Schema(name = "unitId", required = true) String unitId);

    /**
     * Find unit by node consistent id.
     */
    @Operation(operationId = "byConsistentId", description = "Status of units which deployed on node.")
    @ApiResponse(responseCode = "200",
            description = "All statutes returned successful.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = UnitStatusDto.class)))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(APPLICATION_JSON)
    @Produces({
            APPLICATION_JSON,
            PROBLEM_JSON
    })
    @Get("units/consistentId/{consistentId}")
    CompletableFuture<Collection<UnitStatusDto>> findByConsistentId(
            @PathVariable("consistentId") @Schema(name = "consistentId", required = true) String consistentId);
}
