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

import static io.swagger.v3.oas.annotations.media.Schema.RequiredMode.REQUIRED;
import static org.apache.ignite.internal.rest.constants.MediaType.APPLICATION_JSON;
import static org.apache.ignite.internal.rest.constants.MediaType.FORM_DATA;
import static org.apache.ignite.internal.rest.constants.MediaType.PROBLEM_JSON;

import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.multipart.CompletedFileUpload;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.api.deployment.UnitEntry.UnitFolder;
import org.reactivestreams.Publisher;

/**
 * REST endpoint allows to deployment code service.
 */
@SuppressWarnings("OptionalContainsCollection")
@Controller("/management/v1/deployment/")
@Tag(name = "deployment")
public interface DeploymentCodeApi {

    /**
     * Deploy unit REST method.
     */
    @Operation(operationId = "deployUnit", summary = "Deploy unit", description = "Deploys provided unit to the cluster.")
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
    @Post("units/{unitId}/{unitVersion}")
    CompletableFuture<Boolean> deploy(
            @Schema(name = "unitId", requiredMode = REQUIRED, description = "The ID of the deployment unit.")
            String unitId,
            @Schema(name = "unitVersion", requiredMode = REQUIRED, description = "The version of the deployment unit.")
            String unitVersion,
            @Schema(name = "unitContent", requiredMode = REQUIRED, description = "The code to deploy.")
            Publisher<CompletedFileUpload> unitContent,
            @QueryValue
            @Schema(name = "deployMode", requiredMode = REQUIRED, description = "ALL or MAJORITY.")
            Optional<InitialDeployMode> deployMode,
            @QueryValue
            @Schema(name = "initialNodes", requiredMode = REQUIRED, description = "List of node identifiers to deploy to.")
            Optional<List<String>> initialNodes
    );

    /**
     * Deploy unit with zip file REST method.
     */
    @Operation(
            operationId = "deployZipUnit",
            summary = "Deploy unit with folders structure in zip.",
            description = "Deploys provided unit in zip file to the cluster with folders structure."
    )
    @ApiResponse(responseCode = "200", description = "Unit deployed successfully.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(type = "boolean"))
    )
    @ApiResponse(responseCode = "409", description = "Unit with same identifier and version is already deployed.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(
            responseCode = "400", description = "Deployment unit with unzip supports only single zip file.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Consumes(FORM_DATA)
    @Post("units/zip/{unitId}/{unitVersion}")
    CompletableFuture<Boolean> deployZip(
            @Schema(name = "unitId", requiredMode = REQUIRED, description = "The ID of the deployment unit.")
            String unitId,
            @Schema(name = "unitVersion", requiredMode = REQUIRED, description = "The version of the deployment unit.")
            String unitVersion,
            @Schema(name = "unitContent", requiredMode = REQUIRED, description = "The zip file with unit content to deploy.")
            Publisher<CompletedFileUpload> unitContent,
            @QueryValue
            @Schema(name = "deployMode", requiredMode = REQUIRED, description = "ALL or MAJORITY.")
            Optional<InitialDeployMode> deployMode,
            @QueryValue
            @Schema(name = "initialNodes", requiredMode = REQUIRED, description = "List of node identifiers to deploy to.")
            Optional<List<String>> initialNodes
    );

    /**
     * Undeploy unit REST method.
     */
    @Operation(
            operationId = "undeployUnit",
            summary = "Undeploy unit",
            description = "Undeploys the unit with provided unitId and unitVersion."
    )
    @ApiResponse(responseCode = "200", description = "Unit undeployed successfully.")
    @ApiResponse(responseCode = "404",
            description = "Unit with provided identifier and version does not exist.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Delete("units/{unitId}/{unitVersion}")
    CompletableFuture<Boolean> undeploy(
            @Schema(name = "unitId", description = "The ID of the deployment unit.", requiredMode = REQUIRED)
            String unitId,
            @Schema(name = "unitVersion", description = "The version of the deployment unit.", requiredMode = REQUIRED)
            String unitVersion
    );

    /**
     * Cluster unit statuses REST method.
     */
    @Operation(operationId = "listClusterStatuses", summary = "Get cluster unit statuses", description = "Cluster unit statuses.")
    @ApiResponse(responseCode = "200",
            description = "All statuses returned successfully.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = UnitStatus.class)))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Get("cluster/units")
    CompletableFuture<Collection<UnitStatus>> clusterStatuses(
            @Schema(name = "statuses", description = "Deployment status filter.")
            Optional<List<DeploymentStatus>> statuses
    );

    /**
     * Cluster unit statuses REST method.
     */
    @Operation(
            operationId = "listClusterStatusesByUnit",
            summary = "Get specific cluster unit statuses",
            description = "Cluster unit statuses by unit."
    )
    @ApiResponse(responseCode = "200",
            description = "All statuses returned successfully.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = UnitStatus.class)))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Get("cluster/units/{unitId}")
    CompletableFuture<Collection<UnitStatus>> clusterStatuses(
            @Schema(name = "unitId", description = "The ID of the deployment unit.")
            String unitId,
            @Schema(name = "version", description = "Unit version filter.")
            Optional<String> version,
            @Schema(name = "statuses", description = "Deployment status filter.")
            Optional<List<DeploymentStatus>> statuses
    );

    /**
     * Node unit statuses REST method.
     */
    @Operation(
            operationId = "listNodeStatuses",
            summary = "Get node unit statuses",
            description = "Returns a list of unit statuses per node."
    )
    @ApiResponse(responseCode = "200",
            description = "All statuses were returned successfully.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = UnitStatus.class)))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Get("node/units")
    CompletableFuture<Collection<UnitStatus>> nodeStatuses(
            @Schema(name = "statuses", description = "Deployment status filter.")
            Optional<List<DeploymentStatus>> statuses
    );

    /**
     * Node unit statuses REST method.
     */
    @Operation(
            operationId = "listNodeStatusesByUnit",
            summary = "Get specific node unit statuses",
            description = "Returns a list of node unit statuses by unit."
    )
    @ApiResponse(responseCode = "200",
            description = "All statuses returned successfully.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = UnitStatus.class)))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Get("node/units/{unitId}")
    CompletableFuture<Collection<UnitStatus>> nodeStatuses(
            @Schema(name = "unitId", description = "The ID of the deployment unit.")
            String unitId,
            @Schema(name = "version", description = "Unit version filter.")
            Optional<String> version,
            @Schema(name = "statuses", description = "Deployment status filter.")
            Optional<List<DeploymentStatus>> statuses
    );

    /**
     * Unit content REST method.
     */
    @Operation(
            operationId = "unitContent",
            summary = "Get unit contents.",
            description = "Returns a folder representation with unit content."
    )
    @ApiResponse(responseCode = "200",
            description = "Unit content returned successfully.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = UnitFolder.class))
    )
    @ApiResponse(responseCode = "500",
            description = "Internal error.",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Get("node/units/structure/{unitId}/{unitVersion}")
    CompletableFuture<UnitFolder> unitStructure(
            @Schema(name = "unitId", description = "The ID of the deployment unit.")
            String unitId,
            @Schema(name = "unitVersion", description = "The version of the deployment unit.")
            String unitVersion
    );
}
