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

package org.apache.ignite.internal.rest.api.compute;

import static io.swagger.v3.oas.annotations.media.Schema.RequiredMode.REQUIRED;
import static org.apache.ignite.internal.rest.constants.MediaType.APPLICATION_JSON;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Put;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.Problem;

/**
 * API for managing compute tasks.
 */
@Controller("/management/v1/compute/")
@Tag(name = "compute")
public interface ComputeApi {
    /**
     * Retrieves the states of all compute jobs.
     *
     * @return A collection of compute job states.
     */
    @Operation(summary = "Retrieve all job states", description = "Fetches the current states of all compute jobs.")
    @ApiResponse(
            responseCode = "200",
            description = "Successful retrieval of job states.",
            content = @Content(mediaType = APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = JobState.class)))
    )
    @Get("jobs")
    CompletableFuture<Collection<JobState>> jobStates();

    /**
     * Retrieves the state of a specific compute job.
     *
     * @param jobId The unique identifier of the compute job.
     * @return The state of the specified compute job.
     */
    @Operation(summary = "Retrieve a job state", description = "Fetches the current state of a specific compute job identified by jobId.")
    @ApiResponse(
            responseCode = "200",
            description = "Successful retrieval of the job state.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = JobState.class))
    )
    @ApiResponse(
            responseCode = "404",
            description = "Compute job not found.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Get("jobs/{jobId}")
    CompletableFuture<JobState> jobState(
            @Schema(name = "jobId", description = "The unique identifier of the compute job.", requiredMode = REQUIRED) UUID jobId
    );

    /**
     * Updates the priority of a compute job.
     *
     * @param jobId The unique identifier of the compute job.
     * @param updateJobPriorityBody The new priority data for the job.
     * @return The result of the operation.
     */
    @Operation(summary = "Update a job's priority", description = "Updates the priority of a specific compute job identified by jobId.")
    @ApiResponse(
            responseCode = "200",
            description = "Successful update of the job priority.",
            content = @Content(mediaType = APPLICATION_JSON)
    )
    @ApiResponse(
            responseCode = "404",
            description = "Compute job not found.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(
            responseCode = "409",
            description = "Compute job is in an illegal state.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Put("jobs/{jobId}/priority")
    CompletableFuture<Void> updatePriority(
            @Schema(name = "jobId", description = "The unique identifier of the compute job.", requiredMode = REQUIRED) UUID jobId,
            @Body UpdateJobPriorityBody updateJobPriorityBody
    );

    /**
     * Cancels a specific compute job.
     *
     * @param jobId The unique identifier of the compute job.
     * @return The result of the cancellation operation.
     */
    @Operation(summary = "Cancel a job", description = "Cancels a specific compute job identified by jobId.")
    @ApiResponse(
            responseCode = "200",
            description = "Successful cancellation of the job.",
            content = @Content(mediaType = APPLICATION_JSON)
    )
    @ApiResponse(
            responseCode = "404",
            description = "Compute job not found.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @ApiResponse(
            responseCode = "409",
            description = "Compute job is in an illegal state.",
            content = @Content(mediaType = APPLICATION_JSON, schema = @Schema(implementation = Problem.class))
    )
    @Delete("jobs/{jobId}")
    CompletableFuture<Void> cancelJob(
            @Schema(name = "jobId", description = "The unique identifier of the compute job.", requiredMode = REQUIRED) UUID jobId
    );
}
