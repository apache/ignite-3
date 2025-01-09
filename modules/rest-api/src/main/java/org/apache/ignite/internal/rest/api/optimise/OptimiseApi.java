package org.apache.ignite.internal.rest.api.optimise;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;

@Controller("/management/v1/optimise")
@Tag(name = "optimise")
public interface OptimiseApi {
    @Post("runBenchmark")
    @Operation(operationId = "runBenchmark", summary = "Run benchmark", description = "Runs benchmark.")
    @ApiResponse(responseCode = "200", description = "Benchmark started.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = UUID.class)))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    CompletableFuture<UUID> runBenchmark(
            @Body RunBenchmarkRequest runBenchmarkRequest
    );

    @Post("optimise")
    @Operation(operationId = "optimise", summary = "Run optimisation", description = "Runs optimisation.")
    @ApiResponse(responseCode = "200", description = "Optimisation started.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = UUID.class)))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    CompletableFuture<UUID> optimise(@Body RunOptimisationRequest runOptimisationRequest);

    @Get("optimisationResult")
    @Operation(operationId = "optimisationResult", summary = "Get result", description = "Returns optimisation or benchmark result.")
    @ApiResponse(responseCode = "200", description = "Got result.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = String.class)))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    CompletableFuture<String> optimisationResult(UUID id);
}
