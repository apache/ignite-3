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

package org.apache.ignite.internal.rest.api.metric;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Collection;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;

/** Node metric endpoint. */
@Controller("/management/v1/metric/node")
@Tag(name = "nodeMetric")
public interface NodeMetricApi {

    /** Enable metric source. */
    @Operation(operationId = "enableNodeMetric", description = "Enables the specified metric source.")
    @ApiResponse(responseCode = "200", description = "Metric source enabled.")
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "404", description = "Metric source not found.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.PROBLEM_JSON)
    @Post("enable")
    void enable(@Body String srcName);

    /** Disable metric source. */
    @Operation(operationId = "disableNodeMetric", description = "Disables the specified metric source.")
    @ApiResponse(responseCode = "200", description = "Metric source disabled.")
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @ApiResponse(responseCode = "404", description = "Metric source not found.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.PROBLEM_JSON)
    @Post("disable")
    void disable(@Body String srcName);

    /** List metric sources. */
    @Operation(operationId = "listNodeMetricSources", description = "Gets a list of all available metric sources.")
    @ApiResponse(responseCode = "200", description = "Returned a list of metric sources.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                    array = @ArraySchema(schema = @Schema(implementation = MetricSource.class))))
    @ApiResponse(responseCode = "500", description = "Internal error.",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Get("source")
    Collection<MetricSource> listMetricSources();

    /** List metric sets. */
    @Operation(operationId = "listNodeMetricSets", description = "Gets a list of all enabled metric sets.")
    @ApiResponse(responseCode = "200", description = "Returned a list of metric sets.",
            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                    array = @ArraySchema(schema = @Schema(implementation = MetricSet.class))))
    @ApiResponse(responseCode = "500", description = "Internal error",
            content = @Content(mediaType = MediaType.PROBLEM_JSON, schema = @Schema(implementation = Problem.class)))
    @Get("set")
    Collection<MetricSet> listMetricSets();
}
