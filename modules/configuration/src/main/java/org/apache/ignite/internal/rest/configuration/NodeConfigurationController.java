/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.rest.configuration;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Patch;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Named;
import org.apache.ignite.internal.configuration.rest.presentation.ConfigurationPresentation;

/**
 * Node configuration controller.
 */
@Controller("/management/v1/configuration/node")
@ApiResponse(responseCode = "400", description = "Incorrect configuration")
@ApiResponse(responseCode = "500", description = "Internal error")
@Tag(name = "nodeConfiguration")
public class NodeConfigurationController extends AbstractConfigurationController {

    public NodeConfigurationController(@Named("nodeCfgPresentation") ConfigurationPresentation<String> nodeCfgPresentation) {
        super(nodeCfgPresentation);
    }

    /**
     * Returns node configuration in HOCON format.
     *
     * @return the whole node configuration in HOCON format.
     */
    @Operation(operationId = "getNodeConfiguration")
    @ApiResponse(responseCode = "200",
            content = @Content(mediaType = MediaType.TEXT_PLAIN,
                    schema = @Schema(type = "string")),
            description = "Whole node configuration")
    @Produces(MediaType.TEXT_PLAIN)
    @Get
    public String getConfiguration() {
        return super.getConfiguration();
    }

    /**
     * Returns configuration in HOCON format represented by path.
     *
     * @param path to represent a node configuration.
     * @return system configuration in HOCON format represented by given path.
     */
    @Operation(operationId = "getNodeConfigurationByPath")
    @ApiResponse(responseCode = "200",
            content = @Content(mediaType = MediaType.TEXT_PLAIN,
                    schema = @Schema(type = "string")),
            description = "Configuration represented by path")
    @Produces(MediaType.TEXT_PLAIN)
    @Get("/{path}")
    public String getConfigurationByPath(@PathVariable String path) {
        return super.getConfigurationByPath(path);
    }

    /**
     * Updates node configuration in HOCON format.
     *
     * @param updatedConfiguration the node configuration to update.
     */
    @Operation(operationId = "updateNodeConfiguration")
    @ApiResponse(responseCode = "200", description = "Configuration updated")
    @Consumes(MediaType.TEXT_PLAIN)
    @Patch
    public void updateConfiguration(@Body String updatedConfiguration) throws Throwable {
        super.updateConfiguration(updatedConfiguration);
    }
}
