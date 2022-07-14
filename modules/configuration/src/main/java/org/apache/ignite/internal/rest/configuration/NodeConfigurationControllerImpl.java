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

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Patch;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Produces;
import jakarta.inject.Named;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.rest.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.rest.api.configuration.NodeConfigurationController;
import org.apache.ignite.internal.rest.constants.MediaType;
import org.apache.ignite.internal.rest.exception.handler.IgniteExceptionHandler;

/**
 * Node configuration controller.
 */
@Controller("/management/v1/configuration/node")
@Requires(classes = IgniteExceptionHandler.class)
public class NodeConfigurationControllerImpl extends AbstractConfigurationController implements NodeConfigurationController {

    public NodeConfigurationControllerImpl(@Named("nodeCfgPresentation") ConfigurationPresentation<String> nodeCfgPresentation) {
        super(nodeCfgPresentation);
    }

    /**
     * Returns node configuration in HOCON format. This is represented as a plain text.
     *
     * @return the whole node configuration in HOCON format.
     */
    @Produces({
            MediaType.TEXT_PLAIN, // todo: IGNITE-17082
            MediaType.PROBLEM_JSON
    })
    @Get
    @Override
    public String getConfiguration() {
        return super.getConfiguration();
    }

    /**
     * Returns configuration in HOCON format represented by path. This is represented as a plain text.
     *
     * @param path to represent a node configuration.
     * @return system configuration in HOCON format represented by given path.
     */
    @Produces({
            MediaType.TEXT_PLAIN, // todo: IGNITE-17082
            MediaType.PROBLEM_JSON
    })
    @Get("/{path}")
    @Override
    public String getConfigurationByPath(@PathVariable String path) {
        return super.getConfigurationByPath(path);
    }

    /**
     * Updates node configuration in HOCON format. This is represented as a plain text.
     *
     * @param updatedConfiguration the node configuration to update. This is represented as a plain text.
     */
    @Consumes(MediaType.TEXT_PLAIN) // todo: IGNITE-17082
    @Produces(MediaType.PROBLEM_JSON)
    @Patch
    @Override
    public CompletableFuture<Void> updateConfiguration(@Body String updatedConfiguration) {
        return super.updateConfiguration(updatedConfiguration);
    }
}
