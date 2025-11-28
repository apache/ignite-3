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

package org.apache.ignite.internal.rest.configuration;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import jakarta.inject.Named;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.rest.api.configuration.ClusterConfigurationApi;
import org.apache.ignite.internal.rest.exception.handler.IgniteExceptionHandler;

/**
 * Cluster configuration controller.
 */
@Controller("/management/v1/configuration/cluster/")
@Requires(classes = IgniteExceptionHandler.class)
public class ClusterConfigurationController extends AbstractConfigurationController implements ClusterConfigurationApi {
    public ClusterConfigurationController(@Named("clusterCfgPresentation") ConfigurationPresentation<String> clusterCfgPresentation) {
        super(clusterCfgPresentation);
    }

    /**
     * Returns cluster configuration in HOCON format. This is represented as a plain text.
     *
     * @return the whole cluster configuration in HOCON format.
     */
    @Override
    public HttpResponse<?> getConfiguration() {
        return super.getConfiguration();
    }

    /**
     * Returns configuration in HOCON format represented by path. This is represented as a plain text.
     *
     * @param path to represent a cluster configuration.
     * @return system configuration in HOCON format represented by given path.
     */
    @Override
    public HttpResponse<?> getConfigurationByPath(String path) {
        return super.getConfigurationByPath(path);
    }

    /**
     * Updates cluster configuration in HOCON format. This is represented as a plain text.
     *
     * @param updatedConfiguration the cluster configuration to update.
     */
    @Override
    public CompletableFuture<Void> updateConfiguration(String updatedConfiguration) {
        return super.updateConfiguration(updatedConfiguration);
    }
}
