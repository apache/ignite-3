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
import io.micronaut.http.annotation.Controller;
import jakarta.inject.Named;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.NodeConfigWriteException;
import org.apache.ignite.internal.configuration.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.configuration.storage.LocalConfigurationStorage;
import org.apache.ignite.internal.rest.api.configuration.NodeConfigurationApi;
import org.apache.ignite.internal.rest.exception.handler.IgniteExceptionHandler;

/**
 * Node configuration controller.
 */
@Controller("/management/v1/configuration/node")
@Requires(classes = IgniteExceptionHandler.class)
public class NodeConfigurationController extends AbstractConfigurationController implements NodeConfigurationApi {
    private final LocalConfigurationStorage localConfigurationStorage;

    public NodeConfigurationController(
            @Named("nodeCfgPresentation") ConfigurationPresentation<String> nodeCfgPresentation,
            LocalConfigurationStorage localConfigurationStorage
    ) {
        super(nodeCfgPresentation);
        this.localConfigurationStorage = localConfigurationStorage;
    }

    /**
     * Returns node configuration in HOCON format. This is represented as a plain text.
     *
     * @return the whole node configuration in HOCON format.
     */
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
    @Override
    public String getConfigurationByPath(String path) {
        return super.getConfigurationByPath(path);
    }

    /**
     * Updates node configuration in HOCON format. This is represented as a plain text.
     *
     * @param updatedConfiguration the node configuration to update. This is represented as a plain text.
     */
    @Override
    public CompletableFuture<Void> updateConfiguration(String updatedConfiguration) {
        if (!localConfigurationStorage.userModificationsAllowed()) {
            throw new NodeConfigWriteException(
                    "The configuration file is read-only, so changes cannot be applied. "
                            + "Check your system configuration. "
                            + "If you are using containerization, such as Kubernetes, "
                            + "the file can only be modified through native Kubernetes methods."
            );
        }
        return super.updateConfiguration(updatedConfiguration);
    }
}
