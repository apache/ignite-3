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

package org.apache.ignite.cli.call.configuration;

import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.exception.CommandExecutionException;
import org.apache.ignite.rest.client.api.ClusterConfigurationApi;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;
import org.jetbrains.annotations.NotNull;

/**
 * Updates configuration for node or ignite cluster.
 */
@Singleton
public class UpdateConfigurationCall implements Call<UpdateConfigurationCallInput, String> {
    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput<String> execute(UpdateConfigurationCallInput updateConfigurationCallInput) {
        ApiClient client = createApiClient(updateConfigurationCallInput);

        try {
            if (updateConfigurationCallInput.getNodeId() != null) {
                return updateNodeConfig(new NodeConfigurationApi(client), updateConfigurationCallInput);
            } else {
                return updateClusterConfig(new ClusterConfigurationApi(client), updateConfigurationCallInput);
            }
        } catch (ApiException e) {
            if (e.getCode() == 400) {
                return DefaultCallOutput.failure(
                        new CommandExecutionException(
                                "update config",
                                "Got error while updating the node configuration. " + System.lineSeparator()
                                        + "Code:  " + e.getCode() + ", response:  " + e.getResponseBody()
                        ));
            }
            return DefaultCallOutput.failure(new CommandExecutionException("update config", "Ignite api return " + e.getCode()));
        }
    }

    private DefaultCallOutput<String> updateClusterConfig(ClusterConfigurationApi api, UpdateConfigurationCallInput input)
            throws ApiException {
        api.updateClusterConfiguration(input.getConfig());
        return DefaultCallOutput.success("Cluster configuration was updated successfully.");
    }

    private DefaultCallOutput<String> updateNodeConfig(NodeConfigurationApi api, UpdateConfigurationCallInput input)
            throws ApiException {
        api.updateNodeConfiguration(input.getConfig());
        return DefaultCallOutput.success("Node configuration was updated successfully.");
    }

    @NotNull
    private ApiClient createApiClient(UpdateConfigurationCallInput input) {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(input.getClusterUrl());
        return client;
    }
}
