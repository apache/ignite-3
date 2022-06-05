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
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;
import org.jetbrains.annotations.NotNull;

/**
 * Updates cluster configuration.
 */
@Singleton
public class ClusterConfigUpdateCall implements Call<ClusterConfigUpdateCallInput, String> {
    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput<String> execute(ClusterConfigUpdateCallInput clusterConfigUpdateCallInput) {
        ClusterConfigurationApi client = createApiClient(clusterConfigUpdateCallInput);

        try {
            return updateClusterConfig(client, clusterConfigUpdateCallInput);
        } catch (ApiException e) {
            if (e.getCode() == 400) {
                return DefaultCallOutput.failure(
                        new CommandExecutionException(
                                "cluster config update",
                                "Got error while updating the cluster configuration. " + System.lineSeparator()
                                        + "Code:  " + e.getCode() + ", response:  " + e.getResponseBody()
                        ));
            }
            return DefaultCallOutput.failure(new CommandExecutionException("cluster config update", "Ignite api return " + e.getCode()));
        }
    }

    private DefaultCallOutput<String> updateClusterConfig(ClusterConfigurationApi api, ClusterConfigUpdateCallInput input)
            throws ApiException {
        api.updateClusterConfiguration(input.getConfig());
        return DefaultCallOutput.success("Cluster configuration was updated successfully.");
    }

    @NotNull
    private ClusterConfigurationApi createApiClient(ClusterConfigUpdateCallInput input) {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(input.getClusterUrl());
        return new ClusterConfigurationApi(client);
    }
}
