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
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;
import org.jetbrains.annotations.NotNull;

/**
 * Updates configuration for node.
 */
@Singleton
public class NodeConfigUpdateCall implements Call<NodeConfigUpdateCallInput, String> {
    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput<String> execute(NodeConfigUpdateCallInput nodeConfigUpdateCallInput) {
        NodeConfigurationApi client = createApiClient(nodeConfigUpdateCallInput);

        try {
            return updateNodeConfig(client, nodeConfigUpdateCallInput);
        } catch (ApiException e) {
            if (e.getCode() == 400) {
                return DefaultCallOutput.failure(
                        new CommandExecutionException(
                                "node config update",
                                "Got error while updating the node configuration. " + System.lineSeparator()
                                        + "Code:  " + e.getCode() + ", response:  " + e.getResponseBody()
                        ));
            }
            return DefaultCallOutput.failure(new CommandExecutionException("node config update", "Ignite api return " + e.getCode()));
        }
    }

    private DefaultCallOutput<String> updateNodeConfig(NodeConfigurationApi api, NodeConfigUpdateCallInput input)
            throws ApiException {
        api.updateNodeConfiguration(input.getConfig());
        return DefaultCallOutput.success("Node configuration was updated successfully.");
    }

    @NotNull
    private NodeConfigurationApi createApiClient(NodeConfigUpdateCallInput input) {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(input.getNodeUrl());
        return new NodeConfigurationApi(client);
    }
}
