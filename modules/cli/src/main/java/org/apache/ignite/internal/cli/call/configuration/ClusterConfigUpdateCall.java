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

package org.apache.ignite.internal.cli.call.configuration;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.ClusterConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Updates cluster configuration.
 */
@Singleton
public class ClusterConfigUpdateCall implements Call<ClusterConfigUpdateCallInput, String> {
    private final ApiClientFactory clientFactory;

    public ClusterConfigUpdateCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput<String> execute(ClusterConfigUpdateCallInput input) {
        ClusterConfigurationApi client = createApiClient(input);

        try {
            return updateClusterConfig(client, input);
        } catch (ApiException | IllegalArgumentException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.getClusterUrl()));
        }
    }

    private DefaultCallOutput<String> updateClusterConfig(ClusterConfigurationApi api, ClusterConfigUpdateCallInput input)
            throws ApiException {
        api.updateClusterConfiguration(input.getConfig());
        return DefaultCallOutput.success("Cluster configuration was updated successfully");
    }

    private ClusterConfigurationApi createApiClient(ClusterConfigUpdateCallInput input) {
        return new ClusterConfigurationApi(clientFactory.getClient(input.getClusterUrl()));
    }
}
