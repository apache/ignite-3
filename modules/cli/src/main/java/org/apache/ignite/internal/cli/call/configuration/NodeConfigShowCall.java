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
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Shows node configuration from ignite cluster.
 */
@Singleton
public class NodeConfigShowCall implements Call<NodeConfigShowCallInput, JsonString> {

    private final ApiClientFactory clientFactory;

    public NodeConfigShowCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput<JsonString> execute(NodeConfigShowCallInput input) {
        NodeConfigurationApi client = createApiClient(input);

        try {
            return DefaultCallOutput.success(readNodeConfig(client, input));
        } catch (ApiException | IllegalArgumentException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.getNodeUrl()));
        }
    }

    private JsonString readNodeConfig(NodeConfigurationApi api, NodeConfigShowCallInput input) throws ApiException {
        return JsonString.fromString(input.getSelector() != null
                ? api.getNodeConfigurationByPath(input.getSelector())
                : api.getNodeConfiguration());
    }

    private NodeConfigurationApi createApiClient(NodeConfigShowCallInput input) {
        return new NodeConfigurationApi(clientFactory.getClient(input.getNodeUrl()));
    }
}
