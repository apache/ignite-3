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

package org.apache.ignite.internal.cli.call.node.metric;

import static org.apache.ignite.internal.cli.core.call.DefaultCallOutput.failure;
import static org.apache.ignite.internal.cli.core.call.DefaultCallOutput.success;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.call.metric.MetricSourceEnableCallInput;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.NodeMetricApi;
import org.apache.ignite.rest.client.invoker.ApiException;

/** Enables or disables metric source. */
@Singleton
public class NodeMetricSourceEnableCall implements Call<MetricSourceEnableCallInput, String> {
    private final ApiClientFactory clientFactory;

    public NodeMetricSourceEnableCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public CallOutput<String> execute(MetricSourceEnableCallInput input) {
        NodeMetricApi api = createApiClient(input);

        try {
            if (input.getEnable()) {
                api.enableNodeMetric(input.getSrcName());
            } else {
                api.disableNodeMetric(input.getSrcName());
            }
            String message = input.getEnable() ? "enabled" : "disabled";
            return success("Metric source was " + message + " successfully");
        } catch (ApiException | IllegalArgumentException e) {
            return failure(new IgniteCliApiException(e, input.getEndpointUrl()));
        }
    }

    private NodeMetricApi createApiClient(MetricSourceEnableCallInput input) {
        return new NodeMetricApi(clientFactory.getClient(input.getEndpointUrl()));
    }
}
