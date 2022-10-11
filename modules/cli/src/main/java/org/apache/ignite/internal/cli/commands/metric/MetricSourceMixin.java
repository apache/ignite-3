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

package org.apache.ignite.internal.cli.commands.metric;

import org.apache.ignite.internal.cli.call.node.metric.NodeMetricEnableCallInput;
import picocli.CommandLine.Parameters;

/** Mixin class for metric source name, provides source name parameter and constructs call input. */
public class MetricSourceMixin {
    /** Name of the metric source name. */
    @Parameters(index = "0", description = "Metric source name")
    private String srcName;

    public NodeMetricEnableCallInput buildEnableCallInput(String endpointUrl) {
        return buildCallInput(endpointUrl, true);
    }

    public NodeMetricEnableCallInput buildDisableCallInput(String endpointUrl) {
        return buildCallInput(endpointUrl, false);
    }

    private NodeMetricEnableCallInput buildCallInput(String endpointUrl, boolean enable) {
        return NodeMetricEnableCallInput.builder()
                .endpointUrl(endpointUrl)
                .srcName(srcName)
                .enable(enable)
                .build();
    }
}
