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

package org.apache.ignite.internal.cli.call.metric;

import org.apache.ignite.internal.cli.call.cluster.metric.ClusterMetricSourceEnableCall;
import org.apache.ignite.internal.cli.call.node.metric.NodeMetricSourceEnableCall;
import org.apache.ignite.internal.cli.core.call.CallInput;

/** Input for {@link NodeMetricSourceEnableCall} and {@link ClusterMetricSourceEnableCall}. */
public class MetricSourceEnableCallInput implements CallInput {
    /** Metric source name. */
    private final String srcName;

    /** Enable or disable metric source. */
    private final boolean enable;

    /** endpoint URL. */
    private final String endpointUrl;

    private MetricSourceEnableCallInput(String srcName, boolean enable, String endpointUrl) {
        this.srcName = srcName;
        this.enable = enable;
        this.endpointUrl = endpointUrl;
    }

    /**
     * Builder method.
     *
     * @return Builder for {@link MetricSourceEnableCallInput}.
     */
    public static MetricSourceEnableCallInputBuilder builder() {
        return new MetricSourceEnableCallInputBuilder();
    }

    /**
     * Get configuration.
     *
     * @return Configuration to update.
     */
    public String getSrcName() {
        return srcName;
    }

    /**
     * Get enable flag.
     *
     * @return {@code true} if metric source needs to be enabled, {@code false} if it needs to be disabled.
     */
    public boolean getEnable() {
        return enable;
    }

    /**
     * Get endpoint URL.
     *
     * @return endpoint URL.
     */
    public String getEndpointUrl() {
        return endpointUrl;
    }

    /**
     * Builder for {@link MetricSourceEnableCallInput}.
     */
    public static class MetricSourceEnableCallInputBuilder {

        private String srcName;

        private boolean enable;

        private String endpointUrl;

        public MetricSourceEnableCallInputBuilder srcName(String srcName) {
            this.srcName = srcName;
            return this;
        }

        public MetricSourceEnableCallInputBuilder enable(boolean enable) {
            this.enable = enable;
            return this;
        }

        public MetricSourceEnableCallInputBuilder endpointUrl(String endpointUrl) {
            this.endpointUrl = endpointUrl;
            return this;
        }

        public MetricSourceEnableCallInput build() {
            return new MetricSourceEnableCallInput(srcName, enable, endpointUrl);
        }
    }
}
