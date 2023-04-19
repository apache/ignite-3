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

import org.apache.ignite.internal.cli.core.call.CallInput;

/** Input for {@link NodeMetricSourceEnableCall}. */
public class NodeMetricSourceEnableCallInput implements CallInput {
    /** Metric source name. */
    private final String srcName;

    /** Enable or disable metric source. */
    private final boolean enable;

    /** endpoint URL. */
    private final String endpointUrl;

    private NodeMetricSourceEnableCallInput(String srcName, boolean enable, String endpointUrl) {
        this.srcName = srcName;
        this.enable = enable;
        this.endpointUrl = endpointUrl;
    }

    /**
     * Builder method.
     *
     * @return Builder for {@link NodeMetricSourceEnableCallInput}.
     */
    public static NodeMetricSourceEnableCallInputBuilder builder() {
        return new NodeMetricSourceEnableCallInputBuilder();
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
     * Builder for {@link NodeMetricSourceEnableCallInput}.
     */
    public static class NodeMetricSourceEnableCallInputBuilder {

        private String srcName;

        private boolean enable;

        private String endpointUrl;

        public NodeMetricSourceEnableCallInputBuilder srcName(String srcName) {
            this.srcName = srcName;
            return this;
        }

        public NodeMetricSourceEnableCallInputBuilder enable(boolean enable) {
            this.enable = enable;
            return this;
        }

        public NodeMetricSourceEnableCallInputBuilder endpointUrl(String endpointUrl) {
            this.endpointUrl = endpointUrl;
            return this;
        }

        public NodeMetricSourceEnableCallInput build() {
            return new NodeMetricSourceEnableCallInput(srcName, enable, endpointUrl);
        }
    }
}
