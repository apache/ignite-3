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

import org.apache.ignite.internal.cli.core.call.CallInput;

/**
 * Input for {@link NodeConfigUpdateCall}.
 */
public class NodeConfigUpdateCallInput implements CallInput {
    /**
     * Configuration to update.
     */
    private final String config;

    /**
     * Cluster url.
     */
    private final String nodeUrl;

    private NodeConfigUpdateCallInput(String config, String clusterUrl) {
        this.config = config;
        this.nodeUrl = clusterUrl;
    }

    /**
     * Builder method.
     *
     * @return Builder for {@link NodeConfigUpdateCallInput}.
     */
    public static UpdateConfigurationCallInputBuilder builder() {
        return new UpdateConfigurationCallInputBuilder();
    }

    /**
     * Get configuration.
     *
     * @return Configuration to update.
     */
    public String getConfig() {
        return config;
    }

    /**
     * Get cluster URL.
     *
     * @return Cluster url.
     */
    public String getNodeUrl() {
        return nodeUrl;
    }

    /**
     * Builder for {@link NodeConfigUpdateCallInput}.
     */
    public static class UpdateConfigurationCallInputBuilder {

        private String config;

        private String nodeUrl;

        public UpdateConfigurationCallInputBuilder config(String config) {
            this.config = config;
            return this;
        }

        public UpdateConfigurationCallInputBuilder nodeUrl(String clusterUrl) {
            this.nodeUrl = clusterUrl;
            return this;
        }

        public NodeConfigUpdateCallInput build() {
            return new NodeConfigUpdateCallInput(config, nodeUrl);
        }
    }
}
