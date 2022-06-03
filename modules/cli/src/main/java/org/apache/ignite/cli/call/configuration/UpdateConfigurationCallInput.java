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

import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for {@link UpdateConfigurationCall}.
 */
public class UpdateConfigurationCallInput implements CallInput {
    /**
     * Node ID.
     */
    private final String nodeId;
    /**
     * Configuration to update.
     */
    private final String config;
    /**
     * Cluster url.
     */
    private final String clusterUrl;

    private UpdateConfigurationCallInput(String nodeId, String config, String clusterUrl) {
        this.nodeId = nodeId;
        this.config = config;
        this.clusterUrl = clusterUrl;
    }

    /**
     * Builder method.
     *
     * @return Builder for {@link UpdateConfigurationCallInput}.
     */
    public static UpdateConfigurationCallInputBuilder builder() {
        return new UpdateConfigurationCallInputBuilder();
    }

    /**
     * Get node ID.
     *
     * @return Node ID.
     */
    public String getNodeId() {
        return nodeId;
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
    public String getClusterUrl() {
        return clusterUrl;
    }

    /**
     * Builder for {@link UpdateConfigurationCallInput}.
     */
    public static class UpdateConfigurationCallInputBuilder {
        private String nodeId;
        private String config;
        private String clusterUrl;

        public UpdateConfigurationCallInputBuilder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public UpdateConfigurationCallInputBuilder config(String config) {
            this.config = config;
            return this;
        }

        public UpdateConfigurationCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public UpdateConfigurationCallInput build() {
            return new UpdateConfigurationCallInput(nodeId, config, clusterUrl);
        }
    }
}
