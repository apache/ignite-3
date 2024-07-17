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
 * Input for {@link ClusterConfigUpdateCall}.
 */
public class ClusterConfigUpdateCallInput implements CallInput {
    /**
     * Configuration to update.
     */
    private final String config;

    /**
     * Cluster url.
     */
    private final String clusterUrl;

    private ClusterConfigUpdateCallInput(String config, String clusterUrl) {
        this.config = config;
        this.clusterUrl = clusterUrl;
    }

    /**
     * Builder method.
     *
     * @return Builder for {@link ClusterConfigUpdateCallInput}.
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
     * @return Cluster URL.
     */
    public String getClusterUrl() {
        return clusterUrl;
    }

    /**
     * Builder for {@link ClusterConfigUpdateCallInput}.
     */
    public static class UpdateConfigurationCallInputBuilder {

        private String config;

        private String clusterUrl;

        public UpdateConfigurationCallInputBuilder config(String config) {
            this.config = config;
            return this;
        }

        public UpdateConfigurationCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public ClusterConfigUpdateCallInput build() {
            return new ClusterConfigUpdateCallInput(config, clusterUrl);
        }
    }
}
