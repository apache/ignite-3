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
 * Input for {@link ShowConfigurationCall}.
 */
public class ShowConfigurationCallInput implements CallInput {
    /**
     * Node ID.
     */
    private final String nodeId;
    /**
     * Selector for configuration tree.
     */
    private final String selector;
    /**
     * Cluster url.
     */
    private final String clusterUrl;

    private ShowConfigurationCallInput(String nodeId, String selector, String clusterUrl) {
        this.nodeId = nodeId;
        this.selector = selector;
        this.clusterUrl = clusterUrl;
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
     * Get selector.
     *
     * @return Selector for configuration tree.
     */
    public String getSelector() {
        return selector;
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
     * Builder for {@link ShowConfigurationCallInput}.
     */
    public static ShowConfigurationCallInputBuilder builder() {
        return new ShowConfigurationCallInputBuilder();
    }

    /**
     * Builder for {@link ShowConfigurationCallInput}.
     */
    public static class ShowConfigurationCallInputBuilder {
        private String nodeId;
        private String selector;
        private String clusterUrl;

        public ShowConfigurationCallInputBuilder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public ShowConfigurationCallInputBuilder selector(String selector) {
            this.selector = selector;
            return this;
        }

        public ShowConfigurationCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public ShowConfigurationCallInput build() {
            return new ShowConfigurationCallInput(nodeId, selector, clusterUrl);
        }
    }
}
