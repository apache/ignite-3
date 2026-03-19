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

package org.apache.ignite.internal.cli.call.cluster.status;

import java.util.List;

/**
 * Class that represents the cluster status.
 */
public class ClusterStatus {

    private final int nodeCount;

    private final boolean initialized;

    private final String name;

    private final boolean connected;

    private final String nodeUrl;

    private final List<String> cmgNodes;

    private final List<String> metadataStorageNodes;

    private ClusterStatus(int nodeCount, boolean initialized, String name,
            boolean connected, String nodeUrl, List<String> cmgNodes, List<String> metadataStorageNodes) {
        this.nodeCount = nodeCount;
        this.initialized = initialized;
        this.name = name;
        this.connected = connected;
        this.nodeUrl = nodeUrl;
        this.cmgNodes = cmgNodes;
        this.metadataStorageNodes = metadataStorageNodes;
    }

    public String nodeCount() {
        return nodeCount < 0 ? "N/A" : String.valueOf(nodeCount);
    }

    public boolean isInitialized() {
        return initialized;
    }

    public String getName() {
        return name;
    }

    public boolean isConnected() {
        return connected;
    }

    public String getNodeUrl() {
        return nodeUrl;
    }

    public List<String> getCmgNodes() {
        return cmgNodes;
    }

    public List<String> getMsNodes() {
        return metadataStorageNodes;
    }

    /**
     * Builder for {@link ClusterStatus}.
     */
    public static ClusterStatusBuilder builder() {
        return new ClusterStatusBuilder();
    }

    /**
     * Builder for {@link ClusterStatus}.
     */
    public static class ClusterStatusBuilder {
        private int nodeCount;

        private boolean initialized;

        private String name;

        private boolean connected;

        private String connectedNodeUrl;

        private List<String> cmgNodes;

        private List<String> metadataStorageNodes;

        private ClusterStatusBuilder() {

        }

        public ClusterStatusBuilder nodeCount(int nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public ClusterStatusBuilder initialized(boolean initialized) {
            this.initialized = initialized;
            return this;
        }

        public ClusterStatusBuilder name(String name) {
            this.name = name;
            return this;
        }

        public ClusterStatusBuilder connected(boolean connected) {
            this.connected = connected;
            return this;
        }

        public ClusterStatusBuilder connectedNodeUrl(String connectedNodeUrl) {
            this.connectedNodeUrl = connectedNodeUrl;
            return this;
        }

        public ClusterStatusBuilder cmgNodes(List<String> cmgNodes) {
            this.cmgNodes = cmgNodes;
            return this;
        }

        public ClusterStatusBuilder metadataStorageNodes(List<String> metadataStorageNodes) {
            this.metadataStorageNodes = metadataStorageNodes;
            return this;
        }

        public ClusterStatus build() {
            return new ClusterStatus(nodeCount, initialized, name, connected, connectedNodeUrl, cmgNodes, metadataStorageNodes);
        }
    }
}
