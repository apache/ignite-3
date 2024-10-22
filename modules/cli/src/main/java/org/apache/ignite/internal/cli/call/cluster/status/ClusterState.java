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
import org.apache.ignite.rest.client.model.ClusterStatus;

/**
 * Class that represents the cluster status.
 */
public class ClusterState {

    private final int nodeCount;

    private final boolean initialized;

    private final String name;

    private final boolean connected;

    private final String nodeUrl;

    private final List<String> cmgNodes;

    private final List<String> metadataStorageNodes;

    private final ClusterStatus clusterStatus;

    private ClusterState(
            int nodeCount,
            boolean initialized,
            String name,
            boolean connected,
            String nodeUrl,
            List<String> cmgNodes,
            List<String> metadataStorageNodes,
            ClusterStatus clusterStatus
    ) {
        this.nodeCount = nodeCount;
        this.initialized = initialized;
        this.name = name;
        this.connected = connected;
        this.nodeUrl = nodeUrl;
        this.cmgNodes = cmgNodes;
        this.metadataStorageNodes = metadataStorageNodes;
        this.clusterStatus = clusterStatus;
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

    public ClusterStatus clusterStatus() {
        return clusterStatus;
    }

    /**
     * Builder for {@link ClusterState}.
     */
    public static ClusterStateBuilder builder() {
        return new ClusterStateBuilder();
    }

    /**
     * Builder for {@link ClusterState}.
     */
    public static class ClusterStateBuilder {
        private int nodeCount;

        private boolean initialized;

        private String name;

        private boolean connected;

        private String connectedNodeUrl;

        private List<String> cmgNodes;

        private List<String> metadataStorageNodes;

        private ClusterStatus clusterStatus;

        private ClusterStateBuilder() {

        }

        public ClusterStateBuilder nodeCount(int nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public ClusterStateBuilder initialized(boolean initialized) {
            this.initialized = initialized;
            return this;
        }

        public ClusterStateBuilder name(String name) {
            this.name = name;
            return this;
        }

        public ClusterStateBuilder connected(boolean connected) {
            this.connected = connected;
            return this;
        }

        public ClusterStateBuilder connectedNodeUrl(String connectedNodeUrl) {
            this.connectedNodeUrl = connectedNodeUrl;
            return this;
        }

        public ClusterStateBuilder cmgNodes(List<String> cmgNodes) {
            this.cmgNodes = cmgNodes;
            return this;
        }

        public ClusterStateBuilder metadataStorageNodes(List<String> metadataStorageNodes) {
            this.metadataStorageNodes = metadataStorageNodes;
            return this;
        }

        public ClusterStateBuilder clusterStatus(ClusterStatus clusterStatus) {
            this.clusterStatus = clusterStatus;
            return this;
        }

        /**
         * Returns new cluster state instance.
         */
        public ClusterState build() {
            return new ClusterState(
                    nodeCount,
                    initialized,
                    name,
                    connected,
                    connectedNodeUrl,
                    cmgNodes,
                    metadataStorageNodes,
                    clusterStatus
            );
        }
    }
}
