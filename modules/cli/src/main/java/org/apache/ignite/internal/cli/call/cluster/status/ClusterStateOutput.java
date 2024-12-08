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

import org.apache.ignite.rest.client.model.ClusterStateDtoCmgStatus;
import org.apache.ignite.rest.client.model.ClusterStateDtoMetastoreStatus;

/**
 * Class that represents the cluster status.
 */
public class ClusterStateOutput {

    private final int nodeCount;

    private final boolean initialized;

    private final String name;

    private final boolean connected;

    private final String nodeUrl;

    private final ClusterStateDtoCmgStatus cmgStatus;

    private final ClusterStateDtoMetastoreStatus metastoreStatus;

    private ClusterStateOutput(
            int nodeCount,
            boolean initialized,
            String name,
            boolean connected,
            String nodeUrl,
            ClusterStateDtoMetastoreStatus metastoreStatus,
            ClusterStateDtoCmgStatus cmgStatus
    ) {
        this.nodeCount = nodeCount;
        this.initialized = initialized;
        this.name = name;
        this.connected = connected;
        this.nodeUrl = nodeUrl;
        this.cmgStatus = cmgStatus;
        this.metastoreStatus = metastoreStatus;
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

    public ClusterStateDtoCmgStatus getCmgStatus() {
        return cmgStatus;
    }

    public ClusterStateDtoMetastoreStatus metastoreStatus() {
        return metastoreStatus;
    }

    /**
     * Builder for {@link ClusterStateOutput}.
     */
    public static ClusterStateBuilder builder() {
        return new ClusterStateBuilder();
    }

    /**
     * Builder for {@link ClusterStateOutput}.
     */
    public static class ClusterStateBuilder {
        private int nodeCount;

        private boolean initialized;

        private String name;

        private boolean connected;

        private String connectedNodeUrl;

        private ClusterStateDtoMetastoreStatus metastoreStatus;

        private ClusterStateDtoCmgStatus cmgStatus;

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

        public ClusterStateBuilder metastoreStatus(ClusterStateDtoMetastoreStatus status) {
            this.metastoreStatus = status;
            return this;
        }

        public ClusterStateBuilder cmgStatus(ClusterStateDtoCmgStatus status) {
            this.cmgStatus = status;
            return this;
        }

        /**
         * Returns new cluster state instance.
         */
        public ClusterStateOutput build() {
            return new ClusterStateOutput(
                    nodeCount,
                    initialized,
                    name,
                    connected,
                    connectedNodeUrl,
                    metastoreStatus,
                    cmgStatus
            );
        }
    }
}
