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

package org.apache.ignite.cli.call.status;

import java.util.List;

/**
 * Class that represents the cluster status.
 */
public class Status {

    private final int nodeCount;

    private final boolean initialized;

    private final String name;

    private final boolean connected;

    private final String connectedNodeUrl;

    private final List<String> cmgNodes;

    private Status(int nodeCount, boolean initialized, String name, boolean connected, String connectedNodeUrl, List<String> cmgNodes) {
        this.nodeCount = nodeCount;
        this.initialized = initialized;
        this.name = name;
        this.connected = connected;
        this.connectedNodeUrl = connectedNodeUrl;
        this.cmgNodes = cmgNodes;
    }

    public int getNodeCount() {
        return nodeCount;
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

    public String getConnectedNodeUrl() {
        return connectedNodeUrl;
    }

    public List<String> getCmgNodes() {
        return cmgNodes;
    }

    /**
     * Builder for {@link Status}.
     */
    public static StatusBuilder builder() {
        return new StatusBuilder();
    }

    /**
     * Builder for {@link Status}.
     */
    public static class StatusBuilder {
        private int nodeCount;

        private boolean initialized;

        private String name;

        private boolean connected;

        private String connectedNodeUrl;

        private List<String> cmgNodes;

        private StatusBuilder() {

        }

        public StatusBuilder nodeCount(int nodeCount) {
            this.nodeCount = nodeCount;
            return this;
        }

        public StatusBuilder initialized(boolean initialized) {
            this.initialized = initialized;
            return this;
        }

        public StatusBuilder name(String name) {
            this.name = name;
            return this;
        }

        public StatusBuilder connected(boolean connected) {
            this.connected = connected;
            return this;
        }

        public StatusBuilder connectedNodeUrl(String connectedNodeUrl) {
            this.connectedNodeUrl = connectedNodeUrl;
            return this;
        }

        public StatusBuilder cmgNodes(List<String> cmgNodes) {
            this.cmgNodes = cmgNodes;
            return this;
        }

        public Status build() {
            return new Status(nodeCount, initialized, name, connected, connectedNodeUrl, cmgNodes);
        }
    }
}
