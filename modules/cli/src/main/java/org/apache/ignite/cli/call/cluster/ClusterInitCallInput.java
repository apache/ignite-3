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

package org.apache.ignite.cli.call.cluster;

import java.util.List;
import org.apache.ignite.cli.commands.cluster.init.ClusterInitOptions;
import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for {@link ClusterInitCall}.
 */
public class ClusterInitCallInput implements CallInput {
    private final String clusterUrl;
    private final List<String> metaStorageNodes;
    private final List<String> cmgNodes;
    private final String clusterName;

    private ClusterInitCallInput(
            String clusterUrl,
            List<String> metaStorageNodes,
            List<String> cmgNodes,
            String clusterName
    ) {
        this.clusterUrl = clusterUrl;
        this.metaStorageNodes = metaStorageNodes;
        this.cmgNodes = cmgNodes;
        this.clusterName = clusterName;
    }

    /**
     * Builder for {@link ClusterInitCallInput}.
     */
    public static ClusterInitCallInputBuilder builder() {
        return new ClusterInitCallInputBuilder();
    }

    /**
     * Gets cluster URL.
     *
     * @return Cluster URL.
     */
    public String getClusterUrl() {
        return clusterUrl;
    }

    /**
     * Consistent IDs of the nodes that will host the Meta Storage.
     *
     * @return Meta storage node ids.
     */
    public List<String> getMetaStorageNodes() {
        return metaStorageNodes;
    }

    /**
     * Consistent IDs of the nodes that will host the Cluster Management Group; if empty,
     * {@code metaStorageNodeIds} will be used to host the CMG as well.
     *
     * @return Cluster management node ids.
     */
    public List<String> getCmgNodes() {
        return cmgNodes;
    }

    /**
     * Human-readable name of the cluster.
     *
     * @return Cluster name.
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Builder for {@link ClusterInitCallInput}.
     */
    public static class ClusterInitCallInputBuilder {
        private String clusterUrl;

        private List<String> metaStorageNodes;

        private List<String> cmgNodes;

        private String clusterName;

        public ClusterInitCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        /**
         * Extract cluster initialization options.
         *
         * @param clusterInitOptions mixin class with options
         * @return this builder
         */
        public ClusterInitCallInputBuilder fromClusterInitOptions(ClusterInitOptions clusterInitOptions) {
            this.metaStorageNodes = clusterInitOptions.getMetaStorageNodes();
            this.cmgNodes = clusterInitOptions.getCmgNodes();
            this.clusterName = clusterInitOptions.getClusterName();
            return this;
        }

        public ClusterInitCallInput build() {
            return new ClusterInitCallInput(clusterUrl, metaStorageNodes, cmgNodes, clusterName);
        }
    }
}
