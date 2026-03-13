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

import org.apache.ignite.internal.cli.core.call.CallInput;

/**
 * Input for {@link ClusterRenameCall}.
 */
public class ClusterRenameCallInput implements CallInput {
    /**
     * New name.
     */
    private final String name;

    /**
     * Cluster url.
     */
    private final String clusterUrl;

    private ClusterRenameCallInput(String name, String clusterUrl) {
        this.name = name;
        this.clusterUrl = clusterUrl;
    }

    /**
     * Builder method.
     *
     * @return Builder for {@link ClusterRenameCallInput}.
     */
    public static RenameCallInputBuilder builder() {
        return new RenameCallInputBuilder();
    }

    /**
     * Get configuration.
     *
     * @return Configuration to update.
     */
    public String getName() {
        return name;
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
     * Builder for {@link ClusterRenameCallInput}.
     */
    public static class RenameCallInputBuilder {

        private String name;

        private String clusterUrl;

        public RenameCallInputBuilder name(String name) {
            this.name = name;
            return this;
        }

        public RenameCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public ClusterRenameCallInput build() {
            return new ClusterRenameCallInput(name, clusterUrl);
        }
    }
}
