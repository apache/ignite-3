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

package org.apache.ignite.cli.call.cluster.topology;

import org.apache.ignite.cli.core.call.CallInput;

/**
 * Input for physical or logical topology call.
 */
public class TopologyCallInput implements CallInput {
    /**
     * Cluster url.
     */
    private final String clusterUrl;

    private TopologyCallInput(String clusterUrl) {
        this.clusterUrl = clusterUrl;
    }

    /**
     * Builder for {@link TopologyCallInput}.
     */
    public static TopologyCallInputBuilder builder() {
        return new TopologyCallInputBuilder();
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
     * Builder for {@link org.apache.ignite.cli.call.configuration.ClusterConfigShowCallInput}.
     */
    public static class TopologyCallInputBuilder {

        private String clusterUrl;

        public TopologyCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public TopologyCallInput build() {
            return new TopologyCallInput(clusterUrl);
        }
    }
}
