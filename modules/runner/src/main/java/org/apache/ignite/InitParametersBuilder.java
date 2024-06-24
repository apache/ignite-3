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

package org.apache.ignite;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Builder of {@link org.apache.ignite.InitParameters}. */
public class InitParametersBuilder {
    private Collection<String> metaStorageNodeNames;
    private Collection<String> cmgNodeNames;
    private String clusterName;
    private String clusterConfiguration;

    /**
     * Sets names of nodes that will host the Meta Storage.
     *
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder metaStorageNodeNames(String... metaStorageNodeNames) {
        if (metaStorageNodeNames == null) {
            throw new IllegalArgumentException("Meta storage node names cannot be null.");
        }
        if (metaStorageNodeNames.length == 0) {
            throw new IllegalArgumentException("Meta storage node names cannot be empty.");
        }
        this.metaStorageNodeNames = List.of(metaStorageNodeNames);
        return this;
    }

    /**
     * Sets nodes that will host the Meta Storage.
     *
     * @param metaStorageNodes Nodes that will host the Meta Storage.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder metaStorageNodes(IgniteServer... metaStorageNodes) {
        if (metaStorageNodes == null) {
            throw new IllegalArgumentException("Meta storage nodes cannot be null.");
        }
        if (metaStorageNodes.length == 0) {
            throw new IllegalArgumentException("Meta storage nodes cannot be empty.");
        }
        this.metaStorageNodeNames = Arrays.stream(metaStorageNodes).map(IgniteServer::name).collect(Collectors.toList());
        return this;
    }

    /**
     * Sets nodes that will host the Meta Storage.
     *
     * @param metaStorageNodes Nodes that will host the Meta Storage.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder metaStorageNodes(Collection<IgniteServer> metaStorageNodes) {
        if (metaStorageNodes == null) {
            throw new IllegalArgumentException("Meta storage nodes cannot be null.");
        }
        if (metaStorageNodes.isEmpty()) {
            throw new IllegalArgumentException("Meta storage nodes cannot be empty.");
        }
        this.metaStorageNodeNames = metaStorageNodes.stream().map(IgniteServer::name).collect(Collectors.toList());
        return this;
    }

    /**
     * Sets names of nodes that will host the CMG. If not set, {@link InitParametersBuilder#metaStorageNodeNames} will be used.
     *
     * @param cmgNodeNames Names of nodes that will host the CMG.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder cmgNodeNames(String... cmgNodeNames) {
        if (cmgNodeNames == null) {
            throw new IllegalArgumentException("CMG node names cannot be null.");
        }
        if (cmgNodeNames.length == 0) {
            throw new IllegalArgumentException("CMG node names cannot be empty.");
        }
        this.cmgNodeNames = List.of(cmgNodeNames);
        return this;
    }

    /**
     * Sets Nodes that will host the CMG. If not set, {@link InitParametersBuilder#metaStorageNodes} will be used.
     *
     * @param cmgNodes Nodes that will host the CMG.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder cmgNodeNames(IgniteServer... cmgNodes) {
        if (cmgNodes == null) {
            throw new IllegalArgumentException("CMG nodes cannot be null.");
        }
        if (cmgNodes.length == 0) {
            throw new IllegalArgumentException("CMG nodes cannot be empty.");
        }
        this.cmgNodeNames = Arrays.stream(cmgNodes).map(IgniteServer::name).collect(Collectors.toList());
        return this;
    }

    /**
     * Sets Human-readable name of the cluster.
     *
     * @param clusterName Human-readable name of the cluster.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder clusterName(String clusterName) {
        if (clusterName == null || clusterName.isBlank()) {
            throw new IllegalArgumentException("Cluster name cannot be null or empty.");
        }
        this.clusterName = clusterName;
        return this;
    }

    /**
     * Sets cluster configuration, that will be applied after initialization.
     *
     * @param clusterConfiguration Cluster configuration.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder clusterConfiguration(String clusterConfiguration) {
        this.clusterConfiguration = clusterConfiguration;
        return this;
    }

    /** Builds {@link InitParameters}. */
    public InitParameters build() {
        cmgNodeNames = cmgNodeNames == null ? metaStorageNodeNames : cmgNodeNames;

        if (metaStorageNodeNames == null) {
            throw new IllegalStateException("Meta storage node names is not set.");
        }
        if (cmgNodeNames == null) {
            throw new IllegalStateException("CMG node names is not set.");
        }
        if (clusterName == null) {
            throw new IllegalStateException("Cluster name is not set.");
        }

        return new InitParameters(metaStorageNodeNames, cmgNodeNames, clusterName, clusterConfiguration);
    }
}
