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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** Builder of {@link InitParameters}. */
public class InitParametersBuilder {
    private Collection<String> metaStorageNodeNames;
    private Collection<String> cmgNodeNames;
    private String clusterName;
    @Nullable
    private String clusterConfiguration;

    /**
     * Sets names of nodes that will host the Meta Storage.
     *
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder metaStorageNodeNames(String... metaStorageNodeNames) {
        if (metaStorageNodeNames == null || metaStorageNodeNames.length == 0) {
            this.metaStorageNodeNames = Collections.emptyList();
            return this;
        }
        this.metaStorageNodeNames = List.of(metaStorageNodeNames);
        return this;
    }

    /**
     * Sets names of nodes that will host the Meta Storage.
     *
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder metaStorageNodeNames(@Nullable Collection<String> metaStorageNodeNames) {
        if (metaStorageNodeNames == null || metaStorageNodeNames.isEmpty()) {
            this.metaStorageNodeNames = Collections.emptyList();
            return this;
        }
        this.metaStorageNodeNames = new ArrayList<>(metaStorageNodeNames);
        return this;
    }

    /**
     * Sets nodes that will host the Meta Storage.
     *
     * @param metaStorageNodes Nodes that will host the Meta Storage.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder metaStorageNodes(IgniteServer... metaStorageNodes) {
        if (metaStorageNodes == null || metaStorageNodes.length == 0) {
            this.metaStorageNodeNames = Collections.emptyList();
            return this;
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
        if (metaStorageNodes == null || metaStorageNodes.isEmpty()) {
            this.metaStorageNodeNames = Collections.emptyList();
            return this;
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
        if (cmgNodeNames == null || cmgNodeNames.length == 0) {
            this.cmgNodeNames = Collections.emptyList();
            return this;
        }
        this.cmgNodeNames = List.of(cmgNodeNames);
        return this;
    }

    /**
     * Sets names of nodes that will host the CMG. If not set, {@link InitParametersBuilder#metaStorageNodeNames} will be used.
     *
     * @param cmgNodeNames Names of nodes that will host the CMG.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder cmgNodeNames(@Nullable Collection<String> cmgNodeNames) {
        if (cmgNodeNames == null || cmgNodeNames.isEmpty()) {
            this.cmgNodeNames = Collections.emptyList();
            return this;
        }
        this.cmgNodeNames = new ArrayList<>(cmgNodeNames);
        return this;
    }

    /**
     * Sets nodes that will host the CMG. If not set, {@link InitParametersBuilder#metaStorageNodes} will be used.
     *
     * @param cmgNodes Nodes that will host the CMG.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder cmgNodes(IgniteServer... cmgNodes) {
        if (cmgNodes == null || cmgNodes.length == 0) {
            this.cmgNodeNames = Collections.emptyList();
            return this;
        }
        this.cmgNodeNames = Arrays.stream(cmgNodes).map(IgniteServer::name).collect(Collectors.toList());
        return this;
    }

    /**
     * Sets nodes that will host the CMG. If not set, {@link InitParametersBuilder#metaStorageNodes} will be used.
     *
     * @param cmgNodes Nodes that will host the CMG.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder cmgNodes(@Nullable Collection<IgniteServer> cmgNodes) {
        if (cmgNodes == null || cmgNodes.isEmpty()) {
            this.cmgNodeNames = Collections.emptyList();
            return this;
        }
        this.cmgNodeNames = cmgNodes.stream().map(IgniteServer::name).collect(Collectors.toList());
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
    public InitParametersBuilder clusterConfiguration(@Nullable String clusterConfiguration) {
        this.clusterConfiguration = clusterConfiguration;
        return this;
    }

    /** Builds {@link InitParameters}. */
    public InitParameters build() {
        if (clusterName == null) {
            throw new IllegalStateException("Cluster name is not set.");
        }

        return new InitParameters(metaStorageNodeNames, cmgNodeNames, clusterName, clusterConfiguration);
    }
}
