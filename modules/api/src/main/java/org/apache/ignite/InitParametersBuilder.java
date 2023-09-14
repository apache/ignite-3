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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.lang.util.StringUtils;
import org.jetbrains.annotations.Nullable;

/** Builder of {@link org.apache.ignite.InitParameters}. */
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
    public InitParametersBuilder metaStorageNodeNames(Collection<String> metaStorageNodeNames) {
        this.metaStorageNodeNames = List.copyOf(metaStorageNodeNames);
        return this;
    }

    /**
     * Sets names of nodes that will host the CMG. If not set, {@link InitParametersBuilder#metaStorageNodeNames} will be used.
     *
     * @param cmgNodeNames Names of nodes that will host the CMG.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder cmgNodeNames(Collection<String> cmgNodeNames) {
        this.cmgNodeNames = List.copyOf(cmgNodeNames);
        return this;
    }

    /**
     * Sets Human-readable name of the cluster.
     *
     * @param clusterName Human-readable name of the cluster.
     * @return {@code this} for chaining.
     */
    public InitParametersBuilder clusterName(String clusterName) {
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
        Objects.requireNonNull(metaStorageNodeNames);
        Objects.requireNonNull(clusterName);

        cmgNodeNames = cmgNodeNames == null ? metaStorageNodeNames : cmgNodeNames;

        if (metaStorageNodeNames.isEmpty()) {
            throw new IllegalArgumentException("Meta Storage node names list must not be empty");
        }

        if (metaStorageNodeNames.stream().anyMatch(StringUtils::nullOrBlank)) {
            throw new IllegalArgumentException("Meta Storage node names must not contain blank strings: " + metaStorageNodeNames);
        }

        if (!cmgNodeNames.isEmpty() && cmgNodeNames.stream().anyMatch(StringUtils::nullOrBlank)) {
            throw new IllegalArgumentException("CMG node names must not contain blank strings: " + cmgNodeNames);
        }

        if (clusterName.isBlank()) {
            throw new IllegalArgumentException("Cluster name must not be empty");
        }

        return new InitParameters(metaStorageNodeNames, cmgNodeNames, clusterName, clusterConfiguration);
    }
}
