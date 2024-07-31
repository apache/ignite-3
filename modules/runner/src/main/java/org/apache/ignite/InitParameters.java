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
import org.jetbrains.annotations.Nullable;

/** Initialization parameters. */
public class InitParameters {
    /** Names of nodes that will host the Meta Storage <b>and</b> the CMG. */
    private final Collection<String> metaStorageNodeNames;

    /** Names of nodes that will host the CMG. */
    private final Collection<String> cmgNodeNames;

    /** Human-readable name of the cluster. */
    private final String clusterName;

    /** Cluster configuration, that will be applied after initialization. */
    private final String clusterConfiguration;

    /**
     * Constructor.
     *
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @param cmgNodeNames Names of nodes that will host the CMG.
     * @param clusterName Human-readable name of the cluster.
     * @param clusterConfiguration Cluster configuration.
     */
    InitParameters(
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName,
            @Nullable String clusterConfiguration
    ) {
        Objects.requireNonNull(metaStorageNodeNames);
        Objects.requireNonNull(cmgNodeNames);
        Objects.requireNonNull(clusterName);

        this.metaStorageNodeNames = List.copyOf(metaStorageNodeNames);
        this.cmgNodeNames = List.copyOf(cmgNodeNames);
        this.clusterName = clusterName;
        this.clusterConfiguration = clusterConfiguration;
    }

    public static InitParametersBuilder builder() {
        return new InitParametersBuilder();
    }

    public Collection<String> metaStorageNodeNames() {
        return metaStorageNodeNames;
    }

    public Collection<String> cmgNodeNames() {
        return cmgNodeNames;
    }

    public String clusterName() {
        return clusterName;
    }

    public String clusterConfiguration() {
        return clusterConfiguration;
    }
}
