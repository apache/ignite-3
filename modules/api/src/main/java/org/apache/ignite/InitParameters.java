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
import org.apache.ignite.security.AuthenticationConfig;

/** Initialization parameters. */
public class InitParameters {

    /** Name of the node that the initialization request will be sent to. */
    private final String destinationNodeName;

    /** Names of nodes that will host the Meta Storage <b>and</b> the CMG. */
    private final Collection<String> metaStorageNodeNames;

    /** Names of nodes that will host the CMG. */
    private final Collection<String> cmgNodeNames;

    /** Human-readable name of the cluster. */
    private final String clusterName;

    /** Authentication configuration, that will be applied after initialization. */
    private final AuthenticationConfig authenticationConfig;

    /**
     * Constructor.
     *
     * @param destinationNodeName Name of the node that the initialization request will be sent to.
     * @param metaStorageNodeNames Names of nodes that will host the Meta Storage.
     * @param cmgNodeNames Names of nodes that will host the CMG.
     * @param clusterName Human-readable name of the cluster.
     * @param authenticationConfig authentication configuration.
     */
    InitParameters(
            String destinationNodeName,
            Collection<String> metaStorageNodeNames,
            Collection<String> cmgNodeNames,
            String clusterName,
            AuthenticationConfig authenticationConfig
    ) {

        Objects.requireNonNull(destinationNodeName);
        Objects.requireNonNull(metaStorageNodeNames);
        Objects.requireNonNull(cmgNodeNames);
        Objects.requireNonNull(clusterName);
        Objects.requireNonNull(authenticationConfig);

        this.destinationNodeName = destinationNodeName;
        this.metaStorageNodeNames = List.copyOf(metaStorageNodeNames);
        this.cmgNodeNames = List.copyOf(cmgNodeNames);
        this.clusterName = clusterName;
        this.authenticationConfig = authenticationConfig;
    }

    public static InitParametersBuilder builder() {
        return new InitParametersBuilder();
    }

    public String nodeName() {
        return destinationNodeName;
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

    public AuthenticationConfig restAuthenticationConfig() {
        return authenticationConfig;
    }
}
