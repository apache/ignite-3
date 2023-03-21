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

package org.apache.ignite.internal.cli.call.cluster;

import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.commands.cluster.init.AuthenticationOptions;
import org.apache.ignite.internal.cli.commands.cluster.init.ClusterInitOptions;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.apache.ignite.security.AuthenticationConfig;
import org.apache.ignite.security.AuthenticationProviderConfig;
import org.apache.ignite.security.BasicAuthenticationProviderConfig;

/**
 * Input for {@link ClusterInitCall}.
 */
public class ClusterInitCallInput implements CallInput {
    private final String clusterUrl;
    private final List<String> metaStorageNodes;
    private final List<String> cmgNodes;
    private final String clusterName;
    private final AuthenticationConfig authenticationConfig;

    private ClusterInitCallInput(
            String clusterUrl,
            List<String> metaStorageNodes,
            List<String> cmgNodes,
            String clusterName,
            AuthenticationConfig authenticationConfig) {
        this.clusterUrl = clusterUrl;
        this.metaStorageNodes = metaStorageNodes;
        this.cmgNodes = cmgNodes;
        this.clusterName = clusterName;
        this.authenticationConfig = authenticationConfig;
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

    public AuthenticationConfig authenticationConfig() {
        return authenticationConfig;
    }

    /**
     * Builder for {@link ClusterInitCallInput}.
     */
    public static class ClusterInitCallInputBuilder {
        private String clusterUrl;

        private List<String> metaStorageNodes;

        private List<String> cmgNodes;

        private String clusterName;

        private AuthenticationConfig authenticationSettings;

        public ClusterInitCallInputBuilder authenticationSettings(AuthenticationConfig authenticationConfig) {
            this.authenticationSettings = authenticationConfig;
            return this;
        }

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
            this.metaStorageNodes = clusterInitOptions.metaStorageNodes();
            this.cmgNodes = clusterInitOptions.cmgNodes();
            this.clusterName = clusterInitOptions.clusterName();
            this.authenticationSettings = toAuthenticationConfig(clusterInitOptions.authenticationOptions());
            return this;
        }

        public ClusterInitCallInput build() {
            return new ClusterInitCallInput(clusterUrl, metaStorageNodes, cmgNodes, clusterName, authenticationSettings);
        }

        private AuthenticationConfig toAuthenticationConfig(AuthenticationOptions options) {
            if (options == null) {
                return null;
            }

            return new AuthenticationConfig(options.enabled(), extractAuthenticationProviders(options));
        }

        private List<AuthenticationProviderConfig> extractAuthenticationProviders(AuthenticationOptions options) {
            return Stream.of(extractBasicAuthenticationProviderConfig(options))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        private BasicAuthenticationProviderConfig extractBasicAuthenticationProviderConfig(AuthenticationOptions options) {
            if (!nullOrBlank(options.basicLogin()) && !nullOrBlank(options.basicPassword())) {
                return new BasicAuthenticationProviderConfig("basic", options.basicLogin(), options.basicPassword());
            } else {
                return null;
            }
        }
    }
}
