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

package org.apache.ignite.internal.cli.commands.cluster;

import static org.apache.ignite.internal.cli.commands.CommandConstants.CLUSTER_URL_OPTION_ORDER;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION_DESC;

import jakarta.inject.Inject;
import java.net.URL;
import org.apache.ignite.internal.cli.commands.ProfileMixin;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.converters.RestEndpointUrlConverter;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Mixin class for cluster URL and profile options in REPL mode.
 */
public class ClusterUrlMixin {
    /** Cluster endpoint URL option. */
    @Option(
            names = CLUSTER_URL_OPTION,
            description = CLUSTER_URL_OPTION_DESC,
            converter = RestEndpointUrlConverter.class,
            order = CLUSTER_URL_OPTION_ORDER
    )
    private URL clusterUrl;

    /** Profile to get default values from. */
    @Mixin
    private ProfileMixin profile;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    @Inject
    private Session session;

    /**
     * Gets cluster URL from either the command line, the specified profile, the session, or the config default.
     *
     * @return cluster URL
     */
    @Nullable
    public String getClusterUrl() {
        // First, get the cluster from the url option.
        if (clusterUrl != null) {
            return clusterUrl.toString();
        }

        // Then get it from the specified profile.
        String profileName = profile.getProfileName();
        if (profileName != null) {
            return configManagerProvider.get().getProperty(CliConfigKeys.CLUSTER_URL.value(), profileName);
        }

        // Then get it from the session.
        SessionInfo sessionInfo = session.info();
        if (sessionInfo != null) {
            return sessionInfo.nodeUrl();
        }

        // Finally, get it from the default profile in the config.
        return configManagerProvider.get().getCurrentProperty(CliConfigKeys.CLUSTER_URL.value());
    }
}
