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

package org.apache.ignite.cli.commands.cluster;

import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_DESC;
import static org.apache.ignite.cli.commands.OptionsConstants.CLUSTER_URL_OPTION;
import static org.apache.ignite.cli.commands.OptionsConstants.PROFILE_OPTION;
import static org.apache.ignite.cli.commands.OptionsConstants.PROFILE_OPTION_DESC;
import static org.apache.ignite.cli.commands.OptionsConstants.PROFILE_OPTION_SHORT;

import jakarta.inject.Inject;
import org.apache.ignite.cli.config.ConfigConstants;
import org.apache.ignite.cli.config.ConfigManager;
import org.apache.ignite.cli.config.ConfigManagerProvider;
import picocli.CommandLine.Option;

/**
 * Helper class to combine cluster URL and profile options.
 */
public class ClusterUrlOptions {
    /**
     * Cluster endpoint URL option.
     */
    @Option(names = {CLUSTER_URL_OPTION}, description = CLUSTER_URL_DESC)
    private String clusterUrl;

    /**
     * Profile to get default values from.
     */
    @Option(names = {PROFILE_OPTION, PROFILE_OPTION_SHORT}, description = PROFILE_OPTION_DESC)
    private String profileName;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    /**
     * Gets cluster URL from either the command line or from the config with specified or default profile.
     *
     * @return cluster URL
     */
    public String get() {
        if (clusterUrl != null) {
            return clusterUrl;
        } else {
            ConfigManager configManager = configManagerProvider.get();
            return configManager.getProperty(ConfigConstants.CLUSTER_URL, profileName);
        }
    }
}
