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

package org.apache.ignite.internal.cli.commands.node;

import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_URL_OPTION_DESC;

import jakarta.inject.Inject;
import java.net.URL;
import org.apache.ignite.internal.cli.commands.ProfileMixin;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.converters.UrlConverter;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Mixin class to combine node URL and profile options.
 */
public class NodeUrlProfileMixin {
    /** Node URL option. */
    @Option(names = NODE_URL_OPTION, description = NODE_URL_OPTION_DESC, converter = UrlConverter.class)
    private URL nodeUrl;

    /** Profile to get default values from. */
    @Mixin
    private ProfileMixin profileName;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    /**
     * Gets node URL from either the command line or from the config with specified or default profile.
     *
     * @return node URL
     */
    public String getNodeUrl() {
        if (nodeUrl != null) {
            return nodeUrl.toString();
        } else {
            ConfigManager configManager = configManagerProvider.get();
            return configManager.getProperty(CliConfigKeys.CLUSTER_URL.value(), profileName.getProfileName());
        }
    }
}
