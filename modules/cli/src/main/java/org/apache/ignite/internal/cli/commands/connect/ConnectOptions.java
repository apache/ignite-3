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

package org.apache.ignite.internal.cli.commands.connect;

import static org.apache.ignite.internal.cli.commands.CommandConstants.PROFILE_OPTION_ORDER;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_KEY;
import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_URL_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PROFILE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PROFILE_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION_SHORT;

import jakarta.inject.Inject;
import java.net.URL;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput.ConnectCallInputBuilder;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.converters.RestEndpointUrlConverter;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Mixin class for connect command options.
 */
public class ConnectOptions {
    // Required mutually exclusive group: either URL parameter, or profile option.
    @ArgGroup(multiplicity = "1")
    private UrlOptions urlOptions;

    @ArgGroup(exclusive = false)
    private AuthOptions authOptions;

    private static class UrlOptions {
        /** Node URL option. */
        @Parameters(description = NODE_URL_OPTION_DESC, descriptionKey = CLUSTER_URL_KEY, converter = RestEndpointUrlConverter.class)
        private URL nodeUrl;

        /** Profile to get default values from. Mixins are not supported in ArgGroup so copy-paste from ProfileMixin. */
        @Option(names = PROFILE_OPTION, description = PROFILE_OPTION_DESC, order = PROFILE_OPTION_ORDER)
        private String profileName;
    }

    private static class AuthOptions {
        @Option(names = {USERNAME_OPTION, USERNAME_OPTION_SHORT}, description = USERNAME_OPTION_DESC,
                required = true, defaultValue = Option.NULL_VALUE)
        private String username;

        @Option(names = {PASSWORD_OPTION, PASSWORD_OPTION_SHORT}, description = PASSWORD_OPTION_DESC,
                required = true, defaultValue = Option.NULL_VALUE)
        private String password;
    }

    @Inject
    private ConfigManagerProvider configManagerProvider;

    public ConnectCallInput buildCallInput() {
        ConnectCallInputBuilder builder = ConnectCallInput.builder()
                .url(getNodeUrl())
                .checkClusterInit(true);

        if (authOptions != null) {
            builder.username(authOptions.username).password(authOptions.password);
        }

        return builder.build();
    }

    @Nullable
    private String getNodeUrl() {
        // Sanity check required mutually exclusive group.
        assert urlOptions != null;
        assert urlOptions.nodeUrl != null || urlOptions.profileName != null;

        if (urlOptions.nodeUrl != null) {
            return urlOptions.nodeUrl.toString();
        } else {
            ConfigManager configManager = configManagerProvider.get();
            return configManager.getProperty(CliConfigKeys.CLUSTER_URL.value(), urlOptions.profileName);
        }
    }
}
