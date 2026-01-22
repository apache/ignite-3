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

import static org.apache.ignite.internal.cli.commands.Options.Constants.NODE_URL_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.PASSWORD_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.USERNAME_OPTION_SHORT;

import jakarta.inject.Inject;
import java.net.URL;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput.ConnectCallInputBuilder;
import org.apache.ignite.internal.cli.commands.ProfileMixin;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.converters.RestEndpointUrlConverter;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Mixin class for connect command options.
 */
public class ConnectOptions {
    /** Node URL option. */
    @Parameters(description = NODE_URL_OPTION_DESC, converter = RestEndpointUrlConverter.class, arity = "0..1")
    private URL nodeUrl;

    /** Profile to get default values from. */
    @Mixin
    private ProfileMixin profile;

    @ArgGroup(exclusive = false)
    private AuthOptions authOptions;

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

    @Spec
    private CommandSpec spec;

    /**
     * Builds input for the connect call.
     *
     * @return Created call input.
     */
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
        if (nodeUrl != null) {
            return nodeUrl.toString();
        } else {
            ConfigManager configManager = configManagerProvider.get();
            String profileName = profile.getProfileName();
            String url = configManager.getProperty(CliConfigKeys.CLUSTER_URL.value(), profileName);
            if (url == null) {
                if (profileName != null) {
                    throw new ParameterException(spec.commandLine(), "Node URL is not found in the specified profile");
                }
                throw new ParameterException(spec.commandLine(), "Node URL is not found in the default profile");
            }
            return url;
        }
    }
}
