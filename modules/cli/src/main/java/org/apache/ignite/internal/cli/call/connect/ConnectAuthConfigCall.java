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

package org.apache.ignite.internal.cli.call.connect;

import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_USERNAME;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.rest.ApiClientSettings;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Call which connects to the Ignite 3 node with provided basic auth configuration and saves the configuration to the cli config.
 */
@Singleton
public class ConnectAuthConfigCall implements Call<ConnectCallInput, String> {

    @Inject
    private ConnectCall connectCall;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    @Override
    public CallOutput<String> execute(ConnectCallInput input) {
        try {
            checkConnection(input);
            saveCredentials(input);
            return connectCall.execute(input);
        } catch (ApiException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.url()));
        }
    }

    private static void checkConnection(ConnectCallInput input) throws ApiException {
        ApiClientSettings settings = ApiClientSettings.builder()
                .basePath(input.url())
                .basicAuthenticationUsername(input.username())
                .basicAuthenticationPassword(input.password())
                .build();
        ApiClient client = ApiClientFactory.buildClient(settings);
        new NodeConfigurationApi(client).getNodeConfiguration();
    }

    private void saveCredentials(ConnectCallInput input) {
        ConfigManager manager = configManagerProvider.get();
        manager.setProperty(BASIC_AUTHENTICATION_USERNAME.value(), input.username());
        manager.setProperty(BASIC_AUTHENTICATION_PASSWORD.value(), input.password());
    }
}
