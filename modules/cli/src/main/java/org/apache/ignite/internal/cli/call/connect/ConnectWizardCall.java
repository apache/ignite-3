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

import io.micronaut.http.HttpStatus;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import javax.net.ssl.SSLException;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.flow.Flowable;
import org.apache.ignite.internal.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.rest.ApiClientSettings;
import org.apache.ignite.lang.util.StringUtils;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Call which tries to connect to the Ignite 3 node and in case of error (SSL error or auth error) asks the user for the
 * SSL configuration or Auth configuration and tries again.
 */
@Singleton
public class ConnectWizardCall implements Call<ConnectCallInput, String> {
    @Inject
    private ConnectCall connectCall;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    @Inject
    private ApiClientFactory clientFactory;

    @Override
    public CallOutput<String> execute(ConnectCallInput input) {
        CallOutput<String> output = connectCall.execute(input);
        if (output.hasError()) {
            return processErrorOutput(input, output);
        }
        return output;
    }

    private CallOutput<String> processErrorOutput(ConnectCallInput input, CallOutput<String> output) {
        if (output.errorCause().getCause() instanceof ApiException) {
            ApiException cause = (ApiException) output.errorCause().getCause();
            Throwable apiCause = cause.getCause();

            if (apiCause instanceof SSLException) { // Configure SSL
                return configureSsl(input, output);
            } else if (cause.getCode() == HttpStatus.UNAUTHORIZED.getCode()) { // Configure rest basic authentication
                return configureAuth(input, output);
            }
        }
        return output;
    }

    private CallOutput<String> configureSsl(ConnectCallInput input, CallOutput<String> output) {

        FlowBuilder<Void, SslConfig> flowBuilder = ConnectToClusterQuestion.askQuestionOnSslError();
        Flowable<SslConfig> result = flowBuilder.build().start(Flowable.empty());
        if (result.hasResult()) {
            try {
                checkConnectionSsl(new ConnectSslConfigCallInput(input.url(), result.value()));
                saveConfigSsl(result.value());
                return connectCall.execute(input);
            } catch (ApiException exception) {
                Throwable apiCause = exception.getCause();

                // SSL params were wrong
                if (apiCause instanceof SSLException) {
                    return DefaultCallOutput.failure(new IgniteCliApiException(exception, input.url()));
                } else {
                    // SSL params are correct but auth params not provided
                    saveConfigSsl(result.value());
                    return configureAuth(input, output);
                }
            } catch (IgniteCliApiException cliApiException) {
                return DefaultCallOutput.failure(cliApiException);
            }
        }
        return output;
    }

    private CallOutput<String> configureAuth(ConnectCallInput input, CallOutput<String> output) {
        FlowBuilder<Void, AuthConfig> flowBuilder = ConnectToClusterQuestion.askQuestionOnAuthError();
        Flowable<AuthConfig> result = flowBuilder.build().start(Flowable.empty());
        if (result.hasResult()) {
            ConnectCallInput connectCallInput = ConnectCallInput.builder()
                    .url(input.url())
                    .username(result.value().username())
                    .password(result.value().password())
                    .build();
            try {
                checkConnection(connectCallInput);
                saveCredentials(connectCallInput);
                return connectCall.execute(connectCallInput);
            } catch (ApiException e) {
                return DefaultCallOutput.failure(new IgniteCliApiException(e, input.url()));
            }
        }
        return output;
    }

    private void checkConnectionSsl(ConnectSslConfigCallInput input) throws ApiException {
        SslConfig config = input.getConfig();
        ApiClientSettings settings = ApiClientSettings.builder()
                .basePath(input.getUrl())
                .keyStorePath(config.keyStorePath())
                .keyStorePassword(config.keyStorePassword())
                .trustStorePath(config.trustStorePath())
                .trustStorePassword(config.trustStorePassword())
                .build();
        ApiClient client = ApiClientFactory.buildClient(settings);
        new NodeConfigurationApi(client).getNodeConfiguration();
    }

    private void saveConfigSsl(SslConfig config) {
        ConfigManager manager = configManagerProvider.get();
        manager.setProperty(CliConfigKeys.REST_TRUST_STORE_PATH.value(), config.trustStorePath());
        manager.setProperty(CliConfigKeys.REST_TRUST_STORE_PASSWORD.value(), config.trustStorePassword());
        if (!StringUtils.nullOrBlank(config.keyStorePath())) {
            manager.setProperty(CliConfigKeys.REST_KEY_STORE_PATH.value(), config.keyStorePath());
            manager.setProperty(CliConfigKeys.REST_KEY_STORE_PASSWORD.value(), config.keyStorePassword());
        }
    }

    private void checkConnection(ConnectCallInput input) throws ApiException {
        ApiClientSettings settings = ApiClientSettings.builder()
                .basePath(input.url())
                .basicAuthenticationUsername(input.username())
                .basicAuthenticationPassword(input.password())
                .build();
        ApiClient client = clientFactory.getClient(input.url(), input.username(), input.password());
        new NodeConfigurationApi(client).getNodeConfiguration();
    }

    private void saveCredentials(ConnectCallInput input) {
        ConfigManager manager = configManagerProvider.get();
        manager.setProperty(BASIC_AUTHENTICATION_USERNAME.value(), input.username());
        manager.setProperty(BASIC_AUTHENTICATION_PASSWORD.value(), input.password());
    }
}
