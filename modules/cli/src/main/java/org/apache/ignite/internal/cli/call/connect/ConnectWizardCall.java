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

import static org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion.askQuestionOnAuthError;
import static org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion.askQuestionOnSslError;
import static org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion.askQuestionToStoreCredentials;

import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import javax.net.ssl.SSLException;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.flow.Flowable;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Call which tries to connect to the Ignite 3 node and in case of error (SSL error or auth error) asks the user for the
 * SSL configuration or Auth configuration and tries again.
 */
@Singleton
public class ConnectWizardCall implements Call<ConnectCallInput, String> {

    private final ConnectCall connectCall;

    private final ConnectSuccessCall connectSuccessCall;

    private final ConnectionChecker connectionChecker;

    private final ConfigManagerProvider configManagerProvider;

    /**
     * Constructor.
     */
    public ConnectWizardCall(
            ConnectCall connectCall,
            ConnectionChecker connectionChecker,
            ConnectSuccessCall connectSuccessCall,
            ConfigManagerProvider configManagerProvider
    ) {
        this.connectCall = connectCall;
        this.connectionChecker = connectionChecker;
        this.connectSuccessCall = connectSuccessCall;
        this.configManagerProvider = configManagerProvider;
    }

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
        Flowable<SslConfig> result = askQuestionOnSslError().start(Flowable.empty());
        if (result.hasResult()) {
            try {
                // Try to connect with ssl settings, create SessionInfo on success
                SessionInfo sessionInfo = connectionChecker.checkConnection(input, result.value());
                connectionChecker.saveSettings(result.value());

                askQuestionToStoreCredentials(configManagerProvider.get(), input.username(), input.password());
                return connectSuccessCall.execute(sessionInfo, input.checkClusterInit());
            } catch (ApiException exception) {
                Throwable apiCause = exception.getCause();

                // SSL params were wrong
                if (apiCause instanceof SSLException) {
                    return DefaultCallOutput.failure(new IgniteCliApiException(exception, input.url()));
                } else {
                    // SSL params are correct but auth params not provided
                    connectionChecker.saveSettings(result.value());

                    // Try to connect with ssl and basic auth settings
                    return configureAuth(input, output);
                }
            } catch (IgniteCliApiException cliApiException) {
                return DefaultCallOutput.failure(cliApiException);
            }
        }
        return output;
    }

    private CallOutput<String> configureAuth(ConnectCallInput input, CallOutput<String> output) {
        Flowable<AuthConfig> result = askQuestionOnAuthError().start(Flowable.empty());
        if (result.hasResult()) {
            String username = result.value().username();
            String password = result.value().password();
            ConnectCallInput connectCallInput = ConnectCallInput.builder()
                    .url(input.url())
                    .username(username)
                    .password(password)
                    .build();
            try {
                SessionInfo sessionInfo = connectionChecker.checkConnection(connectCallInput);
                connectionChecker.saveSettings(null);
                askQuestionToStoreCredentials(configManagerProvider.get(), username, password);
                return connectSuccessCall.execute(sessionInfo, input.checkClusterInit());
            } catch (ApiException e) {
                return DefaultCallOutput.failure(new IgniteCliApiException(e, input.url()));
            }
        }
        return output;
    }
}
