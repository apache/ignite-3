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

import static org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion.askQuestionToStoreCredentials;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_USERNAME;
import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.util.Objects;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.rest.ApiClientSettings;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.internal.cli.event.EventPublisher;
import org.apache.ignite.internal.cli.event.Events;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.jetbrains.annotations.Nullable;


/**
 * Call for connect to Ignite 3 node. As a result {@link Session} will hold a valid node-url.
 */
@Singleton
public class ConnectCall implements Call<ConnectCallInput, String> {
    private final Session session;

    private final ApiClientFactory clientFactory;

    private final EventPublisher eventPublisher;

    private final ConnectSuccessCall connectSuccessCall;

    private final ConnectionChecker connectionChecker;

    private final ConfigManagerProvider configManagerProvider;

    /**
     * Constructor.
     */
    public ConnectCall(
            Session session,
            ApiClientFactory clientFactory,
            EventPublisher eventPublisher,
            ConnectSuccessCall connectSuccessCall,
            ConnectionChecker connectionChecker,
            ConfigManagerProvider configManagerProvider
    ) {
        this.session = session;
        this.clientFactory = clientFactory;
        this.eventPublisher = eventPublisher;
        this.connectSuccessCall = connectSuccessCall;
        this.connectionChecker = connectionChecker;
        this.configManagerProvider = configManagerProvider;
    }

    @Override
    public CallOutput<String> execute(ConnectCallInput input) {
        String nodeUrl = input.url();
        SessionInfo sessionInfo = session.info();
        if (sessionInfo != null && Objects.equals(sessionInfo.nodeUrl(), nodeUrl)) {
            // This username will be used for connect by the connection checker.
            String username = input.username() != null
                    ? input.username()
                    : configManagerProvider.get().getCurrentProperty(BASIC_AUTHENTICATION_USERNAME.value());
            if (Objects.equals(sessionInfo.username(), username)) {
                MessageUiComponent message = MessageUiComponent.fromMessage("You are already connected to %s", UiElements.url(nodeUrl));
                return DefaultCallOutput.success(message.render());
            }
        }
        try {
            // Try without authentication first to check whether the authentication is enabled on the cluster.
            sessionInfo = connectWithoutAuthentication(input);
            if (sessionInfo == null) {
                // Try with authentication
                sessionInfo = connectionChecker.checkConnection(input);
                if (!nullOrBlank(input.username()) && !nullOrBlank(input.password())) {
                    // Use current credentials as default for api clients
                    ApiClientSettings clientSettings = ApiClientSettings.builder()
                            .basicAuthenticationUsername(input.username())
                            .basicAuthenticationPassword(input.password())
                            .build();
                    clientFactory.setSessionSettings(clientSettings);
                }
            } else if (!nullOrBlank(input.username()) || !nullOrBlank(input.password())) {
                // Cluster without authentication but connect command invoked with username/password
                return DefaultCallOutput.failure(
                        handleException(new IgniteCliException(
                                "Authentication is not enabled on cluster but username or password were provided."),
                                sessionInfo.nodeUrl()));
            }

            askQuestionToStoreCredentials(configManagerProvider.get(), input.username(), input.password());
            return connectSuccessCall.execute(sessionInfo);
        } catch (Exception e) {
            if (session.info() != null) {
                eventPublisher.publish(Events.disconnect());
            }
            return DefaultCallOutput.failure(handleException(e, nodeUrl));
        }
    }

    @Nullable
    private SessionInfo connectWithoutAuthentication(ConnectCallInput connectCallInput) throws ApiException {
        try {
            return connectionChecker.checkConnectionWithoutAuthentication(connectCallInput);
        } catch (ApiException e) {
            if (e.getCause() == null && e.getCode() == HttpStatus.UNAUTHORIZED.getCode()) {
                return null;
            } else {
                throw e;
            }
        }
    }

    private static IgniteCliApiException handleException(Exception e, String nodeUrl) {
        if (e instanceof IgniteCliApiException) {
            return (IgniteCliApiException) e;
        } else {
            return new IgniteCliApiException(e, nodeUrl);
        }
    }
}
