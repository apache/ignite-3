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

import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_USERNAME;
import static org.apache.ignite.lang.util.StringUtils.nullOrBlank;

import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.util.Objects;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.config.StateConfigProvider;
import org.apache.ignite.internal.cli.core.JdbcUrlFactory;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.internal.cli.event.EventPublisher;
import org.apache.ignite.internal.cli.event.Events;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.jetbrains.annotations.Nullable;


/**
 * Call for connect to Ignite 3 node. As a result {@link Session} will hold a valid node-url.
 */
@Singleton
public class ConnectCall implements Call<ConnectCallInput, String> {
    private final Session session;

    private final StateConfigProvider stateConfigProvider;

    private final ApiClientFactory clientFactory;

    private final JdbcUrlFactory jdbcUrlFactory;

    private final ConfigManagerProvider configManagerProvider;

    private final EventPublisher eventPublisher;

    /**
     * Constructor.
     */
    public ConnectCall(Session session, StateConfigProvider stateConfigProvider, ApiClientFactory clientFactory,
            JdbcUrlFactory jdbcUrlFactory, ConfigManagerProvider configManagerProvider,
            EventPublisher eventPublisher) {
        this.session = session;
        this.stateConfigProvider = stateConfigProvider;
        this.clientFactory = clientFactory;
        this.jdbcUrlFactory = jdbcUrlFactory;
        this.configManagerProvider = configManagerProvider;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public CallOutput<String> execute(ConnectCallInput input) {
        String nodeUrl = input.url();
        SessionInfo sessionInfo = session.info();
        if (sessionInfo != null && Objects.equals(sessionInfo.nodeUrl(), nodeUrl)) {
            MessageUiComponent message = MessageUiComponent.fromMessage("You are already connected to %s", UiElements.url(nodeUrl));
            return DefaultCallOutput.success(message.render());
        }
        try {
            // Try without authentication first to check whether the authentication is enabled on the cluster.
            sessionInfo = connectWithoutAuthentication(nodeUrl);
            if (sessionInfo == null) {
                // Try with authentication
                if (!nullOrBlank(input.username()) && !nullOrBlank(input.password())) {
                    sessionInfo = connectWithAuthentication(nodeUrl, input.username(), input.password());
                } else {
                    sessionInfo = connectWithAuthentication(nodeUrl);
                }
            }

            stateConfigProvider.get().setProperty(CliConfigKeys.LAST_CONNECTED_URL.value(), nodeUrl);

            eventPublisher.publish(Events.connect(sessionInfo));

            return DefaultCallOutput.success(MessageUiComponent.fromMessage("Connected to %s", UiElements.url(nodeUrl)).render());
        } catch (Exception e) {
            if (session.info() != null) {
                eventPublisher.publish(Events.disconnect());
            }
            return DefaultCallOutput.failure(handleException(e, nodeUrl));
        }
    }

    @Nullable
    private SessionInfo connectWithoutAuthentication(String nodeUrl) throws ApiException {
        try {
            ApiClient apiClient = clientFactory.getClientWithoutBasicAuthentication(nodeUrl);
            return constructSessionInfo(apiClient, nodeUrl, null);
        } catch (ApiException e) {
            if (e.getCause() == null) {
                if (e.getCode() == HttpStatus.UNAUTHORIZED.getCode()) {
                    return null;
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    private SessionInfo connectWithAuthentication(String nodeUrl, String inputUsername, String inputPassword) throws ApiException {
        ApiClient apiClient = clientFactory.getClient(nodeUrl, inputUsername, inputPassword);
        return constructSessionInfo(apiClient, nodeUrl, inputUsername);
    }

    private SessionInfo connectWithAuthentication(String nodeUrl) throws ApiException {
        ApiClient apiClient = clientFactory.getClient(nodeUrl);
        String username = configManagerProvider.get().getCurrentProperty(BASIC_AUTHENTICATION_USERNAME.value());
        return constructSessionInfo(apiClient, nodeUrl, username);
    }

    private SessionInfo constructSessionInfo(ApiClient apiClient, String nodeUrl, @Nullable String username) throws ApiException {
        String configuration = new NodeConfigurationApi(apiClient).getNodeConfiguration();
        String nodeName = new NodeManagementApi(apiClient).nodeState().getName();
        String jdbcUrl = jdbcUrlFactory.constructJdbcUrl(configuration, nodeUrl);
        return SessionInfo.builder().nodeUrl(nodeUrl).nodeName(nodeName).jdbcUrl(jdbcUrl).username(username).build();
    }

    private static IgniteCliApiException handleException(Exception e, String nodeUrl) {
        if (e instanceof IgniteCliApiException) {
            return (IgniteCliApiException) e;
        } else {
            return new IgniteCliApiException(e, nodeUrl);
        }
    }
}
