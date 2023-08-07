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

import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.util.Objects;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.StateConfigProvider;
import org.apache.ignite.internal.cli.core.JdbcUrlFactory;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.exception.handler.IgniteCliApiExceptionHandler;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.internal.cli.event.EventPublisher;
import org.apache.ignite.internal.cli.event.Events;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.Problem;
import org.jetbrains.annotations.Nullable;


/**
 * Call for connect to Ignite 3 node. As a result {@link Session} will hold a valid node-url.
 */
@Singleton
public class ConnectCall implements Call<UrlCallInput, String> {
    private final Session session;

    private final StateConfigProvider stateConfigProvider;

    private final ApiClientFactory clientFactory;

    private final JdbcUrlFactory jdbcUrlFactory;

    private final EventPublisher eventPublisher;

    /**
     * Constructor.
     */
    public ConnectCall(Session session, StateConfigProvider stateConfigProvider, ApiClientFactory clientFactory,
            JdbcUrlFactory jdbcUrlFactory, EventPublisher eventPublisher) {
        this.session = session;
        this.stateConfigProvider = stateConfigProvider;
        this.clientFactory = clientFactory;
        this.jdbcUrlFactory = jdbcUrlFactory;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public CallOutput<String> execute(UrlCallInput input) {
        String nodeUrl = input.getUrl();
        SessionInfo sessionInfo = session.info();
        if (sessionInfo != null && Objects.equals(sessionInfo.nodeUrl(), nodeUrl)) {
            MessageUiComponent message = MessageUiComponent.fromMessage("You are already connected to %s", UiElements.url(nodeUrl));
            return DefaultCallOutput.success(message.render());
        }
        try {
            String username = getAuthenticatedUsername(nodeUrl);
            String configuration = fetchNodeConfiguration(nodeUrl);
            stateConfigProvider.get().setProperty(CliConfigKeys.LAST_CONNECTED_URL.value(), nodeUrl);

            String jdbcUrl = jdbcUrlFactory.constructJdbcUrl(configuration, nodeUrl);
            sessionInfo = SessionInfo.builder()
                    .nodeUrl(nodeUrl)
                    .nodeName(fetchNodeName(nodeUrl))
                    .jdbcUrl(jdbcUrl)
                    .username(username)
                    .build();
            eventPublisher.publish(Events.connect(sessionInfo));

            return DefaultCallOutput.success(MessageUiComponent.fromMessage("Connected to %s", UiElements.url(nodeUrl)).render());
        } catch (Exception e) {
            if (session.info() != null) {
                eventPublisher.publish(Events.disconnect());
            }
            return DefaultCallOutput.failure(handleException(e, nodeUrl));
        }
    }

    private String fetchNodeName(String nodeUrl) throws ApiException {
        return new NodeManagementApi(clientFactory.getClient(nodeUrl)).nodeState().getName();
    }

    /**
     * Deduces whether the cluster has the authentication turned on and returns a name of successfully authenticated user or {@code null}.
     *
     * @param nodeUrl Node URL.
     * @return Username if authentication was successful or {@code null} if not or if the cluster has no authentication turned on.
     * @throws ApiException If fails to call an API.
     */
    @Nullable
    private String getAuthenticatedUsername(String nodeUrl) throws ApiException {
        // Try without authentication first to check whether the authentication is enabled on the cluster.
        String username = clientFactory.basicAuthenticationUsername();
        if (!nullOrBlank(username)) {
            try {
                new NodeConfigurationApi(clientFactory.getClientWithoutBasicAuthentication(nodeUrl)).getNodeConfiguration();
                return null;
            } catch (ApiException e) {
                if (e.getCause() == null) {
                    Problem problem = IgniteCliApiExceptionHandler.extractProblem(e);
                    if (problem.getStatus() == HttpStatus.UNAUTHORIZED.getCode()) {
                        new NodeConfigurationApi(clientFactory.getClient(nodeUrl)).getNodeConfiguration();
                        return username;
                    }
                }
                throw e;
            }
        }
        return null;
    }

    private String fetchNodeConfiguration(String nodeUrl) throws ApiException {
        return new NodeConfigurationApi(clientFactory.getClient(nodeUrl)).getNodeConfiguration();
    }

    private static IgniteCliApiException handleException(Exception e, String nodeUrl) {
        if (e instanceof IgniteCliApiException) {
            return (IgniteCliApiException) e;
        } else {
            return new IgniteCliApiException(e, nodeUrl);
        }
    }
}
