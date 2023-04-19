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

import com.google.gson.Gson;
import jakarta.inject.Singleton;
import java.net.MalformedURLException;
import java.util.Objects;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.StateConfigProvider;
import org.apache.ignite.internal.cli.core.JdbcUrl;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.config.RootConfig;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;


/**
 * Call for connect to Ignite 3 node. As a result {@link Session} will hold a valid node-url.
 */
@Singleton
public class ConnectCall implements Call<UrlCallInput, String> {
    private final Session session;

    private final StateConfigProvider stateConfigProvider;

    private final ApiClientFactory clientFactory;

    /**
     * Constructor.
     */
    public ConnectCall(Session session, StateConfigProvider stateConfigProvider, ApiClientFactory clientFactory) {
        this.session = session;
        this.stateConfigProvider = stateConfigProvider;
        this.clientFactory = clientFactory;
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
            String configuration = fetchNodeConfiguration(nodeUrl);
            stateConfigProvider.get().setProperty(CliConfigKeys.LAST_CONNECTED_URL.value(), nodeUrl);
            session.connect(new SessionInfo(nodeUrl, fetchNodeName(nodeUrl), constructJdbcUrl(configuration, nodeUrl)));
            return DefaultCallOutput.success(MessageUiComponent.fromMessage("Connected to %s", UiElements.url(nodeUrl)).render());
        } catch (ApiException | IllegalArgumentException e) {
            session.disconnect();
            return DefaultCallOutput.failure(new IgniteCliApiException(e, nodeUrl));
        }
    }

    private String fetchNodeName(String nodeUrl) throws ApiException {
        return new NodeManagementApi(clientFactory.getClient(nodeUrl)).nodeState().getName();
    }

    private String fetchNodeConfiguration(String nodeUrl) throws ApiException {
        return new NodeConfigurationApi(clientFactory.getClient(nodeUrl)).getNodeConfiguration();
    }

    private String constructJdbcUrl(String configuration, String nodeUrl) {
        try {
            int port = new Gson().fromJson(configuration, RootConfig.class).clientConnector.port;
            return JdbcUrl.of(nodeUrl, port).toString();
        } catch (MalformedURLException ignored) {
            // Shouldn't happen ever since we are now connected to this URL
            return null;
        }
    }
}
