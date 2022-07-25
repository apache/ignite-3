/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.call.connect;

import com.google.gson.Gson;
import jakarta.inject.Singleton;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.ignite.cli.config.ConfigConstants;
import org.apache.ignite.cli.config.StateConfigProvider;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.cli.core.repl.Session;
import org.apache.ignite.cli.core.repl.config.RootConfig;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;


/**
 * Call for connect to Ignite 3 node. As a result {@link Session} will hold a valid node-url.
 */
@Singleton
public class ConnectCall implements Call<ConnectCallInput, String> {

    private final Session session;

    private final StateConfigProvider stateConfigProvider;

    public ConnectCall(Session session, StateConfigProvider stateConfigProvider) {
        this.session = session;
        this.stateConfigProvider = stateConfigProvider;
    }

    @Override
    public CallOutput<String> execute(ConnectCallInput input) {
        try {
            String nodeUrl = input.getNodeUrl();
            session.setNodeUrl(nodeUrl);
            stateConfigProvider.get().setProperty(ConfigConstants.LAST_CONNECTED_URL, nodeUrl);
            session.setNodeName(fetchNodeName(input));
            String configuration = fetchNodeConfiguration(input);
            session.setJdbcUrl(constructJdbcUrl(configuration, nodeUrl));
            session.setConnectedToNode(true);

            return DefaultCallOutput.success("Connected to " + nodeUrl);

        } catch (ApiException | IllegalArgumentException e) {
            session.setConnectedToNode(false);
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.getNodeUrl()));
        }
    }

    private String fetchNodeName(ConnectCallInput input) throws ApiException {
        return new NodeManagementApi(Configuration.getDefaultApiClient().setBasePath(input.getNodeUrl())).nodeState().getName();
    }

    private String fetchNodeConfiguration(ConnectCallInput input) throws ApiException {
        return new NodeConfigurationApi(Configuration.getDefaultApiClient().setBasePath(input.getNodeUrl())).getNodeConfiguration();
    }

    private String constructJdbcUrl(String configuration, String nodeUrl) {
        try {
            String host = new URL(nodeUrl).getHost();
            RootConfig config = new Gson().fromJson(configuration, RootConfig.class);
            return "jdbc:ignite:thin://" + host + ":" + config.clientConnector.port;
        } catch (MalformedURLException ignored) {
            // Shouldn't happen ever since we are now connected to this URL
            return null;
        }
    }
}
