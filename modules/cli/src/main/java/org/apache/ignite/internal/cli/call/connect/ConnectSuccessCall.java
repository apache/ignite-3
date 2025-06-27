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

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.StateConfigProvider;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent.MessageComponentBuilder;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.internal.cli.event.EventPublisher;
import org.apache.ignite.internal.cli.event.Events;
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Call which store connection info and notify all listeners about successful connection to the Ignite 3 node.
 */
@Singleton
public class ConnectSuccessCall {

    private final StateConfigProvider stateConfigProvider;

    private final EventPublisher eventPublisher;

    private final ApiClientFactory clientFactory;

    /**
     * Constructor.
     */
    public ConnectSuccessCall(StateConfigProvider stateConfigProvider, EventPublisher eventPublisher, ApiClientFactory clientFactory) {
        this.stateConfigProvider = stateConfigProvider;
        this.eventPublisher = eventPublisher;
        this.clientFactory = clientFactory;
    }

    /**
     * Executes a series of steps after successful connection to the node. Sets the last connected config property, publishes an event,
     * optionally checks whether the cluster is initialized or not and returns a message indicating a success.
     *
     * @param sessionInfo Session details.
     * @param checkClusterInit If {@code true}, the method will call a REST API to check if the cluster is initialized.
     * @return Call output with the message string.
     */
    public CallOutput<String> execute(SessionInfo sessionInfo, boolean checkClusterInit) {
        stateConfigProvider.get().setProperty(CliConfigKeys.LAST_CONNECTED_URL.value(), sessionInfo.nodeUrl());

        eventPublisher.publish(Events.connect(sessionInfo));

        MessageComponentBuilder builder = MessageUiComponent.builder()
                .message("Connected to %s", UiElements.url(sessionInfo.nodeUrl()));

        if (checkClusterInit) {
            checkClusterInit(sessionInfo, builder);
        }

        return DefaultCallOutput.success(builder.build().render());
    }

    private void checkClusterInit(SessionInfo sessionInfo, MessageComponentBuilder builder) {
        try {
            new ClusterManagementApi(clientFactory.getClient(sessionInfo.nodeUrl())).clusterState();
        } catch (ApiException e) {
            if (e.getCode() == 409) { // CONFLICT means the cluster is not initialized yet
                builder.hint("The cluster is not initialized. Run %s command to initialize it.", UiElements.command("cluster init"));
            }
        }
    }
}
