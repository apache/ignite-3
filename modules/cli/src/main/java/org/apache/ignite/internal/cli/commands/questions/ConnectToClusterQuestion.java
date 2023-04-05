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

package org.apache.ignite.internal.cli.commands.questions;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Objects;
import org.apache.ignite.internal.cli.call.connect.ConnectCall;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.config.StateConfigProvider;
import org.apache.ignite.internal.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.style.component.QuestionUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.jetbrains.annotations.Nullable;


/**
 * Wrapper of command call to question with connection checking.
 */
@Singleton
public class ConnectToClusterQuestion {
    @Inject
    private ConnectCall connectCall;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    @Inject
    private StateConfigProvider stateConfigProvider;

    @Inject
    private Session session;


    /**
     * Asks whether the user wants to connect to the default node when user hasn't passed a URL explicitly and we're not connected.
     *
     * @param clusterUrl cluster url.
     * @return {@link FlowBuilder} instance which returns a URL.
     */
    public FlowBuilder<Void, String> askQuestionIfNotConnected(String clusterUrl) {
        String url = clusterUrlOrSessionNode(clusterUrl);
        if (url != null) {
            return Flows.from(url);
        }

        String defaultUrl = configManagerProvider.get().getCurrentProperty(CliConfigKeys.CLUSTER_URL.value());

        QuestionUiComponent questionUiComponent = QuestionUiComponent.fromQuestion(
                "You are not connected to node. Do you want to connect to the default node %s? %s ",
                UiElements.url(defaultUrl), UiElements.yesNo()
        );

        return Flows.<Void, ConnectCallInput>acceptQuestion(questionUiComponent, () -> new ConnectCallInput(defaultUrl))
                .then(Flows.fromCall(connectCall))
                .print()
                .map(ignored -> sessionNodeUrl());
    }

    @Nullable
    private String clusterUrlOrSessionNode(String clusterUrl) {
        return clusterUrl != null ? clusterUrl : sessionNodeUrl();
    }

    @Nullable
    private String sessionNodeUrl() {
        return session.info() != null ? session.info().nodeUrl() : null;
    }

    /**
     * Ask if the user really wants to connect if we are already connected and the URL is different.
     *
     * @param clusterUrl cluster url.
     * @return {@link FlowBuilder} instance with question in case when cluster url.
     */
    public FlowBuilder<Void, String> askQuestionIfConnected(String clusterUrl) {
        SessionInfo sessionInfo = session.info();
        if (sessionInfo != null && !Objects.equals(sessionInfo.nodeUrl(), clusterUrl)) {
            QuestionUiComponent question = QuestionUiComponent.fromQuestion(
                    "You are already connected to the %s, do you want to connect to the %s? %s ",
                    UiElements.url(sessionInfo.nodeUrl()), UiElements.url(clusterUrl), UiElements.yesNo()
            );
            return Flows.acceptQuestion(question, () -> clusterUrl);
        }
        return Flows.from(clusterUrl);
    }

    /**
     * Ask for connect to the cluster and suggest to save the last connected URL as default.
     */
    public void askQuestionOnReplStart() {
        if (session.info() != null) {
            return;
        }
        String defaultUrl = configManagerProvider.get().getCurrentProperty(CliConfigKeys.CLUSTER_URL.value());
        String lastConnectedUrl = stateConfigProvider.get().getProperty(CliConfigKeys.LAST_CONNECTED_URL.value());
        QuestionUiComponent question;
        String clusterUrl;
        if (lastConnectedUrl != null) {
            question = QuestionUiComponent.fromQuestion(
                    "Do you want to reconnect to the last connected node %s? %s ",
                    UiElements.url(lastConnectedUrl), UiElements.yesNo()
            );
            clusterUrl = lastConnectedUrl;
        } else if (defaultUrl != null) {
            question = QuestionUiComponent.fromQuestion(
                    "You appear to have not connected to any node yet. Do you want to connect to the default node %s? %s ",
                    UiElements.url(defaultUrl), UiElements.yesNo()
            );
            clusterUrl = defaultUrl;
        } else {
            return;
        }

        Flows.acceptQuestion(question, () -> new ConnectCallInput(clusterUrl))
                .then(Flows.fromCall(connectCall))
                .print()
                .ifThen(s -> !Objects.equals(clusterUrl, defaultUrl) && session.info() != null,
                        defaultUrlQuestion(clusterUrl).print().build())
                .start();
    }

    private FlowBuilder<String, String> defaultUrlQuestion(String lastConnectedUrl) {
        return Flows.acceptQuestion(QuestionUiComponent.fromQuestion(
                "Would you like to use %s as the default URL? %s ", UiElements.url(lastConnectedUrl), UiElements.yesNo()
                ), () -> {
                    configManagerProvider.get().setProperty(CliConfigKeys.CLUSTER_URL.value(), lastConnectedUrl);
                    return "Config saved";
                }
        );
    }
}
