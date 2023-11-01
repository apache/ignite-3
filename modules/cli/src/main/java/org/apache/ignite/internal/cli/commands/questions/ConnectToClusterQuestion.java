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

import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_USERNAME;
import static org.apache.ignite.internal.cli.core.style.component.QuestionUiComponent.fromYesNoQuestion;
import static org.apache.ignite.internal.cli.core.style.element.UiElements.username;
import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Objects;
import org.apache.ignite.internal.cli.call.connect.AuthConfig;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput;
import org.apache.ignite.internal.cli.call.connect.ConnectWizardCall;
import org.apache.ignite.internal.cli.call.connect.SslConfig;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.config.StateConfigProvider;
import org.apache.ignite.internal.cli.core.flow.Flow;
import org.apache.ignite.internal.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.apache.ignite.internal.cli.core.flow.question.QuestionAskerFactory;
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
    private ConnectWizardCall connectCall;

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

        QuestionUiComponent questionUiComponent = fromYesNoQuestion(
                "You are not connected to node. Do you want to connect to the default node %s?",
                UiElements.url(defaultUrl)
        );

        return Flows.<Void, ConnectCallInput>acceptQuestion(questionUiComponent,
                        () -> ConnectCallInput.builder().url(defaultUrl).build())
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
     * Ask if the user really wants to connect if we are already connected and the URL or authentication parameters are different.
     *
     * @param input Node URL and authentication parameters.
     * @return {@link FlowBuilder} instance which provides the connect call input if we are not connected or user agreed to reconnect or
     *         interrupts if user's answer is negative.
     */
    public FlowBuilder<Void, ConnectCallInput> askQuestionIfConnected(ConnectCallInput input) {
        SessionInfo sessionInfo = session.info();
        if (sessionInfo != null) {
            if (!Objects.equals(sessionInfo.nodeUrl(), input.url())) {
                return Flows.acceptQuestion(fromYesNoQuestion(
                        "You are already connected to the %s, do you want to connect to the %s?",
                        UiElements.url(sessionInfo.nodeUrl()), UiElements.url(input.url())
                ), () -> input);
            }

            String oldUsername = sessionInfo.username();
            // This username will be used for connect by the connection checker.
            String newUsername = input.username() != null
                    ? input.username()
                    : configManagerProvider.get().getCurrentProperty(BASIC_AUTHENTICATION_USERNAME.value());

            if (newUsername != null) {
                if (oldUsername == null) {
                    return Flows.acceptQuestion(fromYesNoQuestion(
                            "You are already connected to the %s, do you want to connect as %s?",
                            UiElements.url(sessionInfo.nodeUrl()), username(newUsername)
                    ), () -> input);
                }
                if (!oldUsername.equals(newUsername)) {
                    return Flows.acceptQuestion(fromYesNoQuestion(
                            "You are already connected to the %s as %s, do you want to connect as %s?",
                            UiElements.url(sessionInfo.nodeUrl()), username(oldUsername), username(newUsername)
                    ), () -> input);
                }
            }
        }
        return Flows.from(input);
    }

    /**
     * Ask if the user wants to store credentials in config.
     *
     * @param configManager Config manager.
     * @param username username.
     * @param password password
     */
    public static void askQuestionToStoreCredentials(ConfigManager configManager, @Nullable String username, @Nullable String password) {
        if (!nullOrBlank(username) && !nullOrBlank(password)) {
            String storedUsername = configManager.getCurrentProperty(BASIC_AUTHENTICATION_USERNAME.value());
            String storedPassword = configManager.getCurrentProperty(BASIC_AUTHENTICATION_PASSWORD.value());

            // Ask question only if cli config has different values.
            if (!username.equals(storedUsername) || !password.equals(storedPassword)) {
                QuestionUiComponent question = fromYesNoQuestion("Remember current credentials?");
                Flows.acceptQuestion(question, () -> {
                    configManager.setProperty(BASIC_AUTHENTICATION_USERNAME.value(), username);
                    configManager.setProperty(BASIC_AUTHENTICATION_PASSWORD.value(), password);
                    return "Config saved";
                }).print().start();
            }
        }
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
            question = fromYesNoQuestion(
                    "Do you want to reconnect to the last connected node %s?",
                    UiElements.url(lastConnectedUrl)
            );
            clusterUrl = lastConnectedUrl;
        } else if (defaultUrl != null) {
            question = fromYesNoQuestion(
                    "You appear to have not connected to any node yet. Do you want to connect to the default node %s?",
                    UiElements.url(defaultUrl)
            );
            clusterUrl = defaultUrl;
        } else {
            return;
        }

        Flows.acceptQuestion(question, () -> ConnectCallInput.builder().url(clusterUrl).build())
                .then(Flows.fromCall(connectCall))
                .print()
                .ifThen(s -> !Objects.equals(clusterUrl, defaultUrl) && session.info() != null,
                        defaultUrlQuestion(clusterUrl).print().build())
                .start();
    }

    private FlowBuilder<String, String> defaultUrlQuestion(String lastConnectedUrl) {
        return Flows.acceptQuestion(
                fromYesNoQuestion("Would you like to use %s as the default URL?", UiElements.url(lastConnectedUrl)),
                () -> {
                    configManagerProvider.get().setProperty(CliConfigKeys.CLUSTER_URL.value(), lastConnectedUrl);
                    return "Config saved";
                }
        );
    }

    /**
     * Ask if the user wants to enter SSL configuration to retry connect.
     *
     * @return {@link FlowBuilder} instance which provides the {@link SslConfig} or interrupts if user's answer is negative.
     */
    public static Flow<Void, SslConfig> askQuestionOnSslError() {
        QuestionUiComponent question = fromYesNoQuestion(
                "SSL error occurred while connecting to the node, it could be due to the wrong trust store/key store configuration. "
                        + "Do you want to configure them now?"
        );
        QuestionUiComponent question2 = fromYesNoQuestion("Do you want to configure key store?");

        return Flows.<Void, SslConfig>acceptQuestion(question, () -> {
            SslConfig config = new SslConfig();
            config.trustStorePath(escapeWindowsPath(enterFilePath("trust store path")));
            config.trustStorePassword(enterPassword("trust store password"));
            return config;
        }).then(Flows.acceptQuestionFlow(question2, config -> {
            config.keyStorePath(escapeWindowsPath(enterFilePath("key store path")));
            config.keyStorePassword(enterPassword("key store password"));
        })).build();
    }


    /**
     * Ask if the user wants to enter basic auth configuration to retry connect.
     *
     * @return {@link FlowBuilder} instance which provides the {@link ConnectCallInput} or interrupts if user's answer is negative.
     */
    public static Flow<Void, AuthConfig> askQuestionOnAuthError() {
        QuestionUiComponent question = fromYesNoQuestion(
                "Authentication error occurred while connecting to the node, it could be due to the wrong basic auth configuration. "
                        + "Do you want to configure them now?"
        );

        return Flows.<Void, AuthConfig>acceptQuestion(question, () -> {
            AuthConfig authConfig = new AuthConfig();
            authConfig.username(enterString("username"));
            authConfig.password(enterPassword("user password"));
            return authConfig;
        }).build();
    }

    private static String enterString(String question) {
        return QuestionAskerFactory.newQuestionAsker()
                .askQuestion("Enter " + question + ": ");
    }

    private static String enterFilePath(String question) {
        return QuestionAskerFactory.newQuestionAsker()
                .completeFilePaths(true)
                .askQuestion("Enter " + question + ": ");
    }

    private static String enterPassword(String question) {
        return QuestionAskerFactory.newQuestionAsker()
                .maskInput(true)
                .askQuestion("Enter " + question + ": ");
    }

    /** Escapes single backslashes to double backslashes, skips double backslashes if they are present. */
    private static String escapeWindowsPath(String string) {
        if (string.indexOf('\\') < 0) {
            return string;
        }
        StringBuilder sb = new StringBuilder(string.length());
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            sb.append(c);
            if (c == '\\') {
                if (i == string.length() - 1 || string.charAt(i + 1) != '\\') {
                    sb.append('\\');
                } else {
                    i++;
                }
            }

        }
        return sb.toString();
    }
}
