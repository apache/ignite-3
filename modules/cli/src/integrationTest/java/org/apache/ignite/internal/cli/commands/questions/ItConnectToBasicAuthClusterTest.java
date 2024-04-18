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

import static org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper.readClusterConfigurationWithEnabledAuth;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.cli.commands.ItConnectToClusterTestBase;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ItConnectToBasicAuthClusterTest extends ItConnectToClusterTestBase {

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration(readClusterConfigurationWithEnabledAuth());
    }

    @Test
    @DisplayName("Should ask for auth configuration connect to last connected cluster HTTPS url")
    void connectOnStartAskAuth() throws IOException {
        // Given prompt before connect
        assertThat(getPrompt()).isEqualTo("[disconnected]> ");

        // And answer to the reconnect question is "y", to the auth configuration question is "y",
        // username and password are provided and answer to save authentication is "y"
        bindAnswers("y", "y", "admin", "password", "y");

        // When asked the question
        question.askQuestionOnReplStart();

        // Then
        assertAll(
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Connected to http://localhost:10300")
        );

        // And prompt shows user name and node name
        assertThat(getPrompt()).isEqualTo("[admin:" + nodeName() + "]> ");

        assertThat(getConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_USERNAME)).isEqualTo("admin");
        assertThat(getConfigProperty(CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD)).isEqualTo("password");
    }
}
