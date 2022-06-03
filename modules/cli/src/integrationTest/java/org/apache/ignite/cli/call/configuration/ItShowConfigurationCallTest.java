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

package org.apache.ignite.cli.call.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.primitives.Chars;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cli.call.CallIntegrationTestBase;
import org.apache.ignite.cli.core.call.CallExecutionPipeline;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.exception.handler.CommandExecutionExceptionHandler;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ShowConfigurationCall}.
 */
class ItShowConfigurationCallTest extends CallIntegrationTestBase {

    @Inject
    ShowConfigurationCall call;

    @Test
    @DisplayName("Should show cluster configuration when cluster up and running")
    void readClusterConfiguration() {
        // Given
        var input = ShowConfigurationCallInput.builder()
                .clusterUrl(NODE_URL)
                .build();

        // When
        DefaultCallOutput<String> output = call.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body()).isNotEmpty();
    }

    @Test
    @DisplayName("Should show cluster configuration by path when cluster up and running")
    void readClusterConfigurationByPath() {
        // Given
        var input = ShowConfigurationCallInput.builder()
                .clusterUrl(NODE_URL)
                .selector("rocksDb.defaultRegion.cache")
                .build();

        // When
        DefaultCallOutput<String> output = call.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body()).isEqualTo("\"lru\"");
    }

    @Test
    @DisplayName("Should show node configuration when cluster up and running")
    void readNodeConfiguration() {
        // Given
        var input = ShowConfigurationCallInput.builder()
                .clusterUrl(NODE_URL)
                .nodeId(CLUSTER_NODES.get(0).name())
                .build();

        // When
        DefaultCallOutput<String> output = call.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body()).isNotEmpty();
    }

    @Test
    @DisplayName("Should show node configuration by path when cluster up and running")
    void readNodeConfigurationByPath() {
        // Given
        var input = ShowConfigurationCallInput.builder()
                .clusterUrl(NODE_URL)
                .nodeId(CLUSTER_NODES.get(0).name())
                .selector("clientConnector.connectTimeout")
                .build();

        // When
        DefaultCallOutput<String> output = call.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body()).isEqualTo("5000");
    }

    @Test
    @DisplayName("Should return error if wrong nodename is given")
    @Disabled
    //TODO: https://issues.apache.org/jira/browse/IGNITE-17089
    void readNodeConfigurationWithWrongNodename() {
        // Given
        var input = ShowConfigurationCallInput.builder()
                .clusterUrl(NODE_URL)
                .nodeId("no-such-node")
                .build();

        // When
        DefaultCallOutput<String> output = call.execute(input);

        // Then
        assertThat(output.hasError()).isTrue();
    }

    @Test
    @DisplayName("Should display error when wrong port is given")
    public void incorrectPortTest() {
        var input = ShowConfigurationCallInput.builder()
                .clusterUrl(NODE_URL + "incorrect")
                .build();
        List<Character> list = new ArrayList<>();

        CallExecutionPipeline.builder(call)
                .inputProvider(() -> input)
                .exceptionHandler(new CommandExecutionExceptionHandler())
                .errOutput(output(list))
                .build()
                .runPipeline();

        assertThat(new String(Chars.toArray(list)))
                .contains("Invalid URL port: \"10300incorrect");
    }

    @Test
    @DisplayName("Should display error when wrong url is given")
    public void incorrectSchemeTest() {
        var input = ShowConfigurationCallInput.builder()
                .clusterUrl("incorrect" + NODE_URL)
                .build();
        List<Character> list = new ArrayList<>();

        CallExecutionPipeline.builder(call)
                .inputProvider(() -> input)
                .exceptionHandler(new CommandExecutionExceptionHandler())
                .errOutput(output(list))
                .build()
                .runPipeline();

        assertThat(new String(Chars.toArray(list)))
                .contains("Expected URL scheme 'http' or 'https' but was 'incorrecthttp'");
    }
}
