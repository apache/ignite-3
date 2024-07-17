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

package org.apache.ignite.internal.cli.call.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link NodeConfigShowCall}.
 */
class ItShowConfigurationCallTest extends CliIntegrationTest {

    @Inject
    NodeConfigShowCall nodeConfigShowCall;

    @Inject
    ClusterConfigShowCall clusterConfigShowCall;

    @Test
    @DisplayName("Should show cluster configuration when cluster up and running")
    void readClusterConfiguration() {
        // Given
        var input = ClusterConfigShowCallInput.builder()
                .clusterUrl(NODE_URL)
                .build();

        // When
        DefaultCallOutput<JsonString> output = clusterConfigShowCall.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body().getValue()).isNotEmpty();
    }

    @Test
    @DisplayName("Should show cluster configuration by path when cluster up and running")
    void readClusterConfigurationByPath() {
        // Given
        var input = ClusterConfigShowCallInput.builder()
                .clusterUrl(NODE_URL)
                .selector("gc.batchSize")
                .build();

        // When
        DefaultCallOutput<JsonString> output = clusterConfigShowCall.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body().getValue()).isEqualTo("5");
    }

    @Test
    @DisplayName("Should show node configuration when cluster up and running")
    void readNodeConfiguration() {
        // Given
        var input = NodeConfigShowCallInput.builder()
                .nodeUrl(NODE_URL)
                .build();

        // When
        DefaultCallOutput<JsonString> output = nodeConfigShowCall.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body().getValue()).isNotEmpty();
    }

    @Test
    @DisplayName("Should show node configuration by path when cluster up and running")
    void readNodeConfigurationByPath() {
        // Given
        var input = NodeConfigShowCallInput.builder()
                .nodeUrl(NODE_URL)
                .selector("clientConnector.connectTimeout")
                .build();

        // When
        DefaultCallOutput<JsonString> output = nodeConfigShowCall.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body().getValue()).isEqualTo("5000");
    }
}
