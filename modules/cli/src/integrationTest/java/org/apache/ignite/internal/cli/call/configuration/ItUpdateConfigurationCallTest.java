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
 * Tests for {@link NodeConfigUpdateCall}.
 */
public class ItUpdateConfigurationCallTest extends CliIntegrationTest {

    @Inject
    ClusterConfigUpdateCall updateCall;

    @Inject
    ClusterConfigShowCall readCall;

    @Test
    @DisplayName("Should update cluster configuration")
    void shouldUpdateClusterConfiguration() {
        // Given default write buffer size
        String givenConfigurationProperty = readConfigurationProperty("gc.batchSize");
        assertThat(givenConfigurationProperty).isEqualTo("5");
        // And
        var input = ClusterConfigUpdateCallInput.builder()
                .clusterUrl(NODE_URL)
                .config("{gc: {batchSize: 1}}")
                .build();

        // When update buffer size
        DefaultCallOutput<String> output = updateCall.execute(input);

        // Then
        assertThat(output.hasError()).isFalse();
        // And
        assertThat(output.body()).contains("Cluster configuration was updated successfully");
        // And buffer size is updated
        String updatedConfigurationProperty = readConfigurationProperty("gc.batchSize");
        assertThat(updatedConfigurationProperty).isEqualTo("1");

        // When update buffer size back to default but using key-value format
        updateCall.execute(
                ClusterConfigUpdateCallInput.builder()
                        .clusterUrl(NODE_URL)
                        .config("gc.batchSize=5")
                        .build()
        );

        // Then buffer size is updated
        assertThat(readConfigurationProperty("gc.batchSize")).isEqualTo("5");
    }

    private String readConfigurationProperty(String selector) {
        var input = ClusterConfigShowCallInput.builder().clusterUrl(NODE_URL).selector(selector).build();
        return readCall.execute(input).body().getValue();
    }
}
