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

package org.apache.ignite.internal.cli.commands.metric;

import static org.junit.jupiter.api.Assertions.assertAll;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.node.metric.NodeMetricSourceEnableCall;
import org.apache.ignite.internal.cli.call.node.metric.NodeMetricSourceEnableCallInput;
import org.apache.ignite.internal.cli.commands.CliCommandTestInitializedIntegrationBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for node metric commands with enabled metrics. */
class ItEnabledNodeMetricCommandTest extends CliCommandTestInitializedIntegrationBase {

    private static final String NL = System.lineSeparator();

    @Inject
    NodeMetricSourceEnableCall nodeMetricSourceEnableCall;

    @BeforeAll
    void beforeAll() {
        var inputEnable = NodeMetricSourceEnableCallInput.builder()
                .endpointUrl(NODE_URL)
                .srcName("jvm")
                .enable(true)
                .build();

        nodeMetricSourceEnableCall.execute(inputEnable);
    }

    @Test
    @DisplayName("Should display node metric sources list when valid node-url is given")
    void nodeMetricSourcesList() {
        // When list node metric sources with valid url
        execute("node", "metric", "source", "list", "--node-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Enabled metric sources:" + NL
                        + "jvm" + NL
                        + "Disabled metric sources:" + NL)
        );
    }

    @Test
    @DisplayName("Should display node metrics list when valid node-url is given")
    void nodeMetricEnableNonexistent() {
        // When list node metric with valid url
        execute("node", "metric", "list", "--node-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Metric sets:" + NL
                        + "  jvm" + NL
                        + "    memory.heap.committed - Committed amount of heap memory" + NL
                        + "    memory.heap.max - Maximum amount of heap memory" + NL
                        + "    memory.non-heap.max - Maximum amount of non-heap memory" + NL
                        + "    memory.non-heap.init - Initial amount of non-heap memory" + NL
                        + "    memory.non-heap.committed - Committed amount of non-heap memory" + NL
                        + "    memory.non-heap.used - Used amount of non-heap memory" + NL
                        + "    memory.heap.used - Current used amount of heap memory" + NL
                        + "    memory.heap.init - Initial amount of heap memory" + NL
                )
        );
    }
}
