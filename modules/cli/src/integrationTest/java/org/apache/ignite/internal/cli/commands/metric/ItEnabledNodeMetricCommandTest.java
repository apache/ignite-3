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
    @DisplayName("Should display enabled jvm metric source when valid node-url is given")
    void nodeMetricSourcesList() {
        // When list node metric sources with valid url
        execute("node", "metric", "source", "list", "--plain", "--node-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Set name\tEnabled" + NL
                        + "jvm\tenabled" + NL)
        );
    }

    @Test
    @DisplayName("Should display node metrics list when valid node-url is given")
    void nodeMetricEnableNonexistent() {
        // When list node metric with valid url
        execute("node", "metric", "list", "--plain", "--node-url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("Set name\tMetric name\tDescription" + NL
                        + "jvm\t\t" + NL
                        + "\tmemory.heap.committed\tCommitted amount of heap memory" + NL
                        + "\tmemory.heap.max\tMaximum amount of heap memory" + NL
                        + "\tmemory.non-heap.max\tMaximum amount of non-heap memory" + NL
                        + "\tmemory.non-heap.init\tInitial amount of non-heap memory" + NL
                        + "\tmemory.non-heap.committed\tCommitted amount of non-heap memory" + NL
                        + "\tmemory.non-heap.used\tUsed amount of non-heap memory" + NL
                        + "\tmemory.heap.used\tCurrent used amount of heap memory" + NL
                        + "\tmemory.heap.init\tInitial amount of heap memory" + NL
                )
        );
    }
}
