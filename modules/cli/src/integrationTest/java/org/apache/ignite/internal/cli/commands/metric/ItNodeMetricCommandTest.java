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

import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for node metric commands. */
class ItNodeMetricCommandTest extends CliIntegrationTest {
    private static final String NL = System.lineSeparator();

    @Test
    @DisplayName("Should display disabled jvm metric source when valid node-url is given")
    void nodeMetricList() {
        // When list node metric with valid url
        execute("node", "metric", "source", "list", "--plain", "--url", NODE_URL);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Set name\tEnabled" + NL),
                () -> assertOutputContains("jvm\tdisabled" + NL),
                () -> assertOutputContains("client.handler\tdisabled" + NL),
                () -> assertOutputContains("sql.client\tdisabled" + NL),
                () -> assertOutputContains("sql.plan.cache\tdisabled" + NL)
        );
    }

    @Test
    @DisplayName("Should display error message when enabling nonexistent metric source and valid node-url is given")
    void nodeMetricEnableNonexistent() {
        // When list node metric with valid url
        execute("node", "metric", "source", "enable", "no.such.metric", "--url", NODE_URL);

        // Then
        assertAll(
                () -> assertExitCodeIs(1),
                () -> assertErrOutputContains("Metrics source with given name doesn't exist: no.such.metric"),
                this::assertOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should display error message when disabling nonexistent metric source and valid node-url is given")
    void nodeMetricDisableNonexistent() {
        // When list node metric with valid url
        execute("node", "metric", "source", "disable", "no.such.metric", "--url", NODE_URL);

        // Then
        assertAll(
                () -> assertExitCodeIs(1),
                () -> assertErrOutputContains("Metrics source with given name doesn't exist: no.such.metric"),
                this::assertOutputIsEmpty
        );
    }
}
