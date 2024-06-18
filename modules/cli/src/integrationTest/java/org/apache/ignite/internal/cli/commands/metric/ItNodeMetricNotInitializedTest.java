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

import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for node metric commands on not initialized cluster.
 */
class ItNodeMetricNotInitializedTest extends CliCommandTestNotInitializedIntegrationBase {

    protected String getExpectedErrorMessage() {
        return CLUSTER_NOT_INITIALIZED_ERROR_MESSAGE;
    }

    @Test
    @DisplayName("Should print error message when run node metric list on not initialized cluster")
    void nodeMetricListError() {
        execute("node", "metric", "list");

        assertAll(
                () -> assertErrOutputContains("Cannot list metrics"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }

    @Test
    @DisplayName("Should print error message when run node metric source list on not initialized cluster")
    void nodeMetricSourceListError() {
        execute("node", "metric", "source", "list");

        assertAll(
                () -> assertErrOutputContains("Cannot list metrics"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }

    @Test
    @DisplayName("Should print error message when run node metric source enable on not initialized cluster")
    void nodeMetricSourceEnableError() {
        execute("node", "metric", "source", "enable", "metricName");

        assertAll(
                () -> assertErrOutputContains("Cannot enable metrics"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }

    @Test
    @DisplayName("Should print error message when run node metric source disable on not initialized cluster")
    void nodeMetricSourceDisableError() {
        execute("node", "metric", "source", "disable", "metricName");

        assertAll(
                () -> assertErrOutputContains("Cannot disable metrics"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }
}
