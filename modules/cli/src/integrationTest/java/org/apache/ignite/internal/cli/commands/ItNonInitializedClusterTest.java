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

package org.apache.ignite.internal.cli.commands;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_GLOBAL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_TABLE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAME_OPTION;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for non-initialized cluster fir Non-REPL mode.
 */
public class ItNonInitializedClusterTest extends CliCommandTestNotInitializedIntegrationBase  {

    protected String getExpectedErrorMessage() {
        return CLUSTER_NOT_INITIALIZED_ERROR_MESSAGE;
    }

    private Path testDirectory;
    private String testFile;

    @BeforeAll
    void beforeAll() throws IOException {
        testDirectory = Files.createDirectory(WORK_DIR.resolve("test"));
        testFile = Files.createFile(testDirectory.resolve("test.txt")).toString();
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

    @Test
    @DisplayName("Should print error message when recovery reset-partitions on not initialized cluster")
    void resetPartitionsError() {
        execute("recovery", "partitions", "reset", CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, "tableName",
                RECOVERY_ZONE_NAME_OPTION, "zoneName");
        assertAll(
                () -> assertErrOutputContains("Cannot reset partitions"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }

    @Test
    @DisplayName("Should print error message when recovery partition-states on not initialized cluster")
    void partitionsStatesError() {
        execute("recovery", "partitions", "states", CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_PARTITION_GLOBAL_OPTION);
        assertAll(
                () -> assertErrOutputContains("Cannot list partition states"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }

    @Test
    @DisplayName("Should print error message when recovery restart-partitions on not initialized cluster")
    void partitionsRestartErrror() {
        execute("recovery", "partitions", "restart", CLUSTER_URL_OPTION, NODE_URL,
                RECOVERY_TABLE_NAME_OPTION, "tableName", RECOVERY_ZONE_NAME_OPTION, "zoneName");
        assertAll(
                () -> assertErrOutputContains("Cannot restart partitions"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }

    @Test
    @DisplayName("Should print error message when deploy a unit on not initialized cluster")
    void unitDeployError() {
        execute("cluster", "unit", "deploy", "test.unit.id.1", "--version", "1.0.0", "--path", testFile);

        assertAll(
                () -> assertErrOutputContains("Cannot deploy unit"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }

    @Test
    @DisplayName("Should display error when undeploy a unit on not initialized cluster")
    void unitUndeployError() {
        execute("cluster", "unit", "undeploy", "test.unit.id.2",  "--version", "1.0.0");

        assertAll(
                () -> assertErrOutputContains("Cannot undeploy unit"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }

    @Test
    @DisplayName("Should display error when listing deploy units on not initialized cluster")
    void listUnitsError() {
        execute("node", "unit", "list");

        assertAll(
                () -> assertErrOutputContains("Cannot list units"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }
}
