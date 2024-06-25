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

package org.apache.ignite.internal.cli.commands.unit;

import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.cli.commands.CliCommandTestNotInitializedIntegrationBase;
import org.apache.ignite.internal.cli.commands.cluster.unit.ClusterUnitDeployCommand;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for {@link ClusterUnitDeployCommand} on non-initialized cluster.
 */
public class ItDeploymentUnitNonInitilizedTest extends CliCommandTestNotInitializedIntegrationBase {

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
    @DisplayName("Should print error message when deploy a unit on not initialized cluster")
    void deploy() {
        execute("cluster", "unit", "deploy", "test.unit.id.1", "--version", "1.0.0", "--path", testFile);

        assertAll(
                () -> assertErrOutputContains("Cannot deploy unit"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }

    @Test
    @DisplayName("Should display error when undeploy a unit on not initialized cluster")
    void undeploy() {
        execute("cluster", "unit", "undeploy", "test.unit.id.2",  "--version", "1.0.0");

        assertAll(
                () -> assertErrOutputContains("Cannot undeploy unit"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }

    @Test
    @DisplayName("Should display error when listing deploy units on not initialized cluster")
    void listUnits() {
        execute("node", "unit", "list");

        assertAll(
                () -> assertErrOutputContains("Cannot list units"),
                () -> assertErrOutputContains(getExpectedErrorMessage())
        );
    }
}
