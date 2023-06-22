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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.cli.commands.CliCommandTestInitializedIntegrationBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Integration test for deployment commands. */
public class ItDeploymentUnitTest extends CliCommandTestInitializedIntegrationBase {

    private String testFile;

    private String testFile2;

    private Path testDirectory;

    @BeforeAll
    void beforeAll() throws IOException {
        testDirectory = Files.createDirectory(WORK_DIR.resolve("test"));
        testFile = Files.createFile(testDirectory.resolve("test.txt")).toString();
        testFile2 = Files.createFile(testDirectory.resolve("test2.txt")).toString();
    }

    @Test
    @DisplayName("Should deploy a unit with version")
    void deploy() {
        // When deploy with version
        execute("cluster", "unit", "deploy", "test.unit.id.1", "--version", "1.0.0", "--path", testFile);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Done")
        );
    }

    @Test
    @DisplayName("Should display error when deploy a unit without version")
    void deployVersionIsMandatory() {
        // When deploy without version
        execute("cluster", "unit", "deploy", "test.unit.id.2", "--path", testFile);

        // Then
        assertAll(
                () -> assertExitCodeIs(2),
                () -> assertErrOutputContains("Missing required option: '--version=<version>'"),
                this::assertOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should display error when undeploy a unit without version")
    void undeployVersionIsMandatory() {
        // When deploy without version
        execute("cluster", "unit", "undeploy", "test.unit.id.2");

        // Then
        assertAll(
                () -> assertExitCodeIs(2),
                () -> assertErrOutputContains("Missing required option: '--version=<version>'"),
                this::assertOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should undeploy a unit with version")
    void undeploy() {
        // When deploy with version
        execute("cluster", "unit", "deploy", "test.unit.id.3", "--version", "1.0.0", "--path", testFile);

        // And undeploy
        execute("cluster", "unit", "undeploy", "test.unit.id.3", "--version", "1.0.0");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Done")
        );
    }

    @Test
    @DisplayName("Should display error when undeploy non-existing unit")
    void undeployNoSuch() {
        // When undeploy non-existing unit
        execute("cluster", "unit", "undeploy", "un.such.unit.id.4", "--version", "1.0.0");

        // Then
        assertAll(
                () -> assertExitCodeIs(1),
                () -> assertErrOutputContains("Unit not found")
        );
    }

    @Test
    @DisplayName("Should display correct status after deploy")
    void deployAndStatusCheck() {
        // When deploy with version
        String id = "test.unit.id.5";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", testFile);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Done")
        );

        await().untilAsserted(() -> {
            resetOutput();
            execute("cluster", "unit", "list", "--plain", id);

            assertDeployed(id);
        });
    }

    @Test
    @DisplayName("Should deploy a unit from directory")
    void deployDirectory() {
        // When deploy with version
        execute("cluster", "unit", "deploy", "test.unit.id.6", "--version", "1.0.0", "--path", testDirectory.toString());

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Done")
        );
    }

    @Test
    @DisplayName("Should display correct status after deploy to the specified nodes")
    void deployToNodesAndStatusCheck() {
        // When deploy with version
        String node = allNodeNames().get(1);
        String id = "test.unit.id.7";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", testFile, "--nodes", node);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Done")
        );

        await().untilAsserted(() -> {
            resetOutput();
            execute("cluster", "unit", "list", "--plain", id);

            // Unit is deployed on all requested nodes
            assertDeployed(id);

            resetOutput();
            execute("node", "unit", "list", "--plain", "--node-url", "http://localhost:10300", id);

            // Unit is deployed on the CMG node
            assertDeployed(id);

            resetOutput();
            execute("node", "unit", "list", "--plain", "--node-url", "http://localhost:10301", id);

            // Unit is deployed on the requested node
            assertDeployed(id);

            resetOutput();
            execute("node", "unit", "list", "--plain", "--node-url", "http://localhost:10302", id);

            // Unit is not deployed on the other node
            assertAll(
                    this::assertExitCodeIsZero,
                    this::assertErrOutputIsEmpty,
                    this::assertOutputIsEmpty
            );
        });
    }

    @Test
    @DisplayName("Should display correct status on after deploy to all nodes")
    void deployToAllAndStatusCheck() {
        // When deploy with version
        String id = "test.unit.id.8";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", testFile, "--nodes", "ALL");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Done")
        );

        await().untilAsserted(() -> {
            resetOutput();
            execute("cluster", "unit", "list", "--plain", id);

            // Unit is deployed on all requested nodes
            assertDeployed(id);

            for (int i = 0; i < CLUSTER_NODES.size(); i++) {
                resetOutput();

                String nodeUrl = "http://localhost:" + (10300 + i);
                execute("node", "unit", "list", "--plain", "--node-url", nodeUrl, id);

                // Unit is deployed on the node
                assertDeployed(id);
            }
        });
    }

    @Test
    @DisplayName("Should display correct status with filters after deploy")
    void deployUnitsAndStatusCheck() {
        // When deploy with version
        execute("cluster", "unit", "deploy", "test-unit", "--version", "1.0.0", "--path", testFile);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Done")
        );

        // When deploy second unit with version
        resetOutput();
        execute("cluster", "unit", "deploy", "test-unit2", "--version", "2.1", "--path", testFile2);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Done")
        );

        await().untilAsserted(() -> {
            resetOutput();
            execute("cluster", "unit", "list", "--plain", "test-unit");

            assertDeployed("test-unit");
        });

        resetOutput();
        execute("node", "unit", "list", "--plain", "test-unit");

        assertDeployed("test-unit");
    }

    private void assertDeployed(String id) {
        assertDeployed(id, "1.0.0");
    }

    private void assertDeployed(String id, String version) {
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("id\tversion\tstatus" + System.lineSeparator()
                        + id + "\t" + version + "\tDEPLOYED" + System.lineSeparator())
        );
    }
}
