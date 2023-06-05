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
        execute("cluster", "unit", "deploy", "test.unit.id.5", "--version", "1.0.0", "--path", testFile);

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("Done")
        );

        await().untilAsserted(() -> {
            resetOutput();
            execute("cluster", "unit", "list", "test.unit.id.5");

            assertAll(
                    this::assertExitCodeIsZero,
                    this::assertErrOutputIsEmpty,
                    () -> assertOutputContains("1.0.0"),
                    () -> assertOutputContains("DEPLOYED")
            );
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

            assertAll(
                    this::assertExitCodeIsZero,
                    this::assertErrOutputIsEmpty,
                    () -> assertOutputIs("id\tversion\tstatus" + System.lineSeparator()
                            + "test-unit\t1.0.0\tDEPLOYED" + System.lineSeparator())
            );
        });

        resetOutput();
        execute("node", "unit", "list", "--plain", "test-unit");

        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputIs("id\tversion\tstatus" + System.lineSeparator()
                        + "test-unit\t1.0.0\tDEPLOYED" + System.lineSeparator())
        );
    }
}
