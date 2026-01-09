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
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Integration test for node unit structure command. */
public class ItNodeUnitStructureCommandTest extends CliIntegrationTest {
    private static String testFile;

    private static Path testDirectory;

    @BeforeAll
    static void beforeAll() throws IOException {
        testDirectory = Files.createDirectory(WORK_DIR.resolve("test-structure"));
        testFile = Files.createFile(testDirectory.resolve("test.txt")).toString();
        Files.createFile(testDirectory.resolve("test2.txt"));
        Files.createFile(testDirectory.resolve("test3.txt"));

        // Files with special characters
        Files.createFile(testDirectory.resolve("file with spaces.txt"));
        Files.createFile(testDirectory.resolve("file-with-dashes.txt"));
        Files.createFile(testDirectory.resolve("file_with_underscores.txt"));
        Files.createFile(testDirectory.resolve("file.multiple.dots.txt"));
        Files.createFile(testDirectory.resolve("file(with)parentheses.txt"));
        Files.createFile(testDirectory.resolve("UPPERCASE.TXT"));
        Files.createFile(testDirectory.resolve("MixedCase.Txt"));

        // Subdirectory with files
        Path subDir = Files.createDirectory(testDirectory.resolve("sub-directory"));
        Files.createFile(subDir.resolve("nested file.txt"));
        Files.createFile(subDir.resolve("nested-file.txt"));

        // Deeply nested directory
        Path deepDir = Files.createDirectories(testDirectory.resolve("level1").resolve("level2"));
        Files.createFile(deepDir.resolve("deep file.txt"));
    }

    @Test
    @DisplayName("Should display unit structure with tree view")
    void structureTreeView() {
        // Given deployed unit
        String id = "test.structure.unit.1";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", testDirectory.toString());

        await().untilAsserted(() -> {
            execute("cluster", "unit", "list", "--plain", id);
            assertExitCodeIsZero();
        });

        // When get structure
        execute("node", "unit", "structure", id, "--version", "1.0.0");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains(id),
                () -> assertOutputContains("test.txt"),
                () -> assertOutputContains("test2.txt"),
                () -> assertOutputContains("test3.txt"),
                () -> assertOutputContains("file with spaces.txt"),
                () -> assertOutputContains("file-with-dashes.txt"),
                () -> assertOutputContains("file_with_underscores.txt"),
                () -> assertOutputContains("file.multiple.dots.txt"),
                () -> assertOutputContains("file(with)parentheses.txt"),
                () -> assertOutputContains("UPPERCASE.TXT"),
                () -> assertOutputContains("MixedCase.Txt"),
                () -> assertOutputContains("sub-directory"),
                () -> assertOutputContains("nested file.txt"),
                () -> assertOutputContains("nested-file.txt"),
                () -> assertOutputContains("level1"),
                () -> assertOutputContains("level2"),
                () -> assertOutputContains("deep file.txt"),
                () -> assertOutputContains(" B)") // File size display
        );
    }

    @Test
    @DisplayName("Should display unit structure with plain view")
    void structurePlainView() {
        // Given deployed unit
        String id = "test.structure.unit.2";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", testDirectory.toString());

        await().untilAsserted(() -> {
            execute("cluster", "unit", "list", "--plain", id);
            assertExitCodeIsZero();
        });

        // When get structure with plain option
        execute("node", "unit", "structure", id, "--version", "1.0.0", "--plain");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("test.txt"),
                () -> assertOutputContains("test2.txt"),
                () -> assertOutputContains("test3.txt"),
                () -> assertOutputContains("file with spaces.txt"),
                () -> assertOutputContains("file-with-dashes.txt"),
                () -> assertOutputContains("file_with_underscores.txt"),
                () -> assertOutputContains("file.multiple.dots.txt"),
                () -> assertOutputContains("file(with)parentheses.txt"),
                () -> assertOutputContains("sub-directory"),
                () -> assertOutputContains("level1"),
                () -> assertOutputContains("level2")
        );
    }

    @Test
    @DisplayName("Should display error when version is missing")
    void structureVersionIsMandatory() {
        // When get structure without version
        execute("node", "unit", "structure", "test.unit.id");

        // Then
        assertAll(
                () -> assertExitCodeIs(2),
                () -> assertErrOutputContains("Missing required option: '--version=<version>'"),
                this::assertOutputIsEmpty
        );
    }

    @Test
    @DisplayName("Should display error when unit does not exist")
    void structureUnitNotFound() {
        // When get structure of non-existing unit
        execute("node", "unit", "structure", "non.existing.unit", "--version", "1.0.0");

        // Then
        assertAll(
                this::assertExitCodeIsError,
                () -> assertErrOutputContains("not found")
        );
    }

    @Test
    @DisplayName("Should display structure from file deployment")
    void structureFromFile() {
        // Given deployed unit from file
        String id = "test.structure.unit.3";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", testFile);

        await().untilAsserted(() -> {
            execute("cluster", "unit", "list", "--plain", id);
            assertExitCodeIsZero();
        });

        // When get structure
        execute("node", "unit", "structure", id, "--version", "1.0.0");

        // Then
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains(id),
                () -> assertOutputContains("test.txt")
        );
    }

    @Test
    @DisplayName("Should display file sizes in human readable format")
    void structureWithFileSizes() {
        // Given deployed unit
        String id = "test.structure.unit.4";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", testDirectory.toString());

        await().untilAsserted(() -> {
            execute("cluster", "unit", "list", "--plain", id);
            assertExitCodeIsZero();
        });

        // When get structure
        execute("node", "unit", "structure", id, "--version", "1.0.0");

        // Then verify file sizes are displayed
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains(" B)") // Size in bytes
        );
    }
}
