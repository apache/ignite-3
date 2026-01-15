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

/** Integration test for node unit inspect command. */
public class ItNodeUnitInspectCommandTest extends CliIntegrationTest {
    private static String testFile;

    private static Path testDirectory;

    private static Path recursiveDirectory;

    private static Path emptyFolderDirectory;

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

        // Setup for recursive deployment test with 3-level structure
        recursiveDirectory = Files.createDirectory(WORK_DIR.resolve("test-recursive"));

        // Level 1 - root files with different naming conventions
        Files.createFile(recursiveDirectory.resolve("root-config.xml"));
        Files.createFile(recursiveDirectory.resolve("ROOT_README.md"));
        Files.createFile(recursiveDirectory.resolve("root file.txt"));

        // Level 2 - subdirectories with different naming styles
        Path srcDir = Files.createDirectory(recursiveDirectory.resolve("src"));
        Files.createFile(srcDir.resolve("Main.java"));
        Files.createFile(srcDir.resolve("Helper Class.java"));
        Files.createFile(srcDir.resolve("util_functions.py"));

        Path resourcesDir = Files.createDirectory(recursiveDirectory.resolve("resources-dir"));
        Files.createFile(resourcesDir.resolve("application.properties"));
        Files.createFile(resourcesDir.resolve("messages_en.properties"));

        Path libDir = Files.createDirectory(recursiveDirectory.resolve("lib_folder"));
        Files.createFile(libDir.resolve("dependency-1.0.jar"));
        Files.createFile(libDir.resolve("NATIVE LIB.so"));

        // Level 3 - deeply nested with various cases
        Path srcSubDir = Files.createDirectory(srcDir.resolve("com.example.app"));
        Files.createFile(srcSubDir.resolve("Application.java"));
        Files.createFile(srcSubDir.resolve("Service Impl.java"));
        Files.createFile(srcSubDir.resolve("data_model.java"));

        Path resourceSubDir = Files.createDirectory(resourcesDir.resolve("i18n"));
        Files.createFile(resourceSubDir.resolve("messages_ru.properties"));
        Files.createFile(resourceSubDir.resolve("MESSAGES_DE.properties"));

        Path libSubDir = Files.createDirectory(libDir.resolve("native-libs"));
        Files.createFile(libSubDir.resolve("lib_native.dll"));
        Files.createFile(libSubDir.resolve("Native Lib.dylib"));

        // Setup for empty folder test
        emptyFolderDirectory = Files.createDirectory(WORK_DIR.resolve("test-empty-folder"));
        Files.createFile(emptyFolderDirectory.resolve("existing-file.txt"));
        Files.createDirectory(emptyFolderDirectory.resolve("empty-folder"));
        Path nonEmptyFolder = Files.createDirectory(emptyFolderDirectory.resolve("non-empty-folder"));
        Files.createFile(nonEmptyFolder.resolve("file-in-folder.txt"));
        Files.createDirectory(nonEmptyFolder.resolve("nested-empty-folder"));
    }

    @Test
    @DisplayName("Should display unit structure with tree view")
    void structureTreeView() {
        // Given deployed unit with recursive option to include subdirectories
        String id = "test.structure.unit.1";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", testDirectory.toString(), "--recursive");

        await().untilAsserted(() -> {
            execute("cluster", "unit", "list", "--plain", id);
            assertExitCodeIsZero();
        });

        // When get structure
        execute("node", "unit", "inspect", id, "--version", "1.0.0");

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
        // Given deployed unit with recursive option to include subdirectories
        String id = "test.structure.unit.2";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", testDirectory.toString(), "--recursive");

        await().untilAsserted(() -> {
            execute("cluster", "unit", "list", "--plain", id);
            assertExitCodeIsZero();
        });

        // When get structure with plain option
        execute("node", "unit", "inspect", id, "--version", "1.0.0", "--plain");

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
        execute("node", "unit", "inspect", "test.unit.id");

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
        execute("node", "unit", "inspect", "non.existing.unit", "--version", "1.0.0");

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
        execute("node", "unit", "inspect", id, "--version", "1.0.0");

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
        execute("node", "unit", "inspect", id, "--version", "1.0.0");

        // Then verify file sizes are displayed
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains(" B)") // Size in bytes
        );
    }

    @Test
    @DisplayName("Should display 3-level structure with recursive deployment")
    void structureWithRecursiveDeployment() {
        // Given deployed unit with recursive option
        String id = "test.recursive.unit.1";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", recursiveDirectory.toString(), "--recursive");

        await().untilAsserted(() -> {
            execute("cluster", "unit", "list", "--plain", id);
            assertExitCodeIsZero();
        });

        // When get structure
        execute("node", "unit", "inspect", id, "--version", "1.0.0");

        // Then verify all 3 levels are present with different naming cases
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                // Level 1 - root files
                () -> assertOutputContains("root-config.xml"),
                () -> assertOutputContains("ROOT_README.md"),
                () -> assertOutputContains("root file.txt"),
                // Level 2 - directories and their files
                () -> assertOutputContains("src"),
                () -> assertOutputContains("Main.java"),
                () -> assertOutputContains("Helper Class.java"),
                () -> assertOutputContains("util_functions.py"),
                () -> assertOutputContains("resources-dir"),
                () -> assertOutputContains("application.properties"),
                () -> assertOutputContains("messages_en.properties"),
                () -> assertOutputContains("lib_folder"),
                () -> assertOutputContains("dependency-1.0.jar"),
                () -> assertOutputContains("NATIVE LIB.so"),
                // Level 3 - deeply nested directories and files
                () -> assertOutputContains("com.example.app"),
                () -> assertOutputContains("Application.java"),
                () -> assertOutputContains("Service Impl.java"),
                () -> assertOutputContains("data_model.java"),
                () -> assertOutputContains("i18n"),
                () -> assertOutputContains("messages_ru.properties"),
                () -> assertOutputContains("MESSAGES_DE.properties"),
                () -> assertOutputContains("native-libs"),
                () -> assertOutputContains("lib_native.dll"),
                () -> assertOutputContains("Native Lib.dylib")
        );
    }

    @Test
    @DisplayName("Should display 3-level structure with recursive deployment in plain view")
    void structureWithRecursiveDeploymentPlainView() {
        // Given deployed unit with recursive option
        String id = "test.recursive.unit.2";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", recursiveDirectory.toString(), "--recursive");

        await().untilAsserted(() -> {
            execute("cluster", "unit", "list", "--plain", id);
            assertExitCodeIsZero();
        });

        // When get structure with plain view
        execute("node", "unit", "inspect", id, "--version", "1.0.0", "--plain");

        // Then verify nested paths are displayed correctly
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                // Verify nested paths include parent directories
                () -> assertOutputContains("src/Main.java"),
                () -> assertOutputContains("src/com.example.app/Application.java"),
                () -> assertOutputContains("resources-dir/i18n/messages_ru.properties"),
                () -> assertOutputContains("lib_folder/native-libs/lib_native.dll")
        );
    }

    @Test
    @DisplayName("Should display structure with empty folders using recursive deployment")
    void structureWithEmptyFolders() {
        // Given deployed unit with recursive option containing empty folders
        String id = "test.empty.folder.unit.1";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", emptyFolderDirectory.toString(), "--recursive");

        await().untilAsserted(() -> {
            execute("cluster", "unit", "list", "--plain", id);
            assertExitCodeIsZero();
        });

        // When get structure
        execute("node", "unit", "inspect", id, "--version", "1.0.0");

        // Then verify structure includes empty folders
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                // Root level file
                () -> assertOutputContains("existing-file.txt"),
                // Empty folder at root level
                () -> assertOutputContains("empty-folder"),
                // Non-empty folder with contents
                () -> assertOutputContains("non-empty-folder"),
                () -> assertOutputContains("file-in-folder.txt"),
                // Nested empty folder
                () -> assertOutputContains("nested-empty-folder")
        );
    }

    @Test
    @DisplayName("Should display structure with empty folders in plain view")
    void structureWithEmptyFoldersPlainView() {
        // Given deployed unit with recursive option containing empty folders
        String id = "test.empty.folder.unit.2";
        execute("cluster", "unit", "deploy", id, "--version", "1.0.0", "--path", emptyFolderDirectory.toString(), "--recursive");

        await().untilAsserted(() -> {
            execute("cluster", "unit", "list", "--plain", id);
            assertExitCodeIsZero();
        });

        // When get structure with plain view
        execute("node", "unit", "inspect", id, "--version", "1.0.0", "--plain");

        // Then verify files are shown with correct paths
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty,
                () -> assertOutputContains("existing-file.txt"),
                () -> assertOutputContains("non-empty-folder/file-in-folder.txt")
        );
    }
}
