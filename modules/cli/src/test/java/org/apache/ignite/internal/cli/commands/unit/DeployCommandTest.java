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
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.commands.CliCommandTestBase;
import org.apache.ignite.internal.cli.commands.cluster.unit.ClusterUnitDeployCommand;
import org.apache.ignite.internal.cli.commands.cluster.unit.NodesAlias;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class DeployCommandTest extends CliCommandTestBase {
    @Override
    protected Class<?> getCommandClass() {
        return ClusterUnitDeployCommand.class;
    }

    @Test
    @DisplayName("Aliases couldn't be used with explicit nodes list")
    void aliasesWithExplicitNodesList(@WorkDirectory Path workDir) throws IOException {
        Path testFile = Files.createFile(workDir.resolve("test.txt"));

        // When executed with multiple nodes options including deploy mode
        execute("--path", testFile.toString(), "--version", "1.0.0", "--nodes", "ALL, foo", "id");

        // Error is printed
        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Alias ALL couldn't be used with explicit nodes names list [foo]. "
                        + "Provide either node names list or single alias.")
        );
    }

    @Test
    @DisplayName("Only one alias could be used")
    void multipleAliases(@WorkDirectory Path workDir) throws IOException {
        Path testFile = Files.createFile(workDir.resolve("test.txt"));

        String aliases = Arrays.stream(NodesAlias.values())
                .map(NodesAlias::name)
                .collect(Collectors.joining(", "));
        // When executed with multiple nodes options including deploy mode
        execute("--path", testFile.toString(), "--version", "1.0.0", "--nodes", aliases, "id");

        // Error is printed
        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Aliases [ALL, MAJORITY] can not be specified together. "
                        + "Provide single alias, please.")
        );
    }
}
