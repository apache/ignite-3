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

package org.apache.ignite.internal.cli.commands.cluster.rename;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.apache.ignite.internal.cli.call.cluster.rename.ClusterRenameCall;
import org.apache.ignite.internal.cli.call.cluster.rename.ClusterRenameCallInput;
import org.apache.ignite.internal.cli.commands.CliCommandTestBase;
import org.apache.ignite.internal.cli.commands.TopLevelCliCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ClusterRenameCommandTest extends CliCommandTestBase {
    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliCommand.class;
    }

    private static Stream<Arguments> names() {
        return Stream.of(
                arguments("cluster", "cluster"), // normal
                arguments("", ""), // empty
                arguments("!@#!@$#%@#^%#$^#&^#*^#*$&*", "!@#!@$#%@#^%#$^#&^#*^#*$&*"), // special chars
                arguments("'cluster'", "cluster"), // single quotes
                arguments("\"cluster\"", "cluster"), // double quotes
                arguments("'cluster with spaces'", "cluster with spaces"), // single quotes with spaces
                arguments("\"cluster with spaces\"", "cluster with spaces") // double quotes with spaces
        );
    }

    @Test
    void noParameter() {
        // When executed without arguments
        execute("cluster rename");

        assertAll(
                () -> assertExitCodeIs(1),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Missing the required parameter 'body' when calling rename(Async)")
        );
    }

    @ParameterizedTest
    @MethodSource("names")
    void nameParameterTests(String name, String expectedName) {
        String parameters = String.format("--name=%s", name);

        checkParameters("cluster rename", ClusterRenameCall.class, ClusterRenameCallInput.class, ClusterRenameCallInput::getName,
                parameters, expectedName);
    }
}
