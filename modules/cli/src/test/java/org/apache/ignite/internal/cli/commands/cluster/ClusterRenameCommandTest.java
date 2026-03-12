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

package org.apache.ignite.internal.cli.commands.cluster;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterRenameCall;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterRenameCallInput;
import org.apache.ignite.internal.cli.commands.CliCommandTestBase;
import org.apache.ignite.internal.cli.commands.TopLevelCliCommand;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ClusterRenameCommandTest extends CliCommandTestBase {
    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliCommand.class;
    }

    private static Stream<Arguments> calls() {
        return Stream.of(
                arguments(
                        "cluster rename",
                        ClusterRenameCall.class,
                        ClusterRenameCallInput.class,
                        (Function<ClusterRenameCallInput, String>) ClusterRenameCallInput::getName
                )
        );
    }

    @ParameterizedTest
    @MethodSource("calls")
    void noParameter(String command) {
        // When executed without arguments
        execute(command);

        assertAll(
                () -> assertExitCodeIs(2),
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Failed to parse name.")
        );
    }

    @ParameterizedTest
    @MethodSource("calls")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void unquotedParameter(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> nameFunction
    ) {
        checkParameters(command, callClass, callInputClass, nameFunction, "test", "test");
    }

    @ParameterizedTest
    @MethodSource("calls")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void quotedParameter(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> nameFunction
    ) {
        checkParameters(command, callClass, callInputClass, nameFunction, "\"test\"", "test");
    }

    @ParameterizedTest
    @MethodSource("calls")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void unquotedParameters(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> nameFunction
    ) {
        checkParameters(command, callClass, callInputClass, nameFunction, "test1 test2", "test1 test2");
    }

    @ParameterizedTest
    @MethodSource("calls")
    <IT extends CallInput, OT, T extends Call<IT, OT>> void quotedParameters(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> nameFunction
    ) {
        checkParameters(command, callClass, callInputClass, nameFunction, "\"test1\" \"test2\"", "test1 test2");
    }
}
