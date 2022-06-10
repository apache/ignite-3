/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cli.commands.configuration.cluster.ClusterConfigShowSubCommand;
import org.apache.ignite.cli.commands.configuration.cluster.ClusterConfigUpdateSubCommand;
import org.apache.ignite.cli.commands.configuration.node.NodeConfigShowSubCommand;
import org.apache.ignite.cli.commands.configuration.node.NodeConfigUpdateSubCommand;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

/**
 * Base class for testing error handling in CLI commands that use REST.
 */
@MicronautTest
public class CliCommandsErrorTest {
    private static final String NODE_URL = "http://localhost:10300";

    @Inject
    private ApplicationContext context;

    private CommandLine cmd;
    private StringWriter sout;
    private StringWriter serr;
    private int exitCode = Integer.MIN_VALUE;

    private void setUp(Class<?> cmdClass) {
        cmd = new CommandLine(cmdClass, new MicronautFactory(context));
        sout = new StringWriter();
        serr = new StringWriter();
        cmd.setOut(new PrintWriter(sout));
        cmd.setErr(new PrintWriter(serr));
    }

    private void execute(Class<?> cmdClass, String urlOptionName, String urlOptionValue, List<String> additionalOptions) {
        setUp(cmdClass);
        List<String> options = new ArrayList<>();
        options.add("--" + urlOptionName + "=" + urlOptionValue);
        options.addAll(additionalOptions);
        exitCode = cmd.execute(options.toArray(new String[0]));
    }

    static List<Arguments> cmdClassAndOptionsProvider() {
        return List.of(
                Arguments.arguments(NodeConfigShowSubCommand.class, "node-url", List.of()),
                Arguments.arguments(NodeConfigUpdateSubCommand.class, "node-url", List.of("config")),
                Arguments.arguments(ClusterConfigShowSubCommand.class, "cluster-url", List.of()),
                Arguments.arguments(ClusterConfigUpdateSubCommand.class, "cluster-url", List.of("config"))
        );
    }

    @ParameterizedTest
    @MethodSource("cmdClassAndOptionsProvider")
    @DisplayName("Should display error when wrong port is given")
    public void incorrectPortTest(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, NODE_URL + "incorrect", additionalOptions);

        assertAll(
                this::assertExitCodeIsFailure,
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Invalid URL port: \"10300incorrect\"" + System.lineSeparator())
        );
    }

    @ParameterizedTest
    @MethodSource("cmdClassAndOptionsProvider")
    @DisplayName("Should display error when wrong url is given")
    void invalidUrlScheme(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, "incorrect" + NODE_URL, additionalOptions);

        assertAll(
                this::assertExitCodeIsFailure,
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Expected URL scheme 'http' or 'https' but was 'incorrecthttp'" + System.lineSeparator())
        );
    }

    @ParameterizedTest
    @MethodSource("cmdClassAndOptionsProvider")
    @DisplayName("Should display error when unknown host is given")
    void invalidUrl(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        System.out.println("executing");
        execute(cmdClass, urlOptionName, "http://no-such-host.com", additionalOptions);
        System.out.println("done");
        assertAll(
                this::assertExitCodeIsFailure,
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Could not determine IP address when connecting to URL: http://no-such-host.com" + System.lineSeparator())
        );
    }

    @ParameterizedTest
    @MethodSource("cmdClassAndOptionsProvider")
    @DisplayName("Should display error when failed to connect to host")
    void connectError(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, NODE_URL, additionalOptions);

        assertAll(
                this::assertExitCodeIsFailure,
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Could not connect to URL: " + NODE_URL + System.lineSeparator())
        );
    }

    private void assertExitCodeIsFailure() {
        assertThat(exitCode)
                .as("Check exit code")
                .isEqualTo(1);
    }

    private void assertOutputIsEmpty() {
        assertThat(sout.toString())
                .as("Check command output")
                .isEmpty();
    }

    private void assertErrOutputIs(String expectedErrOutput) {
        assertThat(serr.toString())
                .as("Check command error output")
                .isEqualTo(expectedErrOutput);
    }

}
