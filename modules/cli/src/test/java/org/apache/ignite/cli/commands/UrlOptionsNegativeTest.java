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
import org.apache.ignite.cli.commands.cliconfig.TestConfigManagerHelper;
import org.apache.ignite.cli.commands.cliconfig.TestConfigManagerProvider;
import org.apache.ignite.cli.commands.configuration.cluster.ClusterConfigShowReplSubCommand;
import org.apache.ignite.cli.commands.configuration.cluster.ClusterConfigShowSubCommand;
import org.apache.ignite.cli.commands.configuration.cluster.ClusterConfigUpdateReplSubCommand;
import org.apache.ignite.cli.commands.configuration.cluster.ClusterConfigUpdateSubCommand;
import org.apache.ignite.cli.commands.configuration.node.NodeConfigShowReplSubCommand;
import org.apache.ignite.cli.commands.configuration.node.NodeConfigShowSubCommand;
import org.apache.ignite.cli.commands.configuration.node.NodeConfigUpdateReplSubCommand;
import org.apache.ignite.cli.commands.configuration.node.NodeConfigUpdateSubCommand;
import org.apache.ignite.cli.commands.connect.ConnectCommand;
import org.apache.ignite.cli.config.ini.IniConfigManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

/**
 * Tests error handling with various invalid URLs in CLI commands that use REST.
 */
@MicronautTest
public class UrlOptionsNegativeTest {
    private static final String NODE_URL = "http://localhost:10300";

    @Inject
    private ApplicationContext context;

    private CommandLine cmd;

    private StringWriter sout;

    private StringWriter serr;

    private int exitCode = Integer.MIN_VALUE;

    @Inject
    TestConfigManagerProvider configManagerProvider;

    private void setUp(Class<?> cmdClass) {
        configManagerProvider.configManager = new IniConfigManager(TestConfigManagerHelper.createSectionWithInternalPart());
        cmd = new CommandLine(cmdClass, new MicronautFactory(context));
        sout = new StringWriter();
        serr = new StringWriter();
        cmd.setOut(new PrintWriter(sout));
        cmd.setErr(new PrintWriter(serr));
    }

    private void execute(Class<?> cmdClass, String urlOptionName, String urlOptionValue, List<String> additionalOptions) {
        setUp(cmdClass);
        List<String> options = new ArrayList<>();
        options.add(urlOptionName + urlOptionValue);
        options.addAll(additionalOptions);
        exitCode = cmd.execute(options.toArray(new String[0]));
    }

    static List<Arguments> cmdClassAndOptionsProvider() {
        return List.of(
                Arguments.arguments(NodeConfigShowSubCommand.class, "--node-url=", List.of()),
                Arguments.arguments(NodeConfigUpdateSubCommand.class, "--node-url=", List.of("{key: value}")),
                Arguments.arguments(ClusterConfigShowSubCommand.class, "--cluster-url=", List.of()),
                Arguments.arguments(ClusterConfigUpdateSubCommand.class, "--cluster-url=", List.of("{key: value}"))
        // TODO https://issues.apache.org/jira/browse/IGNITE-17091
        //                Arguments.arguments(StatusCommand.class, "--cluster-url=", List.of()),
        // TODO https://issues.apache.org/jira/browse/IGNITE-17102
        //                Arguments.arguments(ClusterShowCommand.class, "--cluster-url=", List.of()),
        // TODO https://issues.apache.org/jira/browse/IGNITE-17092
        //                Arguments.arguments(TopologyCommand.class, "--cluster-url", List.of()),
        // TODO https://issues.apache.org/jira/browse/IGNITE-17162
        //                Arguments.arguments(ClusterCommandSpec.InitClusterCommandSpec.class, "---cluster-url=",
        //                        List.of("--cluster-name=cluster", "--meta-storage-node=test"))
        );
    }

    static List<Arguments> cmdReplClassAndOptionsProvider() {
        return List.of(
                Arguments.arguments(NodeConfigShowReplSubCommand.class, "--node-url=", List.of()),
                Arguments.arguments(NodeConfigUpdateReplSubCommand.class, "--node-url=", List.of("{key: value}")),
                Arguments.arguments(ClusterConfigShowReplSubCommand.class, "--cluster-url=", List.of()),
                Arguments.arguments(ClusterConfigUpdateReplSubCommand.class, "--cluster-url=", List.of("{key: value}")),
                Arguments.arguments(ConnectCommand.class, "", List.of())
        // TODO https://issues.apache.org/jira/browse/IGNITE-17091
        //                Arguments.arguments(StatusReplCommand.class, "--cluster-url=", List.of()),
        // TODO https://issues.apache.org/jira/browse/IGNITE-17102
        //                Arguments.arguments(ClusterShowReplCommand.class, "--cluster-url=", List.of()),
        // TODO https://issues.apache.org/jira/browse/IGNITE-17092
        //                Arguments.arguments(TopologyReplCommand.class, "--cluster-url", List.of()),
        // TODO https://issues.apache.org/jira/browse/IGNITE-17162
        //                Arguments.arguments(ClusterReplCommandSpec.InitClusterCommandSpec.class, "---cluster-url=",
        //                        List.of("--cluster-name=cluster", "--meta-storage-node=test"))
        );
    }

    @ParameterizedTest
    @MethodSource("cmdClassAndOptionsProvider")
    @DisplayName("Should display error when wrong port is given")
    void incorrectPort(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
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
        execute(cmdClass, urlOptionName, "http://no-such-host.com", additionalOptions);

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

    @ParameterizedTest
    @MethodSource("cmdReplClassAndOptionsProvider")
    @DisplayName("Should display error when wrong port is given")
    void incorrectPortRepl(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, NODE_URL + "incorrect", additionalOptions);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Invalid URL port: \"10300incorrect\"" + System.lineSeparator())
        );
    }

    @ParameterizedTest
    @MethodSource("cmdReplClassAndOptionsProvider")
    @DisplayName("Should display error when wrong url is given")
    void invalidUrlSchemeRepl(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, "incorrect" + NODE_URL, additionalOptions);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Expected URL scheme 'http' or 'https' but was 'incorrecthttp'" + System.lineSeparator())
        );
    }

    @ParameterizedTest
    @MethodSource("cmdReplClassAndOptionsProvider")
    @DisplayName("Should display error when unknown host is given")
    void invalidUrlRepl(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, "http://no-such-host.com", additionalOptions);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Could not determine IP address when connecting to URL: http://no-such-host.com" + System.lineSeparator())
        );
    }

    @ParameterizedTest
    @MethodSource("cmdReplClassAndOptionsProvider")
    @DisplayName("Should display error when failed to connect to host")
    void connectErrorRepl(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, NODE_URL, additionalOptions);

        assertAll(
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

    private void assertErrOutputIsEmpty() {
        assertThat(serr.toString())
                .as("Check command output")
                .isEmpty();
    }

    private void assertErrOutputIs(String expectedErrOutput) {
        assertThat(serr.toString())
                .as("Check command error output")
                .isEqualTo(expectedErrOutput);
    }

    private void assertOutputContains(String expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to contain: " + expectedOutput + " but was " + sout.toString())
                .contains(expectedOutput);
    }

}
