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
import static org.junit.jupiter.params.provider.Arguments.arguments;

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
import org.apache.ignite.cli.commands.cluster.config.ClusterConfigShowCommand;
import org.apache.ignite.cli.commands.cluster.config.ClusterConfigShowReplCommand;
import org.apache.ignite.cli.commands.cluster.config.ClusterConfigUpdateCommand;
import org.apache.ignite.cli.commands.cluster.config.ClusterConfigUpdateReplCommand;
import org.apache.ignite.cli.commands.cluster.init.ClusterInitCommand;
import org.apache.ignite.cli.commands.cluster.init.ClusterInitReplCommand;
import org.apache.ignite.cli.commands.cluster.status.ClusterStatusCommand;
import org.apache.ignite.cli.commands.cluster.status.ClusterStatusReplCommand;
import org.apache.ignite.cli.commands.connect.ConnectCommand;
import org.apache.ignite.cli.commands.node.config.NodeConfigShowCommand;
import org.apache.ignite.cli.commands.node.config.NodeConfigShowReplCommand;
import org.apache.ignite.cli.commands.node.config.NodeConfigUpdateCommand;
import org.apache.ignite.cli.commands.node.config.NodeConfigUpdateReplCommand;
import org.apache.ignite.cli.commands.node.status.NodeStatusCommand;
import org.apache.ignite.cli.commands.node.status.NodeStatusReplCommand;
import org.apache.ignite.cli.commands.topology.LogicalTopologyCommand;
import org.apache.ignite.cli.commands.topology.LogicalTopologyReplCommand;
import org.apache.ignite.cli.commands.topology.PhysicalTopologyCommand;
import org.apache.ignite.cli.commands.topology.PhysicalTopologyReplCommand;
import org.apache.ignite.cli.config.ini.IniConfigManager;
import org.apache.ignite.cli.core.repl.context.CommandLineContextProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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

    private static final String CLUSTER_URL_OPTION = "--cluster-endpoint-url=";
    private static final String NODE_URL_OPTION = "--node-url=";

    @Inject
    private ApplicationContext context;

    private CommandLine cmd;

    private StringWriter sout;

    private StringWriter serr;

    private int exitCode = Integer.MIN_VALUE;

    @Inject
    TestConfigManagerProvider configManagerProvider;

    private void setUp(Class<?> cmdClass) {
        configManagerProvider.configManager = new IniConfigManager(TestConfigManagerHelper.createSectionWithDefaultProfile());
        MicronautFactory factory = new MicronautFactory(context);
        cmd = new CommandLine(cmdClass, factory);
        CommandLineContextProvider.setCmd(cmd);
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
                arguments(NodeConfigShowCommand.class, NODE_URL_OPTION, List.of()),
                arguments(NodeConfigUpdateCommand.class, NODE_URL_OPTION, List.of("{key: value}")),
                arguments(NodeStatusCommand.class, NODE_URL_OPTION, List.of()),
                arguments(ClusterConfigShowCommand.class, CLUSTER_URL_OPTION, List.of()),
                arguments(ClusterConfigUpdateCommand.class, CLUSTER_URL_OPTION, List.of("{key: value}")),
                arguments(ClusterStatusCommand.class, CLUSTER_URL_OPTION, List.of()),
                arguments(LogicalTopologyCommand.class, CLUSTER_URL_OPTION, List.of()),
                arguments(PhysicalTopologyCommand.class, CLUSTER_URL_OPTION, List.of()),
                arguments(ClusterInitCommand.class, CLUSTER_URL_OPTION, List.of("--cluster-name=cluster", "--meta-storage-node=test"))
        // TODO https://issues.apache.org/jira/browse/IGNITE-17102
        //                Arguments.arguments(ClusterShowCommand.class, CLUSTER_URL_OPTION, List.of()),
        );
    }

    static List<Arguments> cmdReplClassAndOptionsProvider() {
        return List.of(
                arguments(NodeConfigShowReplCommand.class, NODE_URL_OPTION, List.of()),
                arguments(NodeConfigUpdateReplCommand.class, NODE_URL_OPTION, List.of("{key: value}")),
                arguments(NodeStatusReplCommand.class, NODE_URL_OPTION, List.of()),
                arguments(ClusterConfigShowReplCommand.class, CLUSTER_URL_OPTION, List.of()),
                arguments(ClusterConfigUpdateReplCommand.class, CLUSTER_URL_OPTION, List.of("{key: value}")),
                arguments(ClusterStatusReplCommand.class, CLUSTER_URL_OPTION, List.of()),
                arguments(LogicalTopologyReplCommand.class, CLUSTER_URL_OPTION, List.of()),
                arguments(PhysicalTopologyReplCommand.class, CLUSTER_URL_OPTION, List.of()),
                arguments(ClusterInitReplCommand.class, CLUSTER_URL_OPTION, List.of("--cluster-name=cluster", "--meta-storage-node=test")),
                arguments(ConnectCommand.class, "", List.of())
        // TODO https://issues.apache.org/jira/browse/IGNITE-17102
        //                Arguments.arguments(ClusterShowReplCommand.class, CLUSTER_URL_OPTION, List.of()),
        );
    }

    @ParameterizedTest
    @MethodSource("cmdClassAndOptionsProvider")
    @DisplayName("Should display error when wrong port is given")
    void incorrectPort(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, NODE_URL + "incorrect", additionalOptions);

        String expectedErrOutput = "Invalid URL '" + NODE_URL
                + "incorrect' (Error at index 5 in: \"10300incorrect\")";
        assertAll(
                this::assertExitCodeIsParseError,
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains(expectedErrOutput)
        );
    }

    @ParameterizedTest
    @MethodSource("cmdClassAndOptionsProvider")
    @DisplayName("Should display error when wrong url is given")
    void invalidUrlScheme(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, "incorrect" + NODE_URL, additionalOptions);

        String expectedErrOutput = "Invalid URL 'incorrect" + NODE_URL
                + "' (unknown protocol: incorrecthttp)";
        assertAll(
                this::assertExitCodeIsParseError,
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains(expectedErrOutput)
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
                () -> assertErrOutputIs(
                        "Unknown host: http://no-such-host.com" + System.lineSeparator())
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
                () -> assertErrOutputIs("Node unavailable" + System.lineSeparator()
                        + "Could not connect to node with URL " + NODE_URL + System.lineSeparator())
        );
    }

    @ParameterizedTest
    @MethodSource("cmdReplClassAndOptionsProvider")
    @DisplayName("Should display error when wrong port is given")
    void incorrectPortRepl(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, NODE_URL + "incorrect", additionalOptions);

        String expectedErrOutput = "Invalid URL '" + NODE_URL
                + "incorrect' (Error at index 5 in: \"10300incorrect\")";
        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains(expectedErrOutput)
        );
    }

    @ParameterizedTest
    @MethodSource("cmdReplClassAndOptionsProvider")
    @DisplayName("Should display error when wrong url is given")
    void invalidUrlSchemeRepl(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, "incorrect" + NODE_URL, additionalOptions);

        String expectedErrOutput = "Invalid URL 'incorrect" + NODE_URL
                + "' (unknown protocol: incorrecthttp)";
        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains(expectedErrOutput)
        );
    }

    @ParameterizedTest
    @MethodSource("cmdReplClassAndOptionsProvider")
    @DisplayName("Should display error when unknown host is given")
    void invalidUrlRepl(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, "http://no-such-host.com", additionalOptions);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Unknown host: http://no-such-host.com" + System.lineSeparator())
        );
    }

    @ParameterizedTest
    @MethodSource("cmdReplClassAndOptionsProvider")
    @DisplayName("Should display error when failed to connect to host")
    void connectErrorRepl(Class<?> cmdClass, String urlOptionName, List<String> additionalOptions) {
        execute(cmdClass, urlOptionName, NODE_URL, additionalOptions);

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputIs("Node unavailable" + System.lineSeparator()
                        + "Could not connect to node with URL " + NODE_URL + System.lineSeparator())
        );
    }

    @Test
    void testConnectCommandWithoutParametersWithEmptyConfig() {
        configManagerProvider.configManager = new IniConfigManager(TestConfigManagerHelper.createEmptyConfig());
        setUp(ConnectCommand.class);
        cmd.execute();

        assertAll(
                this::assertOutputIsEmpty,
                () -> assertErrOutputContains("Missing required parameter: '<nodeUrl>'")
        );
    }

    private void assertExitCodeIsFailure() {
        assertThat(exitCode)
                .as("Check exit code")
                .isEqualTo(1);
    }

    private void assertExitCodeIsParseError() {
        assertThat(exitCode)
                .as("Check exit code")
                .isEqualTo(2);
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

    private void assertErrOutputContains(String expectedErrOutput) {
        assertThat(serr.toString())
                .as("Check command error output")
                .contains(expectedErrOutput);
    }

}
