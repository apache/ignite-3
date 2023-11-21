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

package org.apache.ignite.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.ignite.internal.cli.CliIntegrationTestBase;
import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper;
import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerProvider;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigDefaultValueProvider;
import org.apache.ignite.internal.cli.core.repl.EventListeningActivationPoint;
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContextProvider;
import org.apache.ignite.internal.cli.core.repl.registry.JdbcUrlRegistry;
import org.apache.ignite.internal.cli.core.repl.registry.NodeNameRegistry;
import org.apache.ignite.internal.cli.event.EventPublisher;
import org.apache.ignite.internal.cli.event.Events;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import picocli.CommandLine;

/**
 * Integration test base for cli commands. Setup commands, ignite cluster, and provides useful fixtures and assertions. Note: ignite cluster
 * won't be initialized. If you want to use initialized cluster use {@link CliCommandTestInitializedIntegrationBase}.
 */
public class CliCommandTestNotInitializedIntegrationBase extends CliIntegrationTestBase {
    /** Correct ignite jdbc url. */
    protected static final String JDBC_URL = "jdbc:ignite:thin://127.0.0.1:10800";

    @Inject
    private ConfigDefaultValueProvider configDefaultValueProvider;

    @Inject
    protected TestConfigManagerProvider configManagerProvider;

    @Inject
    private ApplicationContext context;

    @Inject
    protected NodeNameRegistry nodeNameRegistry;

    @Inject
    protected JdbcUrlRegistry jdbcUrlRegistry;

    private CommandLine cmd;

    private StringWriter sout;

    private StringWriter serr;

    private int exitCode = Integer.MIN_VALUE;

    @Inject
    private EventPublisher eventPublisher;

    @Inject
    private EventListeningActivationPoint eventListeningActivationPoint;

    @BeforeEach
    public void setUp() {
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createIntegrationTestsConfig());
        cmd = new CommandLine(getCommandClass(), new MicronautFactory(context));
        cmd.setDefaultValueProvider(configDefaultValueProvider);
        eventListeningActivationPoint.subscribe();
        resetOutput();
        CommandLineContextProvider.setCmd(cmd);
    }

    @AfterEach
    public void tearDown() {
        eventPublisher.publish(Events.disconnect());
    }

    protected void resetOutput() {
        sout = new StringWriter();
        serr = new StringWriter();
        cmd.setOut(new PrintWriter(sout));
        cmd.setErr(new PrintWriter(serr));
    }

    @BeforeAll
    void beforeAll(TestInfo testInfo) {
        startNodes(testInfo);
    }

    @AfterAll
    void afterAll(TestInfo testInfo) throws Exception {
        stopNodes(testInfo);
    }

    protected Class<?> getCommandClass() {
        return TopLevelCliCommand.class;
    }

    protected void execute(String... args) {
        exitCode = cmd.execute(args);
    }

    protected CommandLine commandLine() {
        return cmd;
    }

    protected void assertExitCodeIs(int expectedExitCode) {
        assertThat(exitCode)
                .as("Expected exit code to be: " + expectedExitCode + " but was " + exitCode)
                .isEqualTo(expectedExitCode);
    }

    protected void assertExitCodeIsZero() {
        assertExitCodeIs(0);
    }

    protected void assertOutputIsNotEmpty() {
        assertThat(sout.toString())
                .as("Expected command output not to be empty")
                .isNotEmpty();
    }

    protected void assertOutputIs(String expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to be: " + expectedOutput + " but was " + sout.toString())
                .isEqualTo(expectedOutput);
    }

    protected void assertOutputContains(String expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to contain: " + expectedOutput + " but was " + sout.toString())
                .contains(expectedOutput);
    }

    protected void assertOutputMatches(String regex) {
        assertThat(sout.toString())
                .as("Expected command output to match regex: " + regex + " but it is not: " + sout.toString())
                .matches(regex);
    }

    protected void assertOutputIsEmpty() {
        assertThat(sout.toString())
                .as("Expected command output to be empty")
                .isEmpty();
    }

    protected void assertErrOutputIsNotEmpty() {
        assertThat(serr.toString())
                .as("Expected command error output not to be empty")
                .isNotEmpty();
    }

    protected void assertErrOutputIsEmpty() {
        assertThat(serr.toString())
                .as("Expected command error output to be empty")
                .isEmpty();
    }

    protected void assertErrOutputIs(String expectedErrOutput) {
        assertThat(serr.toString())
                .as("Expected command error output to be equal to: " + expectedErrOutput)
                .isEqualTo(expectedErrOutput);
    }

    protected void assertErrOutputContains(String expectedErrOutput) {
        assertThat(serr.toString())
                .as("Expected command error output to contain: " + expectedErrOutput)
                .contains(expectedErrOutput);
    }

    protected void setConfigProperty(CliConfigKeys key, String value) {
        configManagerProvider.configManager.setProperty(key.value(), value);
    }

    protected String getConfigProperty(CliConfigKeys key) {
        return configManagerProvider.get().getCurrentProperty(key.value());
    }
}
