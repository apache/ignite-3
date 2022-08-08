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

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.cli.IntegrationTestBase;
import org.apache.ignite.cli.commands.cliconfig.TestConfigManagerHelper;
import org.apache.ignite.cli.commands.cliconfig.TestConfigManagerProvider;
import org.apache.ignite.cli.config.ConfigDefaultValueProvider;
import org.apache.ignite.cli.config.ini.IniConfigManager;
import org.apache.ignite.cli.core.repl.context.CommandLineContextProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import picocli.CommandLine;

/**
 * Integration test base for cli commands. Setup commands, ignite cluster, and provides useful fixtures and assertions. Note: ignite cluster
 * won't be initialized. If you want to use initialized cluster use {@link CliCommandTestInitializedIntegrationBase}.
 */
public class CliCommandTestNotInitializedIntegrationBase extends IntegrationTestBase {
    /** Correct ignite jdbc url. */
    protected static final String JDBC_URL = "jdbc:ignite:thin://127.0.0.1:10800";

    @Inject
    private ConfigDefaultValueProvider configDefaultValueProvider;

    @Inject
    protected TestConfigManagerProvider configManagerProvider;

    @Inject
    private ApplicationContext context;

    private CommandLine cmd;

    private StringWriter sout;

    private StringWriter serr;

    private int exitCode = Integer.MIN_VALUE;

    /**
     * Invokes before the test will start.
     *
     * @param testInfo Test information object.
     * @throws Exception If failed.
     */
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        configManagerProvider.configManager = new IniConfigManager(TestConfigManagerHelper.createIntegrationTests());
        cmd = new CommandLine(getCommandClass(), new MicronautFactory(context));
        cmd.setDefaultValueProvider(configDefaultValueProvider);
        sout = new StringWriter();
        serr = new StringWriter();
        cmd.setOut(new PrintWriter(sout));
        cmd.setErr(new PrintWriter(serr));
        CommandLineContextProvider.setCmd(cmd);
    }

    @BeforeAll
    void beforeAll(TestInfo testInfo) throws ExecutionException, InterruptedException {
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
}
