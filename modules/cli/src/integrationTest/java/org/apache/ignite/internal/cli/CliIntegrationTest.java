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

package org.apache.ignite.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.cli.call.connect.ConnectCall;
import org.apache.ignite.internal.cli.call.connect.ConnectCallInput;
import org.apache.ignite.internal.cli.commands.TopLevelCliCommand;
import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerHelper;
import org.apache.ignite.internal.cli.commands.cliconfig.TestConfigManagerProvider;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigDefaultValueProvider;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.apache.ignite.internal.cli.core.repl.EventListeningActivationPoint;
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContextProvider;
import org.apache.ignite.internal.cli.core.repl.registry.JdbcUrlRegistry;
import org.apache.ignite.internal.cli.core.repl.registry.NodeNameRegistry;
import org.apache.ignite.internal.cli.decorators.TableDecorator;
import org.apache.ignite.internal.cli.event.EventPublisher;
import org.apache.ignite.internal.cli.event.Events;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.apache.ignite.rest.client.model.MetricSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import picocli.CommandLine;

/**
 * Integration test base. Setups ignite cluster per test class and provides useful fixtures and assertions.
 */
@MicronautTest(rebuildContext = true)
public abstract class CliIntegrationTest extends ClusterPerClassIntegrationTest {

    public static final MetricSource[] ALL_METRIC_SOURCES = {
            new MetricSource().name("jvm").enabled(true),
            new MetricSource().name("os").enabled(true),
            new MetricSource().name("raft").enabled(true),
            new MetricSource().name("metastorage").enabled(true),
            new MetricSource().name("client.handler").enabled(true),
            new MetricSource().name("sql.client").enabled(true),
            new MetricSource().name("sql.plan.cache").enabled(true),
            new MetricSource().name("sql.queries").enabled(true),
            new MetricSource().name("storage.aipersist").enabled(true),
            new MetricSource().name("storage.aipersist.default").enabled(true),
            new MetricSource().name("storage.aipersist.default_aipersist").enabled(true),
            new MetricSource().name("storage.aipersist.checkpoint").enabled(true),
            new MetricSource().name("topology.cluster").enabled(true),
            new MetricSource().name("topology.local").enabled(true),
            new MetricSource().name("thread.pools.partitions-executor").enabled(true),
            new MetricSource().name("thread.pools.sql-executor").enabled(true),
            new MetricSource().name("thread.pools.sql-planning-executor").enabled(true),
            new MetricSource().name("transactions").enabled(true),
            new MetricSource().name("placement-driver").enabled(true),
            new MetricSource().name("resource.vacuum").enabled(true),
            new MetricSource().name("clock.service").enabled(true),
            new MetricSource().name("index.builder").enabled(true),
            new MetricSource().name("raft.snapshots").enabled(true),
            new MetricSource().name("network.messaging.default").enabled(true),
            new MetricSource().name("network.messaging.default.executor.inbound.Default").enabled(true),
            new MetricSource().name("network.messaging.default.executor.inbound.DeploymentUnits").enabled(true),
            new MetricSource().name("network.messaging.default.executor.inbound.ScaleCube").enabled(true),
            new MetricSource().name("network.messaging.default.executor.outbound").enabled(true),
    };

    /** Correct ignite jdbc url. */
    protected static final String JDBC_URL = "jdbc:ignite:thin://127.0.0.1:" + ClusterConfiguration.DEFAULT_BASE_CLIENT_PORT;

    protected static final String NODE_URL = "http://localhost:" + ClusterConfiguration.DEFAULT_BASE_HTTP_PORT;

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

    @Inject
    protected ConnectCall connectCall;

    private CommandLine cmd;

    private StringWriter sout;

    private StringWriter serr;

    private int exitCode = Integer.MIN_VALUE;

    @Inject
    private EventPublisher eventPublisher;

    @Inject
    private EventListeningActivationPoint eventListeningActivationPoint;

    @BeforeAll
    static void setDumbTerminal() {
        System.setProperty("org.jline.terminal.dumb", "true");
    }

    @BeforeEach
    void setUp() {
        configManagerProvider.setConfigFile(TestConfigManagerHelper.createIntegrationTestsConfig());
        cmd = new CommandLine(getCommandClass(), new MicronautFactory(context));
        cmd.setDefaultValueProvider(configDefaultValueProvider);
        if (needToSubscribe()) {
            eventListeningActivationPoint.subscribe();
        }
        resetOutput();
        CommandLineContextProvider.setCmd(cmd);
    }

    @AfterEach
    public void tearDown() {
        eventPublisher.publish(Events.disconnect());
    }

    private void resetOutput() {
        sout = new StringWriter();
        serr = new StringWriter();
        cmd.setOut(new PrintWriter(sout));
        cmd.setErr(new PrintWriter(serr));
    }

    public String getOutput() {
        return sout.toString();
    }

    protected Class<?> getCommandClass() {
        return TopLevelCliCommand.class;
    }

    /**
     * If {@code true}, the test subscribes to CLI event. Return {@code false} to more closely emulate real CLI app, where we subscribe
     * explicitly before the REPL mode start.
     *
     * @return Whether to subscribe to CLI events or not.
     */
    protected boolean needToSubscribe() {
        return true;
    }

    protected void execute(String... args) {
        resetOutput();
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

    protected void assertExitCodeIsError() {
        assertExitCodeIs(errorExitCode());
    }

    // REPL mode has no exit code for error, override this method in tests for repl commands.
    protected int errorExitCode() {
        return 1;
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

    protected void assertOutputIsSqlResultWithColumns(String... columns) {
        String tableWithContent = new TableDecorator(false)
                .decorate(new Table(List.of(columns), List.of()))
                .toTerminalString();
        String expectedHeader = Arrays.stream(tableWithContent.split("\n"))
                .limit(2)
                .collect(Collectors.joining("\n"));

        assertThat(sout.toString())
                .as("Expected command output to start with: " + expectedHeader + " but was " + sout.toString())
                .startsWith(expectedHeader);
    }

    protected void assertOutputStartsWith(String expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to start with: " + expectedOutput + " but was " + sout.toString())
                .startsWith(expectedOutput);
    }

    protected void assertOutputContains(String expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to contain: " + expectedOutput + " but was " + sout.toString())
                .contains(expectedOutput);
    }

    protected void assertOutputHasLineCount(int expectedLineCount) {
        assertThat(sout.toString())
                .as("Expected command output to has " + expectedLineCount + " lines but was " + sout.toString())
                .hasLineCount(expectedLineCount);
    }

    protected void assertOutputContainsAnyIgnoringCase(Set<String> expectedOutput) {
        CharSequence[] expectedUpperCase = expectedOutput.stream().map(String::toUpperCase).toArray(CharSequence[]::new);

        assertThat(sout.toString().toUpperCase())
                .as("Expected command output to contain any of, ignoring case: " + expectedOutput + " but was " + sout.toString())
                .containsAnyOf(expectedUpperCase);
    }

    protected void assertOutputContainsAny(Set<String> expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to contain any of: " + expectedOutput + " but was " + sout.toString())
                .containsAnyOf(expectedOutput.toArray(CharSequence[]::new));
    }

    protected void assertOutputContainsAllIgnoringCase(Set<String> expectedOutput) {
        CharSequence[] expectedUpperCase = expectedOutput.stream().map(String::toUpperCase).toArray(CharSequence[]::new);

        assertThat(sout.toString().toUpperCase())
                .as("Expected command output to contain all of, ignoring case: " + expectedOutput + " but was " + sout.toString())
                .contains(expectedUpperCase);
    }

    protected void assertOutputContainsAll(Set<String> expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to contain all of: " + expectedOutput + " but was " + sout.toString())
                .contains(expectedOutput.toArray(CharSequence[]::new));
    }

    protected void assertOutputDoesNotContain(String expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to not contain: " + expectedOutput + " but was " + sout.toString())
                .doesNotContain(expectedOutput);
    }

    protected void assertOutputDoesNotContain(Set<String> expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to not contain: " + expectedOutput + " but was " + sout.toString())
                .doesNotContain(expectedOutput.toArray(CharSequence[]::new));
    }

    protected void assertOutputDoesNotContainIgnoreCase(Set<String> expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to not contain: " + expectedOutput + " but was " + sout.toString())
                .doesNotContainIgnoringCase(expectedOutput.toArray(CharSequence[]::new));
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

    protected void assertOutputContainsSubsequence(Iterable<String> substrings) {
        assertThat(sout.toString())
                .as("Expected command output will contain the substrings in the given order")
                .containsSubsequence(substrings);
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

    protected void assertErrOutputDoesNotContain(String expectedOutput) {
        assertThat(serr.toString())
                .as("Expected command error output to not contain: " + expectedOutput + " but was " + serr.toString())
                .doesNotContain(expectedOutput);
    }

    protected void setConfigProperty(CliConfigKeys key, String value) {
        configManagerProvider.configManager.setProperty(key.value(), value);
    }

    protected String getConfigProperty(CliConfigKeys key) {
        return configManagerProvider.get().getCurrentProperty(key.value());
    }

    /** Mimics non-REPL "connect" command without starting REPL mode. Overriding getCommandClass and returning TopLevelCliReplCommand
     * wouldn't help because it will start to ask questions.
     */
    protected void connect(String url) {
        Flows.from(ConnectCallInput.builder().url(url).build())
                .then(Flows.fromCall(connectCall))
                .print()
                .start();
    }

    protected static void createAndPopulateTable() {
        createTableOnly(DEFAULT_TABLE_NAME);

        int idx = 0;

        insertPeople(
                DEFAULT_TABLE_NAME,
                new Person(idx++, "Igor", 10.0d),
                new Person(idx++, null, 15.0d),
                new Person(idx++, "Ilya", 15.0d),
                new Person(idx++, "Roma", 10.0d),
                new Person(idx, "Roma", 10.0d)
        );
    }

    protected static PrintWriter output(List<Character> buffer) {
        return new PrintWriter(new Writer() {
            @Override
            public void write(char[] cbuf, int off, int len) {
                for (int i = off; i < off + len; i++) {
                    buffer.add(cbuf[i]);
                }
            }

            @Override
            public void flush() {

            }

            @Override
            public void close() {

            }
        });
    }
}
