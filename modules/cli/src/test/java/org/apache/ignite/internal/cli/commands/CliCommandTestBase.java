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

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.handler.PicocliExecutionExceptionHandler;
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContextProvider;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.ArgumentCaptor;
import picocli.CommandLine;

/**
 * Base class for testing CLI commands.
 */
@MicronautTest
public abstract class CliCommandTestBase extends BaseIgniteAbstractTest {
    @Inject
    private ApplicationContext context;

    private StringWriter sout;

    private StringWriter serr;

    private int exitCode = Integer.MIN_VALUE;

    @BeforeAll
    static void setDumbTerminal() {
        System.setProperty("org.jline.terminal.dumb", "true");
    }

    protected abstract Class<?> getCommandClass();

    protected void execute(String argsLine) {
        execute(argsLine.split(" "));
    }

    protected void execute(String... args) {
        // Create command just before execution as some tests could register singletons which should be used by the command
        CommandLine cmd = new CommandLine(getCommandClass(), new MicronautFactory(context));
        cmd.setExecutionExceptionHandler(new PicocliExecutionExceptionHandler());

        sout = new StringWriter();
        serr = new StringWriter();
        cmd.setOut(new PrintWriter(sout));
        cmd.setErr(new PrintWriter(serr));
        CommandLineContextProvider.setCmd(cmd);

        exitCode = cmd.execute(args);
    }

    protected void assertExitCodeIs(int expectedExitCode) {
        assertThat("Unexpected exit code", exitCode, is(expectedExitCode));
    }

    protected void assertExitCodeIsError() {
        assertExitCodeIs(errorExitCode());
    }

    // REPL mode has no exit code for error, override this method in tests for repl commands.
    protected int errorExitCode() {
        return 1;
    }

    protected void assertExitCodeIsZero() {
        assertExitCodeIs(0);
    }

    protected void assertOutputIsNotEmpty() {
        assertThat("Unexpected command output", sout.toString(), is(not(emptyString())));
    }

    protected void assertOutputIs(String expectedOutput) {
        assertEqualsIgnoreLineSeparators("Unexpected command output", sout.toString(), expectedOutput);
    }

    protected void assertOutputContains(String... expectedOutput) {
        List<Matcher<? super String>> matchers = Arrays.stream(expectedOutput).map(Matchers::containsString).collect(toList());
        assertThat("Unexpected command output", sout.toString(), allOf(matchers));
    }

    protected void assertOutputIsEmpty() {
        assertThat("Unexpected command output", sout.toString(), is(emptyString()));
    }

    protected void assertErrOutputIsNotEmpty() {
        assertThat("Unexpected command error output", serr.toString(), is(not(emptyString())));
    }

    protected void assertErrOutputIsEmpty() {
        assertThat("Unexpected command error output", serr.toString(), is(emptyString()));
    }

    protected void assertErrOutputIs(String expectedErrOutput) {
        assertEqualsIgnoreLineSeparators("Unexpected command error output", serr.toString(), expectedErrOutput);
    }

    protected void assertErrOutputContains(String expectedErrOutput) {
        assertThat("Unexpected command error output", serr.toString(), containsString(expectedErrOutput));
    }

    /**
     * Asserts that the command's exit code is zero, output is equal to the expected output and the error output is empty.
     *
     * @param expectedOutput Expected command output.
     */
    protected void assertSuccessfulOutputIs(String expectedOutput) {
        log.info(sout.toString());
        log.info(serr.toString());
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs(expectedOutput),
                this::assertErrOutputIsEmpty
        );
    }

    /**
     * Asserts that {@code expected} and {@code actual} are equals ignoring differences in line separators.
     *
     * @param reason Description of the assertion.
     * @param exp Expected result.
     * @param actual Actual result.
     */
    private static void assertEqualsIgnoreLineSeparators(String reason, String exp, String actual) {
        assertThat(reason, exp.lines().collect(toList()), contains(actual.lines().toArray(String[]::new)));
    }

    protected <IT extends CallInput, OT, T extends Call<IT, OT>> T registerMockCall(Class<T> callClass) {
        T mock = mock(callClass);
        context.registerSingleton(mock);
        when(mock.execute(any())).thenReturn(DefaultCallOutput.empty());
        return mock;
    }

    protected static <IT extends CallInput, OT, T extends Call<IT, OT>> IT verifyCallInput(T call, Class<IT> inputClass) {
        ArgumentCaptor<IT> captor = ArgumentCaptor.forClass(inputClass);
        verify(call).execute(captor.capture());
        return captor.getValue();
    }
}
