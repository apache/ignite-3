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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
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
import java.util.function.Function;
import org.apache.ignite.internal.cli.core.call.AsyncCall;
import org.apache.ignite.internal.cli.core.call.AsyncCallFactory;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.handler.PicocliExecutionExceptionHandler;
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContextProvider;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

    private CommandLine cmd;

    @BeforeAll
    static void setDumbTerminal() {
        System.setProperty("org.jline.terminal.dumb", "true");
    }

    protected abstract Class<?> getCommandClass();

    @BeforeEach
    void setUp() {
        createCommand();
    }

    private void createCommand() {
        cmd = new CommandLine(getCommandClass(), new MicronautFactory(context));
        cmd.setExecutionExceptionHandler(new PicocliExecutionExceptionHandler());
        CommandLineContextProvider.setCmd(cmd);
    }

    protected void execute(String argsLine) {
        execute(argsLine.split(" "));
    }

    protected void execute(String... args) {
        sout = new StringWriter();
        serr = new StringWriter();
        cmd.setOut(new PrintWriter(sout));
        cmd.setErr(new PrintWriter(serr));

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

    protected void assertOutputMatches(String regex) {
        assertThat("Unexpected command output", sout.toString(), matchesRegex(regex));
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
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputIs(expectedOutput),
                this::assertErrOutputIsEmpty
        );
    }

    /**
     * Asserts that the command's exit code is zero, output contains expected output and the error output is empty.
     *
     * @param expectedOutput Expected command output.
     */
    protected void assertSuccessfulOutputContains(String expectedOutput) {
        assertAll(
                this::assertExitCodeIsZero,
                () -> assertOutputContains(expectedOutput),
                this::assertErrOutputIsEmpty
        );
    }

    /**
     * Asserts that {@code expected} and {@code actual} are equals ignoring differences in line separators.
     *
     * @param reason Description of the assertion.
     * @param actual Actual result.
     * @param exp Expected result.
     */
    private static void assertEqualsIgnoreLineSeparators(String reason, String actual, String exp) {
        String actualJoined = actual.lines().collect(joining(System.lineSeparator()));
        String expJoined = exp.lines().collect(joining(System.lineSeparator()));
        assertThat(reason, actualJoined, is(expJoined));
    }

    /**
     * Runs the command with the mock call and verifies that the call was executed with the expected input.
     *
     * @param command Command string.
     * @param callClass Call class.
     * @param callInputClass Call input class.
     * @param inputTransformer Function which transforms the call input to string.
     * @param parameters Command arguments.
     * @param expected Expected call input string.
     * @param <IT> Input for the call.
     * @param <OT> Output of the call.
     * @param <T> Call type.
     */
    protected <IT extends CallInput, OT, T extends Call<IT, OT>> void checkParameters(
            String command,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> inputTransformer,
            String parameters,
            String expected
    ) {
        T call = registerMockCall(callClass);
        // Recreate the CommandLine object so that the registered mocks are available to this command.
        createCommand();

        execute(command + " " + parameters);
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty
        );

        IT callInput = verifyCallInput(call, callInputClass);
        assertThat(inputTransformer.apply(callInput), is(expected));
    }

    /**
     * Runs the command with the mock call and verifies that the call was executed with the expected input.
     *
     * @param command Command string.
     * @param callFactoryClass Call factory class.
     * @param callClass Call class.
     * @param callInputClass Call input class.
     * @param inputTransformer Function which transforms the call input to string.
     * @param parameters Command arguments.
     * @param expected Expected call input string.
     * @param <IT> Input for the call.
     * @param <OT> Output of the call.
     * @param <T> Call type.
     */
    protected <IT extends CallInput, OT, T extends AsyncCall<IT, OT>, FT extends AsyncCallFactory<IT, OT>>
    void checkParametersAsync(
            String command,
            Class<FT> callFactoryClass,
            Class<T> callClass,
            Class<IT> callInputClass,
            Function<IT, String> inputTransformer,
            String parameters,
            String expected
    ) {
        AsyncCall<IT, OT> call = registerMockCallAsync(callFactoryClass, callClass);
        // Recreate the CommandLine object so that the registered mocks are available to this command.
        createCommand();

        execute(command + " " + parameters);
        assertAll(
                this::assertExitCodeIsZero,
                this::assertErrOutputIsEmpty
        );

        IT callInput = verifyCallInputAsync(call, callInputClass);
        assertThat(inputTransformer.apply(callInput), is(expected));
    }

    /**
     * Registers mock call of the specified class into the Micronaut's context. Mock returns empty output when executed.
     *
     * @param callClass Call class.
     * @param <IT> Input for the call.
     * @param <OT> Output of the call.
     * @param <T> Call type.
     * @return Created mock.
     */
    private <IT extends CallInput, OT, T extends Call<IT, OT>> T registerMockCall(Class<T> callClass) {
        T mock = mock(callClass);
        context.registerSingleton(mock);
        when(mock.execute(any())).thenReturn(DefaultCallOutput.empty());
        return mock;
    }

    /**
     * Registers mock async call factory of the specified class into the Micronaut's context. Mock factory creates mock calls. Mock call
     * returns empty output when executed.
     *
     * @param callFactoryClass Call class.
     * @param <IT> Input for the call.
     * @param <OT> Output of the call.
     * @param <FT> Call factory type.
     * @param <T> Call type.
     * @return Created mock.
     */
    private <IT extends CallInput, OT, T extends AsyncCall<IT, OT>, FT extends AsyncCallFactory<IT, OT>>
    AsyncCall<IT, OT> registerMockCallAsync(Class<FT> callFactoryClass, Class<T> callClass) {
        FT mockCallFactory = mock(callFactoryClass);
        context.registerSingleton(mockCallFactory);

        T mockCall = mock(callClass);
        when(mockCall.execute(any())).thenReturn(completedFuture(DefaultCallOutput.empty()));

        when(mockCallFactory.create(any())).thenReturn(mockCall);
        return mockCall;
    }

    /**
     * Verifies that the call was executed and returns its input.
     *
     * @param call Call mock.
     * @param inputClass Call input class.
     * @param <IT> Input for the call.
     * @param <OT> Output of the call.
     * @param <T> Call type.
     * @return Call input.
     */
    private static <IT extends CallInput, OT, T extends Call<IT, OT>> IT verifyCallInput(T call, Class<IT> inputClass) {
        ArgumentCaptor<IT> captor = ArgumentCaptor.forClass(inputClass);
        verify(call).execute(captor.capture());
        return captor.getValue();
    }

    /**
     * Verifies that the async call was executed and returns its input.
     *
     * @param call Call mock.
     * @param inputClass Call input class.
     * @param <IT> Input for the call.
     * @param <OT> Output of the call.
     * @param <T> Call type.
     * @return Call input.
     */
    private static <IT extends CallInput, OT, T extends AsyncCall<IT, OT>> IT verifyCallInputAsync(T call, Class<IT> inputClass) {
        ArgumentCaptor<IT> captor = ArgumentCaptor.forClass(inputClass);
        verify(call).execute(captor.capture());
        return captor.getValue();
    }
}
