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
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
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

    protected abstract Class<?> getCommandClass();

    protected void execute(String argsLine) {
        execute(argsLine.split(" "));
    }

    protected void execute(String... args) {
        // Create command just before execution as some tests could register singletons which should be used by the command
        CommandLine cmd = new CommandLine(getCommandClass(), new MicronautFactory(context));

        sout = new StringWriter();
        serr = new StringWriter();
        cmd.setOut(new PrintWriter(sout));
        cmd.setErr(new PrintWriter(serr));

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

    protected void assertOutputContains(String... expectedOutput) {
        assertThat(sout.toString())
                .as("Expected command output to contain: " + Arrays.toString(expectedOutput) + " but was " + sout.toString())
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
                .as("Expected command error output to contain: " + expectedErrOutput + " but was " + serr.toString())
                .contains(expectedErrOutput);
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
