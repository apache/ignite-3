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

package org.apache.ignite.internal.cli.commands.flow;

import static org.apache.ignite.internal.cli.core.call.DefaultCallOutput.success;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.apache.ignite.internal.cli.core.call.ThrowingStrCall;
import org.apache.ignite.internal.cli.core.exception.TestExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.Flow;
import org.apache.ignite.internal.cli.core.flow.Flowable;
import org.apache.ignite.internal.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.apache.ignite.internal.cli.core.flow.question.JlineQuestionWriterReaderFactory;
import org.apache.ignite.internal.cli.core.flow.question.QuestionAnswer;
import org.apache.ignite.internal.cli.core.flow.question.QuestionAskerFactory;
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContextProvider;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FlowTest {
    private Terminal terminal;

    private Path input;
    private StringWriter out;
    private StringWriter errOut;

    @Test
    void testVerbose() throws IOException {
        // Given
        bindAnswers("no");

        // When start flow with print and verbose
        askQuestion()
                .then(Flows.fromCall(new ThrowingStrCall(), StringCallInput::new))
                .exceptionHandler(new TestExceptionHandler())
                .verbose(true)
                .print()
                .start();

        // Then error output starts with the message from exception and contains verbose output
        assertThat(out.toString(), emptyString());
        assertThat(errOut.toString(), startsWith("Ooops!" + System.lineSeparator()));
        assertThat(errOut.toString(), containsString("verbose output"));
    }

    private static Flow<Object, Integer> createFlow() {
        return askQuestion()
                .question(s -> "Here is your number " + s + ":, would you like to multiply it by 2?",
                        List.of(new QuestionAnswer<>("yes"::equals, (a, i) -> Integer.parseInt(i) * 2),
                                new QuestionAnswer<>("no"::equals, (a, i) -> Integer.parseInt(i))))
                .ifThen(num -> num == 1, Flows.fromCall(new IntCall(), IntCallInput::new))
                .ifThen(num -> num > 1, Flows.fromCall(new StrCall(), integer -> new StringCallInput(String.valueOf(integer))))
                .build();
    }

    private static FlowBuilder<Object, String> askQuestion() {
        return Flows.question("Do you like this?",
                        List.of(new QuestionAnswer<>("yes"::equals, (a, i) -> 1),
                                new QuestionAnswer<>("no"::equals, (a, i) -> 2))
                )
                .map(String::valueOf);
    }

    @BeforeEach
    @SuppressWarnings("PMD.AvoidFileStream")
    public void setup() throws IOException {
        input = Files.createTempFile("input", "");
        input.toFile().deleteOnExit();
        terminal = new DumbTerminal(Files.newInputStream(input), new FileOutputStream(FileDescriptor.out));
        QuestionAskerFactory.setWriterReaderFactory(new JlineQuestionWriterReaderFactory(terminal));
        out = new StringWriter();
        errOut = new StringWriter();
        CommandLineContextProvider.setWriters(new PrintWriter(out), new PrintWriter(errOut));
    }

    @AfterEach
    public void cleanUp() throws IOException {
        terminal.input().close();
        terminal.close();
    }

    @Test
    @DisplayName("Basic flow with yes/yes question")
    void test1() throws IOException {
        bindAnswers("yes", "yes");
        Flowable<Integer> call = createFlow().start(Flowable.empty());
        Assertions.assertEquals(2, call.value());
    }

    @Test
    @DisplayName("Basic flow with no/yes question")
    void test2() throws IOException {
        bindAnswers("no", "yes");
        Flowable<Integer> call = createFlow().start(Flowable.empty());
        Assertions.assertEquals(4, call.value());
    }

    @Test
    @DisplayName("Basic flow with no/no question")
    void test3() throws IOException {
        bindAnswers("no", "no");
        Flowable<Integer> call = createFlow().start(Flowable.empty());
        Assertions.assertEquals(2, call.value());
    }

    @Test
    @DisplayName("Basic flow with yes/no question")
    void test4() throws IOException {
        bindAnswers("yes", "no");
        Flowable<Integer> call = createFlow().start(Flowable.empty());
        Assertions.assertEquals(1, call.value());
    }

    @Test
    @DisplayName("exceptionHandler -> then -> print")
    void handlerThenPrint() throws IOException {
        // Given
        bindAnswers("no"); // we don't care about answer in this test

        // When build flow and start
        askQuestion()
                .exceptionHandler(new TestExceptionHandler())
                .then(Flows.fromCall(new ThrowingStrCall(), StringCallInput::new))
                .print()
                .start();

        // Then output equals to the message from the exception because we use TestExceptionHandler
        assertThat(errOut.toString(), equalTo("Ooops!" + System.lineSeparator()));
    }

    @Test
    @DisplayName("print -> then -> exceptionHandler")
    void printThenHandler() throws IOException {
        // Given
        bindAnswers("no"); // we don't care about answer in this test

        // When build flow and start
        askQuestion()
                .print()
                .then(Flows.fromCall(new ThrowingStrCall(), StringCallInput::new))
                .exceptionHandler(new TestExceptionHandler())
                .start();

        // Then output is empty because print was used before the call
        assertThat(errOut.toString(), emptyString());
    }

    @Test
    @DisplayName("then -> exceptionHandler -> print")
    void thenHandlerPrint() throws IOException {
        // Given
        bindAnswers("no"); // we don't care about answer in this test

        // When build flow and start
        askQuestion()
                .then(Flows.fromCall(new ThrowingStrCall(), StringCallInput::new))
                .exceptionHandler(new TestExceptionHandler())
                .print()
                .start();

        // Then output equals to the message from the exception because we use TestExceptionHandler
        assertThat(errOut.toString(), equalTo("Ooops!" + System.lineSeparator()));
    }

    @Test
    void multiplePrints() throws IOException {
        // Given
        bindAnswers("no");

        // When build flow and start
        askQuestion()
                .print()
                .print()
                .start();

        // Then output equals to 2 messages from print operations
        assertThat(out.toString(), equalTo("2" + System.lineSeparator()
                + "2" + System.lineSeparator()));
        assertThat(errOut.toString(), emptyString());
    }

    @Test
    void multiplePrintsWithError() throws IOException {
        // Given
        bindAnswers("no");

        // When build flow and start
        askQuestion()
                .then(Flows.fromCall(new ThrowingStrCall(), StringCallInput::new))
                .exceptionHandler(new TestExceptionHandler())
                .print()
                .print()
                .start();

        // Then error output equals to 2 messages from exception handler
        assertThat(out.toString(), emptyString());
        assertThat(errOut.toString(), equalTo("Ooops!" + System.lineSeparator()
                + "Ooops!" + System.lineSeparator()));
    }

    @Test
    void printAndStart() throws IOException {
        // Given
        bindAnswers("no");

        // When start flow with print
        askQuestion()
                .print()
                .start();

        // Then error output equals to the message from answer
        assertThat(out.toString(), equalTo("2" + System.lineSeparator()));
        assertThat(errOut.toString(), emptyString());
    }

    @Test
    void printAndStartError() throws IOException {
        // Given
        bindAnswers("no");

        // When start flow with print
        askQuestion()
                .then(Flows.fromCall(new ThrowingStrCall(), StringCallInput::new))
                .exceptionHandler(new TestExceptionHandler())
                .print()
                .start();

        // Then error output equals to the message from exception
        assertThat(out.toString(), emptyString());
        assertThat(errOut.toString(), equalTo("Ooops!" + System.lineSeparator()));
    }

    @Test
    void customDecorator() throws IOException {
        // Given
        bindAnswers("no");

        // When start flow with print
        askQuestion()
                .print(data -> () -> "*" + data + "*")
                .start();

        // Then error output equals to the message from exception
        assertThat(out.toString(), equalTo("*2*" + System.lineSeparator()));
        assertThat(errOut.toString(), emptyString());
    }

    @Test
    void flatMap() {
        Flows.from("fizz")
                .flatMap(v -> Flows.from(it -> it + "buzz"))
                .print()
                .start();
        assertThat(out.toString(), equalTo("fizzbuzz" + System.lineSeparator()));
    }

    @Test
    void interruptFlatMap() {
        Flows.from("fizz")
                .map(it -> it + "1")
                .print()
                .flatMap(v -> Flows.<String, String>from(ignored -> Flowable.interrupt()))
                .print()
                .map(it -> it + "2")
                .print()
                .start();
        assertThat(out.toString(), equalTo("fizz1" + System.lineSeparator()));
    }

    @Test
    void interruptThen() {
        Flows.from("fizz")
                .map(it -> it + "1")
                .print()
                .then(v -> Flowable.interrupt())
                .print()
                .map(it -> it + "2")
                .print()
                .start();
        assertThat(out.toString(), equalTo("fizz1" + System.lineSeparator()));
    }

    @Test
    void testHandlers() {
        String value = "foo";
        Exception exception = new Exception();
        Flows.from(value)
                .onSuccess(result -> assertThat(result, equalTo(value)))
                .then(input -> Flowable.failure(exception))
                .onFailure(ex -> assertThat(ex, equalTo(exception)))
                .start();
    }

    @Test
    void failurePropagationMap() {
        Function<String, String> throwingFunction = input -> {
            throw new RuntimeException("Ooops!");
        };

        Flows.from(throwingFunction)
                .map(it -> it + "1")
                .exceptionHandler(new TestExceptionHandler())
                .print()
                .start();

        // Then error output equals to the message from exception
        assertThat(out.toString(), emptyString());
        assertThat(errOut.toString(), equalTo("Ooops!" + System.lineSeparator()));
    }

    @Test
    void failurePropagationFromCall() {
        Function<String, StringCallInput> throwingFunction = input -> {
            throw new RuntimeException("Ooops!");
        };

        Flows.from(throwingFunction)
                .then(Flows.fromCall(it -> success(it.getString() + "1")))
                .exceptionHandler(new TestExceptionHandler())
                .print()
                .start();

        // Then error output equals to the message from exception
        assertThat(out.toString(), emptyString());
        assertThat(errOut.toString(), equalTo("Ooops!" + System.lineSeparator()));
    }

    @Test
    void failurePropagationFromCallWithMapper() {
        Function<String, StringCallInput> throwingFunction = input -> {
            throw new RuntimeException("Ooops!");
        };

        Flows.from(throwingFunction)
                .then(Flows.fromCall(it -> success(it.getString() + "1"), Function.identity()))
                .exceptionHandler(new TestExceptionHandler())
                .print()
                .start();

        // Then error output equals to the message from exception
        assertThat(out.toString(), emptyString());
        assertThat(errOut.toString(), equalTo("Ooops!" + System.lineSeparator()));
    }

    private void bindAnswers(String... answers) throws IOException {
        Files.writeString(input, String.join("\n", answers) + "\n");
    }
}
