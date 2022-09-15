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

package org.apache.ignite.cli.commands.flow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.cli.core.flow.Flow;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.cli.core.flow.builder.Flows;
import org.apache.ignite.cli.core.flow.question.JlineQuestionWriterReader;
import org.apache.ignite.cli.core.flow.question.QuestionAnswer;
import org.apache.ignite.cli.core.flow.question.QuestionAskerFactory;
import org.apache.ignite.cli.core.repl.context.CommandLineContextProvider;
import org.jline.reader.impl.LineReaderImpl;
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

    @BeforeEach
    public void setup() throws IOException {
        input = Files.createTempFile("input", "");
        input.toFile().deleteOnExit();
        terminal = new DumbTerminal(Files.newInputStream(input), new FileOutputStream(FileDescriptor.out));
        LineReaderImpl reader = new LineReaderImpl(terminal);
        QuestionAskerFactory.setReadWriter(new JlineQuestionWriterReader(reader));
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

        // When build flow
        Flow<Object, String> flow = askQuestion()
                .exceptionHandler(new TestExceptionHandler())
                .then(Flows.fromCall(new ThrowingStrCall(), StrCallInput::new))
                .print()
                .build();

        // Then the output is empty
        assertThat(errOut.toString(), emptyString());

        // When start flow
        flow.start(Flowable.empty());

        // Then output equals to the message from the exception because we use TestExceptionHandler
        assertThat(errOut.toString(), equalTo("Ooops!" + System.lineSeparator()));
    }

    @Test
    @DisplayName("print -> then -> exceptionHandler")
    void printThenHandler() throws IOException {
        // Given
        bindAnswers("no"); // we don't care about answer in this test

        // When build flow
        Flow<Object, String> flow = askQuestion()
                .print()
                .then(Flows.fromCall(new ThrowingStrCall(), StrCallInput::new))
                .exceptionHandler(new TestExceptionHandler())
                .build();

        // Then the output is empty
        assertThat(errOut.toString(), emptyString());

        // When start flow
        flow.start(Flowable.empty());

        // Then output is empty because print was used before the call
        assertThat(errOut.toString(), emptyString());
    }

    @Test
    @DisplayName("then -> exceptionHandler -> print")
    void thenHandlerPrint() throws IOException {
        // Given
        bindAnswers("no"); // we don't care about answer in this test

        // When build flow
        Flow<Object, String> flow = askQuestion()
                .then(Flows.fromCall(new ThrowingStrCall(), StrCallInput::new))
                .exceptionHandler(new TestExceptionHandler())
                .print()
                .build();

        // Then the output is empty
        assertThat(errOut.toString(), emptyString());

        // When start flow
        flow.start(Flowable.empty());

        // Then output equals to the message from the exception because we use TestExceptionHandler
        assertThat(errOut.toString(), equalTo("Ooops!" + System.lineSeparator()));
    }

    @Test
    void multiplePrints() throws IOException {
        // Given
        bindAnswers("no");

        // When build flow
        Flow<Object, String> flow = askQuestion()
                .print()
                .print()
                .build();

        // Then the output is empty
        assertThat(out.toString(), emptyString());
        assertThat(errOut.toString(), emptyString());

        // When start flow
        flow.start(Flowable.empty());

        // Then output equals to 2 messages from print operations
        assertThat(out.toString(), equalTo("2" + System.lineSeparator()
                        + "2" + System.lineSeparator()));
        assertThat(errOut.toString(), emptyString());
    }

    @Test
    void multiplePrintsWithError() throws IOException {
        // Given
        bindAnswers("no");

        // When build flow
        Flow<Object, String> flow = askQuestion()
                .then(Flows.fromCall(new ThrowingStrCall(), StrCallInput::new))
                .exceptionHandler(new TestExceptionHandler())
                .print()
                .print()
                .build();

        // Then the output is empty
        assertThat(out.toString(), emptyString());
        assertThat(errOut.toString(), emptyString());

        // When start flow
        flow.start(Flowable.empty());

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
                .then(Flows.fromCall(new ThrowingStrCall(), StrCallInput::new))
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

    private static Flow<Object, Integer> createFlow() {
        return askQuestion()
                .question(s -> "Here is your number " + s + ":, would you like to multiply it by 2?",
                        List.of(new QuestionAnswer<>("yes"::equals, (a, i) -> Integer.parseInt(i) * 2),
                                new QuestionAnswer<>("no"::equals, (a, i) -> Integer.parseInt(i))))
                .ifThen(num -> num == 1, Flows.fromCall(new IntCall(), IntCallInput::new))
                .ifThen(num -> num > 1, Flows.fromCall(new StrCall(), integer -> new StrCallInput(String.valueOf(integer))))
                .build();
    }

    private static FlowBuilder<Object, String> askQuestion() {
        return Flows.question("Do you like this?",
                        List.of(new QuestionAnswer<>("yes"::equals, (a, i) -> 1),
                                new QuestionAnswer<>("no"::equals, (a, i) -> 2))
                )
                .map(String::valueOf);
    }

    private void bindAnswers(String... answers) throws IOException {
        Files.writeString(input, String.join("\n", answers) + "\n");
    }
}
