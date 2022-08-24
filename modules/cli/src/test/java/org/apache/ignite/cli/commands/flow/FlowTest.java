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

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
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
import org.apache.ignite.cli.core.flow.builder.Flows;
import org.apache.ignite.cli.core.flow.question.JlineQuestionWriterReader;
import org.apache.ignite.cli.core.flow.question.QuestionAnswer;
import org.apache.ignite.cli.core.flow.question.QuestionAskerFactory;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@MicronautTest
class FlowTest {
    private Terminal terminal;

    private Path input;

    @BeforeEach
    public void setup() throws IOException {
        input = Files.createTempFile("input", "");
        input.toFile().deleteOnExit();
        terminal = new DumbTerminal(Files.newInputStream(input), new FileOutputStream(FileDescriptor.out));
        LineReaderImpl reader = new LineReaderImpl(terminal);
        QuestionAskerFactory.setReadWriter(new JlineQuestionWriterReader(reader));
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
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17519")
    void printsToOutput() throws IOException {
        // Given
        bindAnswers("no"); // we don't care about answer in this test

        StringWriter out = new StringWriter();
        PrintWriter output = new PrintWriter(out);
        StringWriter errOut = new StringWriter();
        PrintWriter errOutput = new PrintWriter(errOut);

        // When build flow
        Flow<Object, String> build = Flows.question("Do you like this?",
                        List.of(new QuestionAnswer<>("yes"::equals, (a, i) -> 1),
                                new QuestionAnswer<>("no"::equals, (a, i) -> 2))
                )
                .map(String::valueOf)
                .exceptionHandler(new TestExceptionHandler())
                .then(Flows.fromCall(new ThrowingStrCall(), StrCallInput::new))
                .toOutput(output, errOutput)
                .build();

        // Then the output is empty
        assertThat(errOut.toString(), emptyString());

        // When start flow
        build.start(Flowable.empty());

        // Then output equals to the message from the exception because we use TestExceptionHandler
        assertThat(errOut.toString(), equalTo("Ooops!")); // BUT there is the message taken from default exception handler
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17519")
    void printsToOutputThatWorks() throws IOException {
        // Given
        bindAnswers("no"); // we don't care about answer in this test

        StringWriter out = new StringWriter();
        PrintWriter output = new PrintWriter(out);
        StringWriter errOut = new StringWriter();
        PrintWriter errOutput = new PrintWriter(errOut);

        // When build flow
        Flow<Object, String> build = Flows.question("Do you like this?",
                        List.of(new QuestionAnswer<>("yes"::equals, (a, i) -> 1),
                                new QuestionAnswer<>("no"::equals, (a, i) -> 2))
                )
                .map(String::valueOf)
                .toOutput(output, errOutput)
                .then(Flows.fromCall(new ThrowingStrCall(), StrCallInput::new))
                .exceptionHandler(new TestExceptionHandler())
                .build();

        // Then the output is empty
        assertThat(errOut.toString(), emptyString());

        // When start flow
        build.start(Flowable.empty());

        // Then output equals to the message from the exception because we use TestExceptionHandler
        assertThat(errOut.toString(), equalTo("Ooops!")); // BUT it is empty
    }

    private static Flow<Object, Integer> createFlow() {
        return Flows.question("Do you like this?",
                        List.of(new QuestionAnswer<>("yes"::equals, (a, i) -> 1),
                                new QuestionAnswer<>("no"::equals, (a, i) -> 2))
                )
                .map(String::valueOf)
                .question(s -> "Here is your number " + s + ":, would you like to multiply it by 2?",
                        List.of(new QuestionAnswer<>("yes"::equals, (a, i) -> Integer.parseInt(i) * 2),
                                new QuestionAnswer<>("no"::equals, (a, i) -> Integer.parseInt(i))))
                .ifThen(num -> num == 1, Flows.fromCall(new IntCall(), IntCallInput::new))
                .ifThen(num -> num > 1, Flows.fromCall(new StrCall(), integer -> new StrCallInput(String.valueOf(integer))))
                .build();
    }

    private void bindAnswers(String... answers) throws IOException {
        Files.writeString(input, String.join("\n", answers) + "\n");
    }
}
