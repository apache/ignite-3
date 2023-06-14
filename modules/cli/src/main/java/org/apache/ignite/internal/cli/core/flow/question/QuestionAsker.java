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

package org.apache.ignite.internal.cli.core.flow.question;

import java.util.List;

/**
 * Question asker.
 */
public class QuestionAsker {

    private final QuestionWriterReaderFactory writerReaderFactory;

    private boolean completeFilePaths;

    private boolean maskInput;

    public QuestionAsker(QuestionWriterReaderFactory writerReaderFactory) {
        this.writerReaderFactory = writerReaderFactory;
    }

    /**
     * Ask question.
     *
     * @param question text of question.
     * @param answers list of possible answers.
     * @param <I> input type.
     * @param <O> output type.
     * @return value of answer.
     */
    public <I, O> O askQuestion(String question, I input, List<QuestionAnswer<I, O>> answers) {
        String userAnswer = askQuestion(question);

        for (QuestionAnswer<I, O> answer : answers) {
            if (answer.isAnswer(userAnswer)) {
                return answer.transform(userAnswer, input);
            }
        }
        return askQuestion(question, input, answers);
    }

    /**
     * Ask for input.
     *
     * @param question text of question.
     * @return value of answer.
     */
    public String askQuestion(String question) {
        QuestionWriterReader readWriter = writerReaderFactory.createWriterReader(completeFilePaths);
        return readWriter.readAnswer(question, maskInput);
    }

    /**
     * Enables file paths completion.
     *
     * @param completeFilePaths If {@code true}, file paths will be completed.
     */
    public QuestionAsker completeFilePaths(boolean completeFilePaths) {
        this.completeFilePaths = completeFilePaths;
        return this;
    }

    /**
     * Enables input masking.
     *
     * @param maskInput If {@code true}, user input will be masked with asterisks.
     */
    public QuestionAsker maskInput(boolean maskInput) {
        this.maskInput = maskInput;
        return this;
    }

}
