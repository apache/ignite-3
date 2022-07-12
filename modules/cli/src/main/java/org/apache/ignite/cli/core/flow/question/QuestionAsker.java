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

package org.apache.ignite.cli.core.flow.question;

import java.util.List;

/**
 * Question asker.
 */
public class QuestionAsker {

    private final QuestionWriterReader readWriter;

    public QuestionAsker(QuestionWriterReader readWriter) {
        this.readWriter = readWriter;
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
        readWriter.writeQuestion(question);
        String userAnswer = readWriter.readAnswer();

        for (QuestionAnswer<I, O> answer : answers) {
            if (answer.isAnswer(userAnswer)) {
                return answer.transform(userAnswer, input);
            }
        }
        readWriter.writeQuestion("");
        return askQuestion(question, input, answers);
    }
}
