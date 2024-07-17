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

import java.util.function.Predicate;

/**
 * Question answer representation.
 *
 * @param <I> input type.
 * @param <O> output type.
 */
public class QuestionAnswer<I, O> {
    private final Predicate<String> answer;
    private final AnswerAction<I, O> action;

    public QuestionAnswer(Predicate<String> answerTester, AnswerAction<I, O> action) {
        this.answer = answerTester;
        this.action = action;
    }

    public boolean isAnswer(String userAnswer) {
        return answer.test(userAnswer);
    }

    public O transform(String answer, I input) {
        return action.accept(answer, input);
    }

    /**
     * Functional interface of answer action.
     *
     * @param <I> input type.
     * @param <O> output type.
     */
    public interface AnswerAction<I, O> {
        O accept(String answer, I input);
    }
}
