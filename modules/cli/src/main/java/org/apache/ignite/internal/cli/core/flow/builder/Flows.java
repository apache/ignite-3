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

package org.apache.ignite.internal.cli.core.flow.builder;

import static org.apache.ignite.internal.cli.core.flow.Flowable.failure;
import static org.apache.ignite.internal.cli.core.flow.Flowable.success;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.flow.DefaultFlowable;
import org.apache.ignite.internal.cli.core.flow.Flow;
import org.apache.ignite.internal.cli.core.flow.Flowable;
import org.apache.ignite.internal.cli.core.flow.question.AcceptedQuestionAnswer;
import org.apache.ignite.internal.cli.core.flow.question.InterruptQuestionAnswer;
import org.apache.ignite.internal.cli.core.flow.question.QuestionAnswer;
import org.apache.ignite.internal.cli.core.flow.question.QuestionAskerFactory;
import org.apache.ignite.internal.cli.core.style.component.QuestionUiComponent;

/**
 * Helper class for operating and creating {@link Flow} and {@link FlowBuilder}.
 */
public final class Flows {
    private Flows() {
    }

    /**
     * Transform call to flow.
     *
     * @param call {@link Call} instance.
     * @param <I> input type.
     * @param <T> output type.
     * @return new {@link Flow} will call action.
     */
    public static <I extends CallInput, T> Flow<I, T> fromCall(Call<I, T> call) {
        return flowable -> {
            if (flowable.hasError()) {
                return failure(flowable.errorCause());
            }
            return fromOutput(call.execute(flowable.value()));
        };
    }

    /**
     * Transform call to flow.
     *
     * @param call {@link Call} instance.
     * @param mapper function to transform input to call input.
     * @param <I> input type.
     * @param <CIT> call input type.
     * @param <T> output type.
     * @return new {@link Flow} will call action.
     */
    public static <I, CIT extends CallInput, T> Flow<I, T> fromCall(Call<CIT, T> call, Function<I, CIT> mapper) {
        return flowable -> {
            if (flowable.hasError()) {
                return failure(flowable.errorCause());
            }
            return fromOutput(call.execute(mapper.apply(flowable.value())));
        };
    }

    /**
     * Create flow which return provided value as result.
     *
     * @param value result value.
     * @param <T> output type.
     * @return {@link FlowBuilder} which started from constant flow.
     */
    public static <T> FlowBuilder<Void, T> from(T value) {
        return new FlowBuilderImpl<>(input -> success(value));
    }

    /**
     * Create flow from transform action.
     *
     * @param function flow action.
     * @param <I> input type.
     * @param <O> output type.
     * @return new {@link FlowBuilder} which starter from {@link Flow} with {@param function}.
     */
    public static <I, O> FlowBuilder<I, O> from(Function<I, O> function) {
        return new FlowBuilderImpl<>(mono(function));
    }

    /**
     * Return mono {@link Flow} with provided transform function.
     *
     * @param function flow action.
     * @param <I> input type.
     * @param <O> output type.
     * @return new {@link Flow} with action {@param function}.
     */
    public static <I, O> Flow<I, O> mono(Function<I, O> function) {
        return input -> {
            if (input.hasError()) {
                return failure(input.errorCause());
            }
            return Flowable.process(() -> function.apply(input.value()));
        };
    }

    private static <T> Flowable<T> fromOutput(CallOutput<T> output) {
        return DefaultFlowable
                .<T>builder()
                .body(output.body())
                .cause(output.errorCause())
                .build();
    }

    /**
     * Create new {@link FlowBuilder} which starts from question.
     *
     * @param question question text.
     * @param answers all possible answers.
     * @param <I> input type.
     * @param <O> output type.
     * @return new {@link FlowBuilder}.
     */
    public static <I, O> FlowBuilder<I, O> question(String question, List<QuestionAnswer<I, O>> answers) {
        return new FlowBuilderImpl<>(questionFlow(question, answers));
    }

    /**
     * Create new {@link FlowBuilder} which starts from yes/no question and pass the result of the {@code onAccept}
     * call on positive answer or interrupts the flow on negative answer.
     *
     * @param question question text.
     * @param onAccept callback to call on positive answer.
     * @param <I> input type.
     * @param <O> output type.
     * @return new {@link FlowBuilder}.
     */
    public static <I, O> FlowBuilder<I, O> acceptQuestion(String question, Supplier<O> onAccept) {
        return question(question,
                List.of(new AcceptedQuestionAnswer<>((a, i) -> onAccept.get()),
                        new InterruptQuestionAnswer<>())
        );
    }

    /**
     * Create new {@link FlowBuilder} which starts from yes/no question and pass the result of the {@code onAccept}
     * call on positive answer or interrupts the flow on negative answer.
     *
     * @param question question UI component.
     * @param onAccept callback to call on positive answer.
     * @param <I> input type.
     * @param <O> output type.
     * @return new {@link FlowBuilder}.
     */
    public static <I, O> FlowBuilder<I, O> acceptQuestion(QuestionUiComponent question, Supplier<O> onAccept) {
        return acceptQuestion(question.render(), onAccept);
    }

    /**
     * Create new {@link Flow} which asks questions and returns the result of answer.
     *
     * @param question question text.
     * @param answers all possible answers.
     * @param <I> input type.
     * @param <O> output type.
     * @return new {@link Flow}.
     */
    public static <I, O> Flow<I, O> questionFlow(String question, List<QuestionAnswer<I, O>> answers) {
        return input -> success(QuestionAskerFactory
                .newQuestionAsker().askQuestion(question, input.value(), answers));
    }

    /**
     * Create new {@link Flow} which asks yes/no question, passes the input to the {@code onAccept} handler
     * on positive answer or returns an input on negative answer.
     *
     * @param question question UI component.
     * @param onAccept callback to call on positive answer.
     * @param <I> input type.
     * @return new {@link Flow}.
     */
    public static <I> Flow<I, I> acceptQuestionFlow(QuestionUiComponent question, Consumer<I> onAccept) {
        return questionFlow(question.render(),
                List.of(new AcceptedQuestionAnswer<>((a, i) -> {
                    onAccept.accept(i);
                    return i;
                }), new QuestionAnswer<>(s -> true, (a, i) -> i))
        );
    }

    /**
     * Returns a {@link FlowBuilder} that always returns its input argument.
     */
    public static <I> FlowBuilder<I, I> identity() {
        return new FlowBuilderImpl<>(input -> success(input.value()));
    }
}
