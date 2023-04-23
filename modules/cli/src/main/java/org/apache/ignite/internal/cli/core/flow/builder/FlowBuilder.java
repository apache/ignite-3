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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite.internal.cli.core.flow.Flow;
import org.apache.ignite.internal.cli.core.flow.question.QuestionAnswer;

/**
 * Builder of {@link Flow}.
 *
 * @param <I> input type.
 * @param <O> output type.
 */
public interface FlowBuilder<I, O>  {

    /**
     * Appends flow to this builder.
     *
     * @param flow flow to append
     * @param <OT> output type of appended flow
     * @return instance of builder with appended flow
     */
    <OT> FlowBuilder<I, OT> then(Flow<O, OT> flow);

    /**
     * Transforms current flow result.
     *
     * @param mapper function to transform the result of the current flow
     * @param <OT> output type of transformation function
     * @return instance of builder with transform
     */
    default <OT> FlowBuilder<I, OT> map(Function<O, OT> mapper) {
        return then(Flows.mono(mapper));
    }

    /**
     * Returns a {@link FlowBuilder} consisting of the results of replacing each element
     * of this flow with the contents of a mapped flow produced
     * by applying the provided mapping function to each element.
     *
     * @param mapper function to apply to each element which produces a flow of new values
     * @return the new {@link FlowBuilder}
     */
    <OT> FlowBuilder<I, OT> flatMap(Function<O, FlowBuilder<O, OT>> mapper);

    /**
     * Appends the flow to this builder if the result of the current flow matches the predicate.
     *
     * @param tester predicate to test
     * @param flow flow to append
     * @param <OT> output type of appended flow
     * @return instance of builder
     */
    <OT> FlowBuilder<I, O> ifThen(Predicate<O> tester, Flow<O, OT> flow);

    /**
     * Appends the flow which will ask a question based on the result of the current flow and return the question answer.
     *
     * @param questionText text to display as a question
     * @param answers list of answers
     * @param <QT> type of the answer
     * @return instance of builder
     */
    <QT> FlowBuilder<I, QT> question(String questionText, List<QuestionAnswer<O, QT>> answers);

    /**
     * Appends the flow which will ask a question based on the result of the current flow and return the question answer.
     *
     * @param questionText function which takes a result of the current flow and returns a question text
     * @param answers list of answers
     * @param <QT> type of the answer
     * @return instance of builder
     */
    <QT> FlowBuilder<I, QT> question(Function<O, String> questionText, List<QuestionAnswer<O, QT>> answers);

    /**
     * Adds exception handler to the flow chain which will be called during print operation if flow resulted in error.
     *
     * @param exceptionHandler exception handler
     * @return instance of builder
     */
    FlowBuilder<I, O> exceptionHandler(ExceptionHandler<?> exceptionHandler);

    /**
     * Adds success handler to the flow chain which will be called at the end of the flow if the flow has succeeded. The order in which
     * handlers are executed is not determined.
     *
     * @param handler handler
     * @return instance of builder
     */
    FlowBuilder<I, O> onSuccess(Consumer<O> handler);

    /**
     * Adds success handler to the flow chain which will be called at the end of the flow if the flow has succeeded. The order in which
     * handlers are executed is not determined.
     *
     * @param handler handler
     * @return instance of builder
     */
    default FlowBuilder<I, O> onSuccess(Runnable handler) {
        return onSuccess(result -> handler.run());
    }

    /**
     * Adds failure handler to the flow chain which will be called at the end of the flow if flow resulted in error. The order in which
     * handlers are executed is not determined.
     *
     * @param handler handler
     * @return instance of builder
     */
    FlowBuilder<I, O> onFailure(Consumer<Throwable> handler);

    /**
     * Adds failure handler to the flow chain which will be called at the end of the flow if flow resulted in error. The order in which
     * handlers are executed is not determined.
     *
     * @param handler handler
     * @return instance of builder
     */
    default FlowBuilder<I, O> onFailure(Runnable handler) {
        return onFailure(throwable -> handler.run());
    }

    /**
     * Adds verbose output from debug log to the output.
     *
     * @param verbose If @{code true}, flow execution will print debug logs.
     * @return Builder instance.
     */
    FlowBuilder<I, O> verbose(boolean verbose);

    /**
     * Appends print operation which will print the result of the current flow using provided {@code decorator} or call the exception
     * handler.
     *
     * @param decorator output decorator
     * @return instance of builder
     */
    FlowBuilder<I, O> print(Decorator<O, TerminalOutput> decorator);

    /**
     * Appends print operation which will print the result of the current flow using decorator found in registry or call the exception
     * handler.
     *
     * @return instance of builder
     */
    FlowBuilder<I, O> print();

    /**
     * Builds the flow from the builder.
     *
     * @return resulting flow
     */
    Flow<I, O> build();

    /**
     * Convenience method which is equivalent to the {@code build().start(Flowable.empty())}. It builds the flow and starts it with the
     * empty input.
     */
    void start();
}
