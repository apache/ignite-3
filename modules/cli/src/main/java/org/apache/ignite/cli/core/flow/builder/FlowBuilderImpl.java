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

package org.apache.ignite.cli.core.flow.builder;

import java.io.PrintWriter;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.cli.commands.decorators.DefaultDecoratorRegistry;
import org.apache.ignite.cli.core.decorator.DecoratorRegistry;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.cli.core.flow.Flow;
import org.apache.ignite.cli.core.flow.FlowInterruptException;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.flow.question.QuestionAnswer;
import org.apache.ignite.cli.core.flow.question.QuestionAskerFactory;

/**
 * Implementation of {@link FlowBuilder}.
 *
 * @param <I> flow input type.
 * @param <O> flow output type.
 */
public class FlowBuilderImpl<I, O> implements FlowBuilder<I, O> {
    private final Flow<I, O> flow;
    private final ExceptionHandlers exceptionHandlers = new DefaultExceptionHandlers();
    private final DecoratorRegistry decoratorRegistry = new DefaultDecoratorRegistry();

    public FlowBuilderImpl(Flow<I, O> flow) {
        this(flow, new DefaultExceptionHandlers(), new DefaultDecoratorRegistry());
    }

    /**
     * Constructor.
     *
     * @param flow flow instance.
     * @param exceptionHandlers exception handlers.
     * @param decoratorRegistry decorator registry.
     */
    public FlowBuilderImpl(Flow<I, O> flow, ExceptionHandlers exceptionHandlers, DecoratorRegistry decoratorRegistry) {
        this.flow = flow;
        this.exceptionHandlers.addExceptionHandlers(exceptionHandlers);
        this.decoratorRegistry.addAll(decoratorRegistry);
    }

    @Override
    public <OT> FlowBuilder<I, OT> appendFlow(Flow<O, OT> flow) {
        return new FlowBuilderImpl<>(this.flow.composite(flow), exceptionHandlers, decoratorRegistry);
    }

    @Override
    public <OT> FlowBuilder<I, O> ifThen(Predicate<O> tester, Flow<O, OT> flow) {
        return new FlowBuilderImpl<>(this.flow.composite(input -> {
            if (tester.test(input.value())) {
                flow.call(input);
            }
            return input;
        }), exceptionHandlers, decoratorRegistry);
    }

    @Override
    public <QT> FlowBuilder<I, QT> question(String questionText, List<QuestionAnswer<O, QT>> questionAnswers) {
        return new FlowBuilderImpl<>(flow.composite(input -> Flowable.success(
                QuestionAskerFactory.newQuestionAsker().askQuestion(questionText, input.value(), questionAnswers))),
                exceptionHandlers, decoratorRegistry);
    }

    @Override
    public <QT> FlowBuilder<I, QT> question(Function<O, String> questionText, List<QuestionAnswer<O, QT>> answers) {
        return new FlowBuilderImpl<>(flow.composite(input -> Flowable.success(
                QuestionAskerFactory.newQuestionAsker().askQuestion(questionText.apply(input.value()), input.value(), answers))),
                exceptionHandlers, decoratorRegistry);
    }

    @Override
    public FlowBuilder<I, O> exceptionHandler(ExceptionHandler<?> exceptionHandler) {
        exceptionHandlers.addExceptionHandler(exceptionHandler);
        return this;
    }

    @Override
    public FlowBuilder<I, O> toOutput(PrintWriter output, PrintWriter errorOutput) {
        return new FlowBuilderImpl<>(flow.composite(input -> {
            if (input.hasResult()) {
                output.println(decoratorRegistry.getDecorator(input.type()).decorate(input.value()).toTerminalString());
            } else if (input.hasError()) {
                exceptionHandlers.handleException(ExceptionWriter.fromPrintWriter(errorOutput), input.errorCause());
                return Flowable.empty();
            }
            return input;
        }));
    }

    @Override
    public Flow<I, O> build() {
        return input -> {
            try {
                Flowable<O> output = flow.call(input);
                if (output.hasError()) {
                    exceptionHandlers.handleException(output.errorCause());
                }
                return output;
            } catch (FlowInterruptException e) {
                return Flowable.empty();
            }
        };
    }
}
