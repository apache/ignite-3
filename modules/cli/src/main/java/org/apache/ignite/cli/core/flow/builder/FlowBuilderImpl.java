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
import org.apache.ignite.cli.core.decorator.Decorator;
import org.apache.ignite.cli.core.decorator.DecoratorRegistry;
import org.apache.ignite.cli.core.decorator.TerminalOutput;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.cli.core.flow.Flow;
import org.apache.ignite.cli.core.flow.FlowInterruptException;
import org.apache.ignite.cli.core.flow.Flowable;
import org.apache.ignite.cli.core.flow.question.QuestionAnswer;
import org.apache.ignite.cli.core.flow.question.QuestionAskerFactory;
import org.apache.ignite.cli.core.repl.context.CommandLineContextProvider;
import org.apache.ignite.cli.decorators.DefaultDecoratorRegistry;

/**
 * Implementation of {@link FlowBuilder}.
 *
 * @param <I> flow input type.
 * @param <O> flow output type.
 */
public class FlowBuilderImpl<I, O> implements FlowBuilder<I, O> {
    private final Flow<I, O> flow;
    private final ExceptionHandlers exceptionHandlers;
    private final DecoratorRegistry decoratorRegistry;

    FlowBuilderImpl(Flow<I, O> flow) {
        this(flow, new DefaultExceptionHandlers(), new DefaultDecoratorRegistry());
    }

    /**
     * Constructor.
     *
     * @param flow flow instance.
     * @param exceptionHandlers exception handlers.
     * @param decoratorRegistry decorator registry.
     */
    private FlowBuilderImpl(Flow<I, O> flow, ExceptionHandlers exceptionHandlers, DecoratorRegistry decoratorRegistry) {
        this.flow = flow;
        this.exceptionHandlers = exceptionHandlers;
        this.decoratorRegistry = decoratorRegistry;
    }

    @Override
    public <OT> FlowBuilder<I, OT> then(Flow<O, OT> flow) {
        return new FlowBuilderImpl<>(this.flow.composite(flow), exceptionHandlers, decoratorRegistry);
    }

    @Override
    public <OT> FlowBuilder<I, O> ifThen(Predicate<O> tester, Flow<O, OT> flow) {
        return then(input -> {
            if (tester.test(input.value())) {
                flow.start(input);
            }
            return input;
        });
    }

    @Override
    public <QT> FlowBuilder<I, QT> question(String questionText, List<QuestionAnswer<O, QT>> questionAnswers) {
        return then(input -> Flowable.success(
                QuestionAskerFactory.newQuestionAsker().askQuestion(questionText, input.value(), questionAnswers)));
    }

    @Override
    public <QT> FlowBuilder<I, QT> question(Function<O, String> questionText, List<QuestionAnswer<O, QT>> answers) {
        return then(input -> Flowable.success(
                QuestionAskerFactory.newQuestionAsker().askQuestion(questionText.apply(input.value()), input.value(), answers)));
    }

    @Override
    public FlowBuilder<I, O> exceptionHandler(ExceptionHandler<?> exceptionHandler) {
        exceptionHandlers.addExceptionHandler(exceptionHandler);
        return this;
    }

    @Override
    public FlowBuilder<I, O> print(Decorator<O, TerminalOutput> decorator) {
        return then(input -> printResult(input, type -> decorator));
    }

    @Override
    public FlowBuilder<I, O> print() {
        return then(input -> printResult(input, decoratorRegistry::getDecorator));
    }

    @Override
    public Flow<I, O> build() {
        return this::run;
    }

    @Override
    public void start() {
        run(Flowable.empty());
    }

    /**
     * Flow method which starts current flow and returns its result or empty output if flow is interrupted.
     *
     * @param input input flowable
     * @return output flowable
     */
    private Flowable<O> run(Flowable<I> input) {
        try {
            return flow.start(input);
        } catch (FlowInterruptException e) {
            return Flowable.empty();
        }
    }

    /**
     * Flow method which will print the decorated result of the {@code input} to the output provided by the context
     * or handle the exception using the error output from the context.

     * @param input input flowable
     * @return input flowable
     */
    private Flowable<O> printResult(Flowable<O> input, Function<Class<O>, Decorator<O, TerminalOutput>> decoratorProvider) {
        if (input.hasResult()) {
            // Workaround for the https://issues.apache.org/jira/browse/IGNITE-17346
            // This will turn the tailtips off before printing
            CommandLineContextProvider.print(() -> {
                String out = decoratorProvider.apply(input.type()).decorate(input.value()).toTerminalString();
                PrintWriter output = CommandLineContextProvider.getContext().out();
                output.println(out);
            });
        } else if (input.hasError()) {
            PrintWriter errOutput = CommandLineContextProvider.getContext().err();
            exceptionHandlers.handleException(ExceptionWriter.fromPrintWriter(errOutput), input.errorCause());
        }
        return input;
    }
}
