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

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.DecoratorRegistry;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.internal.cli.core.flow.Flow;
import org.apache.ignite.internal.cli.core.flow.FlowInterruptException;
import org.apache.ignite.internal.cli.core.flow.Flowable;
import org.apache.ignite.internal.cli.core.flow.question.QuestionAnswer;
import org.apache.ignite.internal.cli.core.flow.question.QuestionAskerFactory;
import org.apache.ignite.internal.cli.core.repl.context.CommandLineContextProvider;
import org.apache.ignite.internal.cli.decorators.DefaultDecoratorRegistry;
import org.apache.ignite.internal.cli.logger.CliLoggers;

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
    private final Set<Consumer<O>> successHandlers = new HashSet<>();
    private final Set<Consumer<Throwable>> failureHandlers = new HashSet<>();
    private boolean verbose;

    FlowBuilderImpl(Flow<I, O> flow) {
        this(flow, new DefaultExceptionHandlers(), new DefaultDecoratorRegistry(), false);
    }

    /**
     * Constructor.
     *
     * @param flow flow instance.
     * @param exceptionHandlers exception handlers.
     * @param decoratorRegistry decorator registry.
     * @param verbose if @{code true}, flow execution will print debug logs
     */
    private FlowBuilderImpl(Flow<I, O> flow, ExceptionHandlers exceptionHandlers, DecoratorRegistry decoratorRegistry, boolean verbose) {
        this.flow = flow;
        this.exceptionHandlers = exceptionHandlers;
        this.decoratorRegistry = decoratorRegistry;
        this.verbose = verbose;
    }

    @Override
    public <OT> FlowBuilder<I, OT> then(Flow<O, OT> flow) {
        Flow<I, OT> composite = input -> {
            Flowable<O> outputFlowable = this.flow.start(input);
            if (outputFlowable.hasResult()) {
                O result = outputFlowable.value();
                successHandlers.forEach(handler -> handler.accept(result));
            } else if (outputFlowable.hasError()) {
                Throwable error = outputFlowable.errorCause();
                failureHandlers.forEach(handler -> handler.accept(error));
            }
            return flow.start(outputFlowable);
        };
        return new FlowBuilderImpl<>(composite, exceptionHandlers, decoratorRegistry, verbose);
    }

    @Override
    public <OT> FlowBuilder<I, OT> flatMap(Function<O, FlowBuilder<O, OT>> mapper) {
        return then(it -> mapper.apply(it.value()).build().start(it));
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
    public FlowBuilder<I, O> onSuccess(Consumer<O> handler) {
        successHandlers.add(handler);
        return this;
    }

    @Override
    public FlowBuilder<I, O> onFailure(Consumer<Throwable> handler) {
        failureHandlers.add(handler);
        return this;
    }

    @Override
    public FlowBuilder<I, O> verbose(boolean verbose) {
        this.verbose = verbose;
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
        try {
            run(Flowable.empty());
        } catch (FlowInterruptException ignored) {
            // FlowInterruptException is an internal exception and shouldn't be exposed to users
        }
    }

    /**
     * Flow method which starts current flow and returns its result or empty output if flow is interrupted.
     *
     * @param input input flowable
     * @return output flowable
     */
    private Flowable<O> run(Flowable<I> input) {
        try {
            if (verbose) {
                CliLoggers.startOutputRedirect(CommandLineContextProvider.getContext().err());
            }
            return flow.start(input);
        } finally {
            if (verbose) {
                CliLoggers.stopOutputRedirect();
            }
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
            // Workaround for the scroll truncation issue in windows terminal
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
