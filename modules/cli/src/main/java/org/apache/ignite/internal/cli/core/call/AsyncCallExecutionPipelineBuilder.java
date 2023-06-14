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

package org.apache.ignite.internal.cli.core.call;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.internal.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.internal.cli.decorators.DefaultDecorator;

/** Builder for {@link AsyncCallExecutionPipeline}. */
public class AsyncCallExecutionPipelineBuilder<I extends CallInput, T> {

    private final Function<ProgressTracker, AsyncCall<I, T>> callFactory;

    private final ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder()
            .setStyle(ProgressBarStyle.UNICODE_BLOCK)
            .continuousUpdate()
            .setSpeedUnit(ChronoUnit.SECONDS)
            .setInitialMax(100)
            .hideETA()
            .setTaskName("")
            .showSpeed();

    private final ExceptionHandlers exceptionHandlers = new DefaultExceptionHandlers();

    private Supplier<I> inputProvider;

    private PrintWriter output = wrapOutputStream(System.out);

    private PrintWriter errOutput = wrapOutputStream(System.err);

    private Decorator<T, TerminalOutput> decorator = new DefaultDecorator<>();

    private boolean verbose;

    AsyncCallExecutionPipelineBuilder(Function<ProgressTracker, AsyncCall<I, T>> callFactory) {
        this.callFactory = callFactory;
    }

    private static PrintWriter wrapOutputStream(OutputStream output) {
        return new PrintWriter(output, true, getStdoutEncoding());
    }

    private static Charset getStdoutEncoding() {
        String encoding = System.getProperty("sun.stdout.encoding");
        return encoding != null ? Charset.forName(encoding) : Charset.defaultCharset();
    }

    public AsyncCallExecutionPipelineBuilder<I, T> inputProvider(Supplier<I> inputProvider) {
        this.inputProvider = inputProvider;
        return this;
    }

    public AsyncCallExecutionPipelineBuilder<I, T> output(PrintWriter output) {
        this.output = output;
        return this;
    }

    public AsyncCallExecutionPipelineBuilder<I, T> output(OutputStream output) {
        return output(wrapOutputStream(output));
    }

    public AsyncCallExecutionPipelineBuilder<I, T> errOutput(PrintWriter errOutput) {
        this.errOutput = errOutput;
        return this;
    }

    public AsyncCallExecutionPipelineBuilder<I, T> errOutput(OutputStream output) {
        return errOutput(wrapOutputStream(output));
    }

    public AsyncCallExecutionPipelineBuilder<I, T> exceptionHandler(ExceptionHandler<?> exceptionHandler) {
        exceptionHandlers.addExceptionHandler(exceptionHandler);
        return this;
    }

    public AsyncCallExecutionPipelineBuilder<I, T> exceptionHandlers(ExceptionHandlers exceptionHandlers) {
        this.exceptionHandlers.addExceptionHandlers(exceptionHandlers);
        return this;
    }

    public AsyncCallExecutionPipelineBuilder<I, T> decorator(Decorator<T, TerminalOutput> decorator) {
        this.decorator = decorator;
        return this;
    }

    public AsyncCallExecutionPipelineBuilder<I, T> verbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public AsyncCallExecutionPipelineBuilder<I, T> name(String name) {
        this.progressBarBuilder.setTaskName(name);
        return this;
    }

    /** Builds {@link AsyncCallExecutionPipeline}. */
    public CallExecutionPipeline<I, T> build() {
        return new AsyncCallExecutionPipeline<>(
                callFactory, progressBarBuilder, output, errOutput, exceptionHandlers, decorator, inputProvider, verbose
        );
    }
}
