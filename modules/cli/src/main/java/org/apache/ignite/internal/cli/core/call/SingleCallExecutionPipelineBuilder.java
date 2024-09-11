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
import java.util.function.Supplier;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.internal.cli.core.exception.handler.DefaultExceptionHandlers;
import org.apache.ignite.internal.cli.decorators.DefaultDecorator;

/** Builder for {@link CallExecutionPipeline}. */
public class SingleCallExecutionPipelineBuilder<I extends CallInput, T> implements CallExecutionPipelineBuilder<I, T> {

    private final Call<I, T> call;

    private final ExceptionHandlers exceptionHandlers = new DefaultExceptionHandlers();

    private Supplier<I> inputProvider;

    private PrintWriter output = wrapOutputStream(System.out);

    private PrintWriter errOutput = wrapOutputStream(System.err);

    private Decorator<T, TerminalOutput> decorator = new DefaultDecorator<>();

    private boolean verbose;

    SingleCallExecutionPipelineBuilder(Call<I, T> call) {
        this.call = call;
    }

    private static PrintWriter wrapOutputStream(OutputStream output) {
        return new PrintWriter(output, true, getStdoutEncoding());
    }

    private static Charset getStdoutEncoding() {
        String encoding = System.getProperty("sun.stdout.encoding");
        return encoding != null ? Charset.forName(encoding) : Charset.defaultCharset();
    }

    public SingleCallExecutionPipelineBuilder<I, T> inputProvider(Supplier<I> inputProvider) {
        this.inputProvider = inputProvider;
        return this;
    }

    @Override
    public SingleCallExecutionPipelineBuilder<I, T> output(PrintWriter output) {
        this.output = output;
        return this;
    }

    public SingleCallExecutionPipelineBuilder<I, T> output(OutputStream output) {
        return output(wrapOutputStream(output));
    }

    @Override
    public SingleCallExecutionPipelineBuilder<I, T> errOutput(PrintWriter errOutput) {
        this.errOutput = errOutput;
        return this;
    }

    public SingleCallExecutionPipelineBuilder<I, T> errOutput(OutputStream output) {
        return errOutput(wrapOutputStream(output));
    }

    public SingleCallExecutionPipelineBuilder<I, T> exceptionHandler(ExceptionHandler<?> exceptionHandler) {
        exceptionHandlers.addExceptionHandler(exceptionHandler);
        return this;
    }

    public SingleCallExecutionPipelineBuilder<I, T> exceptionHandlers(ExceptionHandlers exceptionHandlers) {
        this.exceptionHandlers.addExceptionHandlers(exceptionHandlers);
        return this;
    }

    public SingleCallExecutionPipelineBuilder<I, T> decorator(Decorator<T, TerminalOutput> decorator) {
        this.decorator = decorator;
        return this;
    }

    @Override
    public SingleCallExecutionPipelineBuilder<I, T> verbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    @Override
    public CallExecutionPipeline<I, T> build() {
        return new SingleCallExecutionPipeline<>(call, output, errOutput, exceptionHandlers, decorator, inputProvider, verbose);
    }
}
