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

import java.io.PrintWriter;
import java.util.function.Supplier;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.DecoratorRegistry;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.decorators.DefaultDecoratorRegistry;
import org.apache.ignite.internal.cli.logger.CliLoggers;

/**
 * Abstract implementation of {@link CallExecutionPipeline}.
 * Implements common error handling logic and verbose output redirection.
 */
public abstract class AbstractCallExecutionPipeline<I extends CallInput, T> implements CallExecutionPipeline<I, T> {

    /** Writer for execution output. */
    protected final PrintWriter output;

    /** Writer for error execution output. */
    protected final PrintWriter errOutput;

    /** Decorator that decorates call's output. */
    protected final Decorator<T, TerminalOutput> decorator;

    private final DecoratorRegistry decoratorRegistry = new DefaultDecoratorRegistry();

    /** Handlers for any exceptions. */
    protected final ExceptionHandlers exceptionHandlers;

    /** Provider for call's input. */
    protected final Supplier<I> inputProvider;

    /** If non-empty, debug output will be printed to console. */
    protected final boolean[] verbose;

    AbstractCallExecutionPipeline(
            PrintWriter output,
            PrintWriter errOutput,
            ExceptionHandlers exceptionHandlers,
            Decorator<T, TerminalOutput> decorator,
            Supplier<I> inputProvider,
            boolean[] verbose
    ) {
        this.output = output;
        this.exceptionHandlers = exceptionHandlers;
        this.errOutput = errOutput;
        this.decorator = decorator;
        this.inputProvider = inputProvider;
        this.verbose = verbose;
    }

    /**
     * Runs the pipeline.
     *
     * @return exit code.
     */
    @Override
    public int runPipeline() {
        try {
            if (verbose.length > 0) {
                CliLoggers.startOutputRedirect(errOutput, verbose);
            }
            return runPipelineInternal();
        } finally {
            if (verbose.length > 0) {
                CliLoggers.stopOutputRedirect();
            }
        }
    }

    int handleResult(CallOutput<T> callOutput) {
        if (callOutput.hasError()) {
            return handleException(callOutput.errorCause());
        }

        if (!callOutput.isEmpty()) {
            TerminalOutput decoratedOutput = decorate(callOutput.body());
            output.println(decoratedOutput.toTerminalString());
        }

        return 0;
    }

    private TerminalOutput decorate(T body) {
        return selectDecorator(body).decorate(body);
    }

    private Decorator<T, TerminalOutput> selectDecorator(T body) {
        if (decorator != null) {
            return decorator;
        }
        return decoratorRegistry.getDecorator((Class<T>) body.getClass());
    }

    /**
     * Runs the pipeline and blocks the thread until the result is ready.
     */
    protected abstract int runPipelineInternal();

    int handleException(Throwable error) {
        return exceptionHandlers.handleException(ExceptionWriter.fromPrintWriter(errOutput), error);
    }
}
