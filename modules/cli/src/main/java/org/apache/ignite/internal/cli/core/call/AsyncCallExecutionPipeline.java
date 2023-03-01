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
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;
import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;

/** Call execution pipeline that executes an async call and displays progress bar. */
public class AsyncCallExecutionPipeline<I extends CallInput, T> extends AbstractCallExecutionPipeline<I, T> {
    /** Async call factory. */
    private final Function<ProgressTracker, AsyncCall<I, T>> callFactory;

    /** Builder for progress bar rendering. */
    private final ProgressBarBuilder progressBarBuilder;

    AsyncCallExecutionPipeline(
            Function<ProgressTracker, AsyncCall<I, T>> callFactory,
            ProgressBarBuilder progressBarBuilder,
            PrintWriter output,
            PrintWriter errOutput,
            ExceptionHandlers exceptionHandlers,
            Decorator<T, TerminalOutput> decorator,
            Supplier<I> inputProvider,
            boolean verbose
    ) {
        super(output, errOutput, exceptionHandlers, decorator, inputProvider, verbose);
        this.callFactory = callFactory;
        this.progressBarBuilder = progressBarBuilder;
    }

    @Override
    public int runPipelineInternal() {
        I callInput = inputProvider.get();

        progressBarBuilder.setConsumer(new DelegatingProgressBarConsumer(this::print));
        ProgressBar progressBar = progressBarBuilder.build();

        try {
            CallOutput<T> result = callFactory.apply(progressBar::step)
                    .execute(callInput)
                    .whenComplete((el, err) -> progressBar.close())
                    .join();

            // move carriage to the next line
            output.println();

            return handleResult(result);
        } catch (CompletionException e) {
            return handleException(e.getCause());
        } catch (Exception e) {
            return handleException(e);
        }
    }

    private void print(String s) {
        output.print("\r" + s); // carriage return to the beginning of the line to overwrite the previous progress bar
        output.flush();
    }
}
