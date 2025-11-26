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
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;

/**
 * Call execution pipeline that executes a single call.
 *
 * @param <I> Call input type.
 * @param <T> Call output's body type.
 */
public class SingleCallExecutionPipeline<I extends CallInput, T> extends AbstractCallExecutionPipeline<I, T> {
    /** Call to execute. */
    private final Call<I, T> call;

    SingleCallExecutionPipeline(
            Call<I, T> call,
            PrintWriter output,
            PrintWriter errOutput,
            ExceptionHandlers exceptionHandlers,
            Decorator<T, TerminalOutput> decorator,
            Supplier<I> inputProvider,
            boolean[] verbose
    ) {
        super(output, errOutput, exceptionHandlers, decorator, inputProvider, verbose);
        this.call = call;
    }

    @Override
    public int runPipelineInternal() {
        I callInput = inputProvider.get();

        CallOutput<T> callOutput = call.execute(callInput);

        return handleResult(callOutput);
    }
}
