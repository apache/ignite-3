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

package org.apache.ignite.internal.cli.core.exception.handler;

import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;
import picocli.CommandLine;
import picocli.CommandLine.IExecutionExceptionHandler;
import picocli.CommandLine.ParseResult;

/**
 * Implementation of {@link IExecutionExceptionHandler} based on {@link ExceptionHandlers}.
 */
public class PicocliExecutionExceptionHandler implements IExecutionExceptionHandler {
    private final ExceptionHandlers exceptionHandlers;

    public PicocliExecutionExceptionHandler() {
        exceptionHandlers = new DefaultExceptionHandlers();
    }

    public PicocliExecutionExceptionHandler(ExceptionHandlers exceptionHandlers) {
        this.exceptionHandlers = new DefaultExceptionHandlers();
        this.exceptionHandlers.addExceptionHandlers(exceptionHandlers);
    }

    @Override
    public int handleExecutionException(Exception ex, CommandLine commandLine, ParseResult parseResult) {
        return exceptionHandlers.handleException(commandLine.getErr()::println, ex);
    }
}
