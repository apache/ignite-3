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

package org.apache.ignite.internal.cli.core.exception;

import java.util.HashMap;
import java.util.Map;

/**
 * Exception handlers collector class.
 */
public class ExceptionHandlers {
    private final Map<Class<? extends Throwable>, ExceptionHandler<? extends Throwable>> map = new HashMap<>();
    private final ExceptionHandler<Throwable> defaultHandler;

    public ExceptionHandlers() {
        this(ExceptionHandler.DEFAULT);
    }

    public ExceptionHandlers(ExceptionHandler<Throwable> defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    /**
     * Appends exception handler to collection.
     * If collection already contains handler for type {@param <T>} it will be replaced by {@param exceptionHandler}.
     *
     * @param exceptionHandler handler instance.
     * @param <T> exception type.
     */
    public <T extends Throwable> void addExceptionHandler(ExceptionHandler<T> exceptionHandler) {
        map.put(exceptionHandler.applicableException(), exceptionHandler);
    }

    /**
     * Append all exception handlers.
     *
     * @param exceptionHandlers handlers to append.
     */
    public void addExceptionHandlers(ExceptionHandlers exceptionHandlers) {
        map.putAll(exceptionHandlers.map);
    }

    /**
     * Handles an exception.
     *
     * @param errOutput error output.
     * @param e exception instance.
     * @param <T> exception type.
     * @return exit code.
     */
    public <T extends Throwable> int handleException(ExceptionWriter errOutput, T e) {
        return processException(errOutput, e instanceof WrappedException ? e.getCause() : e);
    }

    /**
     * Handles an exception.
     *
     * @param e exception instance.
     * @param <T> exception type.
     * @return exit code.
     */
    public <T extends Throwable> int handleException(T e) {
        return processException(ExceptionWriter.nullWriter(), e instanceof WrappedException ? e.getCause() : e);
    }

    @SuppressWarnings("unchecked")
    private <T extends Throwable> int processException(ExceptionWriter errOutput, T e) {
        ExceptionHandler<T> exceptionHandler = (ExceptionHandler<T>) map.get(e.getClass());
        if (exceptionHandler != null) {
            return exceptionHandler.handle(errOutput, e);
        } else {
            return defaultHandler.handle(errOutput, e);
        }
    }

}
