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

import org.apache.ignite.internal.cli.core.style.component.ErrorUiComponent;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;

/**
 * General interface of exception handler.
 *
 * @param <T> exception type.
 */
public interface ExceptionHandler<T extends Throwable> {
    IgniteLogger LOG = CliLoggers.forClass(ExceptionHandler.class);

    ExceptionHandler<Throwable> DEFAULT = new ExceptionHandler<>() {
        @Override
        public int handle(ExceptionWriter err, Throwable e) {
            LOG.error("Unhandled exception", e);
            err.write(
                    ErrorUiComponent.builder()
                            .header("Unknown error")
                            .details(e.getMessage())
                            .build()
                            .render()
            );

            return 1;
        }

        @Override
        public Class<Throwable> applicableException() {
            return Throwable.class;
        }
    };

    /**
     * Handles an exception.
     *
     * @param err writer instance for any error messages.
     * @param e exception instance.
     * @return exit code.
     */
    int handle(ExceptionWriter err, T e);

    /**
     * Exception class getter.
     *
     * @return class of applicable exception for the handler.
     */
    Class<T> applicableException();
}
