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

import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.style.component.ErrorUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * This exception handler is used for cluster commands and handles a case when the cluster is not initialized.
 */
public class ClusterNotInitializedExceptionHandler extends IgniteCliApiExceptionHandler {
    private final String header;

    private final String command;

    /**
     * Constructs a new exception handler with corresponding header and command to suggest.
     *
     * @param header text to display as a header
     * @param command command to suggest
     */
    public ClusterNotInitializedExceptionHandler(String header, String command) {
        this.header = header;
        this.command = command;
    }

    @Override
    public int handle(ExceptionWriter err, IgniteCliApiException e) {
        if (e.getCause() instanceof ApiException) {
            ApiException apiException = (ApiException) e.getCause();
            if (apiException.getCode() == 409) { // CONFLICT means not initialized
                err.write(
                        ErrorUiComponent.builder()
                                .header(header)
                                .details("Probably, you have not initialized the cluster, try to run %s command",
                                        UiElements.command(command))
                                .verbose(apiException.getMessage())
                                .build()
                                .render()
                );
                return 1;
            }
        }
        return super.handle(err, e);
    }

    /**
     * Creates handler for Non-REPL command.
     *
     * @param message command-specific text like 'cannot list nodes'
     */
    public static ClusterNotInitializedExceptionHandler createHandler(String message) {
        return new ClusterNotInitializedExceptionHandler(message, "ignite cluster init");
    }

    /**
     * Creates handler for REPL command.
     *
     * @param message command-specific text like 'cannot list nodes'
     */
    public static ClusterNotInitializedExceptionHandler createReplHandler(String message) {
        return new ClusterNotInitializedExceptionHandler(message, "cluster init");
    }
}
