/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.core.exception.handler;

import java.net.ConnectException;
import java.net.UnknownHostException;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Exception handler for {@link IgniteCliApiException}.
 */
public class IgniteCliApiExceptionHandler implements ExceptionHandler<IgniteCliApiException> {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteCliApiExceptionHandler.class);

    @Override
    public int handle(ExceptionWriter err, IgniteCliApiException e) {
        String message;

        if (e.getCause() instanceof ApiException) {
            ApiException cause = (ApiException) e.getCause();
            Throwable apiCause = cause.getCause();
            if (apiCause instanceof UnknownHostException) {
                message = "Could not determine IP address when connecting to URL [url=" + e.getUrl() + ']';
            } else if (apiCause instanceof ConnectException) {
                message = "Could not connect to URL [url=" + e.getUrl() + ']';
            } else if (apiCause != null) {
                message = apiCause.getMessage();
            } else {
                message = "An error occurred [errorCode=" + cause.getCode() + ", response=" + cause.getResponseBody() + ']';
            }
        } else {
            message = e.getCause() != e ? e.getCause().getMessage() : e.getMessage();
        }

        LOG.error(message, e);

        err.write(message);

        return 1;
    }

    @Override
    public Class<IgniteCliApiException> applicableException() {
        return IgniteCliApiException.class;
    }
}
