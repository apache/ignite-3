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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.stream.Collectors;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.cli.core.style.component.ErrorComponent;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.Problem;

/**
 * Exception handler for {@link IgniteCliApiException}.
 */
public class IgniteCliApiExceptionHandler implements ExceptionHandler<IgniteCliApiException> {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteCliApiExceptionHandler.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public int handle(ExceptionWriter err, IgniteCliApiException e) {
        ErrorComponent.ErrorComponentBuilder errorComponentBuilder = ErrorComponent.builder();

        if (e.getCause() instanceof ApiException) {
            ApiException cause = (ApiException) e.getCause();
            Throwable apiCause = cause.getCause();
            if (apiCause instanceof UnknownHostException) {
                errorComponentBuilder
                        .header("Unknown host: " + e.getUrl());
            } else if (apiCause instanceof ConnectException) {
                errorComponentBuilder
                        .header("Node unavailable")
                        .details("Could not connect to node with URL " + e.getUrl());
            } else if (apiCause != null) {
                errorComponentBuilder
                        .header(apiCause.getMessage());
            } else {
                try {
                    Problem problem = objectMapper.readValue(cause.getResponseBody(), Problem.class);
                    if (!problem.getInvalidParams().isEmpty()) {
                        errorComponentBuilder.details(problem.getInvalidParams().stream()
                                .map(invalidParam -> "" + invalidParam.getName() + ": " + invalidParam.getReason())
                                .collect(Collectors.joining(System.lineSeparator())));
                    }
                    errorComponentBuilder
                            .header(problem.getDetail())
                            .errorCode(problem.getCode())
                            .traceId(problem.getTraceId());
                } catch (JsonProcessingException ex) {
                    throw new RuntimeException(ex);
                }
            }
        } else {
            errorComponentBuilder.header(e.getCause() != e ? e.getCause().getMessage() : e.getMessage());
        }

        ErrorComponent errorComponent = errorComponentBuilder.build();

        LOG.error(errorComponent.header(), e);

        err.write(errorComponent.render());

        return 1;
    }

    @Override
    public Class<IgniteCliApiException> applicableException() {
        return IgniteCliApiException.class;
    }
}
