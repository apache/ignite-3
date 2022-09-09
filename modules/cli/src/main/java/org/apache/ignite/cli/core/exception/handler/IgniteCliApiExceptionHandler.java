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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.cli.core.exception.ExceptionHandler;
import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.cli.core.style.component.ErrorUiComponent;
import org.apache.ignite.cli.core.style.component.ErrorUiComponent.ErrorComponentBuilder;
import org.apache.ignite.cli.core.style.element.UiElements;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.InvalidParam;
import org.apache.ignite.rest.client.model.Problem;
import org.jetbrains.annotations.NotNull;

/**
 * Exception handler for {@link IgniteCliApiException}.
 */
public class IgniteCliApiExceptionHandler implements ExceptionHandler<IgniteCliApiException> {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteCliApiExceptionHandler.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public int handle(ExceptionWriter err, IgniteCliApiException e) {
        ErrorUiComponent.ErrorComponentBuilder errorComponentBuilder = ErrorUiComponent.builder();

        if (e.getCause() instanceof ApiException) {
            ApiException cause = (ApiException) e.getCause();
            Throwable apiCause = cause.getCause();
            if (apiCause instanceof UnknownHostException) {
                errorComponentBuilder
                        .header("Unknown host: %s", UiElements.url(e.getUrl()));
            } else if (apiCause instanceof ConnectException) {
                errorComponentBuilder
                        .header("Node unavailable")
                        .details("Could not connect to node with URL %s", UiElements.url(e.getUrl()));
            } else if (apiCause != null) {
                errorComponentBuilder.header(apiCause.getMessage());
            } else {
                tryToExtractProblem(errorComponentBuilder, cause);
            }
        } else {
            errorComponentBuilder.header(e.getCause() != e ? e.getCause().getMessage() : e.getMessage());
        }

        ErrorUiComponent errorComponent = errorComponentBuilder.build();

        LOG.error(errorComponent.header(), e);

        err.write(errorComponent.render());

        return 1;
    }

    private static void tryToExtractProblem(ErrorComponentBuilder errorComponentBuilder, ApiException cause) {
        try {
            Problem problem = objectMapper.readValue(cause.getResponseBody(), Problem.class);
            List<InvalidParam> invalidParams = problem.getInvalidParams();
            if (invalidParams != null && !invalidParams.isEmpty()) {
                errorComponentBuilder.details(extractInvalidParams(invalidParams));
            }
            errorComponentBuilder
                    .header(problem.getDetail() != null ? problem.getDetail() : problem.getTitle())
                    .errorCode(problem.getCode())
                    .traceId(problem.getTraceId());
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    @NotNull
    private static String extractInvalidParams(List<InvalidParam> invalidParams) {
        return invalidParams.stream()
                .map(invalidParam -> "" + invalidParam.getName() + ": " + invalidParam.getReason())
                .collect(Collectors.joining(System.lineSeparator()));
    }

    @Override
    public Class<IgniteCliApiException> applicableException() {
        return IgniteCliApiException.class;
    }
}
