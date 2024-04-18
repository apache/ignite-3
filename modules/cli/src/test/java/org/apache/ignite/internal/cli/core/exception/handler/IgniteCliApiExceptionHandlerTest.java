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

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.micronaut.http.HttpStatus;
import java.util.Collections;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.junit.jupiter.api.Test;

class IgniteCliApiExceptionHandlerTest extends BaseIgniteAbstractTest {

    private static final String NODE_URL = "nodeUrl";

    private final IgniteCliApiExceptionHandler igniteCliApiExceptionHandler = new IgniteCliApiExceptionHandler();

    @Test
    void handleExceptionWithoutProblemJson() {
        // Given
        ExceptionWriter exceptionWriter = mock(ExceptionWriter.class);
        IgniteCliApiException igniteCliApiException =
                new IgniteCliApiException(new ApiException("Missing the required parameter"), NODE_URL);

        // When
        igniteCliApiExceptionHandler.handle(exceptionWriter, igniteCliApiException);

        // Then
        verify(exceptionWriter, times(1)).write(contains("Message: Missing the required parameter"));
    }

    @Test
    void handleExceptionWithProblem() {
        // Given
        ExceptionWriter exceptionWriter = mock(ExceptionWriter.class);
        String problemJson = "{\"title\":\"Bad Request\",\"status\":400,\"code\":\"IGN-ERR\",\"detail\":"
                + "\"User value 'test_name' has not been found\",\"traceId\":\"567c9a5c-4820-46c5-9994-ff3fd6339efe\"}";
        IgniteCliApiException igniteCliApiException =
                new IgniteCliApiException(
                        new ApiException(HttpStatus.BAD_REQUEST.getCode(), Collections.emptyMap(), problemJson), NODE_URL);

        // When
        igniteCliApiExceptionHandler.handle(exceptionWriter, igniteCliApiException);

        // Then
        verify(exceptionWriter, times(1)).write(
                eq("IGN-ERR Trace ID: 567c9a5c-4820-46c5-9994-ff3fd6339efe" + System.lineSeparator()
                + "User value 'test_name' has not been found"));
    }

    @Test
    void handleExceptionWith404() {
        // Given
        ExceptionWriter exceptionWriter = mock(ExceptionWriter.class);
        IgniteCliApiException igniteCliApiException =
                new IgniteCliApiException(new ApiException(HttpStatus.UNAUTHORIZED.getCode(), "UNAUTHORIZED"), NODE_URL);

        // When
        igniteCliApiExceptionHandler.handle(exceptionWriter, igniteCliApiException);

        // Then
        verify(exceptionWriter, times(1)).write(eq("Authentication error" + System.lineSeparator()
                + String.format("Could not connect to node with URL %s. ", NODE_URL)
                + "Check authentication configuration or provided username/password"));
    }
}
