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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.junit.jupiter.api.Test;

class IgniteCliApiExceptionHandlerTest {

    @Test
    void handleExceptionWithoutProblemJson() {
        // Given
        IgniteCliApiExceptionHandler igniteCliApiExceptionHandler = new IgniteCliApiExceptionHandler();

        ExceptionWriter exceptionWriter = mock(ExceptionWriter.class);
        IgniteCliApiException igniteCliApiException =
                new IgniteCliApiException(new ApiException("Missing the required parameter"), "nodeUrl");

        // When
        igniteCliApiExceptionHandler.handle(exceptionWriter, igniteCliApiException);

        // Then
        verify(exceptionWriter, times(1)).write(eq("Missing the required parameter"));
    }
}