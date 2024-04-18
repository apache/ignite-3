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

package org.apache.ignite.internal.rest.exception.handler;

import static org.apache.ignite.lang.ErrorGroup.extractErrorCode;
import static org.apache.ignite.lang.ErrorGroups.Common.COMMON_ERR_GROUP;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.api.Problem.ProblemBuilder;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.ErrorGroup;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IgniteExceptionHandlerTest extends BaseIgniteAbstractTest {
    private HttpRequest<?> request;

    private IgniteExceptionHandler exceptionHandler;

    static Stream<Arguments> igniteExceptions() {
        UUID traceId = UUID.randomUUID();
        String humanReadableCode = ErrorGroup.ERR_PREFIX + COMMON_ERR_GROUP.name() + '-' + extractErrorCode(INTERNAL_ERR);

        var invalidParams = List.of(
                new InvalidParam("key1", "Some issue1"),
                new InvalidParam("key2", "Some issue2"));

        var validationIssues = List.of(
                new ValidationIssue("key1", "Some issue1"),
                new ValidationIssue("key2", "Some issue2"));

        return Stream.of(
                Arguments.of(
                        // given
                        new IgniteException(traceId, INTERNAL_ERR, "Ooops"),
                        // expected
                        Problem.builder()
                                .status(500)
                                .title("Internal Server Error")
                                .code(humanReadableCode)
                                .detail("Ooops")
                                .traceId(traceId)),
                Arguments.of(
                        // given
                        new IgniteException(traceId, INTERNAL_ERR),
                        // expected
                        Problem.builder()
                                .status(500)
                                .title("Internal Server Error")
                                .code(humanReadableCode)
                                .traceId(traceId)),
                Arguments.of(
                        // given
                        new IgniteException(traceId, INTERNAL_ERR, new IllegalArgumentException("Illegal value")),
                        // expected
                        Problem.builder()
                                .status(400)
                                .title("Bad Request")
                                .code(humanReadableCode)
                                .traceId(traceId)
                                .detail("Illegal value")),
                Arguments.of(
                        // given
                        new IgniteException(
                                traceId,
                                INTERNAL_ERR,
                                new ConfigurationValidationException(validationIssues)),
                        // expected
                        Problem.builder()
                                .status(400)
                                .title("Bad Request")
                                .detail("Validation did not pass for keys: [key1, Some issue1], [key2, Some issue2]")
                                .code(humanReadableCode)
                                .traceId(traceId)
                                .invalidParams(invalidParams))
        );
    }

    @BeforeEach
    void setUp() {
        exceptionHandler = new IgniteExceptionHandler();
        request = mock(HttpRequest.class);
    }

    @ParameterizedTest
    @MethodSource("igniteExceptions")
    void shouldHandleIgniteException(IgniteException givenIgniteException, ProblemBuilder<? extends Problem, ?> expectedProblem) {
        HttpResponse<? extends Problem> response = exceptionHandler.handle(request, givenIgniteException);

        Problem problem = response.body();
        assertEquals(expectedProblem.build(), problem);
    }
}
