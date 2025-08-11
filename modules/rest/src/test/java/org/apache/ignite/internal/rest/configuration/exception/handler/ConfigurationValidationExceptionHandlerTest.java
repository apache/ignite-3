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

package org.apache.ignite.internal.rest.configuration.exception.handler;

import static org.apache.ignite.lang.ErrorGroup.extractErrorCode;
import static org.apache.ignite.lang.ErrorGroups.CommonConfiguration.COMMON_CONF_ERR_GROUP;
import static org.apache.ignite.lang.ErrorGroups.CommonConfiguration.CONFIGURATION_VALIDATION_ERR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.exception.ConfigurationValidationIgniteException;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.ErrorGroups;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test suite for {@link ConfigurationValidationExceptionHandler}. */
public class ConfigurationValidationExceptionHandlerTest extends BaseIgniteAbstractTest {
    private HttpRequest<?> request;

    private ConfigurationValidationExceptionHandler exceptionHandler;

    static Stream<Arguments> args() {
        UUID traceId = UUID.randomUUID();
        String humanReadableCode = ErrorGroups.IGNITE_ERR_PREFIX + "-"
                + COMMON_CONF_ERR_GROUP.name() + '-'
                + Short.toUnsignedInt(extractErrorCode(CONFIGURATION_VALIDATION_ERR));

        var invalidParams = List.of(
                new InvalidParam("key1", "Some issue1"),
                new InvalidParam("key2", "Some issue2"));
        var validationIssues = List.of(
                new ValidationIssue("key1", "Some issue1"),
                new ValidationIssue("key2", "Some issue2"));

        return Stream.of(
                Arguments.of(
                        // given
                        new ConfigurationValidationIgniteException(traceId, new ConfigurationValidationException(validationIssues)),
                        // expected
                        Problem.builder()
                                .status(400)
                                .title("Bad Request")
                                .detail("Validation did not pass for keys: [key1, Some issue1], [key2, Some issue2]")
                                .code(humanReadableCode)
                                .traceId(traceId)
                                .invalidParams(invalidParams).build()
                )
        );
    }

    @BeforeEach
    void setUp() {
        exceptionHandler = new ConfigurationValidationExceptionHandler();
        request = mock(HttpRequest.class);
    }

    @ParameterizedTest
    @MethodSource("args")
    public void handleValidationException(ConfigurationValidationIgniteException givenIgniteException, Problem expectedProblem) {
        HttpResponse<? extends Problem> response = exceptionHandler.handle(request, givenIgniteException);

        Problem problem = response.body();
        assertEquals(expectedProblem, problem);
    }
}
