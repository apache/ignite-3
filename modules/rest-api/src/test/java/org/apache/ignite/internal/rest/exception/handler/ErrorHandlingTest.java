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

import static org.apache.ignite.internal.rest.constants.HttpCode.BAD_REQUEST;
import static org.apache.ignite.internal.rest.constants.HttpCode.METHOD_NOT_ALLOWED;
import static org.apache.ignite.internal.rest.constants.HttpCode.NOT_FOUND;
import static org.apache.ignite.internal.rest.constants.HttpCode.UNSUPPORTED_MEDIA_TYPE;
import static org.apache.ignite.internal.rest.problem.ProblemJsonMediaType.APPLICATION_JSON_PROBLEM_TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.bind.exceptions.UnsatisfiedArgumentException;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.uri.UriBuilder;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthorizationException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.rest.constants.MediaType;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Error handling tests.
 */
@MicronautTest
@Property(name = "micronaut.security.enabled", value = "false")
@Property(name = "ignite.endpoints.filter-non-initialized", value = "false")
public class ErrorHandlingTest {
    @Inject
    @Client("/test")
    HttpClient client;

    private final AtomicReference<Throwable> throwable = new AtomicReference<>(new RuntimeException());

    private static Stream<Arguments> testExceptions() {
        return Stream.of(
                // couldn't find a case when exception is thrown
                Arguments.of(new UnsatisfiedArgumentException(Argument.DOUBLE)),
                // thrown when request uri is invalid, but it's not possible to create such request with HttpClient (it validates uri)
                Arguments.of(new URISyntaxException("uri", "reason")),
                Arguments.of(new AuthenticationException("authentication-exception")),
                Arguments.of(new AuthorizationException(null)),
                Arguments.of(new IgniteException("ignite-exception")),
                Arguments.of(new IgniteInternalCheckedException("ignite-internal-exception")),
                Arguments.of(new IgniteInternalException("ignite-internal-exception")),
                Arguments.of(new RuntimeException("runtime-exception")),
                Arguments.of(new Exception("exception"))
        );
    }

    @ParameterizedTest
    @MethodSource("testExceptions")
    public void testExceptions(Throwable throwable) {
        this.throwable.set(throwable);

        // Invoke endpoint with not allowed method
        HttpClientResponseException thrown = Assertions.assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/test/throw-exception")
        );

        HttpResponse<?> response = thrown.getResponse();
        Problem problem = response.getBody(Problem.class).get();

        assertEquals(APPLICATION_JSON_PROBLEM_TYPE.getType(), response.getContentType().get().getType());
        assertEquals(response.code(), problem.status());
        assertNotNull(problem.title());
    }

    @Test
    public void endpoint404() {
        // Invoke non-existing endpoint
        HttpClientResponseException thrown = Assertions.assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/endpoint404")
        );

        HttpResponse<?> response = thrown.getResponse();
        Problem problem = response.getBody(Problem.class).get();

        assertEquals(NOT_FOUND.code(), response.status().getCode());
        assertEquals(APPLICATION_JSON_PROBLEM_TYPE.getType(), response.getContentType().get().getType());

        assertEquals(NOT_FOUND.code(), problem.status());
        assertEquals("Not Found", problem.title());
        assertEquals("Requested resource not found: /test/endpoint404", problem.detail());
    }

    @Test
    public void invalidDataTypePathVariable() {
        // Invoke endpoint with wrong path variable data type
        HttpClientResponseException thrown = Assertions.assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/list/abc")
        );

        HttpResponse<?> response = thrown.getResponse();
        Problem problem = response.getBody(Problem.class).get();

        assertEquals(BAD_REQUEST.code(), response.status().getCode());
        assertEquals(APPLICATION_JSON_PROBLEM_TYPE.getType(), response.getContentType().get().getType());

        assertEquals(BAD_REQUEST.code(), problem.status());
        assertEquals("Invalid parameter", problem.title());
        assertEquals("Failed to convert argument [id] for value [abc] due to: For input string: \"abc\"", problem.detail());
    }

    @Test
    public void requiredQueryValueNotSpecified() {
        // Invoke endpoint without required query value
        HttpClientResponseException thrown = Assertions.assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/list")
        );

        HttpResponse<?> response = thrown.getResponse();
        Problem problem = response.getBody(Problem.class).get();

        assertEquals(BAD_REQUEST.code(), response.status().getCode());
        assertEquals(APPLICATION_JSON_PROBLEM_TYPE.getType(), response.getContentType().get().getType());

        assertEquals(BAD_REQUEST.code(), problem.status());
        assertEquals("Bad Request", problem.title());
        assertEquals("Required QueryValue [greatThan] not specified", problem.detail());
    }

    @Test
    public void invalidTypeQueryValue() {
        // Invoke endpoint with wrong data type of request argument
        HttpClientResponseException thrown = Assertions.assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/list?greatThan=abc")
        );

        HttpResponse<?> response = thrown.getResponse();
        Problem problem = response.getBody(Problem.class).get();

        assertEquals(BAD_REQUEST.code(), response.status().getCode());
        assertEquals(APPLICATION_JSON_PROBLEM_TYPE.getType(), response.getContentType().get().getType());

        assertEquals(BAD_REQUEST.code(), problem.status());
        assertEquals("Invalid parameter", problem.title());
        assertEquals("Failed to convert argument [greatThan] for value [abc] due to: For input string: \"abc\"", problem.detail());
    }

    @Test
    public void invalidTypeQueryValue1() {
        // Invoke endpoint with wrong request argument values
        HttpClientResponseException thrown = Assertions.assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange("/list?greatThan=-1&lessThan=11")
        );

        HttpResponse<?> response = thrown.getResponse();
        Problem problem = response.getBody(Problem.class).get();

        assertEquals(BAD_REQUEST.code(), response.status().getCode());
        assertEquals(APPLICATION_JSON_PROBLEM_TYPE.getType(), response.getContentType().get().getType());

        assertEquals(BAD_REQUEST.code(), problem.status());
        assertEquals("Bad Request", problem.title());
        assertEquals("Validation failed", problem.detail());

        assertEquals(2, problem.invalidParams().size());

        assertThat(problem.invalidParams(), containsInAnyOrder(
                new InvalidParam("list.greatThan", "greatThan: must be greater than or equal to 0"),
                new InvalidParam("list.lessThan", "lessThan: must be less than or equal to 10")
        ));
    }

    @Test
    public void postWithInvalidMediaType() {
        // Invoke endpoint with invalid media type
        MutableHttpRequest<String> request = HttpRequest.POST(UriBuilder.of("/echo").build(), EchoMessage.class)
                .contentType(MediaType.TEXT_PLAIN)
                .body("text='qwe'");

        HttpClientResponseException thrown = Assertions.assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(request, Argument.of(EchoMessage.class), Argument.of(Problem.class))
        );

        HttpResponse<?> response = thrown.getResponse();
        Problem problem = response.getBody(Problem.class).get();

        assertEquals(UNSUPPORTED_MEDIA_TYPE.code(), response.status().getCode());
        assertEquals(APPLICATION_JSON_PROBLEM_TYPE.getType(), response.getContentType().get().getType());

        assertEquals(UNSUPPORTED_MEDIA_TYPE.code(), problem.status());
        assertEquals("Unsupported Media Type", problem.title());
        assertEquals("Unsupported media type: text", problem.detail());
    }

    @Test
    public void postWithInvalidJson() {
        // Invoke endpoint with invalid json
        MutableHttpRequest<String> request = HttpRequest.POST(UriBuilder.of("/echo").build(), EchoMessage.class)
                .contentType(MediaType.APPLICATION_JSON)
                .body("{text='qwe'");

        HttpClientResponseException thrown = Assertions.assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(request, Argument.of(EchoMessage.class), Argument.of(Problem.class))
        );

        HttpResponse<?> response = thrown.getResponse();
        Problem problem = response.getBody(Problem.class).get();

        assertEquals(BAD_REQUEST.code(), response.status().getCode());
        assertEquals(APPLICATION_JSON_PROBLEM_TYPE.getType(), response.getContentType().get().getType());

        assertEquals(BAD_REQUEST.code(), problem.status());
        assertEquals("Invalid JSON", problem.title());
        assertThat(problem.detail(), containsString("Unexpected character"));
    }

    @Test
    public void postWithMissingBody() {
        // Invoke endpoint with invalid json
        MutableHttpRequest<String> request = HttpRequest.POST(UriBuilder.of("/echo").build(), EchoMessage.class)
                .contentType(MediaType.APPLICATION_JSON)
                .body("");

        HttpClientResponseException thrown = Assertions.assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(request, Argument.of(EchoMessage.class), Argument.of(Problem.class))
        );

        HttpResponse<?> response = thrown.getResponse();
        Problem problem = response.getBody(Problem.class).get();

        assertEquals(BAD_REQUEST.code(), response.status().getCode());
        assertEquals(APPLICATION_JSON_PROBLEM_TYPE.getType(), response.getContentType().get().getType());

        assertEquals(BAD_REQUEST.code(), problem.status());
        assertEquals("Bad Request", problem.title());
        assertThat(problem.detail(), containsString("Required Body [dto] not specified"));
    }

    @Test
    public void methodNotAllowed() {
        // Invoke endpoint with not allowed method
        MutableHttpRequest<String> request = HttpRequest.GET(UriBuilder.of("/echo").build());

        HttpClientResponseException thrown = Assertions.assertThrows(
                HttpClientResponseException.class,
                () -> client.toBlocking().exchange(request, Argument.of(EchoMessage.class), Argument.of(Problem.class))
        );

        HttpResponse<?> response = thrown.getResponse();
        Problem problem = response.getBody(Problem.class).get();

        assertEquals(METHOD_NOT_ALLOWED.code(), response.status().getCode());
        assertEquals(APPLICATION_JSON_PROBLEM_TYPE.getType(), response.getContentType().get().getType());

        assertEquals(METHOD_NOT_ALLOWED.code(), problem.status());
        assertEquals("Method Not Allowed", problem.title());
        assertEquals("Method not allowed: GET", problem.detail());
    }

    @Bean
    @Factory
    public ThrowableProvider exceptionThrowingService() {
        return throwable::get;
    }
}
