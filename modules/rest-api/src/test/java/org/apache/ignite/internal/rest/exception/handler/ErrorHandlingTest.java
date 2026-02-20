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

import static io.micronaut.http.HttpStatus.BAD_REQUEST;
import static io.micronaut.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static io.micronaut.http.HttpStatus.METHOD_NOT_ALLOWED;
import static io.micronaut.http.HttpStatus.NOT_FOUND;
import static io.micronaut.http.HttpStatus.UNAUTHORIZED;
import static io.micronaut.http.HttpStatus.UNSUPPORTED_MEDIA_TYPE;
import static org.apache.ignite.internal.rest.matcher.MicronautHttpResponseMatcher.assertThrowsProblem;
import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.bind.exceptions.UnsatisfiedArgumentException;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Error handling tests.
 */
@MicronautTest
@Property(name = "micronaut.security.enabled", value = "false")
@Property(name = "ignite.endpoints.rest-events", value = "false")
@Property(name = "ignite.endpoints.filter-non-initialized", value = "false")
public class ErrorHandlingTest {
    @Inject
    @Client("/test")
    HttpClient client;

    private static final AtomicReference<Throwable> throwable = new AtomicReference<>(new RuntimeException());

    private static Stream<Arguments> testExceptions() {
        return Stream.of(
                // couldn't find a case when exception is thrown
                arguments(new UnsatisfiedArgumentException(Argument.DOUBLE), BAD_REQUEST,
                        "Bad Request", "Required argument [double] not specified"),
                // thrown when request uri is invalid, but it's not possible to create such request with HttpClient (it validates uri)
                arguments(new URISyntaxException("uri", "reason"), BAD_REQUEST, "Malformed URI", "reason: uri"),
                arguments(new AuthenticationException("authentication-exception"), UNAUTHORIZED,
                        "Unauthorized", "authentication-exception"),
                arguments(new AuthorizationException(null), UNAUTHORIZED, "Unauthorized", null),
                arguments(new IgniteException(INTERNAL_ERR, "ignite-exception"), INTERNAL_SERVER_ERROR,
                        "Internal Server Error", "ignite-exception"),
                arguments(new IgniteInternalCheckedException(INTERNAL_ERR, "ignite-internal-exception"), INTERNAL_SERVER_ERROR,
                        "Internal Server Error", "ignite-internal-exception"),
                arguments(new IgniteInternalException(INTERNAL_ERR, "ignite-internal-exception"), INTERNAL_SERVER_ERROR,
                        "Internal Server Error", "ignite-internal-exception"),
                arguments(new RuntimeException("runtime-exception"), INTERNAL_SERVER_ERROR, "Internal Server Error", "runtime-exception"),
                arguments(new Exception("exception"), INTERNAL_SERVER_ERROR, "Internal Server Error", "exception"),
                // Can't test for the AssertionError because TC will fail if this text is logged. We log unhandled exception in the
                // JavaExceptionHandler
                // arguments(new AssertionError("assert"), INTERNAL_SERVER_ERROR, "Internal Server Error", "assert"),
                arguments(new Throwable("assert"), INTERNAL_SERVER_ERROR, "Internal Server Error", "assert")
        );
    }

    @ParameterizedTest
    @MethodSource("testExceptions")
    public void testExceptions(Throwable throwable, HttpStatus status, String title, String detail) {
        ErrorHandlingTest.throwable.set(throwable);

        assertThrowsProblem(
                () -> client.toBlocking().exchange("/throw-exception"),
                isProblem().withStatus(status).withTitle(title).withDetail(detail)
        );
    }

    @Test
    public void endpoint404() {
        // Invoke non-existing endpoint
        assertThrowsProblem(
                () -> client.toBlocking().exchange("/endpoint404"),
                isProblem()
                        .withStatus(NOT_FOUND)
                        .withTitle("Not Found")
                        .withDetail("Requested resource not found: /test/endpoint404")
        );
    }

    @Test
    public void invalidDataTypePathVariable() {
        // Invoke endpoint with wrong path variable data type
        assertThrowsProblem(
                () -> client.toBlocking().exchange("/list/abc"),
                isProblem()
                        .withStatus(BAD_REQUEST)
                        .withTitle("Invalid parameter")
                        .withDetail("Failed to convert argument [id] for value [abc] due to: For input string: \"abc\"")
        );
    }

    @Test
    public void requiredQueryValueNotSpecified() {
        // Invoke endpoint without required query value
        assertThrowsProblem(
                () -> client.toBlocking().exchange("/list"),
                isProblem()
                        .withStatus(BAD_REQUEST)
                        .withTitle("Bad Request")
                        .withDetail("Required QueryValue [greatThan] not specified")
        );
    }

    @Test
    public void invalidTypeQueryValue() {
        // Invoke endpoint with wrong data type of request argument
        assertThrowsProblem(
                () -> client.toBlocking().exchange("/list?greatThan=abc"),
                isProblem()
                        .withStatus(BAD_REQUEST)
                        .withTitle("Invalid parameter")
                        .withDetail("Failed to convert argument [greatThan] for value [abc] due to: For input string: \"abc\"")
        );
    }

    @Test
    public void invalidTypeQueryValue1() {
        // Invoke endpoint with wrong request argument values
        assertThrowsProblem(
                () -> client.toBlocking().exchange("/list?greatThan=-1&lessThan=11"),
                isProblem()
                        .withStatus(BAD_REQUEST)
                        .withTitle("Bad Request")
                        .withDetail("Validation failed")
                        .withInvalidParams(containsInAnyOrder(
                                new InvalidParam("list.greatThan", "greatThan: must be greater than or equal to 0"),
                                new InvalidParam("list.lessThan", "lessThan: must be less than or equal to 10")
                        ))
        );
    }

    @Test
    public void postWithInvalidMediaType() {
        // Invoke endpoint with invalid media type
        MutableHttpRequest<String> request = HttpRequest.POST(UriBuilder.of("/echo").build(), EchoMessage.class)
                .contentType(MediaType.TEXT_PLAIN)
                .body("text='qwe'");

        assertThrowsProblem(
                () -> client.toBlocking().exchange(request, Argument.of(EchoMessage.class), Argument.of(Problem.class)),
                isProblem()
                        .withStatus(UNSUPPORTED_MEDIA_TYPE)
                        .withTitle("Unsupported Media Type")
                        .withDetail(equalTo("Unsupported media type: text/plain"))
        );
    }

    @Test
    public void postWithInvalidJson() {
        // Invoke endpoint with invalid json
        MutableHttpRequest<String> request = HttpRequest.POST(UriBuilder.of("/echo").build(), EchoMessage.class)
                .contentType(MediaType.APPLICATION_JSON)
                .body("{text='qwe'");

        assertThrowsProblem(
                () -> client.toBlocking().exchange(request, Argument.of(EchoMessage.class), Argument.of(Problem.class)),
                isProblem()
                        .withStatus(BAD_REQUEST)
                        .withTitle("Invalid JSON")
                        .withDetail(containsString("Unexpected character"))
        );
    }

    @Test
    public void postWithMissingBody() {
        // Invoke endpoint with invalid json
        MutableHttpRequest<String> request = HttpRequest.POST(UriBuilder.of("/echo").build(), EchoMessage.class)
                .contentType(MediaType.APPLICATION_JSON)
                .body("");

        assertThrowsProblem(
                () -> client.toBlocking().exchange(request, Argument.of(EchoMessage.class), Argument.of(Problem.class)),
                isProblem()
                        .withStatus(BAD_REQUEST)
                        .withTitle("Bad Request")
                        .withDetail(containsString("Required Body [dto] not specified"))
        );
    }

    @Test
    public void methodNotAllowed() {
        // Invoke endpoint with not allowed method
        MutableHttpRequest<String> request = HttpRequest.GET(UriBuilder.of("/echo").build());

        assertThrowsProblem(
                () -> client.toBlocking().exchange(request, Argument.of(EchoMessage.class), Argument.of(Problem.class)),
                isProblem()
                        .withStatus(METHOD_NOT_ALLOWED)
                        .withTitle("Method Not Allowed")
                        .withDetail(equalTo("Method not allowed: GET"))
        );
    }

    @Bean
    @Factory
    public ThrowableProvider exceptionThrowingService() {
        return throwable::get;
    }
}
