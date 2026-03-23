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

package org.apache.ignite.internal.rest.matcher;

import static org.apache.ignite.internal.rest.matcher.ProblemMatcher.isProblem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import java.util.Optional;
import org.apache.ignite.internal.rest.api.Problem;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.function.Executable;

/**
 * Matcher for {@link HttpResponse}.
 */
public class MicronautHttpResponseMatcher<T> extends TypeSafeMatcher<HttpResponse<?>> {
    private final Matcher<Integer> statusCodeMatcher;

    private Matcher<T> bodyMatcher;

    private Class<T> bodyClass;

    private Matcher<String> mediaTypeMatcher;

    private MicronautHttpResponseMatcher(Matcher<Integer> statusCodeMatcher) {
        this.statusCodeMatcher = statusCodeMatcher;
    }

    /**
     * Creates a matcher that matches when the examined {@link HttpResponse} has a status that matches the specified status.
     *
     * @param status Expected status.
     * @return Matcher.
     */
    public static <T> MicronautHttpResponseMatcher<T> hasStatus(HttpStatus status) {
        return new MicronautHttpResponseMatcher<>(is(status.getCode()));
    }

    /**
     * Creates a matcher that matches when the examined {@link HttpResponse} has a status code that matches the specified status code.
     *
     * @param statusCode Expected status code.
     * @return Matcher.
     */
    public static <T> MicronautHttpResponseMatcher<T> hasStatusCode(int statusCode) {
        return new MicronautHttpResponseMatcher<>(is(statusCode));
    }

    /**
     * Creates a matcher that matches when the examined {@link HttpResponse} is a problem json that matches the specified matcher.
     *
     * @param problemMatcher Expected problem.
     * @return Matcher.
     */
    public static MicronautHttpResponseMatcher<Problem> isProblemResponse(ProblemMatcher problemMatcher) {
        return new MicronautHttpResponseMatcher<Problem>(problemMatcher.statusMatcher())
                .withMediaType(org.apache.ignite.internal.rest.constants.MediaType.PROBLEM_JSON)
                .withBody(problemMatcher, Problem.class);
    }

    /**
     * Shortcut method which asserts that the provided executable will throw a {@link HttpClientResponseException} which contains a
     * {@link Problem} matching provided matcher.
     *
     * @param executable Executable to run.
     * @param problemMatcher Matcher to apply to the problem JSON.
     */
    public static void assertThrowsProblem(Executable executable, ProblemMatcher problemMatcher) {
        HttpClientResponseException thrown = assertThrows(HttpClientResponseException.class, executable);
        assertThat(thrown.getResponse(), isProblemResponse(problemMatcher));
    }

    /**
     * Shortcut method which asserts that the provided executable will throw a {@link HttpClientResponseException} which contains a
     * {@link Problem} with specified status.
     *
     * @param executable Executable to run.
     * @param status Expected HTTP status.
     */
    public static void assertThrowsProblem(Executable executable, HttpStatus status) {
        assertThrowsProblem(executable, isProblem().withStatus(status));
    }

    /**
     * Sets the expected body.
     *
     * @param body Body to match.
     * @return Matcher.
     */
    public MicronautHttpResponseMatcher<T> withBody(T body) {
        this.bodyMatcher = equalTo(body);
        this.bodyClass = (Class<T>) body.getClass();
        return this;
    }

    /**
     * Sets the body matcher.
     *
     * @param bodyMatcher Body matcher.
     * @param bodyClass Body class.
     * @return Matcher.
     */
    public MicronautHttpResponseMatcher<T> withBody(Matcher<T> bodyMatcher, Class<T> bodyClass) {
        this.bodyMatcher = bodyMatcher;
        this.bodyClass = bodyClass;
        return this;
    }

    /**
     * Sets the media type.
     *
     * @param mediaType Media type.
     * @return Matcher.
     */
    public MicronautHttpResponseMatcher<T> withMediaType(String mediaType) {
        this.mediaTypeMatcher = equalTo(mediaType);
        return this;
    }

    @Override
    protected boolean matchesSafely(HttpResponse<?> httpResponse) {
        if (!statusCodeMatcher.matches(httpResponse.code())) {
            return false;
        }

        if (bodyMatcher != null) {
            Optional<T> body = httpResponse.getBody(bodyClass);
            if (body.isEmpty() || !bodyMatcher.matches(body.get())) {
                return false;
            }
        }

        if (mediaTypeMatcher != null) {
            Optional<MediaType> contentType = httpResponse.getContentType();
            if (contentType.isEmpty() || !mediaTypeMatcher.matches(contentType.get().getName())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void describeTo(Description description) {
        if (statusCodeMatcher != null) {
            description.appendText("an HttpResponse with status code matching ").appendDescriptionOf(statusCodeMatcher);
        }

        if (bodyMatcher != null) {
            description.appendText(" and body ").appendDescriptionOf(bodyMatcher);
        }

        if (mediaTypeMatcher != null) {
            description.appendText(" and content type ").appendDescriptionOf(mediaTypeMatcher);
        }
    }

    @Override
    protected void describeMismatchSafely(HttpResponse<?> item, Description mismatchDescription) {
        mismatchDescription.appendText("status code was ")
                .appendValue(item.code())
                .appendText(" and body was ")
                .appendValue(item.getBody(String.class))
                .appendText(" and content type was ")
                .appendValue(item.getContentType().map(MediaType::getName).orElse(null));
    }
}
