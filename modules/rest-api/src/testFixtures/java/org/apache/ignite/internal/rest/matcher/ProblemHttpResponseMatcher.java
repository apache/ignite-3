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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpResponse;
import org.apache.ignite.internal.rest.api.Problem;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link HttpResponse} containing a {@link Problem}.
 */
public class ProblemHttpResponseMatcher extends TypeSafeMatcher<HttpResponse<String>> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Matcher<Integer> statusMatcher;
    private final ProblemMatcher problemMatcher;

    private ProblemHttpResponseMatcher(Matcher<Integer> statusMatcher, ProblemMatcher problemMatcher) {
        this.statusMatcher = statusMatcher;
        this.problemMatcher = problemMatcher;
    }

    @Override
    protected boolean matchesSafely(HttpResponse<String> item) {
        if (!statusMatcher.matches(item.statusCode())) {
            return false;
        }

        try {
            Problem problem = objectMapper.readValue(item.body(), Problem.class);
            return problemMatcher.matches(problem);
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("an HttpResponse with status code ").appendDescriptionOf(statusMatcher)
                .appendText(" and body matching ").appendDescriptionOf(problemMatcher);
    }

    @Override
    protected void describeMismatchSafely(HttpResponse<String> item, Description mismatchDescription) {
        if (!statusMatcher.matches(item.statusCode())) {
            mismatchDescription.appendText("status code was ").appendValue(item.statusCode());
        } else {
            mismatchDescription.appendText("body was ").appendValue(item.body());
        }
    }

    public ProblemHttpResponseMatcher withStatus(Integer status) {
        problemMatcher.withStatus(status);
        return this;
    }

    public ProblemHttpResponseMatcher withTitle(String title) {
        problemMatcher.withTitle(title);
        return this;
    }

    public ProblemHttpResponseMatcher withDetail(String detail) {
        problemMatcher.withDetail(detail);
        return this;
    }

    public ProblemHttpResponseMatcher withDetail(Matcher<String> matcher) {
        problemMatcher.withDetail(matcher);
        return this;
    }

    /**
     * Creates a matcher that matches when the examined {@link HttpResponse} is a problem json that matches the specified matcher.
     *
     * @param problemMatcher Expected problem matcher.
     * @return Matcher.
     */
    public static ProblemHttpResponseMatcher isProblemResponse(ProblemMatcher problemMatcher) {
        return new ProblemHttpResponseMatcher(problemMatcher.statusMatcher(), problemMatcher);
    }

    /**
     * Creates a matcher that matches when the examined {@link HttpResponse} is a problem json.
     *
     * @return Matcher.
     */
    public static ProblemHttpResponseMatcher isProblemResponse() {
        return isProblemResponse(isProblem());
    }
}
