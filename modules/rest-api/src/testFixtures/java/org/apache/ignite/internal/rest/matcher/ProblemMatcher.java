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

import static org.hamcrest.Matchers.equalTo;

import io.micronaut.http.HttpStatus;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.apache.ignite.internal.rest.api.Problem;
import org.apache.ignite.internal.testframework.matchers.AnythingMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link Problem}.
 */
public class ProblemMatcher extends TypeSafeMatcher<Problem> {
    private Matcher<String> titleMatcher = AnythingMatcher.anything();

    private Matcher<Integer> statusMatcher = AnythingMatcher.anything();

    private Matcher<String> codeMatcher = AnythingMatcher.anything();

    private Matcher<String> typeMatcher = AnythingMatcher.anything();

    private Matcher<String> detailMatcher = AnythingMatcher.anything();

    private Matcher<String> nodeMatcher = AnythingMatcher.anything();

    private Matcher<UUID> traceIdMatcher = AnythingMatcher.anything();

    private Matcher<Iterable<? extends InvalidParam>> invalidParamsMatcher = AnythingMatcher.anything();

    /**
     * Creates a matcher for {@link Problem}.
     *
     * @return Matcher.
     */
    public static ProblemMatcher isProblem() {
        return new ProblemMatcher();
    }

    public ProblemMatcher withTitle(String title) {
        return withTitle(equalTo(title));
    }

    public ProblemMatcher withTitle(Matcher<String> matcher) {
        this.titleMatcher = matcher;
        return this;
    }

    public ProblemMatcher withStatus(HttpStatus status) {
        return withStatus(status.getCode());
    }

    public ProblemMatcher withStatus(Integer status) {
        return withStatus(equalTo(status));
    }

    public ProblemMatcher withStatus(Matcher<Integer> matcher) {
        this.statusMatcher = matcher;
        return this;
    }

    public Matcher<Integer> statusMatcher() {
        return statusMatcher;
    }

    public ProblemMatcher withCode(String code) {
        return withCode(equalTo(code));
    }

    public ProblemMatcher withCode(Matcher<String> matcher) {
        this.codeMatcher = matcher;
        return this;
    }

    public ProblemMatcher withType(String type) {
        return withType(equalTo(type));
    }

    public ProblemMatcher withType(Matcher<String> matcher) {
        this.typeMatcher = matcher;
        return this;
    }

    public ProblemMatcher withDetail(String detail) {
        return withDetail(equalTo(detail));
    }

    public ProblemMatcher withDetail(Matcher<String> matcher) {
        this.detailMatcher = matcher;
        return this;
    }

    public ProblemMatcher withNode(String node) {
        return withNode(equalTo(node));
    }

    public ProblemMatcher withNode(Matcher<String> matcher) {
        this.nodeMatcher = matcher;
        return this;
    }

    public ProblemMatcher withTraceId(UUID traceId) {
        return withTraceId(equalTo(traceId));
    }

    public ProblemMatcher withTraceId(Matcher<UUID> matcher) {
        this.traceIdMatcher = matcher;
        return this;
    }

    public ProblemMatcher withInvalidParams(Collection<InvalidParam> invalidParams) {
        return withInvalidParams(equalTo(invalidParams));
    }

    public ProblemMatcher withInvalidParams(Matcher<Iterable<? extends InvalidParam>> matcher) {
        this.invalidParamsMatcher = matcher;
        return this;
    }

    @Override
    protected boolean matchesSafely(Problem problem) {
        return titleMatcher.matches(problem.title())
                && statusMatcher.matches(problem.status())
                && codeMatcher.matches(problem.code())
                && typeMatcher.matches(problem.type())
                && detailMatcher.matches(problem.detail())
                && nodeMatcher.matches(problem.node())
                && traceIdMatcher.matches(problem.traceId())
                && invalidParamsMatcher.matches(problem.invalidParams());
    }

    @Override
    protected void describeMismatchSafely(Problem item, Description mismatchDescription) {
        mismatchDescription.appendText("was a Problem with ")
                .appendText("title: ").appendValue(item.title())
                .appendText(", status: ").appendValue(item.status())
                .appendText(", code: ").appendValue(item.code())
                .appendText(", type: ").appendValue(item.type())
                .appendText(", detail: ").appendValue(item.detail())
                .appendText(", node: ").appendValue(item.node())
                .appendText(", traceId: ").appendValue(item.traceId())
                .appendText(", invalidParams: ").appendValue(item.invalidParams());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a Problem with ")
                .appendText("title ").appendDescriptionOf(titleMatcher)
                .appendText(", status ").appendDescriptionOf(statusMatcher)
                .appendText(", code ").appendDescriptionOf(codeMatcher)
                .appendText(", type ").appendDescriptionOf(typeMatcher)
                .appendText(", detail ").appendDescriptionOf(detailMatcher)
                .appendText(", node ").appendDescriptionOf(nodeMatcher)
                .appendText(", traceId ").appendDescriptionOf(traceIdMatcher)
                .appendText(" and invalidParams ").appendDescriptionOf(invalidParamsMatcher);
    }
}
