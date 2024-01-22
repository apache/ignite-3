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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.rest.api.InvalidParam;
import org.apache.ignite.internal.rest.api.Problem;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link Problem}.
 */
public class ProblemMatcher extends TypeSafeMatcher<Problem> {
    private Matcher<String> titleMatcher;

    private Matcher<Integer> statusMatcher;

    private Matcher<String> codeMatcher;

    private Matcher<String> typeMatcher;

    private Matcher<String> detailMatcher;

    private Matcher<String> nodeMatcher;

    private Matcher<UUID> traceIdMatcher;

    private Matcher<Collection<InvalidParam>> invalidParamsMatcher;

    /**
     * Creates a matcher for {@link Problem}.
     *
     * @return Matcher.
     */
    public static ProblemMatcher isProblem() {
        return new ProblemMatcher();
    }

    public ProblemMatcher withTitle(Matcher<String> matcher) {
        this.titleMatcher = matcher;
        return this;
    }

    public ProblemMatcher withTitle(String title) {
        this.titleMatcher = equalTo(title);
        return this;
    }

    public ProblemMatcher withStatus(Matcher<Integer> matcher) {
        this.statusMatcher = matcher;
        return this;
    }

    public ProblemMatcher withStatus(Integer status) {
        this.statusMatcher = equalTo(status);
        return this;
    }

    public ProblemMatcher withCode(Matcher<String> matcher) {
        this.codeMatcher = matcher;
        return this;
    }

    public ProblemMatcher withCode(String code) {
        this.codeMatcher = equalTo(code);
        return this;
    }

    public ProblemMatcher withType(Matcher<String> matcher) {
        this.typeMatcher = matcher;
        return this;
    }

    public ProblemMatcher withType(String type) {
        this.typeMatcher = equalTo(type);
        return this;
    }

    public ProblemMatcher withDetail(Matcher<String> matcher) {
        this.detailMatcher = matcher;
        return this;
    }

    public ProblemMatcher withDetail(String detail) {
        this.detailMatcher = equalTo(detail);
        return this;
    }

    public ProblemMatcher withNode(Matcher<String> matcher) {
        this.nodeMatcher = matcher;
        return this;
    }

    public ProblemMatcher withNode(String node) {
        this.nodeMatcher = equalTo(node);
        return this;
    }

    public ProblemMatcher withTraceId(Matcher<UUID> matcher) {
        this.traceIdMatcher = matcher;
        return this;
    }

    public ProblemMatcher withTraceId(UUID traceId) {
        this.traceIdMatcher = equalTo(traceId);
        return this;
    }

    public ProblemMatcher withInvalidParams(Matcher<Collection<InvalidParam>> matcher) {
        this.invalidParamsMatcher = matcher;
        return this;
    }

    public ProblemMatcher withInvalidParams(Collection<InvalidParam> invalidParams) {
        this.invalidParamsMatcher = equalTo(invalidParams);
        return this;
    }

    @Override
    protected boolean matchesSafely(Problem problem) {
        if (titleMatcher != null && !titleMatcher.matches(problem.title())) {
            return false;
        }

        if (statusMatcher != null && !statusMatcher.matches(problem.status())) {
            return false;
        }

        if (codeMatcher != null && !codeMatcher.matches(problem.code())) {
            return false;
        }

        if (typeMatcher != null && !typeMatcher.matches(problem.type())) {
            return false;
        }

        if (detailMatcher != null && !detailMatcher.matches(problem.detail())) {
            return false;
        }

        if (nodeMatcher != null && !nodeMatcher.matches(problem.node())) {
            return false;
        }

        if (traceIdMatcher != null && !traceIdMatcher.matches(problem.traceId())) {
            return false;
        }

        if (invalidParamsMatcher != null && !invalidParamsMatcher.matches(problem.invalidParams())) {
            return false;
        }

        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a Problem with ");
        if (titleMatcher != null) {
            description.appendText("title: ").appendDescriptionOf(titleMatcher);
        }

        if (statusMatcher != null) {
            description.appendText(", status: ").appendDescriptionOf(statusMatcher);
        }

        if (codeMatcher != null) {
            description.appendText(", code: ").appendDescriptionOf(codeMatcher);
        }

        if (typeMatcher != null) {
            description.appendText(", type: ").appendDescriptionOf(typeMatcher);
        }

        if (detailMatcher != null) {
            description.appendText(", detail: ").appendDescriptionOf(detailMatcher);
        }

        if (nodeMatcher != null) {
            description.appendText(", node: ").appendDescriptionOf(nodeMatcher);
        }

        if (traceIdMatcher != null) {
            description.appendText(", traceId: ").appendDescriptionOf(traceIdMatcher);
        }

        if (invalidParamsMatcher != null) {
            description.appendText(", invalidParams: ").appendDescriptionOf(invalidParamsMatcher);
        }
    }
}
