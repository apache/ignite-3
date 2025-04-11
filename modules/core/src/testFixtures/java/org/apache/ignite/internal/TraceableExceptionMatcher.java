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

package org.apache.ignite.internal;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.TraceableException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link TraceableException}.
 */
public class TraceableExceptionMatcher extends TypeSafeMatcher<Exception> {
    private final Class<? extends TraceableException> expectedType;

    private final Matcher<? extends TraceableException> typeMatcher;

    private Matcher<Integer> codeMatcher;

    private final Matcher<UUID> traceIdMatcher = is(notNullValue(UUID.class));

    private Matcher<String> messageMatcher;

    private Matcher<? extends Throwable> causeMatcher;

    TraceableExceptionMatcher(Class<? extends TraceableException> expectedType) {
        this.expectedType = expectedType;
        this.typeMatcher = instanceOf(expectedType);
    }

    /**
     * Sets a matcher to match exception code.
     *
     * @param codeMatcher Matcher.
     * @return This instance.
     */
    public TraceableExceptionMatcher withCode(Matcher<Integer> codeMatcher) {
        this.codeMatcher = codeMatcher;
        return this;
    }

    /**
     * Sets a matcher to inspect cause chain.
     *
     * @param messageMatcher Matcher.
     * @return This instance.
     */
    public TraceableExceptionMatcher withMessage(Matcher<String> messageMatcher) {
        this.messageMatcher = messageMatcher;
        return this;
    }

    /**
     * Sets a matcher to inspect cause chain.
     *
     * @param causeMatcher Matcher.
     * @return This instance.
     */
    public TraceableExceptionMatcher withCause(Matcher<? extends Throwable> causeMatcher) {
        this.causeMatcher = causeMatcher;
        return this;
    }

    @Override
    protected boolean matchesSafely(Exception item) {
        Throwable throwable = ExceptionUtils.unwrapCause(item);

        if (!typeMatcher.matches(throwable)) {
            return false;
        }
        TraceableException ex = expectedType.cast(throwable);

        return (codeMatcher == null || codeMatcher.matches(ex.code()))
                && traceIdMatcher.matches(ex.traceId())
                && (messageMatcher == null || messageMatcher.matches(throwable.getMessage()))
                && (causeMatcher == null || matchesWithCause(throwable.getCause()));
    }

    private boolean matchesWithCause(Throwable e) {
        for (Throwable current = e; current != null; current = current.getCause()) {
            if (causeMatcher.matches(current) || Arrays.stream(current.getSuppressed()).anyMatch(this::matchesWithCause)) {
                return true;
            }

            if (current.getCause() == current) {
                return false;
            }
        }

        return false;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("an exception of type ").appendDescriptionOf(typeMatcher);
        if (codeMatcher != null) {
            description.appendText(" with code ").appendDescriptionOf(codeMatcher);
        }
        description.appendText(" with trace id ").appendDescriptionOf(traceIdMatcher);
        if (messageMatcher != null) {
            description.appendText(" with message ").appendDescriptionOf(messageMatcher);
        }
        if (causeMatcher != null) {
            description.appendText(" with cause ").appendDescriptionOf(causeMatcher);
        }
    }

    @Override
    protected void describeMismatchSafely(Exception item, Description mismatchDescription) {
        Throwable throwable = ExceptionUtils.unwrapCause(item);

        if (!typeMatcher.matches(throwable)) {
            mismatchDescription.appendText("type ");
            typeMatcher.describeMismatch(throwable, mismatchDescription);
        }
        TraceableException ex = expectedType.cast(throwable);

        if (codeMatcher != null && !codeMatcher.matches(ex.code())) {
            mismatchDescription.appendText("code ");
            codeMatcher.describeMismatch(ex.code(), mismatchDescription);
        }
        if (!traceIdMatcher.matches(ex.traceId())) {
            mismatchDescription.appendText("trace id ");
            traceIdMatcher.describeMismatch(ex.traceId(), mismatchDescription);
        }
        if (messageMatcher != null && !messageMatcher.matches(throwable.getMessage())) {
            mismatchDescription.appendText("message ");
            messageMatcher.describeMismatch(throwable.getMessage(), mismatchDescription);
        }
        if (causeMatcher != null && !matchesWithCause(throwable.getCause())) {
            mismatchDescription.appendText("cause ");
            causeMatcher.describeMismatch(throwable.getCause(), mismatchDescription);
        }
    }
}
