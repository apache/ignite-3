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

package org.apache.ignite.internal.testframework.matchers;

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;

/**
 * {@link Matcher} that awaits for the given future to complete exceptionally and the forwards the exception to the nested matcher.
 */
public class CompletableFutureExceptionMatcher extends TypeSafeMatcher<CompletableFuture<?>> {
    /** Timeout in seconds. */
    private static final int TIMEOUT_SECONDS = 10;

    /** Matcher to forward the exception of the completable future. */
    private final Matcher<? extends Exception> matcher;

    private final boolean inspectCause;

    private final int timeout;

    private final TimeUnit timeUnit;

    private final @Nullable Matcher<String> errorMessageMatcher;

    /**
     * Constructor.
     *
     * @param matcher Matcher to forward the exception of the completable future.
     * @param inspectCause Flag indicating that the whole exception stacktrace should be explored until either the {@code matcher} matches
     *     or every exception in the stacktrace is explored.
     * @param timeout Timeout value.
     * @param timeUnit Timeout unit.
     * @param errorMessageMatcher Expected error message Matcher, {@code null} if any message is expected.
     */
    private CompletableFutureExceptionMatcher(
            Matcher<? extends Exception> matcher,
            boolean inspectCause,
            int timeout,
            TimeUnit timeUnit,
            @Nullable Matcher<String> errorMessageMatcher
    ) {
        this.matcher = matcher;
        this.inspectCause = inspectCause;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.errorMessageMatcher = errorMessageMatcher;
    }

    @Override
    protected boolean matchesSafely(CompletableFuture<?> item) {
        try {
            item.get(timeout, timeUnit);

            return false;
        } catch (Throwable e) {
            Throwable unwrapped = unwrapCause(e);

            return inspectCause ? matchesWithCause(unwrapped) : matchesException(unwrapped);
        }
    }

    private boolean matchesException(Throwable throwable) {
        return matcher.matches(throwable)
                && (errorMessageMatcher == null || errorMessageMatcher.matches(throwable.getMessage()));
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a future that completes with an exception that ").appendDescriptionOf(matcher);

        if (errorMessageMatcher != null) {
            description.appendText(" and error message that contains ").appendDescriptionOf(errorMessageMatcher);
        }
    }

    @Override
    protected void describeMismatchSafely(CompletableFuture<?> item, Description mismatchDescription) {
        if (item.isCompletedExceptionally()) {
            try {
                item.join();
            } catch (Exception e) {
                mismatchDescription.appendText("was completed exceptionally with ").appendValue(unwrapCause(e));
            }
        } else if (item.isDone()) {
            mismatchDescription.appendText("was completed successfully");
        } else {
            mismatchDescription.appendText("was not completed");
        }
    }

    private boolean matchesWithCause(Throwable e) {
        for (Throwable current = e; current != null; current = current.getCause()) {
            if (matchesException(current) || Arrays.stream(current.getSuppressed()).anyMatch(this::matchesWithCause)) {
                return true;
            }

            if (current.getCause() == current) {
                return false;
            }
        }

        return false;
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally and the exception matches the nested matcher.
     */
    public static CompletableFutureExceptionMatcher willThrow(Matcher<? extends Exception> matcher) {
        return willThrow(matcher, TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally and the exception matches the nested matcher.
     */
    public static CompletableFutureExceptionMatcher willThrow(Matcher<? extends Exception> matcher, int timeout, TimeUnit timeUnit) {
        return new CompletableFutureExceptionMatcher(matcher, false, timeout, timeUnit, null);
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally and the exception matches the nested matcher and its
     * message contains a given substring.
     */
    public static CompletableFutureExceptionMatcher willThrow(
            Matcher<? extends Exception> matcher,
            int timeout,
            TimeUnit timeUnit,
            String errorMessageFragment
    ) {
        return willThrow(matcher, timeout, timeUnit, containsString(errorMessageFragment));
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally and the exception matches the nested matcher and its
     * message matches {@code errorMessageMatcher}.
     */
    public static CompletableFutureExceptionMatcher willThrow(
            Matcher<? extends Exception> matcher,
            int timeout,
            TimeUnit timeUnit,
            Matcher<String> errorMessageMatcher
    ) {
        return new CompletableFutureExceptionMatcher(matcher, false, timeout, timeUnit, errorMessageMatcher);
    }

    /**
     * Creates a matcher that matches a future that completes with an exception of the provided type.
     */
    public static CompletableFutureExceptionMatcher willThrow(Class<? extends Exception> cls) {
        return willThrow(cls, TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Creates a matcher that matches a future that completes with an exception of the provided type and its
     * message contains a given substring.
     */
    public static CompletableFutureExceptionMatcher willThrow(Class<? extends Exception> cls, String errorMessageFragment) {
        return willThrow(cls, TIMEOUT_SECONDS, TimeUnit.SECONDS, errorMessageFragment);
    }

    /**
     * Creates a matcher that matches a future that completes with an exception of the provided type and its
     * message matches {@code errorMessageMatcher}.
     */
    public static CompletableFutureExceptionMatcher willThrow(Class<? extends Exception> cls, Matcher<String> errorMessageMatcher) {
        return willThrow(cls, TIMEOUT_SECONDS, TimeUnit.SECONDS, errorMessageMatcher);
    }

    /**
     * Creates a matcher that matches a future that completes with an exception of the provided type.
     */
    public static CompletableFutureExceptionMatcher willThrow(Class<? extends Exception> cls, int timeout, TimeUnit timeUnit) {
        return willThrow(is(instanceOf(cls)), timeout, timeUnit);
    }

    /**
     * Creates a matcher that matches a future that completes with an exception of the provided type and its
     * message contains a given substring.
     */
    public static CompletableFutureExceptionMatcher willThrow(
            Class<? extends Exception> cls,
            int timeout,
            TimeUnit timeUnit,
            String errorMessageFragment
    ) {
        return willThrow(is(instanceOf(cls)), timeout, timeUnit, errorMessageFragment);
    }

    /**
     * Creates a matcher that matches a future that completes with an exception of the provided type and its
     * message matches {@code errorMessageMatcher}.
     */
    public static CompletableFutureExceptionMatcher willThrow(
            Class<? extends Exception> cls,
            int timeout,
            TimeUnit timeUnit,
            Matcher<String> errorMessageMatcher
    ) {
        return willThrow(is(instanceOf(cls)), timeout, timeUnit, errorMessageMatcher);
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally and decently fast.
     *
     * @param cls The class of cause throwable.
     * @return matcher.
     */
    public static CompletableFutureExceptionMatcher willThrowFast(Class<? extends Exception> cls) {
        return willThrow(cls, 1, TimeUnit.SECONDS);
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally and decently fast.
     *
     * @param cls The class of cause throwable.
     * @param errorMessageFragment Expected error message fragment.
     * @return matcher.
     */
    public static CompletableFutureExceptionMatcher willThrowFast(Class<? extends Exception> cls, String errorMessageFragment) {
        return willThrow(cls, 1, TimeUnit.SECONDS, errorMessageFragment);
    }

    /**
     * Creates a matcher that matches a future that completes with an exception that has a given {@code cause} in the exception stacktrace.
     */
    public static CompletableFutureExceptionMatcher willThrowWithCauseOrSuppressed(Class<? extends Exception> cause) {
        return new CompletableFutureExceptionMatcher(
                is(instanceOf(cause)),
                true,
                TIMEOUT_SECONDS,
                TimeUnit.SECONDS,
                null
        );
    }

    /**
     * Creates a matcher that matches a future that completes with an exception that has a given {@code cause} in the exception stacktrace
     * and its message contains a given substring.
     */
    public static CompletableFutureExceptionMatcher willThrowWithCauseOrSuppressed(
            Class<? extends Exception> cause,
            String errorMessageFragment
    ) {
        return new CompletableFutureExceptionMatcher(
                is(instanceOf(cause)),
                true,
                TIMEOUT_SECONDS,
                TimeUnit.SECONDS,
                containsString(errorMessageFragment)
        );
    }

    /**
     * Creates a matcher that matches a future that never completes within a predefined time period.
     *
     * @return matcher.
     */
    public static CompletableFutureExceptionMatcher willTimeoutFast() {
        return willTimeoutIn(250, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a matcher that matches a future that never completes within the given time period.
     *
     * @param time Timeout.
     * @param timeUnit Time unit for timeout.
     * @return matcher.
     */
    public static CompletableFutureExceptionMatcher willTimeoutIn(int time, TimeUnit timeUnit) {
        return willThrow(TimeoutException.class, time, timeUnit);
    }
}
