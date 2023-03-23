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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;

/**
 * {@link Matcher} that awaits for the given future to complete and then forwards the result to the nested {@code matcher}.
 */
public class CompletableFutureMatcher<T> extends TypeSafeMatcher<CompletableFuture<? extends T>> {
    /** Default timeout in seconds. */
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    /** Matcher to forward the result of the completable future. */
    private final Matcher<T> matcher;

    /** Timeout. */
    private final int timeout;

    /** Time unit for timeout. */
    private final TimeUnit timeoutTimeUnit;

    /**
     * Class of throwable that should be the cause of fail if the future should fail. If {@code null}, the future should be completed
     * successfully.
     */
    private final @Nullable Class<? extends Throwable> causeOfFail;

    /** Fragment that must be a substring of an error message (if {@code null}, message won't be checked). */
    private final @Nullable String errorMessageFragment;

    /**
     * Constructor.
     *
     * @param matcher Matcher to forward the result of the completable future.
     */
    private CompletableFutureMatcher(Matcher<T> matcher) {
        this(matcher, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS, null, null);
    }

    /**
     * Constructor.
     *
     * @param matcher Matcher to forward the result of the completable future.
     * @param timeout Timeout.
     * @param timeoutTimeUnit {@link TimeUnit} for timeout.
     * @param causeOfFail If {@code null}, the future should be completed successfully, otherwise it specifies the class of cause
     *      throwable.
     * @param errorMessageFragment Fragment that must be an substring of a error message (if {@code null}, message won't be checked).
     */
    private CompletableFutureMatcher(
            Matcher<T> matcher,
            int timeout,
            TimeUnit timeoutTimeUnit,
            @Nullable Class<? extends Throwable> causeOfFail,
            @Nullable String errorMessageFragment
    ) {
        this.matcher = matcher;
        this.timeout = timeout;
        this.timeoutTimeUnit = timeoutTimeUnit;
        this.causeOfFail = causeOfFail;
        this.errorMessageFragment = errorMessageFragment;
    }

    @Override
    protected boolean matchesSafely(CompletableFuture<? extends T> item) {
        try {
            T res = item.get(timeout, timeoutTimeUnit);

            return causeOfFail == null && matcher.matches(res);
        } catch (InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
            return causeOfFail != null && hasCause(e, causeOfFail, errorMessageFragment);
        }
    }

    @Override
    public void describeTo(Description description) {
        if (causeOfFail != null) {
            description.appendText("will fall with ").appendValue(causeOfFail.getName());

            if (errorMessageFragment != null) {
                description.appendText(" with error message fragment ").appendValue(errorMessageFragment);
            }
        } else {
            description.appendText("is ").appendDescriptionOf(matcher);
        }
    }

    @Override
    protected void describeMismatchSafely(CompletableFuture<? extends T> item, Description mismatchDescription) {
        Object valueDescription;

        try {
            valueDescription = item.isDone() ? item.join() : item;
        } catch (Throwable t) {
            valueDescription = t;
        }

        mismatchDescription.appendText("was ").appendValue(valueDescription);
    }

    /**
     * Creates a matcher that matches a future that completes successfully with any result.
     *
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willCompleteSuccessfully() {
        return willBe(anything());
    }

    /**
     * Creates a matcher that matches a future that completes successfully and decently fast.
     *
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willSucceedFast() {
        return willSucceedIn(1, TimeUnit.SECONDS);
    }

    /**
     * Creates a matcher that matches a future that completes successfully with any result within the given timeout.
     *
     * @param time Timeout.
     * @param timeUnit Time unit for timeout.
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willSucceedIn(int time, TimeUnit timeUnit) {
        return new CompletableFutureMatcher<>(anything(), time, timeUnit, null, null);
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally and decently fast.
     *
     * @param cause The class of cause throwable.
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willFailFast(Class<? extends Throwable> cause) {
        return willFailIn(1, TimeUnit.SECONDS, cause);
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally and decently fast.
     *
     * @param cause The class of cause throwable.
     * @param errorMessageFragment Fragment that must be a substring of a error message (if {@code null}, message won't be checked).
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willFailFast(Class<? extends Throwable> cause, String errorMessageFragment) {
        return willFailIn(1, TimeUnit.SECONDS, cause, errorMessageFragment);
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally within the given timeout.
     *
     * @param time Timeout.
     * @param timeUnit Time unit for timeout.
     * @param cause The class of cause throwable.
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willFailIn(int time, TimeUnit timeUnit, Class<? extends Throwable> cause) {
        assert cause != null;

        return new CompletableFutureMatcher<>(anything(), time, timeUnit, cause, null);
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally within the given timeout.
     *
     * @param time Timeout.
     * @param timeUnit Time unit for timeout.
     * @param cause The class of cause throwable.
     * @param errorMessageFragment Fragment that must be a substring of a error message (if {@code null}, message won't be checked).
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willFailIn(
            int time,
            TimeUnit timeUnit,
            Class<? extends Throwable> cause,
            String errorMessageFragment
    ) {
        assert cause != null;
        assert errorMessageFragment != null;

        return new CompletableFutureMatcher<>(anything(), time, timeUnit, cause, errorMessageFragment);
    }

    /**
     * Creates a matcher that matches a future that <strong>not</strong> completes decently fast.
     *
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willTimeoutFast() {
        return willTimeoutIn(250, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a matcher that matches a future that <strong>not</strong> completes within the given timeout.
     *
     * @param time Timeout.
     * @param timeUnit Time unit for timeout.
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willTimeoutIn(int time, TimeUnit timeUnit) {
        return new CompletableFutureMatcher<>(anything(), time, timeUnit, TimeoutException.class, null);
    }

    /**
     * Creates a matcher that matches a future that will be cancelled and decently fast.
     *
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willBeCancelledFast() {
        return willBeCancelledIn(1, TimeUnit.SECONDS);
    }

    /**
     * Creates a matcher that matches a future that will be cancelled within the given timeout.
     *
     * @param time Timeout.
     * @param timeUnit Time unit for timeout.
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willBeCancelledIn(int time, TimeUnit timeUnit) {
        return new CompletableFutureMatcher<>(anything(), time, timeUnit, CancellationException.class, null);
    }

    /**
     * A shorter version of {@link #willBe} to be used with some matchers for aesthetic reasons.
     */
    public static <T> CompletableFutureMatcher<T> will(Matcher<T> matcher) {
        return willBe(matcher);
    }

    /**
     * Factory method.
     *
     * @param matcher matcher to forward the result of the completable future.
     */
    public static <T> CompletableFutureMatcher<T> willBe(Matcher<T> matcher) {
        return new CompletableFutureMatcher<>(matcher);
    }

    /**
     * Returns a Matcher matching the {@link CompletableFuture} under match if it completes successfully with the given value.
     *
     * @param value expected value
     * @param <T> value type
     * @return matcher
     */
    public static <T> CompletableFutureMatcher<T> willBe(T value) {
        return willBe(equalTo(value));
    }
}
