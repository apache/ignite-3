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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
    private final Class<? extends Throwable> causeOfFail;

    /**
     * Constructor.
     *
     * @param matcher Matcher to forward the result of the completable future.
     */
    private CompletableFutureMatcher(Matcher<T> matcher) {
        this(matcher, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS, null);
    }

    /**
     * Constructor.
     *
     * @param matcher Matcher to forward the result of the completable future.
     * @param timeout Timeout.
     * @param timeoutTimeUnit {@link TimeUnit} for timeout.
     * @param causeOfFail If {@code null}, the future should be completed successfully, otherwise it specifies the class of cause
     *                    throwable.
     */
    private CompletableFutureMatcher(
            Matcher<T> matcher,
            int timeout,
            TimeUnit timeoutTimeUnit,
            @Nullable Class<? extends Throwable> causeOfFail
    ) {
        this.matcher = matcher;
        this.timeout = timeout;
        this.timeoutTimeUnit = timeoutTimeUnit;
        this.causeOfFail = causeOfFail;
    }

    /** {@inheritDoc} */
    @Override
    protected boolean matchesSafely(CompletableFuture<? extends T> item) {
        try {
            T res = item.get(timeout, timeoutTimeUnit);

            if (causeOfFail != null) {
                fail("The future was supposed to fail, but it completed successfully.");
            }

            return matcher.matches(res);
        } catch (InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
            if (causeOfFail != null) {
                assertTrue(hasCause(e, causeOfFail, null));

                return true;
            } else {
                throw new AssertionError(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void describeTo(Description description) {
        description.appendText("is ").appendDescriptionOf(matcher);
    }

    /** {@inheritDoc} */
    @Override
    protected void describeMismatchSafely(CompletableFuture<? extends T> item, Description mismatchDescription) {
        Object valueDescription = item.isDone() ? item.join() : item;

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
        return new CompletableFutureMatcher<>(anything(), time, timeUnit, null);
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
     * Creates a matcher that matches a future that completes exceptionally within the given timeout.
     *
     * @param time Timeout.
     * @param timeUnit Time unit for timeout.
     * @param cause The class of cause throwable.
     * @return matcher.
     */
    public static CompletableFutureMatcher<Object> willFailIn(int time, TimeUnit timeUnit, Class<? extends Throwable> cause) {
        assert cause != null;

        return new CompletableFutureMatcher<>(anything(), time, timeUnit, cause);
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
        return new CompletableFutureMatcher<>(anything(), time, timeUnit, CancellationException.class);
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
