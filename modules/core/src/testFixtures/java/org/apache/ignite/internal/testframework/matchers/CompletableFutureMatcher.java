/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * {@link Matcher} that awaits for the given future to complete and then forwards the result to the nested {@code matcher}.
 */
public class CompletableFutureMatcher<T> extends TypeSafeMatcher<CompletableFuture<? extends T>> {
    /** Timeout in seconds. */
    private static final int TIMEOUT_SECONDS = 30;

    /** Matcher to forward the result of the completable future. */
    private final Matcher<T> matcher;

    /**
     * Constructor.
     *
     * @param matcher Matcher to forward the result of the completable future.
     */
    private CompletableFutureMatcher(Matcher<T> matcher) {
        this.matcher = matcher;
    }

    /** {@inheritDoc} */
    @Override
    protected boolean matchesSafely(CompletableFuture<? extends T> item) {
        try {
            return matcher.matches(item.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new AssertionError(e);
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
     * A shorter version of {@link #willBe} to be used with some matchers for aesthetical reasons.
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
