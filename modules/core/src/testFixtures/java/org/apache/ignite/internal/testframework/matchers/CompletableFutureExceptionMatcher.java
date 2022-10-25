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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * {@link Matcher} that awaits for the given future to complete exceptionally and the forwards the exception to the nested matcher.
 */
public class CompletableFutureExceptionMatcher extends TypeSafeMatcher<CompletableFuture<?>> {
    /** Timeout in seconds. */
    private static final int TIMEOUT_SECONDS = 30;

    /** Matcher to forward the exception of the completable future. */
    private final Matcher<? extends Exception> matcher;

    /**
     * Constructor.
     *
     * @param matcher Matcher to forward the exception of the completable future.
     */
    private CompletableFutureExceptionMatcher(Matcher<? extends Exception> matcher) {
        this.matcher = matcher;
    }

    @Override
    protected boolean matchesSafely(CompletableFuture<?> item) {
        try {
            item.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            return false;
        } catch (Exception e) {
            return matcher.matches(unwrapException(e));
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a future that completes with an exception that ").appendDescriptionOf(matcher);
    }

    @Override
    protected void describeMismatchSafely(CompletableFuture<?> item, Description mismatchDescription) {
        if (item.isCompletedExceptionally()) {
            try {
                item.join();
            } catch (Exception e) {
                mismatchDescription.appendText("was completed exceptionally with ").appendValue(unwrapException(e));
            }
        } else if (item.isDone()) {
            mismatchDescription.appendText("was completed successfully");
        } else {
            mismatchDescription.appendText("was not completed");
        }
    }

    private static Throwable unwrapException(Exception e) {
        if (e instanceof ExecutionException || e instanceof CompletionException) {
            return e.getCause();
        } else {
            return e;
        }
    }

    /**
     * Creates a matcher that matches a future that completes exceptionally and the exception matches the nested matcher.
     */
    public static CompletableFutureExceptionMatcher willThrow(Matcher<? extends Exception> matcher) {
        return new CompletableFutureExceptionMatcher(matcher);
    }

    /**
     * Creates a matcher that matches a future that completes with an exception of the provided type.
     */
    public static CompletableFutureExceptionMatcher willThrow(Class<? extends Exception> cls) {
        return willThrow(is(instanceOf(cls)));
    }
}
