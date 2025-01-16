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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.hamcrest.Matchers.contains;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.JobExecution;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link JobExecution}.
 */
public class BroadcastExecutionMatcher<R> extends TypeSafeMatcher<BroadcastExecution<R>> {
    private final Matcher<? super CompletableFuture<Collection<R>>> resultsMatcher;

    private BroadcastExecutionMatcher(
            Matcher<? super CompletableFuture<Collection<R>>> resultsMatcher
    ) {
        this.resultsMatcher = resultsMatcher;
    }

    public static <R> BroadcastExecutionMatcher<R> broadcastExecutionWithResults(R... results) {
        return broadcastExecutionWithResultsFuture(will(contains(results)));
    }

    public static <R> BroadcastExecutionMatcher<R> broadcastExecutionWithResults(Matcher<R>... resultMatchers) {
        return broadcastExecutionWithResultsFuture(will(contains(resultMatchers)));
    }

    public static <R> BroadcastExecutionMatcher<R> broadcastExecutionWithResultsFuture(
            Matcher<? super CompletableFuture<Collection<R>>> resultsMatcher
    ) {
        return new BroadcastExecutionMatcher<>(resultsMatcher);
    }

    @Override
    protected boolean matchesSafely(BroadcastExecution<R> execution) {
        return resultsMatcher.matches(execution.resultsAsync());
    }

    @Override
    protected void describeMismatchSafely(BroadcastExecution<R> execution, Description mismatchDescription) {
        mismatchDescription.appendText("results ");
        resultsMatcher.describeMismatch(execution.resultsAsync(), mismatchDescription);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a BroadcastExecution with results ")
                .appendDescriptionOf(resultsMatcher);
    }
}
